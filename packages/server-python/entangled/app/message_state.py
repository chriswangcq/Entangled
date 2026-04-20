"""PR-21 (2026-04-20) — HTTP surface for chat_messages lifecycle transitions.

This module is the out-of-process chokepoint: Business / runtime /
subscriber / any future caller hits ``POST /v1/messages/{id}/transition``
to advance the state machine. It's a thin wrapper over
``entangled.sql.message_state.transition`` — the real rules and docs
live there.

Why an endpoint instead of letting callers update rows directly
---------------------------------------------------------------
* SQLite lives in the Entangled process; external services cannot raw-
  SQL into it anyway (they already use PATCH /v1/entities/messages).
* Putting transition logic in a single endpoint means the orphan-scan
  PR (PR-26) and the message-trace PR (PR-25) both have exactly one
  log line format to parse and one metric to emit.
* ``scripts/ci/lint_lifecycle.sh`` (main repo) bans raw UPDATEs of
  ``chat_messages.lifecycle`` across the codebase — the endpoint is
  the only way through.

Error mapping
-------------
``MessageNotFound``   -> HTTP 404
``InvalidTransition`` -> HTTP 409 (client logic bug — retrying won't
                                    help, need different ``to``)
Any other exception   -> FastAPI default 500 (genuine server problem)
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from ..sql.message_state import (
    ALLOWED_TRANSITIONS,
    InvalidTransition,
    MessageNotFound,
    VALID_STATES,
    transition,
)
from .auth import verify_service_or_user
from .state import get_db

router = APIRouter(prefix="/v1/messages", tags=["MessageState"])


class TransitionRequest(BaseModel):
    to: str = Field(..., description="Target lifecycle state; see /v1/messages/states")
    scope_id: Optional[str] = Field(
        None,
        description=(
            "Cortex scope claiming the message. Required semantically on "
            "'claimed' (omitting it leaves the message orphaned on purpose)."
        ),
    )
    reason: str = Field(
        "",
        description="Free-text breadcrumb logged alongside the transition.",
    )


class TransitionResponse(BaseModel):
    message_id: str
    from_state: str = Field(..., alias="from")
    to: str
    scope_id: Optional[str] = None
    reason: str = ""
    # PR-23 idempotency flag — True when current == to so the caller can
    # distinguish "I did nothing because you already transitioned" from
    # "I just moved the state". Useful for debouncing metrics
    # (``subscriber_transition_total{result=noop}`` vs ``{result=ok}``).
    noop: bool = False

    class Config:
        populate_by_name = True


class StatesResponse(BaseModel):
    """Self-describing state machine — useful for clients building UIs
    and for operational debugging ('what transitions CAN I do from
    orphaned?'). Shape is stable across PR-21; PR-22 may add new states
    but never removes them (see message_state.py docstring)."""

    states: list[str]
    allowed: dict[str, list[str]]


# ── PR-25 (2026-04-15) trace ─────────────────────────────────────────────────
# Read-only view of one chat_messages row plus the associated outbox row.
# Business's ``/internal/messages/{id}/trace`` composes this with Cortex
# scope meta + Queue session state; keeping the join here (one round-trip,
# direct DB access) means Business never has to learn the outbox schema.


class MessageTraceRow(BaseModel):
    message_id: str
    agent_id: str
    type: Optional[str] = None
    sender: Optional[str] = None
    timestamp: Optional[str] = Field(
        None,
        description="chat_messages.timestamp (TEXT, caller-supplied ISO).",
    )
    created_at_iso: Optional[str] = Field(
        None,
        description=(
            "chat_messages.created_at — TEXT 'YYYY-MM-DD HH:MM:SS' UTC. "
            "Kept as the original string so callers see exactly what SQLite "
            "stored; numeric ms is in ``lifecycle_updated_at`` when set."
        ),
    )
    lifecycle: str = Field(..., description="pending|claimed|consumed|orphaned|deduped")
    claimed_by_scope: Optional[str] = None
    lifecycle_updated_at: Optional[int] = Field(
        None,
        description=(
            "ms since epoch; NULL for rows that were never transitioned "
            "(legitimately pending since before PR-21 deploy or fresh)."
        ),
    )
    # Outbox fields (all Optional — LEFT JOIN may miss, and that's a signal):
    outbox_trigger_type: Optional[str] = None
    outbox_created_at: Optional[int] = Field(
        None, description="Outbox INSERT time (ms since epoch)."
    )
    outbox_delivered_at: Optional[int] = None
    outbox_attempts: int = 0
    outbox_last_error: Optional[str] = None


class MessageTraceNotFound(Exception):
    """Raised by ``query_message_trace`` so callers (FastAPI route + tests)
    can map to their own error surface without catching a blanket
    exception. Parallels ``MessageNotFound`` from the transition path."""


def query_message_trace(db, message_id: str) -> MessageTraceRow:
    """Pure-DB core for the trace read — factored out so unit tests can
    drive it directly without standing up the FastAPI app / auth."""
    sql = """
        SELECT m.id                    AS message_id,
               m.agent_id               AS agent_id,
               m.type                   AS type,
               m.sender                 AS sender,
               m.timestamp              AS timestamp,
               m.created_at             AS created_at_iso,
               m.lifecycle              AS lifecycle,
               m.claimed_by_scope       AS claimed_by_scope,
               m.lifecycle_updated_at   AS lifecycle_updated_at,
               o.trigger_type           AS outbox_trigger_type,
               o.created_at             AS outbox_created_at,
               o.delivered_at           AS outbox_delivered_at,
               COALESCE(o.attempts, 0)  AS outbox_attempts,
               o.last_error             AS outbox_last_error
          FROM chat_messages m
          LEFT JOIN message_outbox o ON o.message_id = m.id
         WHERE m.id = ?
    """
    with db.transaction("global"):
        row = db.execute(sql, (message_id,)).fetchone()
    if row is None:
        raise MessageTraceNotFound(message_id)
    return MessageTraceRow(
        message_id=row["message_id"],
        agent_id=row["agent_id"],
        type=row["type"],
        sender=row["sender"],
        timestamp=row["timestamp"],
        created_at_iso=row["created_at_iso"],
        lifecycle=row["lifecycle"] or "pending",
        claimed_by_scope=row["claimed_by_scope"],
        lifecycle_updated_at=row["lifecycle_updated_at"],
        outbox_trigger_type=row["outbox_trigger_type"],
        outbox_created_at=row["outbox_created_at"],
        outbox_delivered_at=row["outbox_delivered_at"],
        outbox_attempts=int(row["outbox_attempts"] or 0),
        outbox_last_error=row["outbox_last_error"],
    )


@router.get("/{message_id}/trace", response_model=MessageTraceRow)
def trace_message(
    message_id: str,
    db=Depends(get_db),
    _: dict = Depends(verify_service_or_user),
):
    """PR-25 — one-hop read of chat_messages + message_outbox for ops trace.

    Why here (not a generic entity GET)
    -----------------------------------
    * ``message_outbox`` is not a registered entity (intentionally — it's
      a write-ahead queue, not user-facing data). A LEFT JOIN here keeps
      Business from having to learn the outbox schema just to read one
      row for trace.
    * Single endpoint means Business's trace composition path and the
      orphan scanner (PR-26) agree on column semantics.

    404 semantics
    -------------
    Returns 404 only when the ``chat_messages`` row is missing — a row
    with no outbox sibling surfaces as HTTP 200 with ``outbox_*=NULL/0``.
    That combination is itself diagnostic: either the outbox co-insert
    from PR-15 failed, or the message predates PR-14.
    """
    try:
        return query_message_trace(db, message_id)
    except MessageTraceNotFound as exc:
        raise HTTPException(status_code=404, detail=f"message not found: {exc}")


@router.post("/{message_id}/transition", response_model=TransitionResponse)
def transition_message(
    message_id: str,
    req: TransitionRequest,
    db=Depends(get_db),
    _: dict = Depends(verify_service_or_user),
):
    try:
        result = transition(
            db,
            message_id,
            to=req.to,
            scope_id=req.scope_id,
            reason=req.reason,
        )
    except MessageNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except InvalidTransition as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return TransitionResponse(
        message_id=result["message_id"],
        **{"from": result["from"]},
        to=result["to"],
        scope_id=result["scope_id"],
        reason=result["reason"],
        noop=result.get("noop", False),
    )


@router.get("/states", response_model=StatesResponse)
def list_states(_: dict = Depends(verify_service_or_user)):
    return StatesResponse(
        states=sorted(VALID_STATES),
        allowed={k: sorted(v) for k, v in ALLOWED_TRANSITIONS.items()},
    )
