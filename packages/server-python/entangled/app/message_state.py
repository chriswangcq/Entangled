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
