"""PR-31 (2026-04-15) — HTTP surface for state transition history.

The message_state_transitions table is populated co-transactionally by
``entangled.sql.message_state.transition`` (the function is in-process
and owns the DB). The subagent_state_transitions table cannot be
populated that way — subagent transitions happen in the Business
process, which talks to Entangled only over HTTP. This router gives
Business a narrow endpoint to POST a transition row, plus generic GET
history endpoints so ops can reconstruct the full lifecycle of either
entity type in one call.

Error mapping mirrors the rest of Entangled's app routers: missing
entity → 404 (nothing to show), malformed body → 400 (pydantic), all
else → 500.

Authentication
--------------
Both endpoints sit behind ``verify_service_or_user`` — same bar as
``/v1/messages/*``. A direct client couldn't even hit this surface
from outside the cluster (Entangled binds to 127.0.0.1), but the
service-token check keeps Business the only writer even inside.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from ..sql.state_transitions import (
    append_subagent_transition,
    list_message_transitions,
    list_subagent_transitions,
)
from .auth import verify_service_or_user
from .state import get_db

router = APIRouter(prefix="/v1/state_transitions", tags=["StateTransitions"])


class SubagentTransitionRecord(BaseModel):
    """Payload accepted by ``POST /v1/state_transitions/subagent``.

    Mirrors the argument shape of
    ``entangled.sql.state_transitions.append_subagent_transition``.
    Business calls this after a successful ``store.update("subagents",
    ..., {"status": ...})`` — see ``business/internal/subagent_state.py``.
    """

    subagent_id: str = Field(..., description="subagents.subagent_id")
    agent_id: Optional[str] = Field(None, description="Owning agent; logged for filtering.")
    from_state: str
    to_state: str
    reason: str = ""
    actor: str = ""
    scope_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class TransitionRow(BaseModel):
    id: int
    from_state: str
    to_state: str
    reason: Optional[str] = None
    actor: Optional[str] = None
    scope_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    created_at: int


class MessageTransitionRow(TransitionRow):
    message_id: str


class SubagentTransitionRow(TransitionRow):
    subagent_id: str
    agent_id: Optional[str] = None


class TransitionHistoryResponse(BaseModel):
    """Wrapper so the endpoint can later grow (cursor, totals) without
    breaking clients that assumed the shape was ``list[row]``."""

    count: int
    rows: List[TransitionRow]


@router.post("/subagent", status_code=201)
def record_subagent_transition(
    req: SubagentTransitionRecord,
    db=Depends(get_db),
    _: dict = Depends(verify_service_or_user),
) -> Dict[str, str]:
    """Append one row to ``subagent_state_transitions``.

    Not idempotent per row (we explicitly want a record of every retry),
    but the caller on the Business side only posts after a non-noop
    ``store.update`` so the volume matches real state changes.
    """
    append_subagent_transition(
        db,
        subagent_id=req.subagent_id,
        agent_id=req.agent_id,
        from_state=req.from_state,
        to_state=req.to_state,
        reason=req.reason,
        actor=req.actor,
        scope_id=req.scope_id,
        metadata=req.metadata,
    )
    return {"status": "ok"}


@router.get("/subagent/{subagent_id}", response_model=TransitionHistoryResponse)
def history_subagent(
    subagent_id: str,
    limit: int = Query(50, ge=1, le=500),
    db=Depends(get_db),
    _: dict = Depends(verify_service_or_user),
) -> TransitionHistoryResponse:
    rows = list_subagent_transitions(db, subagent_id, limit=limit)
    return TransitionHistoryResponse(
        count=len(rows),
        rows=[SubagentTransitionRow(**r) for r in rows],
    )


@router.get("/message/{message_id}", response_model=TransitionHistoryResponse)
def history_message(
    message_id: str,
    limit: int = Query(50, ge=1, le=500),
    db=Depends(get_db),
    _: dict = Depends(verify_service_or_user),
) -> TransitionHistoryResponse:
    rows = list_message_transitions(db, message_id, limit=limit)
    return TransitionHistoryResponse(
        count=len(rows),
        rows=[MessageTransitionRow(**r) for r in rows],
    )
