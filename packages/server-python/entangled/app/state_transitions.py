"""PR-31 (2026-04-15) — HTTP surface for state transition history.

The message_state_transitions table is populated co-transactionally by
``entangled.sql.message_state.transition`` (the function is in-process
and owns the DB). Subagent transition rows are written by
``entangled.sql.subagent_state.transition`` in the same transaction as
the status update. This router exposes GET history endpoints so ops can
reconstruct the full lifecycle of either entity type in one call.

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
from pydantic import BaseModel

from ..sql.state_transitions import (
    list_message_transitions,
    list_subagent_transitions,
)
from .auth import verify_service_or_user
from .state import get_db

router = APIRouter(prefix="/v1/state_transitions", tags=["StateTransitions"])


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
