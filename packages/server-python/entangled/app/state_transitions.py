"""HTTP surface for subagent state transition history.

Subagent transition rows are written by ``entangled.sql.subagent_state`` in
the same transaction as the status update. This router exposes GET history
endpoints so ops can reconstruct a subagent lifecycle in one call.

Error mapping mirrors the rest of Entangled's app routers: missing
entity → 404 (nothing to show), malformed body → 400 (pydantic), all
else → 500.

Authentication
--------------
The endpoint sits behind ``verify_service_token``. Public reverse proxies may
expose ``/v1/*``, so a valid end-user JWT is deliberately insufficient for
control-plane history access.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from ..sql.state_transitions import list_subagent_transitions
from .auth import verify_service_token
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
    _: str = Depends(verify_service_token),
) -> TransitionHistoryResponse:
    rows = list_subagent_transitions(db, subagent_id, limit=limit)
    return TransitionHistoryResponse(
        count=len(rows),
        rows=[SubagentTransitionRow(**r) for r in rows],
    )
