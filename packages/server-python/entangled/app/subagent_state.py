"""PR-31b (2026-04-15) — HTTP surface for subagent status transitions.

Mirrors ``entangled/app/message_state.py``. Business's
``novaic-business/business/internal/subagent_state.py`` now delegates
every non-noop transition here; see the module docstring of
``entangled/sql/subagent_state.py`` for the rationale.

Error mapping
-------------
``SubagentNotFound``  -> HTTP 404
``InvalidTransition`` -> HTTP 409
anything else         -> FastAPI default 500
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from ..sql.subagent_state import (
    ALLOWED_TRANSITIONS,
    InvalidTransition,
    SubagentNotFound,
    VALID_STATES,
    transition,
)
from .auth import verify_service_or_user
from .state import get_db

router = APIRouter(prefix="/v1/subagents", tags=["SubagentState"])


class SubagentTransitionRequest(BaseModel):
    to: str = Field(..., description="Target status; see GET /v1/subagents/states")
    reason: str = Field("", description="snake_case breadcrumb (spawn, timeout, ...)")
    actor: str = Field("", description="Who initiated (business, scheduler, worker, recovery)")
    extra: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "Optional ancillary column writes (e.g. {'error': '...', "
            "'need_rest': 0}). Keys are intersected with the server's "
            "EXTRA_ALLOWLIST; unknown keys are silently dropped."
        ),
    )


class SubagentTransitionResponse(BaseModel):
    subagent_id: str
    agent_id: str
    from_state: str = Field(..., alias="from")
    to: str
    reason: str = ""
    actor: str = ""
    noop: bool = False

    class Config:
        populate_by_name = True


class SubagentStatesResponse(BaseModel):
    """Self-describing state machine for debugging / UI wiring."""

    states: list[str]
    allowed: dict[str, list[str]]


@router.post(
    "/{agent_id}/{subagent_id}/transition",
    response_model=SubagentTransitionResponse,
)
def transition_subagent(
    agent_id: str,
    subagent_id: str,
    req: SubagentTransitionRequest,
    db=Depends(get_db),
    _: dict = Depends(verify_service_or_user),
):
    """Single chokepoint for ``subagents.status`` transitions.

    The ``agent_id`` + ``subagent_id`` pair in the URL mirrors the
    composite key in the ``subagents`` table — putting both in the
    path (instead of ``agent_id`` in a query string) means log lines
    that include the URL already carry enough context to identify the
    owning agent.
    """
    try:
        result = transition(
            db,
            subagent_id,
            agent_id,
            to=req.to,
            reason=req.reason,
            actor=req.actor,
            extra=req.extra,
        )
    except SubagentNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except InvalidTransition as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return SubagentTransitionResponse(
        subagent_id=result["subagent_id"],
        agent_id=result["agent_id"],
        **{"from": result["from"]},
        to=result["to"],
        reason=result["reason"],
        actor=result["actor"],
        noop=result["noop"],
    )


@router.get("/states", response_model=SubagentStatesResponse)
def list_states(_: dict = Depends(verify_service_or_user)):
    return SubagentStatesResponse(
        states=sorted(VALID_STATES),
        allowed={k: sorted(v) for k, v in ALLOWED_TRANSITIONS.items()},
    )
