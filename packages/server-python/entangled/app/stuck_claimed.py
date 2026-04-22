"""PR-51 Part 2 (2026-04-23) — stuck-claimed message listing endpoint.

Companion to :mod:`entangled.app.orphans` but for the *other* half of
the lifecycle ladder. Where ``orphans.py`` surfaces ``lifecycle='pending'``
rows that the subscriber never claimed, this module surfaces
``lifecycle='claimed'`` rows that got claimed but never moved to
``consumed``. Both are dead-row problems; they just fail at different
rungs of ``pending → claimed → consumed``.

Why a separate endpoint rather than unioning into ``/v1/orphans``:

* Different remediation. Orphans get re-dispatched via
  ``TriggerType.RECOVERED`` (PR-27); stuck-claimed rows are terminal
  dead ends (the owning scope is gone) and only make sense to
  short-circuit to ``consumed``. Mixing them in one response would
  force every caller to branch on lifecycle, and would burden the
  orphan dashboards with rows whose "age" axis is semantically
  different (time-since-claim vs time-since-create).
* Different age basis. Part 1 of PR-51 learned the hard way that
  ``lifecycle_updated_at`` alone isn't a reliable age proxy for
  stuck-claimed: subscriber restarts re-claim old rows and bump
  ``lifecycle_updated_at`` to "now" while the scope that owns the
  claim has been dead for days. See
  ``docs/roadmap/tickets/PR-51-stuck-claimed-cleanup.md#part-1-部署记录``.
  This endpoint therefore accepts *two* age filters and returns rows
  matching EITHER:

    - ``min_age_sec`` applied to ``lifecycle_updated_at`` (catches
      the normal case where nobody restarted the subscriber).
    - ``min_created_age_sec`` applied to ``created_at`` (catches the
      subscriber-restart case — a row created 4 days ago is stuck
      regardless of what the last lifecycle bump says).

Type filter: none. Any message type that's been ``claimed`` for days
is unambiguously stuck. ``AGENT_REPLY`` stuck-claimed rows come from
pre-PR-41-amend code paths; ``USER_MESSAGE`` stuck-claimed rows come
from dead scopes; both want the same fix.

Security / auth: same as every other Entangled endpoint
(``verify_service_or_user`` + service-token header).
"""

from __future__ import annotations

import time
from typing import Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from .auth import verify_service_or_user
from .state import get_db

router = APIRouter(prefix="/v1/stuck-claimed", tags=["StuckClaimed"])


# ── Defaults ──────────────────────────────────────────────────────────────
# Defaults are set wide enough that the healthy steady state returns an
# empty list even under slow LLM chains. A scope that's been ``claimed``
# for 24h is unambiguously stuck: the longest legitimate Cortex thinking
# window observed in prod is < 30m (PR-48 Turn Finalizer caps runaway
# chains), 24h is ~50× that with comfortable safety margin.
#
# ``DEFAULT_CREATED_MIN_AGE_SEC`` gates the ``created_at``-based escape
# hatch; it's deliberately 3× the lifecycle axis so a freshly-restarted
# subscriber doesn't sweep up rows that are only hours old but happen
# to have stale ``lifecycle_updated_at``. 72h is the "nothing sane
# should still be claimed this many days after birth" backstop.
DEFAULT_MIN_AGE_SEC = 24 * 3600
DEFAULT_CREATED_MIN_AGE_SEC = 72 * 3600


class StuckClaimedRow(BaseModel):
    message_id: str
    agent_id: str
    type: str
    claimed_by_scope: Optional[str] = Field(
        None,
        description=(
            "Historical truth — the scope that originally claimed this "
            "row. Usually dead by the time the scanner sees it; kept in "
            "the payload purely for forensic queries "
            "(``which dead scopes left claims behind``)."
        ),
    )
    lifecycle_updated_at_ms: int = Field(
        ...,
        description=(
            "Epoch-ms of the last state transition (i.e. pending → "
            "claimed). Bumped by every ``transition()`` call, so a "
            "subscriber restart that re-claims an existing-claimed row "
            "would refresh this — that's the failure mode the "
            "``created_age_seconds`` axis exists to cover."
        ),
    )
    created_at_ms: int = Field(
        ...,
        description=(
            "Epoch-ms derived from ``chat_messages.created_at`` (TEXT "
            "ISO UTC per SQLite's ``datetime('now')`` default). This is "
            "the stable birth timestamp — never rewritten by "
            "transitions. Rows whose ``created_at`` is ancient but "
            "``lifecycle_updated_at`` is recent are the subscriber-"
            "restart artefact the ``created_age_seconds`` filter "
            "targets."
        ),
    )
    lifecycle_age_seconds: float = Field(
        ...,
        description="Seconds since ``lifecycle_updated_at`` (i.e. since the claim).",
    )
    created_age_seconds: float = Field(
        ...,
        description="Seconds since ``created_at`` (i.e. since the message was born).",
    )
    matched_axis: str = Field(
        ...,
        description=(
            "Which age axis triggered the match: ``lifecycle`` (claim "
            "is >= min_age_sec old), ``created`` (message is >= "
            "min_created_age_sec old), or ``both``. Dashboards use this "
            "to spot the subscriber-restart pattern (bulk of matches "
            "on ``created`` with ``lifecycle`` timestamps all clustered "
            "around a recent restart moment)."
        ),
    )


class StuckClaimedResponse(BaseModel):
    stuck: list[StuckClaimedRow]
    count: int
    now_ms: int
    matched_by_lifecycle: int = Field(
        ...,
        description="Rows where ``lifecycle_age_seconds >= min_age_sec``.",
    )
    matched_by_created: int = Field(
        ...,
        description="Rows where ``created_age_seconds >= min_created_age_sec`` but not by lifecycle axis.",
    )


def query_stuck_claimed(
    db,
    *,
    min_age_sec: int = DEFAULT_MIN_AGE_SEC,
    min_created_age_sec: int = DEFAULT_CREATED_MIN_AGE_SEC,
    limit: int = 200,
) -> StuckClaimedResponse:
    """Pure-DB core used by both the FastAPI route and tests.

    Schema trap (documented for the next migration author):
        ``chat_messages.lifecycle_updated_at`` is INTEGER epoch-ms
        (PR-21 convention), while ``chat_messages.created_at`` is
        TEXT ISO-8601 (SQLite's ``datetime('now')`` default). This is
        the same mismatch that broke PR-47's first migration pass —
        so the SQL here converts ``created_at`` to epoch-ms inline
        via ``strftime('%s', ...) * 1000`` to keep every arithmetic
        comparison INTEGER-on-INTEGER.

    Matching logic (two-axis OR):

        match_by_lifecycle := lifecycle_updated_at <= now - min_age_sec*1000
        match_by_created   := created_at_ms        <= now - min_created_age_sec*1000
        WHERE lifecycle='claimed' AND (match_by_lifecycle OR match_by_created)

    The union-OR is deliberate: either axis alone would miss
    half the production cases (see PR-51 Part 1 lessons). A row that
    fails both is legitimately in flight; we never want to touch it.
    """
    now_ms = int(time.time() * 1000)
    lifecycle_cutoff_ms = now_ms - min_age_sec * 1000
    created_cutoff_ms = now_ms - min_created_age_sec * 1000

    # Deliberately unconstrained on ``type``. A message of any type
    # stuck at ``claimed`` for >= 24h is a bug regardless of which
    # lifecycle path was expected: trigger types come from a dead
    # scope, non-trigger types (AGENT_REPLY, SYSTEM_NOTE) are pre-
    # PR-41-amend leftovers. Both want the same ``→ consumed`` fix.
    sql = """
        SELECT m.id                          AS message_id,
               m.agent_id                    AS agent_id,
               m.type                        AS type,
               m.claimed_by_scope            AS claimed_by_scope,
               m.lifecycle_updated_at        AS lifecycle_updated_at_ms,
               CAST(strftime('%s', m.created_at) AS INTEGER) * 1000
                                             AS created_at_ms
          FROM chat_messages m
         WHERE m.lifecycle = 'claimed'
           AND (
                m.lifecycle_updated_at <= ?
                OR CAST(strftime('%s', m.created_at) AS INTEGER) * 1000 <= ?
           )
         ORDER BY m.lifecycle_updated_at ASC
         LIMIT ?
    """
    with db.transaction("global"):
        rows = db.execute(
            sql,
            (lifecycle_cutoff_ms, created_cutoff_ms, limit),
        ).fetchall()

    stuck: list[StuckClaimedRow] = []
    n_lifecycle = n_created = 0
    for r in rows:
        life_ms = r["lifecycle_updated_at_ms"] or 0
        created_ms = r["created_at_ms"] or 0
        life_age = max(0.0, (now_ms - life_ms) / 1000.0) if life_ms else 0.0
        created_age = max(0.0, (now_ms - created_ms) / 1000.0) if created_ms else 0.0

        matched_life = life_ms != 0 and life_ms <= lifecycle_cutoff_ms
        matched_created = created_ms != 0 and created_ms <= created_cutoff_ms
        if matched_life and matched_created:
            axis = "both"
            n_lifecycle += 1
        elif matched_life:
            axis = "lifecycle"
            n_lifecycle += 1
        else:
            # NB: the WHERE clause guarantees at least one axis matched;
            # if we got here and matched_life is False then matched_created
            # must be True, so no else-else branch needed.
            axis = "created"
            n_created += 1

        stuck.append(StuckClaimedRow(
            message_id=r["message_id"],
            agent_id=r["agent_id"],
            type=r["type"],
            claimed_by_scope=r["claimed_by_scope"],
            lifecycle_updated_at_ms=int(life_ms),
            created_at_ms=int(created_ms),
            lifecycle_age_seconds=round(life_age, 1),
            created_age_seconds=round(created_age, 1),
            matched_axis=axis,
        ))

    return StuckClaimedResponse(
        stuck=stuck,
        count=len(stuck),
        now_ms=now_ms,
        matched_by_lifecycle=n_lifecycle,
        matched_by_created=n_created,
    )


@router.get("", response_model=StuckClaimedResponse)
def list_stuck_claimed(
    min_age_sec: int = Query(
        DEFAULT_MIN_AGE_SEC,
        ge=0,
        description=(
            "Seconds since last lifecycle transition. Rows whose last "
            "claim is at least this old match the lifecycle axis. "
            "Default 86400 (24h) — well above any legitimate in-flight "
            "Cortex thinking window."
        ),
    ),
    min_created_age_sec: int = Query(
        DEFAULT_CREATED_MIN_AGE_SEC,
        ge=0,
        description=(
            "Seconds since message birth. Rows whose ``created_at`` is "
            "at least this old match the created-age axis even if "
            "``lifecycle_updated_at`` was refreshed recently (e.g. by "
            "a subscriber restart re-claiming an existing row). "
            "Default 259200 (72h)."
        ),
    ),
    limit: int = Query(200, ge=1, le=1000),
    db=Depends(get_db),
    _: dict = Depends(verify_service_or_user),
):
    """Return ``claimed`` messages older than either age threshold.

    Empty result is HTTP 200 with ``count=0`` (same shape as ``/v1/orphans``).
    Ordering is ``lifecycle_updated_at ASC`` — oldest claims first, so
    ops treating the list top-down naturally handles the most stuck
    rows first.
    """
    return query_stuck_claimed(
        db,
        min_age_sec=min_age_sec,
        min_created_age_sec=min_created_age_sec,
        limit=limit,
    )
