"""PR-26 (2026-04-20) — orphan-message listing endpoint.

Lists ``chat_messages`` rows that are still ``lifecycle='pending'`` past a
given age threshold, joined with ``message_outbox`` so the caller sees in a
single response "did the subscriber even try to dispatch this one, and if
so what went wrong?".

This endpoint is **read-only** and exists so two distinct callers can
answer the same question through the same chokepoint:

* HealthWorker (``recovery_worker`` in spirit) polls it on every tick,
  classifies rows by age into ``warn`` / ``crit``, emits grep-able log
  events (``orphan_warn`` / ``ORPHAN``), and — under PR-27 — re-dispatches
  crit rows via ``TriggerType.RECOVERED``.
* Humans / ops UIs hit it via Business's proxy
  ``GET /internal/messages/orphaned`` when investigating an alert.

Why a dedicated endpoint rather than a generic entity query:

* ``message_outbox`` is NOT a registered Entangled entity (see
  ``_ensure_outbox_schema``), so the generic ``GET /v1/entities/...`` path
  can't see its ``attempts`` / ``last_error`` columns. A LEFT JOIN here is
  strictly cheaper than two round trips.
* We want ``age_seconds`` and ``severity`` computed server-side against
  the same wall clock that tagged ``created_at`` — the subscriber and the
  scanner can drift by multiple seconds if the scanner does its own
  ``time.time()``.

Security: same ``verify_service_or_user`` dep as every other Entangled
endpoint; there is no user-level partitioning on this one because
orphaning is a system-level concern.
"""

from __future__ import annotations

import time
from typing import Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from .auth import verify_service_or_user
from .state import get_db

router = APIRouter(prefix="/v1/orphans", tags=["Orphans"])


# ── Tuning knobs ──────────────────────────────────────────────────────────────
# ``severity='crit'`` when age >= CRIT_AGE_SEC; otherwise ``'warn'``. The
# scanner (HealthWorker) will likely pass its own ``min_age_sec`` but we
# still compute severity here so every consumer agrees on the threshold.
# Kept as module constants (not env) because the enforcement boundary
# between warn / crit is a product decision, not a deploy-time knob.
DEFAULT_WARN_AGE_SEC = 30
DEFAULT_CRIT_AGE_SEC = 300


class OrphanRow(BaseModel):
    message_id: str
    agent_id: str
    user_id: Optional[str] = None  # chat_messages has no user_id column; kept for API stability, always null.
    created_at: int = Field(
        ...,
        description=(
            "ms since epoch. Derived from lifecycle_updated_at when present, "
            "otherwise from chat_messages.created_at (TEXT ISO UTC) via "
            "strftime('%s'). Two sources because the column backfill in "
            "message_state.backfill_lifecycle only set lifecycle_updated_at "
            "for rows that had a legacy signal — legitimately-pending rows "
            "still carry NULL there."
        ),
    )
    age_seconds: float
    severity: str = Field(..., description="'warn' or 'crit'")
    lifecycle: str
    outbox_attempts: int = Field(
        0,
        description=(
            "0 when the outbox row is missing — which itself is a tell: "
            "PR-15's co-transaction insert should make outbox and message "
            "row appear together. A pending message with NULL outbox means "
            "the message predates PR-15 or the co-insert failed."
        ),
    )
    outbox_last_error: Optional[str] = None
    outbox_delivered_at: Optional[int] = Field(
        None,
        description=(
            "Non-NULL with lifecycle='pending' is the sharpest possible "
            "PR-22 wiring bug signal (subscriber delivered but never "
            "transitioned). The runtime scanner surfaces these separately."
        ),
    )


class OrphanListResponse(BaseModel):
    orphans: list[OrphanRow]
    count: int
    warn_count: int
    crit_count: int
    now_ms: int


def query_orphans(
    db,
    *,
    min_age_sec: int = DEFAULT_WARN_AGE_SEC,
    limit: int = 200,
    include_delivered_pending: bool = True,
) -> OrphanListResponse:
    """Pure-DB core used by both the FastAPI route and tests.

    Lives outside the route function so tests can drive it without
    stubbing the ``Depends(verify_service_or_user)`` injection.
    """
    now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - min_age_sec * 1000

    # Age basis is lifecycle_updated_at (INTEGER ms, set by every transition())
    # when non-NULL, else derived from created_at (TEXT 'YYYY-MM-DD HH:MM:SS'
    # in UTC, per SQLite's default datetime('now')). Two sources because
    # backfill_lifecycle only populated lifecycle_updated_at for rows with a
    # legacy signal (processed=1 OR claimed_by NOT NULL) — rows that were
    # always-pending still carry NULL there, and forcing a single-source
    # query would hide them from the orphan view entirely.
    #
    # LEFT JOIN so messages with no outbox row still surface — that
    # combination is itself suspicious (see OrphanRow.outbox_attempts doc).
    # chat_messages has no user_id column (Business's schema uses sender),
    # so we project NULL and keep the field for API stability.
    sql = """
        SELECT m.id              AS message_id,
               m.agent_id        AS agent_id,
               NULL              AS user_id,
               COALESCE(
                   m.lifecycle_updated_at,
                   CAST(strftime('%s', m.created_at) AS INTEGER) * 1000
               )                 AS created_at,
               m.lifecycle       AS lifecycle,
               COALESCE(o.attempts, 0)         AS outbox_attempts,
               o.last_error                    AS outbox_last_error,
               o.delivered_at                  AS outbox_delivered_at
          FROM chat_messages m
          LEFT JOIN message_outbox o ON o.message_id = m.id
         WHERE m.lifecycle = 'pending'
           AND COALESCE(
                   m.lifecycle_updated_at,
                   CAST(strftime('%s', m.created_at) AS INTEGER) * 1000
               ) < ?
         ORDER BY 4 ASC
         LIMIT ?
    """
    with db.transaction("global"):
        rows = db.execute(sql, (cutoff_ms, limit)).fetchall()

    orphans: list[OrphanRow] = []
    warn = crit = 0
    crit_cutoff_ms = now_ms - DEFAULT_CRIT_AGE_SEC * 1000
    for r in rows:
        if not include_delivered_pending and r["outbox_delivered_at"] is not None:
            continue
        age_sec = (now_ms - r["created_at"]) / 1000.0
        severity = "crit" if r["created_at"] < crit_cutoff_ms else "warn"
        if severity == "crit":
            crit += 1
        else:
            warn += 1
        orphans.append(OrphanRow(
            message_id=r["message_id"],
            agent_id=r["agent_id"],
            user_id=r["user_id"],
            created_at=r["created_at"],
            age_seconds=round(age_sec, 1),
            severity=severity,
            lifecycle=r["lifecycle"],
            outbox_attempts=int(r["outbox_attempts"] or 0),
            outbox_last_error=r["outbox_last_error"],
            outbox_delivered_at=r["outbox_delivered_at"],
        ))

    return OrphanListResponse(
        orphans=orphans,
        count=len(orphans),
        warn_count=warn,
        crit_count=crit,
        now_ms=now_ms,
    )


@router.get("", response_model=OrphanListResponse)
def list_orphans(
    min_age_sec: int = Query(
        DEFAULT_WARN_AGE_SEC,
        ge=0,
        description=(
            "Exclude rows newer than this many seconds. Default 30 matches "
            "the PR-26 warn threshold so the scanner's tick and the ops "
            "query see the same set by default."
        ),
    ),
    limit: int = Query(200, ge=1, le=1000),
    include_delivered_pending: bool = Query(
        True,
        description=(
            "Include rows where outbox.delivered_at IS NOT NULL but "
            "lifecycle still = 'pending'. These are the loudest symptom "
            "of a PR-22 wiring regression — do not hide them by default."
        ),
    ),
    db=Depends(get_db),
    _: dict = Depends(verify_service_or_user),
):
    """Return pending messages older than ``min_age_sec`` with outbox context.

    Ordering: oldest first. This matches both the ops SOP ("deal with the
    most stuck first") and the recovery worker's fairness policy.

    Empty result is a HTTP 200 with ``count=0`` — NOT a 404. The absence of
    orphans is the healthy case; 404 would force every caller to special-
    case it.
    """
    return query_orphans(
        db,
        min_age_sec=min_age_sec,
        limit=limit,
        include_delivered_pending=include_delivered_pending,
    )
