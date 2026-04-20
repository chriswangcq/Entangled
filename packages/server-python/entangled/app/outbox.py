import json
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException

from pydantic import BaseModel

from .auth import verify_service_or_user
from .state import get_db

router = APIRouter(prefix="/v1/outbox", tags=["Outbox"])

class ClaimRequest(BaseModel):
    worker_id: str
    batch_size: int = 50
    claim_ttl_ms: int = 30_000
    max_attempts: int = 5

class ClaimedRow(BaseModel):
    id: int
    message_id: str
    agent_id: str
    trigger_type: str
    payload_json: str
    attempts: int
    created_at: int

class ClaimResponse(BaseModel):
    rows: list[ClaimedRow]
    count: int
    # PR-32 (2026-04-21) — piggy-back outbox health signals on each claim
    # response so the subscriber can publish ``outbox_backlog_count`` /
    # ``outbox_lag_seconds`` gauges without a second round-trip. These
    # count ``delivered_at IS NULL`` rows (i.e. the full pending set,
    # including rows already locked by another worker) and take their
    # oldest-row timestamp from ``created_at``, which is millisecond
    # epoch. ``oldest_pending_age_ms=-1`` is the "no pending rows"
    # sentinel so the subscriber can distinguish "backlog is clear"
    # from "couldn't measure".
    backlog_count: int = 0
    oldest_pending_age_ms: int = -1

class MarkDeliveredRequest(BaseModel):
    ids: list[int]

class MarkFailedRequest(BaseModel):
    id: int
    kind: str
    error: str
    permanent: bool
    retry_delay_ms: Optional[int] = None

class MarkAckResponse(BaseModel):
    updated: int

@router.post("/claim", response_model=ClaimResponse)
def claim_outbox(req: ClaimRequest, db=Depends(get_db), _: dict = Depends(verify_service_or_user)):
    import time
    now_ms = int(time.time() * 1000)

    # DLQ semantic: attempts < max_attempts ensures poison messages aren't infinitely claimed
    sql = """
        UPDATE message_outbox
           SET locked_by = ?, locked_until = ?
         WHERE id IN (
             SELECT id FROM message_outbox
              WHERE delivered_at IS NULL
                AND (locked_until IS NULL OR locked_until <= ?)
                AND attempts < ?
              ORDER BY id
              LIMIT ?
         )
         RETURNING id, message_id, agent_id, trigger_type, payload_json, attempts, created_at
    """
    locked_until = now_ms + req.claim_ttl_ms
    # PR-17: wrap in Entangled's global FIFO lock so the claim UPDATE serializes
    # against concurrent message appends (SqlEntityStore.append also uses this
    # lock). Without this, the subscriber's 2 TPS polling + concurrent writes
    # collide under SQLite's busy_timeout and return 500 "database is locked".
    with db.transaction("global"):
        rows = db.execute(sql, (
            req.worker_id,
            locked_until,
            now_ms,
            req.max_attempts,
            req.batch_size
        )).fetchall()

    out = []
    for row in rows:
        out.append(ClaimedRow(
            id=row["id"],
            message_id=row["message_id"],
            agent_id=row["agent_id"],
            trigger_type=row["trigger_type"],
            payload_json=row["payload_json"],
            attempts=row["attempts"],
            created_at=row["created_at"],
        ))

    # PR-32 — compute backlog + oldest-age in the same lock window as
    # the claim so subscriber-side metrics reflect a consistent
    # snapshot (no double-count of rows that move between pending and
    # claimed during this call). One extra scalar SELECT at the end of
    # a claim transaction is negligible compared to the UPDATE.
    backlog = 0
    oldest_age_ms = -1
    try:
        stats_row = db.execute(
            "SELECT COUNT(*) AS c, MIN(created_at) AS oldest "
            "FROM message_outbox WHERE delivered_at IS NULL"
        ).fetchone()
        if stats_row is not None:
            backlog = int(stats_row["c"] or 0)
            oldest = stats_row["oldest"]
            if oldest is not None and backlog > 0:
                oldest_age_ms = max(0, now_ms - int(oldest))
    except Exception:
        # Never fail a claim because the backlog scalar failed — the
        # subscriber can still make progress without a fresh gauge
        # sample this tick.
        pass

    return ClaimResponse(
        rows=out,
        count=len(out),
        backlog_count=backlog,
        oldest_pending_age_ms=oldest_age_ms,
    )

@router.post("/mark_delivered", response_model=MarkAckResponse)
def mark_delivered(req: MarkDeliveredRequest, db=Depends(get_db), _: dict = Depends(verify_service_or_user)):
    if not req.ids:
        return MarkAckResponse(updated=0)

    import time
    now_ms = int(time.time() * 1000)

    placeholders = ",".join(["?"] * len(req.ids))
    sql = f"""
        UPDATE message_outbox
           SET delivered_at = ?, locked_by = NULL, locked_until = NULL, last_error = NULL
         WHERE id IN ({placeholders})
    """
    params = [now_ms] + req.ids

    with db.transaction("global"):
        cur = db.execute(sql, params)
        updated = cur.rowcount
    return MarkAckResponse(updated=updated)

@router.post("/mark_failed", response_model=MarkAckResponse)
def mark_failed(req: MarkFailedRequest, db=Depends(get_db), _: dict = Depends(verify_service_or_user)):
    import time
    now_ms = int(time.time() * 1000)

    with db.transaction("global"):
        row = db.execute("SELECT attempts FROM message_outbox WHERE id = ?", (req.id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Outbox message not found")

        attempts = row["attempts"] + 1

        if req.permanent:
            # Sentinel value 999999 drops the row from the claim query's
            # `attempts < max_attempts` filter. See PR-26 TD about recording
            # the real attempt count for observability.
            attempts = 999999
            locked_until = None
        else:
            locked_until = now_ms + (req.retry_delay_ms or 1000)

        sql = """
            UPDATE message_outbox
               SET attempts = ?, last_error = ?, locked_by = NULL, locked_until = ?
             WHERE id = ?
        """

        error_msg = f"{req.kind}: {req.error}"
        cur = db.execute(sql, (attempts, error_msg, locked_until, req.id))
        updated = cur.rowcount
    return MarkAckResponse(updated=updated)
