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
        
    return ClaimResponse(rows=out, count=len(out))

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
    
    cur = db.execute(sql, params)
    return MarkAckResponse(updated=cur.rowcount)

@router.post("/mark_failed", response_model=MarkAckResponse)
def mark_failed(req: MarkFailedRequest, db=Depends(get_db), _: dict = Depends(verify_service_or_user)):
    import time
    now_ms = int(time.time() * 1000)
    
    row = db.execute("SELECT attempts FROM message_outbox WHERE id = ?", (req.id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Outbox message not found")
        
    attempts = row["attempts"] + 1
    
    if req.permanent:
        # If permanent, we boost attempts to a very high number (or max_attempts) 
        # so it drops out of the claim query. We leave locked_until = NULL so it's technically free
        # but the attempts < max_attempts clause ignores it.
        # Let's set it to a very high number to be safe.
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
    
    return MarkAckResponse(updated=cur.rowcount)
