"""REST CRUD API — unified entity operations.

All requests carry scope via headers:
    X-User-ID: user_123
    X-Params: {"agent_id":"a1"}   (JSON)
    X-Request-ID: req_xxx
    X-Notify: true
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel

from .auth import verify_service_or_user, verify_service_token
from .state import get_store

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/entities", tags=["crud"])


# ── Header extraction helpers ─────────────────────────────────────────────


def _parse_params(x_params: Optional[str] = Header(None)) -> Optional[Dict[str, str]]:
    if not x_params:
        return None
    try:
        return json.loads(x_params)
    except (json.JSONDecodeError, TypeError):
        return None


def _parse_notify(x_notify: Optional[str] = Header(None)) -> bool:
    if x_notify is None:
        return True
    return x_notify.lower() not in ("false", "0", "no")


# ── List ──────────────────────────────────────────────────────────────────


@router.get("/{entity}")
def list_entities(
    entity: str,
    order_by: Optional[str] = None,
    limit: Optional[int] = None,
    user_id: str = Depends(verify_service_or_user),
    params: Optional[Dict[str, str]] = Depends(_parse_params),
):
    store = get_store()
    try:
        data = store.list(entity, user_id, params=params, order_by=order_by, limit=limit)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return {"data": data, "total": len(data)}


class ListAdvancedBody(BaseModel):
    filters: Optional[Dict[str, Any]] = None
    order_by: Optional[str] = None
    limit: Optional[int] = None
    skip_default_not_in: bool = False


@router.post("/{entity}/list")
def list_entities_advanced(
    entity: str,
    body: ListAdvancedBody,
    user_id: str = Depends(verify_service_or_user),
    params: Optional[Dict[str, str]] = Depends(_parse_params),
):
    store = get_store()
    try:
        data = store.list(
            entity,
            user_id,
            params=params,
            filters=body.filters,
            order_by=body.order_by,
            limit=body.limit,
            skip_default_not_in=body.skip_default_not_in,
        )
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return {"data": data, "total": len(data)}


class ExistsBeforeBody(BaseModel):
    before_id: str


@router.post("/{entity}/exists-before")
def exists_before_entity(
    entity: str,
    body: ExistsBeforeBody,
    user_id: str = Depends(verify_service_or_user),
    params: Optional[Dict[str, str]] = Depends(_parse_params),
):
    store = get_store()
    try:
        ok = store.exists_before(entity, user_id, body.before_id, params=params)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return {"exists": ok}


class UpdateWhereBody(BaseModel):
    data: Dict[str, Any]
    filters: Optional[Dict[str, Any]] = None


@router.post("/{entity}/update-where")
def update_where_entity(
    entity: str,
    body: UpdateWhereBody,
    user_id: str = Depends(verify_service_or_user),
    params: Optional[Dict[str, str]] = Depends(_parse_params),
    notify: bool = Depends(_parse_notify),
):
    store = get_store()
    try:
        affected = store.update_where(
            entity,
            user_id,
            body.data,
            params=params,
            filters=body.filters,
            notify=notify,
        )
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    return {"affected": affected}


# ── Get ───────────────────────────────────────────────────────────────────


@router.get("/{entity}/{entity_id}")
def get_entity(
    entity: str,
    entity_id: str,
    include_hidden: bool = Query(False),
    user_id: str = Depends(verify_service_or_user),
    params: Optional[Dict[str, str]] = Depends(_parse_params),
):
    store = get_store()
    try:
        defn = store.get_def(entity)
        if include_hidden:
            item = store._sql_get(defn, user_id, entity_id, params=params, include_hidden=True)
        else:
            item = store.get(entity, user_id, entity_id, params=params)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    if item is None:
        raise HTTPException(status_code=404, detail=f"{entity}/{entity_id} not found")
    return {"data": item}


# ── Create ────────────────────────────────────────────────────────────────


@router.post("/{entity}")
def create_entity(
    entity: str,
    body: dict,
    user_id: str = Depends(verify_service_or_user),
    params: Optional[Dict[str, str]] = Depends(_parse_params),
    x_request_id: Optional[str] = Header(None),
    notify: bool = Depends(_parse_notify),
):
    store = get_store()
    try:
        item = store.create(entity, user_id, body, params=params, notify=notify)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"data": item}


# ── Update ────────────────────────────────────────────────────────────────


@router.patch("/{entity}/{entity_id}")
def update_entity(
    entity: str,
    entity_id: str,
    body: dict,
    user_id: str = Depends(verify_service_or_user),
    params: Optional[Dict[str, str]] = Depends(_parse_params),
    notify: bool = Depends(_parse_notify),
):
    store = get_store()
    try:
        item = store.update(entity, user_id, entity_id, body, params=params, notify=notify)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    if item is None:
        raise HTTPException(status_code=404, detail=f"{entity}/{entity_id} not found")
    return {"data": item}


# ── Delete ────────────────────────────────────────────────────────────────


@router.delete("/{entity}/{entity_id}")
def delete_entity(
    entity: str,
    entity_id: str,
    user_id: str = Depends(verify_service_or_user),
    params: Optional[Dict[str, str]] = Depends(_parse_params),
    notify: bool = Depends(_parse_notify),
):
    store = get_store()
    try:
        ok = store.delete(entity, user_id, entity_id, params=params, notify=notify)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    if not ok:
        raise HTTPException(status_code=404, detail=f"{entity}/{entity_id} not found")
    return {"deleted": True}


# ── Upsert ────────────────────────────────────────────────────────────────


@router.put("/{entity}/{entity_id}")
def upsert_entity(
    entity: str,
    entity_id: str,
    body: dict,
    user_id: str = Depends(verify_service_or_user),
    params: Optional[Dict[str, str]] = Depends(_parse_params),
    notify: bool = Depends(_parse_notify),
):
    store = get_store()
    try:
        item = store.upsert(entity, user_id, entity_id, body, params=params, notify=notify)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    return {"data": item}


# ── Query (list_stream) ──────────────────────────────────────────────────


class QueryBody(BaseModel):
    before_id: Optional[str] = None
    after_id: Optional[str] = None
    limit: int = 50
    filters: Optional[Dict[str, Any]] = None
    in_filters: Optional[Dict[str, List[Any]]] = None
    not_in_filters: Optional[Dict[str, List[Any]]] = None
    order_by: Optional[str] = None
    cursor_field: Optional[str] = None
    skip_default_not_in: bool = False


@router.post("/{entity}/query")
def query_entities(
    entity: str,
    body: QueryBody,
    user_id: str = Depends(verify_service_or_user),
    params: Optional[Dict[str, str]] = Depends(_parse_params),
):
    store = get_store()
    kwargs: Dict[str, Any] = {
        "params": params,
        "limit": body.limit,
    }
    if body.before_id:
        kwargs["before_id"] = body.before_id
    if body.after_id:
        kwargs["after_id"] = body.after_id
    if body.filters:
        kwargs["filters"] = body.filters
    if body.in_filters:
        kwargs["in_filters"] = body.in_filters
    if body.not_in_filters:
        kwargs["not_in_filters"] = body.not_in_filters
    if body.order_by:
        kwargs["order_by"] = body.order_by
    if body.cursor_field:
        kwargs["cursor_field"] = body.cursor_field
    if body.skip_default_not_in:
        kwargs["skip_default_not_in"] = True
    try:
        data = store.list_stream(entity, user_id, **kwargs)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))

    defn = store.get_def(entity)
    has_more = False
    if data and body.before_id is None and len(data) >= body.limit:
        last_id = data[-1].get(defn.id_field)
        if last_id:
            has_more = store.exists_before(entity, user_id, str(last_id), params=params)
    elif data and body.before_id:
        has_more = len(data) >= body.limit

    return {"data": data, "has_more": has_more}


# ── Append (stream) ──────────────────────────────────────────────────────


@router.post("/{entity}/append")
def append_entity(
    entity: str,
    body: dict,
    user_id: str = Depends(verify_service_or_user),
    params: Optional[Dict[str, str]] = Depends(_parse_params),
    notify: bool = Depends(_parse_notify),
):
    store = get_store()
    try:
        item = store.append(entity, user_id, body, params=params, notify=notify)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    return {"data": item}


# ── Batch update ──────────────────────────────────────────────────────────


class TenantOwnershipMigrationItem(BaseModel):
    entity_id: str
    user_id: str
    parent_id: str


class TenantOwnershipMigrationBody(BaseModel):
    source_user_id: str
    items: List[TenantOwnershipMigrationItem]


@router.post("/{entity}/migrate-tenant-ownership")
def migrate_tenant_ownership(
    entity: str,
    body: TenantOwnershipMigrationBody,
    _service_user: str = Depends(verify_service_token),
):
    """Move reserved quarantine rows to verified tenant parents in one batch."""

    store = get_store()
    migrate = getattr(store, "migrate_quarantined_tenant_ownership", None)
    if not callable(migrate):
        raise HTTPException(
            status_code=409,
            detail="storage backend does not support tenant ownership migration",
        )
    try:
        return migrate(
            entity,
            body.source_user_id,
            [item.model_dump() for item in body.items],
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class BatchUpdateBody(BaseModel):
    ids: List[str]
    data: Dict[str, Any]


@router.post("/{entity}/batch-update")
def batch_update_entities(
    entity: str,
    body: BatchUpdateBody,
    user_id: str = Depends(verify_service_or_user),
    params: Optional[Dict[str, str]] = Depends(_parse_params),
    notify: bool = Depends(_parse_notify),
):
    store = get_store()
    try:
        affected = store.batch_update(
            entity, user_id, body.ids, body.data, params=params, emit_notifications=notify
        )
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    return {"affected": affected}


# ── CAS update ────────────────────────────────────────────────────────────


class CasUpdateBody(BaseModel):
    where: Dict[str, Any]
    data: Dict[str, Any]


@router.post("/{entity}/cas-update")
def cas_update_entity(
    entity: str,
    body: CasUpdateBody,
    user_id: str = Depends(verify_service_or_user),
    params: Optional[Dict[str, str]] = Depends(_parse_params),
    notify: bool = Depends(_parse_notify),
):
    store = get_store()
    try:
        result = store.cas_update(
            entity, user_id, body.where, body.data, params=params, emit_notifications=notify
        )
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    if result is None:
        raise HTTPException(status_code=409, detail="cas_failed")
    return {"data": result}


# ── Count ─────────────────────────────────────────────────────────────────


class CountBody(BaseModel):
    filters: Optional[Dict[str, Any]] = None


@router.post("/{entity}/count")
def count_entities(
    entity: str,
    body: CountBody,
    user_id: str = Depends(verify_service_or_user),
    params: Optional[Dict[str, str]] = Depends(_parse_params),
):
    store = get_store()
    try:
        cnt = store.count(entity, user_id, params=params, filters=body.filters)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return {"count": cnt}


# ── Delete where ──────────────────────────────────────────────────────────


class DeleteWhereBody(BaseModel):
    filters: Optional[Dict[str, Any]] = None


# ── Stream chunk (push-only, no persistence) ─────────────────────────────


class StreamChunkBody(BaseModel):
    entity_id: str
    delta: Any
    params: Optional[Dict[str, str]] = None


@router.post("/{entity}/stream-chunk")
def stream_chunk(
    entity: str,
    body: StreamChunkBody,
    user_id: str = Depends(verify_service_or_user),
):
    """Push a streaming delta to entangled peers without writing to the database."""
    store = get_store()
    try:
        store.stream_chunk(
            entity, user_id, body.entity_id, body.delta, params=body.params,
        )
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return {"ok": True}


@router.post("/{entity}/delete-where")
def delete_where(
    entity: str,
    body: DeleteWhereBody,
    user_id: str = Depends(verify_service_or_user),
    params: Optional[Dict[str, str]] = Depends(_parse_params),
    notify: bool = Depends(_parse_notify),
):
    store = get_store()
    try:
        affected = store.delete_where(entity, user_id, params=params, filters=body.filters, notify=notify)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return {"affected": affected}
