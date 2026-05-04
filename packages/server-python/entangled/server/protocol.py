"""Explicit Entangled WebSocket protocol contracts."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class ClientEntangleFrame:
    entity: str
    params: Optional[Dict[str, str]]
    version: Optional[int]
    head: Optional[str]
    depth: Optional[int]
    before_id: Optional[str]
    limit: int
    request_id: str


@dataclass(frozen=True)
class ClientActionFrame:
    request_id: str
    entity: str
    op: str
    entity_id: Optional[str]
    params: Dict[str, str]
    payload: Dict[str, Any]


def _reject_retired_client_aliases(raw: Dict[str, Any]) -> None:
    """Reject legacy client wire aliases instead of silently normalizing them."""
    retired = {
        "requestId": "request_id",
        "beforeId": "before_id",
    }
    for old, new in retired.items():
        if old in raw:
            raise ValueError(f"{old} is retired; use {new}")


def _params(raw: Any, *, default_none: bool) -> Optional[Dict[str, str]]:
    if raw is None or raw == {}:
        return None if default_none else {}
    if not isinstance(raw, dict):
        raise ValueError("params must be an object")
    return {str(k): str(v) for k, v in raw.items() if v is not None}


def _int_or_none(raw: Any, field: str) -> Optional[int]:
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        raise ValueError(f"{field} must be an integer")


def parse_entangle_frame(raw: Dict[str, Any]) -> ClientEntangleFrame:
    """Parse the canonical snake_case entangle frame."""
    _reject_retired_client_aliases(raw)
    limit = _int_or_none(raw.get("limit"), "limit") or 50
    return ClientEntangleFrame(
        entity=str(raw.get("entity") or ""),
        params=_params(raw.get("params"), default_none=True),
        version=_int_or_none(raw.get("version"), "version"),
        head=str(raw["head"]) if raw.get("head") is not None else None,
        depth=_int_or_none(raw.get("depth"), "depth"),
        before_id=str(raw["before_id"]) if raw.get("before_id") is not None else None,
        limit=min(limit, 500),
        request_id=str(raw.get("request_id") or ""),
    )


def parse_disentangle_frame(raw: Dict[str, Any]) -> tuple[str, Optional[Dict[str, str]]]:
    return str(raw.get("entity") or ""), _params(raw.get("params"), default_none=True)


def parse_action_frame(raw: Dict[str, Any]) -> ClientActionFrame:
    """Parse the canonical snake_case action frame."""
    _reject_retired_client_aliases(raw)
    data = raw.get("data") or {}
    if not isinstance(data, dict):
        raise ValueError("data must be an object")
    return ClientActionFrame(
        request_id=str(raw.get("request_id") or ""),
        entity=str(raw.get("entity") or ""),
        op=str(raw.get("op") or ""),
        entity_id=str(raw["id"]) if raw.get("id") is not None else None,
        params=_params(raw.get("params"), default_none=False) or {},
        payload=data,
    )


def build_push_frame(event: str, data: Any) -> Dict[str, Any]:
    if isinstance(data, dict) and data.get("type") == "sync":
        return data
    return {"type": "push", "event": event, "data": data}


def build_schema_push_frame(schema: list[dict[str, Any]], sync_contract_version: int) -> Dict[str, Any]:
    schema_hash = hashlib.md5(json.dumps(schema, sort_keys=True).encode()).hexdigest()[:12]
    return {
        "type": "push",
        "event": "schema",
        "data": {
            "entities": schema,
            "hash": schema_hash,
            "syncContractVersion": sync_contract_version,
        },
    }


def build_page_sync_frame(
    *,
    entity: str,
    params: Optional[Dict[str, str]],
    entries: list[dict[str, Any]],
    has_more: bool,
    request_id: str,
) -> Dict[str, Any]:
    return {
        "type": "sync",
        "entity": entity,
        "params": params if params else None,
        "mode": "page",
        "data": entries,
        "hasMore": has_more,
        "request_id": request_id if request_id else None,
    }


def build_error_frame(*, error: str, entity: Optional[str] = None, request_id: str = "") -> Dict[str, Any]:
    frame: Dict[str, Any] = {"type": "error", "error": error}
    if entity:
        frame["entity"] = entity
    if request_id:
        frame["request_id"] = request_id
    return frame


def build_ack_frame(request_id: str, result: Dict[str, Any]) -> Dict[str, Any]:
    return {"type": "ack", "request_id": request_id, **result}


def build_error_ack_frame(request_id: str, error: str) -> Dict[str, Any]:
    return {"type": "ack", "request_id": request_id, "success": False, "error": error}
