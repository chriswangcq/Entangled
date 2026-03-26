"""
entangled/server/sync.py — Git-like sync engine.

Manages per-(entity, params) version tracking, op-log, and sync decisions.
This is the "smart protocol" that decides whether to send a snapshot,
delta pack, or just confirm "up_to_date".
"""

from __future__ import annotations

import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


# ── Op-log entry ─────────────────────────────────────────────────

@dataclass
class SyncOp:
    """A single mutation — like a Git commit."""
    version: int
    op: str          # "insert" | "update" | "delete" | "invalidate"
    id: str          # entity item ID
    data: Optional[dict]  # item data (None for delete/invalidate)
    ts: float        # timestamp
    request_id: Optional[str] = None  # correlation ID from WS request

    def to_dict(self) -> dict:
        d: dict = {"version": self.version, "op": self.op, "id": self.id, "ts": self.ts}
        if self.data is not None:
            d["data"] = self.data
        if self.request_id:
            d["requestId"] = self.request_id
        return d


# ── Per-(entity, params) sync state ──────────────────────────────

@dataclass
class SyncState:
    """Version + op-log for one (entity, params) combination."""
    current_version: int = 0
    op_log: deque = field(default_factory=lambda: deque(maxlen=1000))
    # client_id → subscribed_at_version
    subscribers: Dict[str, int] = field(default_factory=dict)

    @classmethod
    def with_maxlen(cls, maxlen: int) -> "SyncState":
        """Create with custom op_log size."""
        state = cls()
        state.op_log = deque(maxlen=maxlen)
        return state

    def append_op(self, op: str, entity_id: str, data: Optional[dict] = None, request_id: Optional[str] = None) -> SyncOp:
        self.current_version += 1
        entry = SyncOp(
            version=self.current_version,
            op=op,
            id=entity_id,
            data=data,
            ts=time.time(),
            request_id=request_id,
        )
        self.op_log.append(entry)
        return entry

    def get_ops_since(self, since_version: int) -> Optional[List[SyncOp]]:
        """Get ops since version. Returns None if gap too large (need snapshot)."""
        if since_version >= self.current_version:
            return []

        ops = [e for e in self.op_log if e.version > since_version]

        if ops and ops[0].version != since_version + 1:
            return None  # Gap — op_log gc'd

        return ops

    def subscribe(self, client_id: str) -> None:
        self.subscribers[client_id] = self.current_version

    def unsubscribe(self, client_id: str) -> None:
        self.subscribers.pop(client_id, None)


# ── Sync State Registry ─────────────────────────────────────────

def _normalize_params(params: Optional[Dict[str, str]]) -> Optional[Dict[str, str]]:
    """Normalize params: empty dict → None for consistent state keys."""
    if not params:
        return None
    return params


def _state_key(entity: str, params: Optional[Dict[str, str]] = None) -> str:
    params = _normalize_params(params)
    if params is None:
        return entity
    sorted_params = sorted(params.items())
    return f"{entity}:{json.dumps(sorted_params)}"


class SyncRegistry:
    """Registry of sync states. Bind to an EntityStore, not a global singleton."""

    def __init__(self):
        self._states: Dict[str, SyncState] = {}
        self._client_subs: Dict[str, Set[str]] = {}
        self._op_log_sizes: Dict[str, int] = {}  # entity → maxlen

    def set_op_log_size(self, entity: str, size: int) -> None:
        """Configure op_log size for an entity."""
        self._op_log_sizes[entity] = size

    def get_state(self, entity: str, params: Optional[Dict[str, str]] = None) -> SyncState:
        params = _normalize_params(params)
        key = _state_key(entity, params)
        if key not in self._states:
            maxlen = self._op_log_sizes.get(entity, 1000)
            self._states[key] = SyncState.with_maxlen(maxlen)
        return self._states[key]

    def subscribe(self, client_id: str, entity: str, params: Optional[Dict[str, str]] = None) -> None:
        params = _normalize_params(params)
        state = self.get_state(entity, params)
        state.subscribe(client_id)
        if client_id not in self._client_subs:
            self._client_subs[client_id] = set()
        self._client_subs[client_id].add(_state_key(entity, params))

    def unsubscribe(self, client_id: str, entity: str, params: Optional[Dict[str, str]] = None) -> None:
        params = _normalize_params(params)
        state = self.get_state(entity, params)
        state.unsubscribe(client_id)
        if client_id in self._client_subs:
            self._client_subs[client_id].discard(_state_key(entity, params))

    def unsubscribe_all(self, client_id: str) -> None:
        keys = self._client_subs.pop(client_id, set())
        for key in keys:
            if key in self._states:
                self._states[key].unsubscribe(client_id)

    def record_op(
        self,
        entity: str,
        op: str,
        entity_id: str,
        params: Optional[Dict[str, str]] = None,
        data: Optional[dict] = None,
        request_id: Optional[str] = None,
    ) -> Tuple[SyncState, SyncOp]:
        params = _normalize_params(params)
        state = self.get_state(entity, params)
        entry = state.append_op(op, entity_id, data, request_id=request_id)
        return state, entry

    def get_subscribed_clients(
        self,
        entity: str,
        params: Optional[Dict[str, str]] = None,
    ) -> List[str]:
        params = _normalize_params(params)
        state = self.get_state(entity, params)
        return list(state.subscribers.keys())

    def reset(self) -> None:
        """Clear all state (for testing)."""
        self._states.clear()
        self._client_subs.clear()


# ── Sync decision logic ─────────────────────────────────────────

def resolve_sync(
    state: SyncState,
    client_version: Optional[int],
    client_head: Optional[str],
    depth: Optional[int],
    fetch_data_fn: Callable,
    sync_type: str = "list",
) -> dict:
    """Decide sync mode (like git smart protocol)."""

    # Case 1: First subscribe (git clone)
    if client_version is None and client_head is None:
        data = fetch_data_fn()

        if sync_type == "stream" and depth:
            items = data[-depth:] if len(data) > depth else data
            return {
                "mode": "head_n",
                "version": state.current_version,
                "data": items,
                "hasMore": len(data) > depth,
                "total": len(data),
            }
        else:
            return {
                "mode": "snapshot",
                "version": state.current_version,
                "data": data,
            }

    # Case 2: Reconnect with version (git pull)
    since = client_version or 0

    if since >= state.current_version:
        return {
            "mode": "up_to_date",
            "version": state.current_version,
        }

    ops = state.get_ops_since(since)

    if ops is not None:
        return {
            "mode": "delta",
            "version": state.current_version,
            "baseVersion": since,
            "ops": [op.to_dict() for op in ops],
        }
    else:
        data = fetch_data_fn()
        return {
            "mode": "snapshot",
            "version": state.current_version,
            "data": data,
        }
