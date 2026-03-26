"""
entangled/server/sync.py — Git-like sync engine.

Manages per-(entity, params) version tracking, op-log, and sync decisions.
This is the "smart protocol" that decides whether to send a snapshot,
delta pack, or just confirm "up_to_date".

Concepts:
  - Version: monotonic counter per (entity, params), like commit count
  - Op-log: ring buffer of recent ops, like git reflog
  - Subscription: client registered to receive pushes for (entity, params)
  - Sync: the initial data transfer when subscribing (clone) or reconnecting (pull)
"""

from __future__ import annotations

import hashlib
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
    op: str          # "insert" | "update" | "delete"
    id: str          # entity item ID
    data: Optional[dict]  # item data (None for delete)
    ts: float        # timestamp

    def to_dict(self) -> dict:
        d: dict = {"version": self.version, "op": self.op, "id": self.id, "ts": self.ts}
        if self.data is not None:
            d["data"] = self.data
        return d


# ── Per-(entity, params) sync state ──────────────────────────────

@dataclass
class SyncState:
    """Version + op-log for one (entity, params) combination."""
    current_version: int = 0
    op_log: deque = field(default_factory=lambda: deque(maxlen=1000))
    # client_id → subscribed_at_version
    subscribers: Dict[str, int] = field(default_factory=dict)

    def append_op(self, op: str, entity_id: str, data: Optional[dict] = None) -> SyncOp:
        """Record a new operation (like git commit)."""
        self.current_version += 1
        entry = SyncOp(
            version=self.current_version,
            op=op,
            id=entity_id,
            data=data,
            ts=time.time(),
        )
        self.op_log.append(entry)
        return entry

    def get_ops_since(self, since_version: int) -> Optional[List[SyncOp]]:
        """Get ops from since_version to current (like git log since..HEAD).

        Returns None if the gap is too large (op_log doesn't go back far enough).
        """
        if since_version >= self.current_version:
            return []  # up to date

        # Find the starting point in op_log
        ops = []
        for entry in self.op_log:
            if entry.version > since_version:
                ops.append(entry)

        # Check if we have a complete chain
        if ops and ops[0].version != since_version + 1:
            # Gap in op_log — need full snapshot (like git clone after gc)
            return None

        return ops

    def has_subscriber(self, client_id: str) -> bool:
        return client_id in self.subscribers

    def subscribe(self, client_id: str) -> None:
        self.subscribers[client_id] = self.current_version

    def unsubscribe(self, client_id: str) -> None:
        self.subscribers.pop(client_id, None)


# ── Sync State Registry ─────────────────────────────────────────

def _state_key(entity: str, params: Optional[Dict[str, str]] = None) -> str:
    """Deterministic key for (entity, params)."""
    if not params:
        return entity
    sorted_params = sorted(params.items())
    return f"{entity}:{json.dumps(sorted_params)}"


class SyncRegistry:
    """Global registry of sync states."""

    def __init__(self):
        self._states: Dict[str, SyncState] = {}
        # client_id → set of state_keys they're subscribed to
        self._client_subs: Dict[str, Set[str]] = {}

    def get_state(self, entity: str, params: Optional[Dict[str, str]] = None) -> SyncState:
        key = _state_key(entity, params)
        if key not in self._states:
            self._states[key] = SyncState()
        return self._states[key]

    def subscribe(self, client_id: str, entity: str, params: Optional[Dict[str, str]] = None) -> None:
        state = self.get_state(entity, params)
        state.subscribe(client_id)
        # Track client's subscriptions
        if client_id not in self._client_subs:
            self._client_subs[client_id] = set()
        self._client_subs[client_id].add(_state_key(entity, params))

    def unsubscribe(self, client_id: str, entity: str, params: Optional[Dict[str, str]] = None) -> None:
        state = self.get_state(entity, params)
        state.unsubscribe(client_id)
        if client_id in self._client_subs:
            self._client_subs[client_id].discard(_state_key(entity, params))

    def unsubscribe_all(self, client_id: str) -> None:
        """Remove client from all subscriptions (on disconnect)."""
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
    ) -> Tuple[SyncState, SyncOp]:
        """Record a mutation op and return (state, op) for pushing."""
        state = self.get_state(entity, params)
        entry = state.append_op(op, entity_id, data)
        return state, entry

    def get_subscribed_clients(
        self,
        entity: str,
        params: Optional[Dict[str, str]] = None,
    ) -> List[str]:
        """Get client_ids subscribed to this (entity, params)."""
        state = self.get_state(entity, params)
        return list(state.subscribers.keys())


# Global singleton
sync_registry = SyncRegistry()


# ── Sync decision logic ─────────────────────────────────────────

def resolve_sync(
    state: SyncState,
    client_version: Optional[int],
    client_head: Optional[str],
    depth: Optional[int],
    fetch_data_fn: Callable,
    sync_type: str = "list",
) -> dict:
    """Decide sync mode and build response (like git smart protocol).

    Args:
        state: Current sync state for this (entity, params)
        client_version: Client's last known version (None = first subscribe)
        client_head: Client's last known item ID (for streams)
        depth: Max items to return (like --depth)
        fetch_data_fn: Callable to get current data from DB
        sync_type: "list" or "stream"

    Returns:
        Sync frame dict
    """
    # ── Case 1: First subscribe (git clone) ──────────────────────
    if client_version is None and client_head is None:
        data = fetch_data_fn()

        if sync_type == "stream" and depth:
            # Shallow clone: only latest N items
            items = data[-depth:] if len(data) > depth else data
            return {
                "mode": "head_n",
                "version": state.current_version,
                "data": items,
                "has_more": len(data) > depth,
                "total": len(data),
            }
        else:
            # Full clone
            return {
                "mode": "snapshot",
                "version": state.current_version,
                "data": data,
            }

    # ── Case 2: Reconnect with version (git pull) ────────────────
    since = client_version or 0

    if since >= state.current_version:
        # Up to date — nothing to fetch
        return {
            "mode": "up_to_date",
            "version": state.current_version,
        }

    # Try delta (git pull fast-forward)
    ops = state.get_ops_since(since)

    if ops is not None:
        # Delta available — send pack
        return {
            "mode": "delta",
            "version": state.current_version,
            "base_version": since,
            "ops": [op.to_dict() for op in ops],
        }
    else:
        # Gap too large — full re-clone
        data = fetch_data_fn()
        return {
            "mode": "snapshot",
            "version": state.current_version,
            "data": data,
        }
