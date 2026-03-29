"""
entangled/server/sync.py — Git-like sync engine.

Manages per-(entity, params) version tracking, op-log, and sync decisions.
This is the "smart protocol" that decides whether to send a snapshot,
delta pack, head_n (bounded stream window), or up_to_date.

Stream entities (`sync_type == "stream"`): every path that materializes rows uses
head_n only (`fetch_data_fn(limit=depth+1)`), including op-log gap recovery.
Hosts should pass ``default_stream_depth`` from ``EntityDef.sync_limit``; if both
client ``depth`` and that default are absent, ``DEFAULT_STREAM_HEAD_DEPTH`` applies.
"""


import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

# Fallback when client and EntityDef both omit a stream window (keep in sync with typical sync_limit).
DEFAULT_STREAM_HEAD_DEPTH: int = 50
# Hard cap to avoid accidental huge LIMIT from bad client input.
MAX_STREAM_HEAD_DEPTH: int = 10_000


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

        if not ops:
            # Client is behind persisted current_version but op_log is empty
            # (Gateway restart, deque maxlen GC, etc.) — must not return a bogus delta.
            return None

        if ops[0].version != since_version + 1:
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

    def __init__(
        self,
        on_version_bump: Optional[Callable[[str, int], None]] = None,
    ):
        self._states: Dict[str, SyncState] = {}
        self._client_subs: Dict[str, Set[str]] = {}
        self._op_log_sizes: Dict[str, int] = {}  # entity → maxlen
        # Optional: persist (state_key, current_version) after each mutation (e.g. Gateway SQLite).
        self._on_version_bump = on_version_bump

    def hydrate_versions(self, versions: Dict[str, int]) -> None:
        """Restore current_version from persisted storage after process restart.

        Call after set_op_log_size for all entities so deque maxlen is correct.
        """
        for key, ver in versions.items():
            if ver < 0:
                continue
            entity = key.split(":", 1)[0] if ":" in key else key
            maxlen = self._op_log_sizes.get(entity, 1000)
            if key not in self._states:
                self._states[key] = SyncState.with_maxlen(maxlen)
            st = self._states[key]
            st.current_version = max(st.current_version, ver)

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
        if self._on_version_bump is not None:
            key = _state_key(entity, params)
            try:
                self._on_version_bump(key, state.current_version)
            except Exception as e:
                logger.warning(
                    "[Entangled] on_version_bump failed for %s v=%s: %s",
                    key,
                    state.current_version,
                    e,
                )
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

def _effective_stream_depth(
    depth: Optional[int],
    default_stream_depth: Optional[int],
) -> int:
    """
    Resolve head_n window: prefer client ``depth``, then ``default_stream_depth``
    (EntityDef.sync_limit), then ``DEFAULT_STREAM_HEAD_DEPTH``. Clamp to ``MAX_STREAM_HEAD_DEPTH``.
    """
    for candidate in (depth, default_stream_depth):
        if candidate is None:
            continue
        try:
            d = int(candidate)
        except (TypeError, ValueError):
            continue
        if d <= 0:
            continue
        return min(d, MAX_STREAM_HEAD_DEPTH)
    return DEFAULT_STREAM_HEAD_DEPTH


def _stream_head_n_sync(
    state: SyncState,
    fetch_data_fn: Callable,
    depth: Optional[int],
    default_stream_depth: Optional[int],
    *,
    reason: str = "subscribe",
    exists_before_fn: Optional[Callable[[str], bool]] = None,
    data_order: str = "desc",
) -> dict:
    """
    Bounded sync for stream entities.

    If ``exists_before_fn`` is provided, fetches exactly ``d`` rows and uses
    cursor-based ``SELECT EXISTS(...)`` to determine ``hasMore`` precisely.
    Otherwise falls back to N+1 detection (fetch d+1, compare count).

    Data is always normalized to ASC (oldest first) before sending to clients.
    The ``data_order`` parameter declares the order returned by ``fetch_data_fn``.
    """
    d = _effective_stream_depth(depth, default_stream_depth)

    if exists_before_fn:
        # Cursor-based: fetch exactly d items, then EXISTS check on oldest
        data = fetch_data_fn(limit=d)
        # Normalize to ASC (oldest first) for client consumption
        if data_order == "desc":
            data = data[::-1]
        # Now data is ASC: data[0] = oldest, data[-1] = newest
        if len(data) < d:
            has_more = False
        else:
            oldest = data[0] if data else None
            oldest_id = oldest.get("id") if oldest else None
            has_more = exists_before_fn(oldest_id) if oldest_id else False
        items = data
    else:
        # Fallback: N+1 (for generic Entangled hosts without exists_before_fn)
        data = fetch_data_fn(limit=d + 1)
        has_more = len(data) > d
        if data_order == "desc":
            # Keep the newest `d` items (data[:d] since DESC), then reverse to ASC
            items = data[:d] if has_more else data
            items = items[::-1]
        else:
            # ASC: keep the oldest `d` items (drop the extra newest one)
            items = data[:d] if has_more else data

    logger.debug(
        "[Entangled] head_n (%s): depth=%d rows=%d hasMore=%s order=%s",
        reason,
        d,
        len(items),
        has_more,
        data_order,
    )
    return {
        "mode": "head_n",
        "version": state.current_version,
        "data": items,
        "hasMore": has_more,
    }


def resolve_sync(
    state: SyncState,
    client_version: Optional[int],
    client_head: Optional[str],
    depth: Optional[int],
    fetch_data_fn: Callable,
    sync_type: str = "list",
    *,
    default_stream_depth: Optional[int] = None,
    exists_before_fn: Optional[Callable[[str], bool]] = None,
    data_order: str = "desc",
) -> dict:
    """Decide sync mode (like git smart protocol).

    Args:
        state: Per-(entity, params) version and op-log.
        client_version: Client's last known server version (None = first subscribe).
        client_head: Reserved for stream cursors; None uses version-based sync.
        depth: Client-requested head_n window (WS ``depth``); may be None.
        fetch_data_fn: Host callback; MUST support ``limit`` kw/arg for bounded reads.
        sync_type: ``"list"`` or ``"stream"``.
        default_stream_depth: Entity schema default (``EntityDef.sync_limit``) when
            ``depth`` is omitted or invalid; None uses ``DEFAULT_STREAM_HEAD_DEPTH``.
        exists_before_fn: Optional host callback ``(oldest_id: str) -> bool`` for
            cursor-based ``hasMore`` detection. When provided, stream sync fetches
            exactly ``depth`` rows and uses ``SELECT EXISTS(...)`` to check for older
            rows — precise and efficient. Without it, falls back to N+1 detection.
        data_order: The ordering of data returned by ``fetch_data_fn``.
            ``"desc"`` = newest first (default), ``"asc"`` = oldest first.
            The sync engine normalizes all output to ASC before sending to clients.

    Returns:
        Dict with ``mode``, ``version``, and optional ``data`` / ``ops`` / ``hasMore``.
    """

    # Case 1: First subscribe (git clone)
    if client_version is None and client_head is None:
        if sync_type == "stream":
            return _stream_head_n_sync(
                state,
                fetch_data_fn,
                depth,
                default_stream_depth,
                reason="first_subscribe",
                exists_before_fn=exists_before_fn,
                data_order=data_order,
            )
        data = fetch_data_fn()
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

    # Op-log gap: list entities need a full snapshot; stream must stay bounded.
    if sync_type == "stream":
        logger.warning(
            "[Entangled] stream op-log gap — head_n resync (no unbounded snapshot), "
            "since_client_version=%s current_version=%s",
            since,
            state.current_version,
        )
        return _stream_head_n_sync(
            state,
            fetch_data_fn,
            depth,
            default_stream_depth,
            reason="op_log_gap",
            exists_before_fn=exists_before_fn,
            data_order=data_order,
        )

    data = fetch_data_fn()
    return {
        "mode": "snapshot",
        "version": state.current_version,
        "data": data,
    }
