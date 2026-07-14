"""
entangled/server/sync.py — Git-like sync engine.

Manages per-(entity, user, params) version tracking, op-log, and sync decisions.
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
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

logger = logging.getLogger(__name__)


def _pk_value_from_row(row: Optional[dict], id_field: str) -> Optional[str]:
    """Normalize PK from a sync row for cursor / exists_before (int or str)."""
    if not row:
        return None
    v = row.get(id_field)
    if v is None:
        return None
    return str(v)


# Default stream window when neither client depth nor EntityDef sync_limit is set.
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


# ── Per-(entity, user, params) sync state ────────────────────────

@dataclass
class SyncState:
    """Version + op-log for one (entity, optional user, params) partition."""
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
            # (server restart, deque maxlen GC, etc.) — must not return a bogus delta.
            return None

        if ops[0].version != since_version + 1:
            return None  # Gap — op_log gc'd

        return ops

    def entangle(self, client_id: str) -> None:
        self.subscribers[client_id] = self.current_version

    def disentangle(self, client_id: str) -> None:
        self.subscribers.pop(client_id, None)


class SyncStateSnapshot:
    """Immutable copy of :class:`SyncState` for running :func:`resolve_sync` in ``asyncio.to_thread``.

    The live ``SyncState`` is mutated on the asyncio thread by :meth:`SyncRegistry.record_op`.
    Passing a snapshot avoids races. Call :func:`snapshot_for_resolve` immediately after
    ``entangle``, with no ``await`` between snapshot and entangle, then reconcile version
    / delta on the event loop (see ``ws_handler._entangle_one``).
    """

    __slots__ = ("current_version", "_op_log")

    def __init__(self, current_version: int, op_log: List[SyncOp]):
        self.current_version = current_version
        self._op_log = op_log

    def get_ops_since(self, since_version: int) -> Optional[List[SyncOp]]:
        if since_version >= self.current_version:
            return []
        ops = [e for e in self._op_log if e.version > since_version]
        if not ops:
            return None
        if ops[0].version != since_version + 1:
            return None
        return ops


def snapshot_for_resolve(state: SyncState) -> SyncStateSnapshot:
    """Build a thread-safe snapshot (deque copied to list)."""
    return SyncStateSnapshot(state.current_version, list(state.op_log))


# ── Sync State Registry ─────────────────────────────────────────

def _normalize_params(params: Optional[Dict[str, str]]) -> Optional[Dict[str, str]]:
    """Normalize params: empty dict → None for consistent state keys."""
    if not params:
        return None
    return params


def _state_key(
    entity: str,
    params: Optional[Dict[str, str]] = None,
    *,
    user_id: Optional[str] = None,
) -> str:
    """Return the durable sync partition key.

    ``user_id=None`` deliberately preserves the original global-entity key so
    global subscriptions remain shared. User-owned entities must pass their
    authenticated owner explicitly; their key uses a structured payload so a
    user id can never collide with a business ``params`` key.
    """
    params = _normalize_params(params)
    if user_id is not None:
        payload = {
            "params": sorted(params.items()) if params is not None else [],
            "user_id": user_id,
        }
        return f"{entity}:{json.dumps(payload, sort_keys=True, separators=(',', ':'))}"
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
        # Optional: persist (state_key, current_version) after each mutation.
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

    def get_state(
        self,
        entity: str,
        params: Optional[Dict[str, str]] = None,
        *,
        user_id: Optional[str] = None,
    ) -> SyncState:
        params = _normalize_params(params)
        key = _state_key(entity, params, user_id=user_id)
        if key not in self._states:
            maxlen = self._op_log_sizes.get(entity, 1000)
            state = SyncState.with_maxlen(maxlen)
            if user_id is not None:
                # Migration fence: the pre-user-partition implementation stored
                # every tenant under the legacy (entity, params) key. Start each
                # new user partition one version beyond that history so an old
                # cache can never compare equal and be called up-to-date. The
                # empty op-log then deliberately forces snapshot/head_n.
                legacy_key = _state_key(entity, params)
                legacy_state = self._states.get(legacy_key)
                state.current_version = (
                    legacy_state.current_version if legacy_state is not None else 0
                ) + 1
            self._states[key] = state
        return self._states[key]

    def entangle(
        self,
        client_id: str,
        entity: str,
        params: Optional[Dict[str, str]] = None,
        *,
        user_id: Optional[str] = None,
    ) -> None:
        params = _normalize_params(params)
        state = self.get_state(entity, params, user_id=user_id)
        state.entangle(client_id)
        if client_id not in self._client_subs:
            self._client_subs[client_id] = set()
        self._client_subs[client_id].add(_state_key(entity, params, user_id=user_id))

    def disentangle(
        self,
        client_id: str,
        entity: str,
        params: Optional[Dict[str, str]] = None,
        *,
        user_id: Optional[str] = None,
    ) -> None:
        params = _normalize_params(params)
        state = self.get_state(entity, params, user_id=user_id)
        state.disentangle(client_id)
        if client_id in self._client_subs:
            self._client_subs[client_id].discard(
                _state_key(entity, params, user_id=user_id)
            )

    def disentangle_all(self, client_id: str) -> None:
        keys = self._client_subs.pop(client_id, set())
        for key in keys:
            if key in self._states:
                self._states[key].disentangle(client_id)

    def record_op(
        self,
        entity: str,
        op: str,
        entity_id: str,
        params: Optional[Dict[str, str]] = None,
        data: Optional[dict] = None,
        request_id: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> Tuple[SyncState, SyncOp]:
        params = _normalize_params(params)
        state = self.get_state(entity, params, user_id=user_id)
        entry = state.append_op(op, entity_id, data, request_id=request_id)
        if self._on_version_bump is not None:
            key = _state_key(entity, params, user_id=user_id)
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

    def get_entangled_clients(
        self,
        entity: str,
        params: Optional[Dict[str, str]] = None,
        *,
        user_id: Optional[str] = None,
    ) -> List[str]:
        params = _normalize_params(params)
        state = self.get_state(entity, params, user_id=user_id)
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
    state: Union[SyncState, SyncStateSnapshot],
    fetch_data_fn: Callable,
    depth: Optional[int],
    default_stream_depth: Optional[int],
    *,
    reason: str = "entangle",
    exists_before_fn: Optional[Callable[[str], bool]] = None,
    data_order: str = "desc",
    id_field: str = "id",
) -> dict:
    """
    Bounded sync for stream entities.

    Requires ``exists_before_fn``. It fetches exactly ``d`` rows and uses
    cursor-based ``SELECT EXISTS(...)`` to determine ``hasMore`` precisely.

    Data is always normalized to ASC (oldest first) before sending to clients.
    The ``data_order`` parameter declares the order returned by ``fetch_data_fn``.
    """
    d = _effective_stream_depth(depth, default_stream_depth)

    if not exists_before_fn:
        raise RuntimeError("stream sync requires exists_before_fn")

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
        oldest_id = _pk_value_from_row(oldest, id_field)
        has_more = exists_before_fn(oldest_id) if oldest_id else False
    items = data

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
    state: Union[SyncState, SyncStateSnapshot],
    client_version: Optional[int],
    client_head: Optional[str],
    depth: Optional[int],
    fetch_data_fn: Callable,
    sync_type: str = "list",
    *,
    default_stream_depth: Optional[int] = None,
    exists_before_fn: Optional[Callable[[str], bool]] = None,
    data_order: str = "desc",
    id_field: str = "id",
) -> dict:
    """Decide sync mode (like git smart protocol).

    Args:
        state: Per-(entity, optional user, params) version and op-log.
        client_version: Client's last known server version (None = first entangle).
        client_head: Reserved for stream cursors; None uses version-based sync.
        depth: Client-requested head_n window (WS ``depth``); may be None.
        fetch_data_fn: Host callback; MUST support ``limit`` kw/arg for bounded reads.
        sync_type: ``"list"`` or ``"stream"``.
        default_stream_depth: Entity schema default (``EntityDef.sync_limit``) when
            ``depth`` is omitted or invalid; None uses ``DEFAULT_STREAM_HEAD_DEPTH``.
        exists_before_fn: Required for stream sync. Callback
            ``(oldest_id: str) -> bool`` for cursor-based ``hasMore`` detection.
        data_order: The ordering of data returned by ``fetch_data_fn``.
            ``"desc"`` = newest first (default), ``"asc"`` = oldest first.
            The sync engine normalizes all output to ASC before sending to clients.
        id_field: JSON / row key for the entity primary key (for stream hasMore cursor).

    Returns:
        Dict with ``mode``, ``version``, and optional ``data`` / ``ops`` / ``hasMore``.
    """

    # Case 1: First entangle (git clone)
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
                id_field=id_field,
            )
        data = fetch_data_fn()
        return {
            "mode": "snapshot",
            "version": state.current_version,
            "data": data,
        }

    # Case 2: Reconnect with version (git pull)
    since = client_version or 0

    # A scoped-state migration (or disaster recovery) may reset the server
    # partition below a cached client version. Treat that as a divergent history,
    # never as "up to date", so stale/cross-tenant cache entries are replaced.
    if since > state.current_version:
        if sync_type == "stream":
            return _stream_head_n_sync(
                state,
                fetch_data_fn,
                depth,
                default_stream_depth,
                reason="client_version_ahead",
                exists_before_fn=exists_before_fn,
                data_order=data_order,
                id_field=id_field,
            )
        return {
            "mode": "snapshot",
            "version": state.current_version,
            "data": fetch_data_fn(),
        }

    if since == state.current_version:
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
            id_field=id_field,
        )

    data = fetch_data_fn()
    return {
        "mode": "snapshot",
        "version": state.current_version,
        "data": data,
    }
