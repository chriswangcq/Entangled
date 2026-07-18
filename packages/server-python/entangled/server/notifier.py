"""
entangled/server/notifier.py — Entanglement-based push notification.

One write = one notification.  Writes with key params push to both the exact
(entity, params) entanglement and the unscoped user-level entanglement for that
entity.  This keeps a user-scope full subscription complete while preserving
scoped subscriptions for older clients and focused readers.

User-scoped entities only push to the owning user's clients; global entities
push to all entangled peers.

NOTE: Uses module-level state bound via set_store(). This is intentional
for backwards compatibility — a single process serves one store instance.
For multi-process deployments, use a process-local store per worker.
"""


import logging
from typing import Any, Callable, Dict, List, Optional

from .sync import SyncOp, SyncRegistry

logger = logging.getLogger(__name__)

# ── Runtime state (bound per-process via set_store) ──────────────
#
# These are module-level for simplicity. In production, each worker process
# gets its own copy. For testing, call reset_state() between tests.

_clients: Dict[str, tuple[str, Callable]] = {}  # client_id → (user_id, push_fn)
_store = None
_sync_registry: Optional[SyncRegistry] = None


def set_store(store, *, sync_registry: Optional[SyncRegistry] = None) -> None:
    """Bind the entity store and sync registry.

    Must be called once at startup before any WS connections.

    Args:
        store: The entity store (with get_all_defs()).
        sync_registry: Explicitly provided SyncRegistry. If None, a fresh
            one is created. Hosts should pass the registry they configured
            (e.g. with persistence callbacks).
    """
    global _store, _sync_registry
    _store = store
    _sync_registry = sync_registry if sync_registry is not None else SyncRegistry()
    # Configure op_log sizes from EntityDefs
    for defn in store.get_all_defs():
        _sync_registry.set_op_log_size(defn.name, defn.op_log_size)


def _get_registry() -> SyncRegistry:
    """Get the active sync registry (never creates a new one silently)."""
    if _sync_registry is not None:
        return _sync_registry
    # This should only happen if set_store() was never called — log loudly
    logger.error("[Notifier] SyncRegistry not initialized! Call set_store() first.")
    sr = SyncRegistry()
    return sr


def register_client(client_id: str, user_id: str, push_fn: Callable) -> None:
    _clients[client_id] = (user_id, push_fn)
    logger.debug("[Notifier] Client %s registered (user=%s, total=%d)", client_id, user_id, len(_clients))


def unregister_client(client_id: str) -> None:
    _clients.pop(client_id, None)
    registry = _get_registry()
    registry.disentangle_all(client_id)
    logger.debug("[Notifier] Client %s unregistered (remaining=%d)", client_id, len(_clients))


def get_sync_registry() -> SyncRegistry:
    """Get the active sync registry (for ws_handler to use)."""
    return _get_registry()


def get_connected_count() -> int:
    """Return number of connected clients (for diagnostics)."""
    return len(_clients)


def get_user_client_count(user_id: str) -> int:
    """Return process-local notifier clients for exactly one account."""

    return sum(1 for owner, _push in _clients.values() if owner == user_id)


def unregister_user_clients(user_id: str) -> int:
    """Drop one account's subscriptions immediately; WS cleanup is idempotent."""

    client_ids = [
        client_id
        for client_id, (owner, _push) in list(_clients.items())
        if owner == user_id
    ]
    for client_id in client_ids:
        unregister_client(client_id)
    return len(client_ids)


def reset_state() -> None:
    """Clear all runtime state (for testing)."""
    global _store, _sync_registry
    from .push_port import set_sync_push_port

    _clients.clear()
    _store = None
    if _sync_registry:
        _sync_registry.reset()
    _sync_registry = None
    set_sync_push_port(None)


# ── User-scope resolution ────────────────────────────────────────

def is_user_owned(entity: str, *, store=None) -> bool:
    """Return True if this entity is user-scoped directly or via parent chain.

    User-owned entities must only push deltas to the owning user's clients.
    Global entities (e.g. models) push to all subscribers.
    """
    owner_store = store if store is not None else _store
    return _resolve_user_owned(entity, owner_store, depth=0, seen=set())


# Private alias retained for hosts that imported the pre-existing helper.
_is_user_owned = is_user_owned


def _resolve_user_owned(entity: str, store, depth: int, seen: set[str]) -> bool:
    if store is None:
        return False
    if depth > 32 or entity in seen:
        raise ValueError(f"Invalid parent ownership chain at '{entity}'")
    seen = set(seen)
    seen.add(entity)
    try:
        defn = store.get_def(entity)
    except KeyError as exc:
        raise ValueError(f"Entity '{entity}' is not registered") from exc
    if getattr(defn, "user_scoped", False):
        return True
    if getattr(defn, "parent", None):
        parent_name = defn.parent[0]
        return _resolve_user_owned(parent_name, store, depth + 1, seen)
    return False


# ── Entity change notification (no cascade) ──────────────────────

def _action_to_op(action: str) -> str:
    return {
        "created": "insert", 
        "stream_append": "insert",
        "updated": "update", 
        "deleted": "delete",
        "clear": "invalidate"
    }.get(action, "update")


def _id_field_for(entity: str) -> str:
    if _store is None:
        return "id"
    try:
        _defn = _store.get_def(entity)
        return getattr(_defn, "id_field", "id")
    except KeyError:
        return "id"


def _push_delta_to_entangled_clients(
    *,
    registry: SyncRegistry,
    user_id: str,
    entity: str,
    params: Optional[Dict[str, str]],
    state,
    sync_op: SyncOp,
) -> int:
    user_owned = is_user_owned(entity)
    sync_user_id = user_id if user_owned else None
    entangled = registry.get_entangled_clients(
        entity,
        params,
        user_id=sync_user_id,
    )
    if not entangled:
        return 0

    delta_frame = {
        "type": "sync",
        "entity": entity,
        "params": params if params else None,
        "idField": _id_field_for(entity),
        "mode": "delta",
        "version": state.current_version,
        "baseVersion": state.current_version - 1,
        "ops": [sync_op.to_dict()],
    }

    sent = 0
    for cid in entangled:
        if cid not in _clients:
            continue
        client_uid, push_fn = _clients[cid]
        if user_owned and client_uid != user_id:
            continue
        try:
            push_fn("sync", delta_frame)
            sent += 1
        except Exception as e:
            logger.warning("[Notifier] Push to %s failed: %s", cid, e)

    if sent > 0:
        logger.debug(
            "[Notifier] %s.%s v%d params=%s → %d client(s)",
            entity,
            sync_op.op,
            state.current_version,
            params,
            sent,
        )
    return sent


def _inproc_notify_entity_change(
    user_id: str,
    entity: str,
    action: str,
    *,
    entity_id: Optional[str] = None,
    params: Optional[Dict[str, str]] = None,
    data: Optional[Dict[str, Any]] = None,
    request_id: Optional[str] = None,
) -> None:
    """In-process implementation: record op, push delta to entangled peers."""
    registry = _get_registry()
    op_type = _action_to_op(action)
    sync_user_id = user_id if is_user_owned(entity) else None

    # 1. Record in scoped op_log (with request_id for optimistic correlation).
    state, sync_op = registry.record_op(
        entity, op_type, entity_id or "", params=params, data=data,
        request_id=request_id,
        user_id=sync_user_id,
    )

    # 2. Push exact scoped delta for focused subscribers.
    _push_delta_to_entangled_clients(
        registry=registry,
        user_id=user_id,
        entity=entity,
        params=params,
        state=state,
        sync_op=sync_op,
    )

    # 3. Also bump/push the unscoped user-domain state. This is the canonical
    #    "subscribe all rows for the current user" path used by the desktop app.
    if params:
        global_state, global_sync_op = registry.record_op(
            entity, op_type, entity_id or "", params=None, data=data,
            request_id=request_id,
            user_id=sync_user_id,
        )
        _push_delta_to_entangled_clients(
            registry=registry,
            user_id=user_id,
            entity=entity,
            params=None,
            state=global_state,
            sync_op=global_sync_op,
        )


class InProcSyncPushPort:
    """Default C.1 implementation: process-local registry + client push_fn map."""

    def notify_entity_change(
        self,
        user_id: str,
        entity: str,
        action: str,
        *,
        entity_id: Optional[str] = None,
        params: Optional[Dict[str, str]] = None,
        data: Optional[Dict[str, Any]] = None,
        request_id: Optional[str] = None,
    ) -> None:
        _inproc_notify_entity_change(
            user_id,
            entity,
            action,
            entity_id=entity_id,
            params=params,
            data=data,
            request_id=request_id,
        )


_default_inproc_push_port = InProcSyncPushPort()


def notify_entity_change(
    user_id: str,
    entity: str,
    action: str,
    *,
    entity_id: Optional[str] = None,
    params: Optional[Dict[str, str]] = None,
    data: Optional[Dict[str, Any]] = None,
    request_id: Optional[str] = None,
) -> None:
    """Record mutation + push delta to all entangled clients.

    Delegates to SyncPushPort (see push_port.set_sync_push_port).
    """
    from .push_port import get_sync_push_port

    get_sync_push_port().notify_entity_change(
        user_id,
        entity,
        action,
        entity_id=entity_id,
        params=params,
        data=data,
        request_id=request_id,
    )


def notify_all(event: str, data: Optional[dict] = None) -> None:
    """Broadcast to ALL clients."""
    for cid, (uid, push_fn) in list(_clients.items()):
        try:
            push_fn(event, data or {})
        except Exception as e:
            logger.warning("[Notifier] Broadcast to %s failed: %s", cid, e)
