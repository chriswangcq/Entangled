"""
entangled/server/notifier.py — Subscription-based push with cascade.

Only pushes to clients that have active subscriptions (entanglements).
Pushes to ALL subscribed clients regardless of who triggered the change
(multi-user collaboration).

NOTE: Uses module-level state bound via set_store(). This is intentional
for backwards compatibility — a single process serves one store instance.
For multi-process deployments, use a process-local store per worker.
"""


import logging
from typing import Any, Callable, Dict, List, Optional, Set

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
    registry.unsubscribe_all(client_id)
    logger.debug("[Notifier] Client %s unregistered (remaining=%d)", client_id, len(_clients))


def get_sync_registry() -> SyncRegistry:
    """Get the active sync registry (for ws_handler to use)."""
    return _get_registry()


def get_connected_count() -> int:
    """Return number of connected clients (for diagnostics)."""
    return len(_clients)


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


# ── Entity change notification ───────────────────────────────────

def _action_to_op(action: str) -> str:
    return {
        "created": "insert", 
        "stream_append": "insert",
        "updated": "update", 
        "deleted": "delete",
        "clear": "invalidate"
    }.get(action, "update")


# ADR-7: bound cascade fan-out / graph depth (misconfig or cycles).
_MAX_CASCADE_DEPTH = 48


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
    """In-process implementation: record op, push delta, cascade."""
    registry = _get_registry()
    op_type = _action_to_op(action)

    # 1. Record in op_log (with request_id for optimistic correlation)
    state, sync_op = registry.record_op(
        entity, op_type, entity_id or "", params=params, data=data,
        request_id=request_id,
    )

    # 2. Push delta to ALL subscribed clients (not just triggering user)
    subscribed = registry.get_subscribed_clients(entity, params)
    if subscribed:
        id_field = "id"
        if _store is not None:
            try:
                _defn = _store.get_def(entity)
                id_field = getattr(_defn, "id_field", "id")
            except KeyError:
                pass
        delta_frame = {
            "type": "sync",
            "entity": entity,
            "params": params if params else None,
            "idField": id_field,
            "mode": "delta",
            "version": state.current_version,
            "baseVersion": state.current_version - 1,
            "ops": [sync_op.to_dict()],
        }

        sent = 0
        for cid in subscribed:
            if cid not in _clients:
                continue
            _, push_fn = _clients[cid]
            try:
                push_fn("sync", delta_frame)
                sent += 1
            except Exception as e:
                logger.warning("[Notifier] Push to %s failed: %s", cid, e)

        if sent > 0:
            logger.debug(
                "[Notifier] %s.%s v%d → %d client(s)",
                entity, op_type, state.current_version, sent,
            )

    # 3. Cascade to dependent entities
    if _store is not None:
        _cascade(entity, action, params or {}, entity_id, visited=set(), depth=0)


def _cascade(
    entity: str,
    action: str,
    source_params: Dict[str, str],
    entity_id: Optional[str],
    visited: Set[str],
    *,
    depth: int = 0,
) -> None:
    """Walk relation graph — push invalidation to dependent entities.

    Pushes to ALL subscribers of each dependent entity.
    """
    if depth >= _MAX_CASCADE_DEPTH:
        logger.warning(
            "[Notifier] cascade stopped at depth cap (%d) entity=%s action=%s",
            _MAX_CASCADE_DEPTH,
            entity,
            action,
        )
        return
    try:
        defn = _store.get_def(entity)
    except KeyError:
        return

    registry = _get_registry()

    for rel in defn.relations:
        if rel.on_actions and action not in rel.on_actions:
            continue

        rel_key = f"{rel.target}:{sorted(rel.param_map.items())}"
        if rel_key in visited:
            continue
        visited.add(rel_key)

        # Map params: use entity_id as source if needed
        target_params: Dict[str, str] = {}
        all_source = dict(source_params)
        if entity_id:
            all_source["id"] = entity_id

        has_all_keys = True
        for src_key, tgt_key in rel.param_map.items():
            if src_key in all_source:
                target_params[tgt_key] = all_source[src_key]
            else:
                has_all_keys = False

        if not has_all_keys and not target_params:
            # Can't map params — skip this relation (would push to wrong scope)
            logger.debug(
                "[Notifier] Skipping cascade %s→%s: incomplete param_map",
                entity, rel.target,
            )
            continue

        # Record invalidation
        state, sync_op = registry.record_op(
            rel.target, "invalidate", "", params=target_params if target_params else None,
        )

        # Push to subscribers
        subscribed = registry.get_subscribed_clients(rel.target, target_params if target_params else None)
        if subscribed:
            tgt_id_field = "id"
            try:
                tgt_defn = _store.get_def(rel.target)
                tgt_id_field = getattr(tgt_defn, "id_field", "id")
            except KeyError:
                pass
            frame = {
                "type": "sync",
                "entity": rel.target,
                "params": target_params if target_params else None,
                "idField": tgt_id_field,
                "mode": "delta",
                "version": state.current_version,
                "baseVersion": state.current_version - 1,
                "ops": [sync_op.to_dict()],
            }

            for cid in subscribed:
                if cid not in _clients:
                    continue
                _, push_fn = _clients[cid]
                try:
                    push_fn("sync", frame)
                except Exception as e:
                    logger.warning("[Notifier] Cascade push to %s failed: %s", cid, e)

        # Recurse
        _cascade(rel.target, action, target_params, None, visited, depth=depth + 1)


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
    """Record mutation + push delta to ALL subscribed clients + cascade.

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
