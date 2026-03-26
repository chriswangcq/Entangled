"""
entangled/server/notifier.py — Subscription-based push with cascade.

Only pushes to clients that have active subscriptions (entanglements).
Pushes to ALL subscribed clients regardless of who triggered the change
(multi-user collaboration).
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Set

from .sync import SyncOp

logger = logging.getLogger(__name__)

# ── Client registry ──────────────────────────────────────────────

_clients: Dict[str, tuple[str, Callable]] = {}  # client_id → (user_id, push_fn)
_store = None
_sync_registry = None


def set_store(store) -> None:
    global _store, _sync_registry
    _store = store
    # Bind sync_registry from store if available, else use default
    if hasattr(store, '_sync_registry'):
        _sync_registry = store._sync_registry
    else:
        from .sync import SyncRegistry
        _sync_registry = SyncRegistry()
    # Configure op_log sizes from EntityDefs
    for defn in store.get_all_defs():
        _sync_registry.set_op_log_size(defn.name, defn.op_log_size)


def _get_registry():
    if _sync_registry is not None:
        return _sync_registry
    from .sync import SyncRegistry
    return SyncRegistry()


def register_client(client_id: str, user_id: str, push_fn: Callable) -> None:
    _clients[client_id] = (user_id, push_fn)
    logger.debug("[Notifier] Client %s registered (user=%s)", client_id, user_id)


def unregister_client(client_id: str) -> None:
    _clients.pop(client_id, None)
    registry = _get_registry()
    registry.unsubscribe_all(client_id)
    logger.debug("[Notifier] Client %s unregistered", client_id)


def get_sync_registry():
    """Get the active sync registry (for ws_handler to use)."""
    return _get_registry()


# ── Entity change notification ───────────────────────────────────

def _action_to_op(action: str) -> str:
    return {"created": "insert", "updated": "update", "deleted": "delete"}.get(action, "update")


def notify_entity_change(
    user_id: str,
    entity: str,
    action: str,
    *,
    entity_id: Optional[str] = None,
    params: Optional[Dict[str, str]] = None,
    data: Optional[Dict[str, Any]] = None,
) -> None:
    """Record mutation + push delta to ALL subscribed clients + cascade.

    NOTE: Pushes to all subscribers, not just the triggering user.
    This enables multi-user collaboration — when user A changes data,
    user B sees it automatically if subscribed.
    """
    registry = _get_registry()
    op_type = _action_to_op(action)

    # 1. Record in op_log
    state, sync_op = registry.record_op(
        entity, op_type, entity_id or "", params=params, data=data,
    )

    # 2. Push delta to ALL subscribed clients (not just triggering user)
    subscribed = registry.get_subscribed_clients(entity, params)
    if subscribed:
        delta_frame = {
            "type": "sync",
            "entity": entity,
            "params": params if params else None,
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
        _cascade(entity, action, params or {}, entity_id, visited=set())


def _cascade(
    entity: str,
    action: str,
    source_params: Dict[str, str],
    entity_id: Optional[str],
    visited: Set[str],
) -> None:
    """Walk relation graph — push invalidation to dependent entities.

    Pushes to ALL subscribers of each dependent entity.
    """
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
            frame = {
                "type": "sync",
                "entity": rel.target,
                "params": target_params if target_params else None,
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
        _cascade(rel.target, action, target_params, None, visited)


def notify_all(event: str, data: Optional[dict] = None) -> None:
    """Broadcast to ALL clients."""
    for cid, (uid, push_fn) in list(_clients.items()):
        try:
            push_fn(event, data or {})
        except Exception as e:
            logger.warning("[Notifier] Broadcast to %s failed: %s", cid, e)
