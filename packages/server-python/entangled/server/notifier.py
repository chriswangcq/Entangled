"""
entangled/server/notifier.py — Subscription-based push with cascade.

Only pushes to clients that have active subscriptions (entanglements).
Records mutations to the op-log for delta sync on reconnect.
Handles cascade invalidation by walking the entity relation graph.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Set

from .sync import sync_registry, SyncOp

logger = logging.getLogger(__name__)

# ── Client registry ──────────────────────────────────────────────

_clients: Dict[str, tuple[str, Callable]] = {}  # client_id → (user_id, push_fn)
_store = None  # EntityStore reference for cascade


def set_store(store) -> None:
    global _store
    _store = store


def register_client(client_id: str, user_id: str, push_fn: Callable) -> None:
    _clients[client_id] = (user_id, push_fn)
    logger.debug("[Notifier] Client %s registered (user=%s)", client_id, user_id)


def unregister_client(client_id: str) -> None:
    _clients.pop(client_id, None)
    sync_registry.unsubscribe_all(client_id)
    logger.debug("[Notifier] Client %s unregistered + unsubscribed all", client_id)


# ── Entity change notification ───────────────────────────────────

def _action_to_op(action: str) -> str:
    """Map entity action to sync op type."""
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
    """Record mutation + push delta to subscribed clients + cascade.

    This is the main entry point. It:
    1. Records the op in the sync state (op_log + version bump)
    2. Pushes a delta sync frame to all subscribed clients
    3. Cascade: walks relations and pushes invalidation to dependent entities
    """
    op_type = _action_to_op(action)

    # 1. Record in op_log
    state, sync_op = sync_registry.record_op(
        entity, op_type, entity_id or "", params=params, data=data,
    )

    # 2. Push delta to subscribed clients only
    subscribed = sync_registry.get_subscribed_clients(entity, params)
    if subscribed:
        delta_frame = {
            "type": "sync",
            "entity": entity,
            "params": params,
            "mode": "delta",
            "version": state.current_version,
            "base_version": state.current_version - 1,
            "ops": [sync_op.to_dict()],
        }

        sent = 0
        for cid in subscribed:
            if cid not in _clients:
                continue
            uid, push_fn = _clients[cid]
            if uid != user_id:
                continue
            try:
                push_fn("sync", delta_frame)
                sent += 1
            except Exception as e:
                logger.warning("[Notifier] Push to %s failed: %s", cid, e)

        if sent > 0:
            logger.debug(
                "[Notifier] %s.%s v%d delta → %d client(s)",
                entity, op_type, state.current_version, sent,
            )

    # 3. Cascade to dependent entities
    if _store is not None:
        _cascade(user_id, entity, action, params or {}, visited=set())


def _cascade(
    user_id: str,
    entity: str,
    action: str,
    source_params: Dict[str, str],
    visited: Set[str],
) -> None:
    """Walk relation graph — push invalidation to dependent entities' subscribers."""
    try:
        defn = _store.get_def(entity)
    except KeyError:
        return

    for rel in defn.relations:
        if rel.on_actions and action not in rel.on_actions:
            continue

        rel_key = f"{rel.target}:{rel.param_map}"
        if rel_key in visited:
            continue
        visited.add(rel_key)

        # Map params
        target_params: Dict[str, str] = {}
        for src_key, tgt_key in rel.param_map.items():
            if src_key in source_params:
                target_params[tgt_key] = source_params[src_key]

        # Record invalidation op for the dependent entity
        state, sync_op = sync_registry.record_op(
            rel.target, "invalidate", "", params=target_params,
        )

        # Push to subscribers of the dependent entity
        subscribed = sync_registry.get_subscribed_clients(rel.target, target_params)
        if subscribed:
            frame = {
                "type": "sync",
                "entity": rel.target,
                "params": target_params,
                "mode": "delta",
                "version": state.current_version,
                "base_version": state.current_version - 1,
                "ops": [sync_op.to_dict()],
            }

            for cid in subscribed:
                if cid not in _clients:
                    continue
                uid, push_fn = _clients[cid]
                if uid != user_id:
                    continue
                try:
                    push_fn("sync", frame)
                except Exception as e:
                    logger.warning("[Notifier] Cascade push to %s failed: %s", cid, e)

        # Recurse
        _cascade(user_id, rel.target, action, target_params, visited)


def notify_all(event: str, data: Optional[dict] = None) -> None:
    """Broadcast to ALL clients (schema refresh etc)."""
    for cid, (uid, push_fn) in list(_clients.items()):
        try:
            push_fn(event, data or {})
        except Exception as e:
            logger.warning("[Notifier] Broadcast to %s failed: %s", cid, e)
