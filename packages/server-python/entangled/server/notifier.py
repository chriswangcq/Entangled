"""
entangled/server/notifier.py — Push notification broadcaster with cascade.

When an entity changes, this module:
1. Pushes entity_change to the originating entity's clients
2. Walks the relation graph (cascade) and pushes to all dependent entities
3. All push events are separate — clients don't need to know about relations

Cascade is SERVER-SIDE business logic. Clients just receive push events
and mark their caches stale.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)

# ── Client registry ──────────────────────────────────────────────

# Each connected WS client registers a push callback.
# Key: client_id, Value: (user_id, push_fn)
_clients: Dict[str, tuple[str, Callable]] = {}

# Reference to the EntityStore (set by ws_handler on startup)
_store = None


def set_store(store) -> None:
    """Set the EntityStore reference for cascade resolution."""
    global _store
    _store = store


def register_client(client_id: str, user_id: str, push_fn: Callable) -> None:
    """Register a WS client for receiving push events."""
    _clients[client_id] = (user_id, push_fn)
    logger.debug("[Notifier] Registered client %s (user=%s)", client_id, user_id)


def unregister_client(client_id: str) -> None:
    """Unregister a disconnected WS client."""
    _clients.pop(client_id, None)
    logger.debug("[Notifier] Unregistered client %s", client_id)


# ── Push broadcasting ───────────────────────────────────────────

def notify_entity_change(
    user_id: str,
    entity: str,
    action: str,
    *,
    entity_id: Optional[str] = None,
    params: Optional[Dict[str, str]] = None,
    data: Optional[Dict[str, Any]] = None,
) -> None:
    """Broadcast entity change + cascade to all dependent entities.

    This is the main entry point. It:
    1. Pushes the change for the originating entity
    2. Walks the relation graph and pushes for all dependents

    Clients receive separate push events and don't need relation knowledge.
    """
    # 1. Push for the originating entity
    _push_entity_change(user_id, entity, action, entity_id=entity_id, params=params, data=data)

    # 2. Cascade to dependent entities
    if _store is not None:
        _cascade(user_id, entity, action, params or {}, visited=set())


def _push_entity_change(
    user_id: str,
    entity: str,
    action: str,
    *,
    entity_id: Optional[str] = None,
    params: Optional[Dict[str, str]] = None,
    data: Optional[Dict[str, Any]] = None,
) -> None:
    """Push a single entity_change event to all clients of the user."""
    event = f"entity_change:{entity}"
    payload: Dict[str, Any] = {
        "entity": entity,
        "action": action,
    }
    if entity_id:
        payload["entity_id"] = entity_id
    if params:
        payload["params"] = params
    if data:
        payload["data"] = data

    sent = 0
    for cid, (uid, push_fn) in list(_clients.items()):
        if uid != user_id:
            continue
        try:
            push_fn(event, payload)
            sent += 1
        except Exception as e:
            logger.warning("[Notifier] Push to %s failed: %s", cid, e)

    if sent > 0:
        logger.debug("[Notifier] %s.%s pushed to %d client(s)", entity, action, sent)


def _cascade(
    user_id: str,
    entity: str,
    action: str,
    source_params: Dict[str, str],
    visited: Set[str],
) -> None:
    """Walk the relation graph and push to all dependent entities."""
    try:
        defn = _store.get_def(entity)
    except KeyError:
        return

    for rel in defn.relations:
        # Filter by on_actions
        if rel.on_actions and action not in rel.on_actions:
            continue

        # Avoid cycles
        rel_key = f"{rel.target}:{rel.param_map}"
        if rel_key in visited:
            continue
        visited.add(rel_key)

        # Map params: source.id → target.agent_id
        target_params = {}
        for src_key, tgt_key in rel.param_map.items():
            if src_key in source_params:
                target_params[tgt_key] = source_params[src_key]

        # Push invalidation for the dependent entity
        _push_entity_change(
            user_id,
            rel.target,
            "invalidated",  # special action: "your data may have changed"
            params=target_params,
        )

        # Recurse
        _cascade(user_id, rel.target, action, target_params, visited)


def notify_all(event: str, data: Optional[dict] = None) -> None:
    """Broadcast an event to ALL connected clients."""
    for cid, (uid, push_fn) in list(_clients.items()):
        try:
            push_fn(event, data or {})
        except Exception as e:
            logger.warning("[Notifier] Broadcast to %s failed: %s", cid, e)
