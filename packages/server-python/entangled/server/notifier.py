"""
entangled/server/notifier.py — Push notification broadcaster.

When an entity changes, all connected WS clients for that user receive
a push event. The Rust client engine processes these internally
(cache update + cascade invalidation) without JS involvement.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)

# ── Client registry ──────────────────────────────────────────────

# Each connected WS client registers a push callback.
# Key: client_id, Value: (user_id, push_fn)
_clients: Dict[str, tuple[str, Callable]] = {}


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
    """Broadcast an entity change to all clients of the given user.

    Args:
        user_id:   Target user
        entity:    Entity name, e.g. "todos"
        action:    "created" | "updated" | "deleted"
        entity_id: Optional specific entity ID
        params:    Optional key params
        data:      Optional inline data (avoids client re-fetch)
    """
    event = f"entity_change:{entity}"
    payload = {
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
        logger.debug(
            "[Notifier] %s.%s pushed to %d client(s)", entity, action, sent
        )


def notify_all(event: str, data: Optional[dict] = None) -> None:
    """Broadcast an event to ALL connected clients (e.g. schema refresh)."""
    for cid, (uid, push_fn) in list(_clients.items()):
        try:
            push_fn(event, data or {})
        except Exception as e:
            logger.warning("[Notifier] Broadcast to %s failed: %s", cid, e)
