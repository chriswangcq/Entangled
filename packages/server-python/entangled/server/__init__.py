"""
Entangled Server — Real-time Entity Sync Engine.

Usage:
    from entangled.server import EntityDef, EntityStore, create_ws_handler

    todos = EntityDef(
        name="todos",
        key_params=["project_id"],
        list_fn=lambda store, uid, params: db.query(...),
    )

    store = EntityStore([todos])
    app.add_websocket_route("/ws", create_ws_handler(store))
"""

from .defs import EntityDef, EntityRelation
from .store import EntityStore
from .sync import SyncRegistry
from .ws_handler import create_ws_handler
from .notifier import notify_entity_change

__all__ = [
    "EntityDef",
    "EntityRelation",
    "EntityStore",
    "SyncRegistry",
    "create_ws_handler",
    "notify_entity_change",
]

