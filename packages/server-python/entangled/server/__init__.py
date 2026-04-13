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

from .defs import (
    EntityDef,
    ListFn,
    ListStreamFn,
    GetFn,
    CreateFn,
    UpdateFn,
    DeleteFn,
    UpsertFn,
    ExistsBeforeFn,
    ActionFn,
)
from .store import EntityStore, EntityStoreProtocol
from .sync import (
    DEFAULT_STREAM_HEAD_DEPTH,
    MAX_STREAM_HEAD_DEPTH,
    SyncRegistry,
    resolve_sync,
)
from .ws_handler import (
    create_ws_handler,
    WsSender,
    handle_subscribe,
    handle_unsubscribe,
    handle_load_more,
    handle_request,
)
from .notifier import (
    notify_entity_change,
    reset_state,
    get_connected_count,
    InProcSyncPushPort,
)
from .push_port import SyncPushPort, get_sync_push_port, set_sync_push_port

__all__ = [
    # Types
    "ListFn",
    "ListStreamFn",
    "GetFn",
    "CreateFn",
    "UpdateFn",
    "DeleteFn",
    "UpsertFn",
    "ExistsBeforeFn",
    "ActionFn",
    # Core
    "DEFAULT_STREAM_HEAD_DEPTH",
    "MAX_STREAM_HEAD_DEPTH",
    "EntityDef",
    "EntityStore",
    "EntityStoreProtocol",
    "SyncRegistry",
    "resolve_sync",
    # WS handler
    "create_ws_handler",
    "WsSender",
    "handle_subscribe",
    "handle_unsubscribe",
    "handle_load_more",
    "handle_request",
    # Notifier / push port (C.1)
    "notify_entity_change",
    "reset_state",
    "get_connected_count",
    "InProcSyncPushPort",
    "SyncPushPort",
    "get_sync_push_port",
    "set_sync_push_port",
]

