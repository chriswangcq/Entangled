"""WebSocket Sync endpoint — fronts Entangled's sync engine.

Clients connect with a JWT token, subscribe to entities, and receive
real-time delta/snapshot pushes.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Optional

from starlette.websockets import WebSocket, WebSocketDisconnect

from ..server.notifier import (
    register_client,
    set_store,
    unregister_client,
)
from ..server.sync import SyncRegistry
from ..server.ws_handler import (
    SYNC_CONTRACT_VERSION,
    handle_load_more,
    handle_request,
    handle_subscribe,
    handle_unsubscribe,
)

from .auth import decode_jwt_from_raw
from .state import get_store

logger = logging.getLogger(__name__)

_sync_registry: Optional[SyncRegistry] = None
_initialized = False


def init_sync_engine(on_version_bump=None) -> SyncRegistry:
    """Initialize the Entangled sync engine with our EntityStore.

    Must be called once at startup, after init_store().
    """
    global _sync_registry, _initialized
    if _initialized:
        return _sync_registry

    store = get_store()
    _sync_registry = SyncRegistry(on_version_bump=on_version_bump)

    for defn in store.get_all_defs():
        op_log_size = getattr(defn, "op_log_size", 200)
        _sync_registry.set_op_log_size(defn.name, op_log_size)

    set_store(store, sync_registry=_sync_registry)
    _initialized = True
    logger.info("[WS] Sync engine initialized with %d entities", len(store.entities))
    return _sync_registry


def get_sync_registry() -> SyncRegistry:
    if _sync_registry is None:
        raise RuntimeError("Sync engine not initialized — call init_sync_engine() first")
    return _sync_registry


class _WsSender:
    """Adapter: Starlette WebSocket → Entangled WsSender protocol."""

    def __init__(self, ws: WebSocket):
        self._ws = ws

    async def send_json(self, data) -> None:
        await self._ws.send_json(data)


async def ws_sync_handler(websocket: WebSocket):
    """WS /v1/sync — the main Entangled sync endpoint."""

    # 1. Auth — extract user_id from query param or header
    token = websocket.query_params.get("token")
    if not token:
        auth_header = websocket.headers.get("authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]

    user_id: Optional[str] = None
    if token:
        user_id = decode_jwt_from_raw(token)
    if not user_id:
        await websocket.close(code=4001, reason="Authentication required")
        return

    await websocket.accept()

    # 2. Register client
    client_id = f"ws_{uuid.uuid4().hex[:12]}"
    sender = _WsSender(websocket)

    async def _push_fn(data):
        try:
            await sender.send_json(data)
        except Exception:
            pass

    register_client(client_id, user_id, _push_fn)
    logger.info("[WS] Client %s connected (user=%s)", client_id, user_id)

    store = get_store()

    # 3. Push schema on connect
    try:
        await sender.send_json({
            "type": "schema",
            "schema": store.get_schema(),
            "syncContractVersion": SYNC_CONTRACT_VERSION,
        })
    except Exception as e:
        logger.warning("[WS] Failed to push schema to %s: %s", client_id, e)

    # 4. Message loop
    try:
        while True:
            data = await websocket.receive_json()
            msg_type = data.get("type")

            if msg_type == "subscribe":
                await handle_subscribe(
                    store, sender, client_id, user_id,
                    data.get("entity"), data.get("params"),
                    data.get("version"), data.get("head"),
                    data.get("depth"),
                )
            elif msg_type == "unsubscribe":
                await handle_unsubscribe(
                    sender, client_id,
                    data.get("entity"), data.get("params"),
                )
            elif msg_type == "request":
                request_data = data.get("data", {})
                request_id = data.get("requestId")
                await handle_request(
                    store, sender, user_id,
                    request_data, request_id,
                )
            elif msg_type == "load_more":
                await handle_load_more(
                    store, sender, user_id,
                    data.get("entity"), data.get("params"),
                    data.get("before_id"), data.get("limit", 50),
                    data.get("requestId"),
                )
            elif msg_type == "ping":
                await sender.send_json({"type": "pong"})
            else:
                logger.debug("[WS] Unknown message type from %s: %s", client_id, msg_type)

    except WebSocketDisconnect:
        logger.info("[WS] Client %s disconnected", client_id)
    except Exception as e:
        logger.warning("[WS] Error for client %s: %s", client_id, e)
    finally:
        unregister_client(client_id)
