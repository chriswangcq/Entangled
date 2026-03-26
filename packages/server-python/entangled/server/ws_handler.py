"""
entangled/server/ws_handler.py — WebSocket handler for Entangled protocol.

Handles:
  1. Connect: register client, push schema
  2. Subscribe/Unsubscribe: establish/break entity entanglement
  3. Request: dispatch entity CRUD/action
  4. Disconnect: cleanup subscriptions
"""

from __future__ import annotations

import inspect
import json
import logging
import uuid
from typing import Any, Callable, Optional

from starlette.websockets import WebSocket, WebSocketDisconnect

from .notifier import register_client, unregister_client, set_store
from .store import EntityStore
from .sync import sync_registry, resolve_sync

logger = logging.getLogger(__name__)


def create_ws_handler(
    store: EntityStore,
    *,
    auth_fn: Optional[Callable[[WebSocket], Optional[str]]] = None,
):
    """Create a Starlette WebSocket handler for the Entangled protocol."""

    async def ws_handler(websocket: WebSocket):
        set_store(store)
        await websocket.accept()

        # ── Auth ─────────────────────────────────────────────────
        user_id = "anonymous"
        if auth_fn:
            try:
                uid = auth_fn(websocket)
                if inspect.isawaitable(uid):
                    uid = await uid
                if uid:
                    user_id = uid
            except Exception as e:
                logger.warning("[WS] Auth failed: %s", e)
                await websocket.close(code=4001, reason="Authentication failed")
                return

        client_id = str(uuid.uuid4())

        # ── Push callback ────────────────────────────────────────
        import asyncio

        async def push_fn(event: str, data: Any):
            try:
                if isinstance(data, dict) and data.get("type") == "sync":
                    await websocket.send_json(data)
                else:
                    await websocket.send_json({
                        "type": "push",
                        "event": event,
                        "data": data,
                    })
            except Exception:
                pass

        def sync_push(event: str, data: Any):
            asyncio.ensure_future(push_fn(event, data))

        register_client(client_id, user_id, sync_push)

        # ── Push schema ──────────────────────────────────────────
        try:
            await websocket.send_json({
                "type": "push",
                "event": "schema",
                "data": {"entities": store.get_schema()},
            })
        except Exception as e:
            logger.error("[WS] Schema push failed: %s", e)

        logger.info("[WS] Client %s connected (user=%s)", client_id[:8], user_id)

        # ── Message loop ─────────────────────────────────────────
        try:
            while True:
                raw = await websocket.receive_text()
                msg = json.loads(raw)
                msg_type = msg.get("type")

                if msg_type == "request":
                    await _handle_request(websocket, store, user_id, msg)

                elif msg_type == "subscribe":
                    await _handle_subscribe(
                        websocket, store, user_id, client_id, msg,
                    )

                elif msg_type == "unsubscribe":
                    _handle_unsubscribe(client_id, msg)

                elif msg_type == "ping":
                    await websocket.send_json({"type": "pong"})

                else:
                    logger.debug("[WS] Unknown type: %s", msg_type)

        except WebSocketDisconnect:
            logger.info("[WS] Client %s disconnected", client_id[:8])
        except Exception as e:
            logger.error("[WS] Client %s error: %s", client_id[:8], e)
        finally:
            unregister_client(client_id)

    return ws_handler


# ── Subscribe handler ────────────────────────────────────────────

async def _handle_subscribe(
    ws: WebSocket,
    store: EntityStore,
    user_id: str,
    client_id: str,
    msg: dict,
) -> None:
    """Handle subscribe: register + send initial sync."""
    entity = msg.get("entity", "")
    params = msg.get("params") or {}
    client_version = msg.get("version")
    client_head = msg.get("head")
    depth = msg.get("depth")

    if not entity:
        await ws.send_json({"type": "error", "error": "entity is required"})
        return

    try:
        defn = store.get_def(entity)
    except KeyError:
        await ws.send_json({"type": "error", "error": f"Unknown entity: {entity}"})
        return

    # Register subscription
    sync_registry.subscribe(client_id, entity, params or None)

    # Get sync state
    state = sync_registry.get_state(entity, params or None)

    # Determine sync type
    sync_type = getattr(defn, 'sync_type', 'list')

    # Fetch function for snapshot/head_n
    def fetch_data():
        return store.list(entity, user_id, params=params)

    # Resolve sync strategy (git smart protocol)
    sync_result = resolve_sync(
        state,
        client_version=client_version,
        client_head=client_head,
        depth=depth or getattr(defn, 'sync_limit', None),
        fetch_data_fn=fetch_data,
        sync_type=sync_type,
    )

    # Build and send sync frame
    frame = {
        "type": "sync",
        "entity": entity,
        "params": params if params else None,
        **sync_result,
    }

    await ws.send_json(frame)

    logger.info(
        "[WS] %s subscribed to %s (mode=%s, v%d)",
        client_id[:8], entity, sync_result["mode"], sync_result["version"],
    )


# ── Unsubscribe handler ─────────────────────────────────────────

def _handle_unsubscribe(client_id: str, msg: dict) -> None:
    entity = msg.get("entity", "")
    params = msg.get("params")
    if entity:
        sync_registry.unsubscribe(client_id, entity, params)
        logger.debug("[WS] %s unsubscribed from %s", client_id[:8], entity)


# ── Request handler (unchanged) ──────────────────────────────────

async def _handle_request(ws: WebSocket, store: EntityStore, user_id: str, msg: dict) -> None:
    request_id = msg.get("request_id", "")
    data = msg.get("data", {})

    try:
        result = await _dispatch(store, user_id, data)
        await ws.send_json({
            "type": "response",
            "request_id": request_id,
            "data": result,
        })
    except Exception as e:
        logger.error("[WS] Request %s failed: %s", request_id, e)
        await ws.send_json({
            "type": "response",
            "request_id": request_id,
            "data": {"success": False, "error": str(e)},
        })


async def _dispatch(store: EntityStore, user_id: str, data: dict) -> dict:
    op = data.get("op", "")
    entity = data.get("entity", "")
    entity_id = data.get("id")
    params = data.get("params") or {}
    payload = data.get("data") or {}

    if not entity:
        return {"success": False, "error": "entity is required"}
    if not op:
        return {"success": False, "error": "op is required"}

    try:
        store.get_def(entity)

        if op == "list":
            entries = store.list(entity, user_id, params=params)
            return {"success": True, "entries": entries}
        elif op == "get":
            if not entity_id:
                return {"success": False, "error": "id is required for get"}
            item = store.get(entity, user_id, entity_id, params=params)
            if item is None:
                return {"success": False, "error": f"{entity} {entity_id} not found"}
            return {"success": True, "data": item}
        elif op == "create":
            result = store.create(entity, user_id, payload, params=params)
            return {"success": True, "data": result}
        elif op in ("update", "upsert"):
            if not entity_id:
                return {"success": False, "error": "id is required for update"}
            result = store.update(entity, user_id, entity_id, payload, params=params)
            return {"success": True, "data": result}
        elif op == "delete":
            if not entity_id:
                return {"success": False, "error": "id is required for delete"}
            ok = store.delete(entity, user_id, entity_id, params=params)
            return {"success": True} if ok else {"success": False, "error": "not found"}
        elif op == "action":
            action_name = data.get("action_name", "")
            if not action_name:
                return {"success": False, "error": "action_name is required"}
            result = await store.action(entity, user_id, action_name, params, payload)
            return {"success": True, "data": result}
        else:
            return {"success": False, "error": f"Unknown op: {op}"}

    except (KeyError, PermissionError, NotImplementedError) as e:
        return {"success": False, "error": str(e)}
    except Exception as e:
        logger.error("[WS] Dispatch error: %s.%s %s", entity, op, e)
        return {"success": False, "error": str(e)}
