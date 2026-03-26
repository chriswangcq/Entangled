"""
entangled/server/ws_handler.py — WebSocket handler for Entangled protocol.

Handles the WS connection lifecycle:
  1. Connect: register client, push schema to client
  2. Message loop: dispatch entity CRUD/action requests
  3. Disconnect: unregister client

This is a Starlette-compatible WebSocket handler that can be mounted on
FastAPI, Starlette, or any ASGI framework.
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

logger = logging.getLogger(__name__)


def create_ws_handler(
    store: EntityStore,
    *,
    auth_fn: Optional[Callable[[WebSocket], Optional[str]]] = None,
):
    """Create a Starlette WebSocket handler for the Entangled protocol.

    Args:
        store: The EntityStore containing all entity definitions.
        auth_fn: Optional auth function that extracts user_id from the WS request.
                 If None, user_id defaults to "anonymous".

    Returns:
        An async WebSocket handler function.

    Usage:
        app.add_websocket_route("/ws", create_ws_handler(store, auth_fn=my_auth))
    """

    async def ws_handler(websocket: WebSocket):
        set_store(store)  # Enable cascade in notifier
        await websocket.accept()

        # ── Authentication ───────────────────────────────────────
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
        async def push_fn(event: str, data: Any):
            try:
                await websocket.send_json({
                    "type": "push",
                    "event": event,
                    "data": data,
                })
            except Exception:
                pass  # Client may have disconnected

        # Use sync wrapper for the notifier (which stores sync callables)
        import asyncio
        loop = asyncio.get_event_loop()

        def sync_push(event: str, data: Any):
            asyncio.ensure_future(push_fn(event, data))

        register_client(client_id, user_id, sync_push)

        # ── Push schema on connect ───────────────────────────────
        try:
            await websocket.send_json({
                "type": "push",
                "event": "schema",
                "data": {"entities": store.get_schema()},
            })
        except Exception as e:
            logger.error("[WS] Failed to push schema: %s", e)

        logger.info("[WS] Client %s connected (user=%s)", client_id[:8], user_id)

        # ── Message loop ─────────────────────────────────────────
        try:
            while True:
                raw = await websocket.receive_text()
                msg = json.loads(raw)

                msg_type = msg.get("type")
                if msg_type == "request":
                    await _handle_request(websocket, store, user_id, msg)
                elif msg_type == "ping":
                    await websocket.send_json({"type": "pong"})
                else:
                    logger.debug("[WS] Unknown message type: %s", msg_type)

        except WebSocketDisconnect:
            logger.info("[WS] Client %s disconnected", client_id[:8])
        except Exception as e:
            logger.error("[WS] Client %s error: %s", client_id[:8], e)
        finally:
            unregister_client(client_id)


    return ws_handler


async def _handle_request(
    ws: WebSocket,
    store: EntityStore,
    user_id: str,
    msg: dict,
) -> None:
    """Handle a single WS request frame."""
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
    """Dispatch an entity CRUD/action request."""
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
        # Validate entity exists
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
            if not ok:
                return {"success": False, "error": f"{entity} {entity_id} not found"}
            return {"success": True}

        elif op == "action":
            action_name = data.get("action_name", "")
            if not action_name:
                return {"success": False, "error": "action_name is required"}
            result = await store.action(entity, user_id, action_name, params, payload)
            return {"success": True, "data": result}

        else:
            return {"success": False, "error": f"Unknown op: {op}"}

    except KeyError as e:
        return {"success": False, "error": str(e)}
    except PermissionError as e:
        return {"success": False, "error": str(e)}
    except NotImplementedError as e:
        return {"success": False, "error": str(e)}
    except Exception as e:
        logger.error("[WS] Dispatch error: %s.%s %s", entity, op, e)
        return {"success": False, "error": str(e)}
