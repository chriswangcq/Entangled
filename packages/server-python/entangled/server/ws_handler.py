"""
entangled/server/ws_handler.py — WebSocket handler for Entangled protocol.

Handles:
  1. Connect: register client, push schema
  2. Subscribe/Unsubscribe: establish/break entity entanglement
  3. Request: dispatch entity CRUD/action
  4. Disconnect: cleanup subscriptions
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import traceback
import uuid
from typing import Any, Callable, Optional

from starlette.websockets import WebSocket, WebSocketDisconnect

from .notifier import register_client, unregister_client, set_store, get_sync_registry
from .store import EntityStore
from .sync import resolve_sync

logger = logging.getLogger(__name__)


def create_ws_handler(
    store: EntityStore,
    *,
    auth_fn: Optional[Callable[[WebSocket], Optional[str]]] = None,
):
    """Create a Starlette WebSocket handler for the Entangled protocol."""

    # One-time setup — not per-connection
    set_store(store)

    async def ws_handler(websocket: WebSocket):
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

        # ── Push callback (queue-based, no lost exceptions) ──────
        push_queue: asyncio.Queue = asyncio.Queue()

        async def push_consumer():
            """Drain push queue and send to WS. Exits when client disconnects."""
            try:
                while True:
                    msg = await push_queue.get()
                    if msg is None:
                        break
                    try:
                        await websocket.send_json(msg)
                    except Exception:
                        break
            except asyncio.CancelledError:
                pass

        def sync_push(event: str, data: Any):
            """Sync callback for notifier — enqueue, never blocks."""
            if isinstance(data, dict) and data.get("type") == "sync":
                push_queue.put_nowait(data)
            else:
                push_queue.put_nowait({
                    "type": "push",
                    "event": event,
                    "data": data,
                })

        # Start push consumer task
        consumer_task = asyncio.ensure_future(push_consumer())

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
                    await _handle_subscribe(websocket, store, user_id, client_id, msg)
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
            # Stop push consumer
            push_queue.put_nowait(None)
            consumer_task.cancel()

    return ws_handler


# ── Subscribe ────────────────────────────────────────────────────

async def _handle_subscribe(
    ws: WebSocket,
    store: EntityStore,
    user_id: str,
    client_id: str,
    msg: dict,
) -> None:
    entity = msg.get("entity", "")
    params = msg.get("params") or None  # Normalize
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

    registry = get_sync_registry()

    # Register subscription
    registry.subscribe(client_id, entity, params)

    # Get sync state
    state = registry.get_state(entity, params)

    # Fetch function for snapshot/head_n
    def fetch_data():
        return store.list(entity, user_id, params=params or {})

    # Resolve sync strategy
    sync_result = resolve_sync(
        state,
        client_version=client_version,
        client_head=client_head,
        depth=depth or defn.sync_limit,
        fetch_data_fn=fetch_data,
        sync_type=defn.sync_type,
    )

    # Send sync frame
    frame = {
        "type": "sync",
        "entity": entity,
        "params": params,
        **sync_result,
    }

    await ws.send_json(frame)

    logger.info(
        "[WS] %s subscribed to %s (mode=%s, v%d)",
        client_id[:8], entity, sync_result["mode"], sync_result["version"],
    )


# ── Unsubscribe ──────────────────────────────────────────────────

def _handle_unsubscribe(client_id: str, msg: dict) -> None:
    entity = msg.get("entity", "")
    params = msg.get("params") or None
    if entity:
        registry = get_sync_registry()
        registry.unsubscribe(client_id, entity, params)
        logger.debug("[WS] %s unsubscribed from %s", client_id[:8], entity)


# ── Request ──────────────────────────────────────────────────────

async def _handle_request(ws: WebSocket, store: EntityStore, user_id: str, msg: dict) -> None:
    request_id = msg.get("request_id", "")
    data = msg.get("data", {})

    try:
        result = await _dispatch(store, user_id, data, request_id=request_id)
        await ws.send_json({
            "type": "response",
            "request_id": request_id,
            "data": result,
        })
    except Exception as e:
        logger.error("[WS] Request %s failed: %s\n%s", request_id, e, traceback.format_exc())
        await ws.send_json({
            "type": "response",
            "request_id": request_id,
            "data": {"success": False, "error": str(e)},
        })


async def _dispatch(store: EntityStore, user_id: str, data: dict, request_id: str = "") -> dict:
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
            result = store.create(entity, user_id, payload, params=params, request_id=request_id or None)
            return {"success": True, "data": result}
        elif op in ("update", "upsert"):
            if not entity_id:
                return {"success": False, "error": "id is required for update"}
            result = store.update(entity, user_id, entity_id, payload, params=params, request_id=request_id or None)
            return {"success": True, "data": result}
        elif op == "delete":
            if not entity_id:
                return {"success": False, "error": "id is required for delete"}
            ok = store.delete(entity, user_id, entity_id, params=params, request_id=request_id or None)
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
