"""
entangled/server/ws_handler.py — WebSocket handler for Entangled protocol.

Handles:
  1. Connect: register client, push schema (with hash for skip-if-unchanged)
  2. Subscribe/Unsubscribe: establish/break entity entanglement
  3. Request: dispatch entity CRUD/action
  4. Load More: backward pagination (first-class protocol message)
  5. Heartbeat: server-side dead-connection detection
  6. Disconnect: cleanup subscriptions
"""

import asyncio
import hashlib
import inspect
import json
import logging
import time
import traceback
import uuid
from typing import Any, Callable, Dict, List, Optional, Protocol, runtime_checkable

# Starlette is an optional dependency — only needed for create_ws_handler().
# Hosts that use the public handler APIs (handle_subscribe, etc.) don't need it.
try:
    from starlette.websockets import WebSocket, WebSocketDisconnect
except ImportError:  # pragma: no cover
    WebSocket = None  # type: ignore[assignment, misc]
    WebSocketDisconnect = Exception  # type: ignore[assignment, misc]


# ── WsSender Protocol ────────────────────────────────────────────
# Any object with an async send_json(data) method can be used as a
# WsSender.  This decouples Entangled handlers from Starlette's
# WebSocket class, allowing hosts to multiplex Entangled messages
# over their own transport (e.g. AppBridge WS).

@runtime_checkable
class WsSender(Protocol):
    async def send_json(self, data: Any) -> None: ...

from .notifier import register_client, unregister_client, set_store, get_sync_registry
from .store import EntityStore
from .sync import resolve_sync

logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────

PUSH_QUEUE_MAX_SIZE = 1000       # Backpressure: drop oldest when full
HEARTBEAT_INTERVAL_S = 30        # Server → client heartbeat interval
HEARTBEAT_TIMEOUT_S = 90         # Close connection if no message in this time


def create_ws_handler(
    store: EntityStore,
    *,
    auth_fn: Optional[Callable[[WebSocket], Optional[str]]] = None,
):
    """Create a Starlette WebSocket handler for the Entangled protocol.

    Args:
        store: Entity store with all registered EntityDefs.
        auth_fn: Optional ``(ws) -> user_id`` authentication callback.
    """

    # One-time setup — not per-connection
    set_store(store)

    # Compute schema hash once (recomputed on entity registration changes)
    _schema_cache: dict = {"hash": "", "data": []}

    def _get_schema_with_hash() -> tuple:
        """Return (schema_list, schema_hash). Cached."""
        schema = store.get_schema()
        h = hashlib.md5(json.dumps(schema, sort_keys=True).encode()).hexdigest()[:12]
        _schema_cache["hash"] = h
        _schema_cache["data"] = schema
        return schema, h

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
        last_activity = time.monotonic()

        # ── Push callback (bounded queue with backpressure) ──────
        push_queue: asyncio.Queue = asyncio.Queue(maxsize=PUSH_QUEUE_MAX_SIZE)

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
            """Sync callback for notifier — enqueue with backpressure (drop oldest)."""
            msg = data if isinstance(data, dict) and data.get("type") == "sync" else {
                "type": "push",
                "event": event,
                "data": data,
            }
            if push_queue.full():
                try:
                    push_queue.get_nowait()  # Drop oldest to make room
                except asyncio.QueueEmpty:
                    pass
            push_queue.put_nowait(msg)

        # Start push consumer task
        consumer_task = asyncio.ensure_future(push_consumer())

        register_client(client_id, user_id, sync_push)

        # ── Push schema (with hash for dedup) ────────────────────
        try:
            schema, schema_hash = _get_schema_with_hash()
            await websocket.send_json({
                "type": "push",
                "event": "schema",
                "data": {"entities": schema, "hash": schema_hash},
            })
        except Exception as e:
            logger.error("[WS] Schema push failed: %s", e)

        logger.info("[WS] Client %s connected (user=%s)", client_id[:8], user_id)

        # ── Heartbeat task ───────────────────────────────────────
        async def heartbeat():
            try:
                while True:
                    await asyncio.sleep(HEARTBEAT_INTERVAL_S)
                    elapsed = time.monotonic() - last_activity
                    if elapsed > HEARTBEAT_TIMEOUT_S:
                        logger.warning(
                            "[WS] Client %s heartbeat timeout (%.0fs), closing",
                            client_id[:8], elapsed,
                        )
                        await websocket.close(code=4002, reason="Heartbeat timeout")
                        return
                    try:
                        await websocket.send_json({"type": "heartbeat", "ts": time.time()})
                    except Exception:
                        return
            except asyncio.CancelledError:
                pass

        heartbeat_task = asyncio.ensure_future(heartbeat())

        # ── Message loop ─────────────────────────────────────────
        try:
            while True:
                raw = await websocket.receive_text()
                last_activity = time.monotonic()
                msg = json.loads(raw)
                msg_type = msg.get("type")

                if msg_type == "request":
                    await handle_request(websocket, store, user_id, msg)
                elif msg_type == "subscribe":
                    await handle_subscribe(
                        websocket, store, user_id, client_id, msg,
                    )
                elif msg_type == "unsubscribe":
                    handle_unsubscribe(client_id, msg, store=store)
                elif msg_type == "load_more":
                    await handle_load_more(websocket, store, user_id, msg)
                elif msg_type in ("ping", "pong", "heartbeat"):
                    if msg_type == "ping":
                        await websocket.send_json({"type": "pong"})
                else:
                    logger.debug("[WS] Unknown type: %s", msg_type)

        except WebSocketDisconnect:
            logger.info("[WS] Client %s disconnected", client_id[:8])
        except Exception as e:
            logger.error("[WS] Client %s error: %s", client_id[:8], e)
        finally:
            unregister_client(client_id)
            # Stop background tasks
            push_queue.put_nowait(None)
            consumer_task.cancel()
            heartbeat_task.cancel()

    return ws_handler


# ── Subscribe ────────────────────────────────────────────────────

def cascade_targets(store: EntityStore, entity: str) -> list:
    """Return [entity] + its subscription_cascade targets (deduped, ordered)."""
    targets = [entity]
    try:
        defn = store.get_def(entity)
    except KeyError:
        return targets
    for t in defn.subscription_cascade:
        if t and t not in targets:
            targets.append(t)
    return targets


async def _subscribe_one(
    ws: WsSender,
    store: EntityStore,
    user_id: str,
    client_id: str,
    entity: str,
    params,
    client_version,
    client_head,
    depth,
) -> None:
    """Subscribe + resolve_sync + send frame for a single entity."""
    try:
        defn = store.get_def(entity)
    except KeyError:
        logger.debug("[WS] Cascade target %s unknown, skipping", entity)
        return

    registry = get_sync_registry()
    registry.subscribe(client_id, entity, params)

    state = registry.get_state(entity, params)

    def fetch_data(limit=None):
        return store.list(entity, user_id, params=params or {}, limit=limit)

    # Build exists_before from EntityDef or store's native method
    eb_fn = None
    if defn.sync_type == "stream":
        if defn.exists_before_fn:
            def eb_fn(oldest_id: str) -> bool:
                return store.exists_before(entity, user_id, oldest_id, params=params or {})
        elif hasattr(store, 'exists_before'):
            def eb_fn(oldest_id: str) -> bool:
                return store.exists_before(entity, user_id, oldest_id, params=params or {})

    sync_result = resolve_sync(
        state,
        client_version=client_version,
        client_head=client_head,
        depth=depth,
        fetch_data_fn=fetch_data,
        sync_type=defn.sync_type,
        default_stream_depth=defn.sync_limit,
        exists_before_fn=eb_fn,
        data_order=getattr(defn, 'data_order', 'desc'),
    )

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


async def handle_subscribe(
    ws: WsSender,
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
        store.get_def(entity)
    except KeyError:
        await ws.send_json({"type": "error", "error": f"Unknown entity: {entity}"})
        return

    # Server-side cascade expansion: subscribe to entity + all its cascade targets.
    # Client sends ONE subscribe; server fans out and sends one sync frame per target.
    targets = cascade_targets(store, entity)
    for target in targets:
        await _subscribe_one(
            ws, store, user_id, client_id, target, params,
            client_version, client_head, depth,
        )

    if len(targets) > 1:
        logger.debug(
            "[WS] %s subscribe %s cascaded to %s",
            client_id[:8], entity, targets[1:],
        )


# ── Unsubscribe ──────────────────────────────────────────────────

def handle_unsubscribe(client_id: str, msg: dict, store: Optional[EntityStore] = None) -> None:
    entity = msg.get("entity", "")
    params = msg.get("params") or None
    if not entity:
        return

    registry = get_sync_registry()

    # Server-side cascade: unsubscribe entity + cascade targets
    if store:
        targets = cascade_targets(store, entity)
    else:
        targets = [entity]

    for target in targets:
        registry.unsubscribe(client_id, target, params)

    logger.debug("[WS] %s unsubscribed from %s", client_id[:8], targets)


# ── Load More (backward pagination, first-class protocol) ────────

async def handle_load_more(
    ws: WsSender,
    store: EntityStore,
    user_id: str,
    msg: dict,
) -> None:
    """Handle backward pagination as a first-class Entangled protocol message.

    Protocol:
        Client sends: {type: "load_more", request_id, entity, params, before_id, limit}
        Server responds: {type: "response", request_id, data: {success, entries, has_more}}
    """
    request_id = msg.get("request_id", "")
    entity = msg.get("entity", "")
    params = msg.get("params") or {}
    before_id = msg.get("before_id")
    limit = min(int(msg.get("limit", 50)), 500)  # Cap at 500

    if not entity:
        await ws.send_json({
            "type": "response", "request_id": request_id,
            "data": {"success": False, "error": "entity is required"},
        })
        return

    try:
        defn = store.get_def(entity)

        # Fetch via store.list_stream (cursor-based, returns DESC by default)
        entries = store.list_stream(
            entity, user_id,
            before_id=before_id,
            limit=limit,
            params=params,
        )
        # DATA ORDER CONTRACT:
        # entries are in default_order (DESC for messages, ASC for execution-logs).
        # Rust client prepend_older() expects DESC and reverses internally.
        # For ASC entities, prepend_older still works because the data is
        # already chronological — it just needs to be inserted before the
        # oldest cached item.

        # Cursor-based hasMore: prefer defn.exists_before_fn, then store.exists_before
        if not entries:
            has_more = False
        elif defn.sync_type == "stream" and (defn.exists_before_fn or hasattr(store, 'exists_before')):
            oldest_id = entries[-1].get("id") if entries else None
            has_more = store.exists_before(entity, user_id, oldest_id, params=params) if oldest_id else False
        else:
            # Fallback: if we got exactly `limit` items, assume more exist
            has_more = len(entries) >= limit

        await ws.send_json({
            "type": "response",
            "request_id": request_id,
            "data": {
                "success": True,
                "entries": entries,
                "has_more": has_more,
            },
        })
    except Exception as e:
        logger.error("[WS] load_more %s failed: %s\n%s", entity, e, traceback.format_exc())
        await ws.send_json({
            "type": "response",
            "request_id": request_id,
            "data": {"success": False, "error": str(e)},
        })


# ── Request ──────────────────────────────────────────────────────

async def handle_request(ws: WsSender, store: EntityStore, user_id: str, msg: dict) -> None:
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

        _MAX_LIST_ENTRIES = 5000

        if op == "list":
            entries = store.list(entity, user_id, params=params)
            if len(entries) > _MAX_LIST_ENTRIES:
                entries = entries[:_MAX_LIST_ENTRIES]
            return {"success": True, "entries": entries}
        elif op == "list_stream":
            before_id = data.get("before_id") or data.get("id_lt")
            after_id = data.get("after_id") or data.get("id_gt")
            limit = min(int(data.get("limit", 50)), 500)
            entries = store.list_stream(
                entity, user_id,
                params=params,
                before_id=before_id,
                after_id=after_id,
                limit=limit + 1,
            )
            has_more = len(entries) > limit
            if has_more:
                entries = entries[:limit]
            return {"success": True, "entries": entries, "has_more": has_more}
        elif op == "list_all":
            # list_all: no scope limit (host must implement via list_fn or list_stream_fn)
            entries = store.list(entity, user_id, params=params)
            if len(entries) > _MAX_LIST_ENTRIES:
                entries = entries[:_MAX_LIST_ENTRIES]
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
        elif op == "update":
            if not entity_id:
                return {"success": False, "error": "id is required for update"}
            result = store.update(entity, user_id, entity_id, payload, params=params, request_id=request_id or None)
            return {"success": True, "data": result}
        elif op == "upsert":
            if not entity_id:
                return {"success": False, "error": "id is required for upsert"}
            result = store.upsert(entity, user_id, entity_id, payload, params=params, request_id=request_id or None)
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
