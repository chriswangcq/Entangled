"""
entangled/server/ws_handler.py — WebSocket handler for Entangled protocol.

Three-operation model (quantum entanglement ideal):
  1. Entangle/Disentangle: establish/break entity entanglement
  2. Action: first-class mutation verb (create/update/delete/upsert/custom)
  3. Passive sync: server pushes sync frames automatically

Legacy support (deprecated):
  - Request: generic RPC dispatch (prefer action + local cache reads)
  - Load More: backward pagination (prefer entangle with before_id)

Infrastructure:
  - Heartbeat: server-side dead-connection detection
  - Connect: register client, push schema
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
# Hosts that use the public handler APIs (handle_entangle, etc.) don't need it.
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
from .sync import resolve_sync, snapshot_for_resolve, _pk_value_from_row

logger = logging.getLogger(__name__)

# B.3: entangle delta path called full resolve again (get_ops_since returned None).
_entangle_reconcile_fallback_total = 0


def get_entangle_reconcile_fallback_total() -> int:
    return _entangle_reconcile_fallback_total


# ── Configuration ────────────────────────────────────────────────

PUSH_QUEUE_MAX_SIZE = 1000       # Backpressure: drop oldest when full
HEARTBEAT_INTERVAL_S = 30        # Server → client heartbeat interval
HEARTBEAT_TIMEOUT_S = 90         # Close connection if no message in this time

# Sync Contract: advertised on WS schema push; REST mirrors via gateway.entity.sync_contract (same int).
SYNC_CONTRACT_VERSION = 2


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
                "data": {
                    "entities": schema,
                    "hash": schema_hash,
                    "syncContractVersion": SYNC_CONTRACT_VERSION,
                },
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

                if msg_type in ("entangle", "subscribe"):
                    await handle_entangle(
                        websocket, store, user_id, client_id, msg,
                    )
                elif msg_type in ("disentangle", "unsubscribe"):
                    handle_disentangle(client_id, msg, store=store)
                elif msg_type == "action":
                    await handle_action(websocket, store, user_id, client_id, msg)
                elif msg_type == "request":
                    await handle_request(websocket, store, user_id, msg)
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


# ── Entangle ─────────────────────────────────────────────────────

async def _entangle_one(
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
    """Entangle + resolve_sync + send frame for a single entity."""
    try:
        defn = store.get_def(entity)
    except KeyError:
        logger.debug("[WS] Entity %s unknown, skipping", entity)
        return

    registry = get_sync_registry()
    registry.entangle(client_id, entity, params)

    state = registry.get_state(entity, params)
    # Copy op-log/version before any await — then run resolve_sync + DB in a worker thread
    # (Gateway Database uses thread-local SQLite connections).
    snap = snapshot_for_resolve(state)

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

    id_field = getattr(defn, "id_field", "id")

    def _resolve_in_thread():
        return resolve_sync(
            snap,
            client_version=client_version,
            client_head=client_head,
            depth=depth,
            fetch_data_fn=fetch_data,
            sync_type=defn.sync_type,
            default_stream_depth=defn.sync_limit,
            exists_before_fn=eb_fn,
            data_order=getattr(defn, 'data_order', 'desc'),
            id_field=id_field,
        )

    _t0 = time.perf_counter()
    sync_result = await asyncio.to_thread(_resolve_in_thread)
    _resolve_ms = (time.perf_counter() - _t0) * 1000.0
    logger.debug(
        "[WS] entangle resolve_thread_ms=%.1f entity=%s client=%s mode=%s",
        _resolve_ms,
        entity,
        client_id[:8],
        sync_result.get("mode"),
    )

    # Reconcile: version may have advanced while the thread ran; delta ops must match live op_log.
    fresh = registry.get_state(entity, params)
    sync_result["version"] = fresh.current_version
    if sync_result.get("mode") == "delta" and client_version is not None:
        ops2 = fresh.get_ops_since(client_version)
        if ops2 is None:
            global _entangle_reconcile_fallback_total

            def _reconcile_fallback_resolve():
                return resolve_sync(
                    fresh,
                    client_version=client_version,
                    client_head=client_head,
                    depth=depth,
                    fetch_data_fn=fetch_data,
                    sync_type=defn.sync_type,
                    default_stream_depth=defn.sync_limit,
                    exists_before_fn=eb_fn,
                    data_order=getattr(defn, 'data_order', 'desc'),
                    id_field=id_field,
                )

            sync_result = await asyncio.to_thread(_reconcile_fallback_resolve)
            sync_result["version"] = registry.get_state(entity, params).current_version
            _entangle_reconcile_fallback_total += 1
            logger.debug(
                "[WS] reconcile_fallback_to_thread entity=%s client=%s total=%d",
                entity,
                client_id[:8],
                _entangle_reconcile_fallback_total,
            )
        else:
            sync_result["ops"] = [o.to_dict() for o in ops2]
            sync_result["baseVersion"] = client_version

    frame = {
        "type": "sync",
        "entity": entity,
        "params": params,
        "idField": id_field,
        **sync_result,
    }
    await ws.send_json(frame)

    logger.info(
        "[WS] %s entangled with %s (mode=%s, v%d)",
        client_id[:8], entity, sync_result["mode"], sync_result["version"],
    )


async def handle_entangle(
    ws: WsSender,
    store: EntityStore,
    user_id: str,
    client_id: str,
    msg: dict,
) -> None:
    """Establish entity entanglement, or deepen the entanglement window.

    Standard entangle:
        {type: "entangle", entity, params?, version?, depth?}
        → server sends sync frame (snapshot/delta/head_n)

    Deepen window (replaces load_more):
        {type: "entangle", entity, params?, before_id, limit?}
        → server sends {type: "sync", entity, params, mode: "page", ...}
    """
    entity = msg.get("entity", "")
    params = msg.get("params") or None  # Normalize
    client_version = msg.get("version")
    client_head = msg.get("head")
    depth = msg.get("depth")
    before_id = msg.get("before_id")

    if not entity:
        await ws.send_json({"type": "error", "error": "entity is required"})
        return

    try:
        store.get_def(entity)
    except KeyError:
        await ws.send_json({"type": "error", "error": f"Unknown entity: {entity}"})
        return

    # Deepen window: entangle with before_id → fetch older page
    if before_id is not None:
        limit = min(int(msg.get("limit", 50)), 500)
        request_id = msg.get("request_id") or msg.get("requestId", "")
        await _entangle_deepen(ws, store, user_id, entity, params, before_id, limit, request_id)
        return

    # Standard entangle
    await _entangle_one(
        ws, store, user_id, client_id, entity, params,
        client_version, client_head, depth,
    )


async def _entangle_deepen(
    ws: WsSender,
    store: EntityStore,
    user_id: str,
    entity: str,
    params: Optional[dict],
    before_id: str,
    limit: int,
    request_id: str,
) -> None:
    """Deepen the entanglement window — fetch older entries for a stream entity."""
    try:
        defn = store.get_def(entity)

        def _fetch():
            entries = store.list_stream(
                entity, user_id,
                before_id=before_id,
                limit=limit,
                params=params or {},
            )
            if not entries:
                has_more = False
            elif defn.sync_type == "stream" and (defn.exists_before_fn or hasattr(store, 'exists_before')):
                id_field = getattr(defn, "id_field", "id")
                oldest_id = _pk_value_from_row(entries[-1], id_field) if entries else None
                has_more = store.exists_before(entity, user_id, oldest_id, params=params or {}) if oldest_id else False
            else:
                has_more = len(entries) >= limit
            return entries, has_more

        entries, has_more = await asyncio.to_thread(_fetch)

        await ws.send_json({
            "type": "sync",
            "entity": entity,
            "params": params if params else None,
            "mode": "page",
            "data": entries,
            "hasMore": has_more,
            "requestId": request_id if request_id else None,
        })
    except Exception as e:
        logger.error("[WS] entangle deepen %s failed: %s\n%s", entity, e, traceback.format_exc())
        await ws.send_json({
            "type": "error",
            "entity": entity,
            "error": str(e),
            "requestId": request_id if request_id else None,
        })


# Backward-compat alias
handle_subscribe = handle_entangle


# ── Disentangle ──────────────────────────────────────────────────

def handle_disentangle(client_id: str, msg: dict, store: Optional[EntityStore] = None) -> None:
    entity = msg.get("entity", "")
    params = msg.get("params") or None
    if not entity:
        return

    registry = get_sync_registry()
    registry.disentangle(client_id, entity, params)
    logger.debug("[WS] %s disentangled from %s", client_id[:8], entity)


# Backward-compat alias
handle_unsubscribe = handle_disentangle


# ── Load More (DEPRECATED — use entangle with before_id) ─────────

async def handle_load_more(
    ws: WsSender,
    store: EntityStore,
    user_id: str,
    msg: dict,
) -> None:
    """[DEPRECATED] Backward pagination — prefer ``entangle`` with ``before_id``.

    New clients should send ``{type: "entangle", entity, before_id, limit}``
    to deepen their entanglement window.  This handler is kept for backward
    compatibility only.

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

        def _load_more_sync():
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

            if not entries:
                has_more = False
            elif defn.sync_type == "stream" and (defn.exists_before_fn or hasattr(store, 'exists_before')):
                id_field = getattr(defn, "id_field", "id")
                oldest_id = _pk_value_from_row(entries[-1], id_field) if entries else None
                has_more = store.exists_before(entity, user_id, oldest_id, params=params) if oldest_id else False
            else:
                has_more = len(entries) >= limit
            return entries, has_more

        entries, has_more = await asyncio.to_thread(_load_more_sync)

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


# ── Action (first-class mutation verb) ────────────────────────────
#
# The "action" message type is the canonical way for clients to mutate
# entities.  Built-in ops (create/update/delete/upsert) and custom
# EntityDef actions all flow through the same path.
#
# Protocol:
#   Client sends: {type: "action", request_id, entity, op, id?, params?, data?}
#   Server sends: {type: "ack", request_id, success, data?, error?}
#
# On success the mutation also triggers a sync delta to all entangled
# clients (including the originator, with request_id for optimistic-
# update correlation).


async def handle_action(
    ws: WsSender,
    store: EntityStore,
    user_id: str,
    client_id: str,
    msg: dict,
) -> None:
    """Handle a first-class action message (mutation intent)."""
    request_id = msg.get("request_id") or msg.get("requestId", "")
    entity = msg.get("entity", "")
    op = msg.get("op", "")
    entity_id = msg.get("id")
    params = msg.get("params") or {}
    payload = msg.get("data") or {}

    if not entity:
        await ws.send_json({"type": "ack", "request_id": request_id, "success": False, "error": "entity is required"})
        return
    if not op:
        await ws.send_json({"type": "ack", "request_id": request_id, "success": False, "error": "op is required"})
        return

    try:
        result = await _dispatch_action(store, user_id, entity, op, entity_id, params, payload, request_id)
        await ws.send_json({
            "type": "ack",
            "request_id": request_id,
            **result,
        })
    except Exception as e:
        logger.error("[WS] Action %s.%s failed: %s\n%s", entity, op, e, traceback.format_exc())
        await ws.send_json({
            "type": "ack",
            "request_id": request_id,
            "success": False,
            "error": str(e),
        })


def _dispatch_action_blocking(
    store: EntityStore, user_id: str,
    entity: str, op: str, entity_id: Optional[str],
    params: dict, payload: dict, request_id: str,
) -> dict:
    """Execute a built-in mutation (create/update/delete/upsert) in a worker thread."""
    try:
        store.get_def(entity)

        if op == "create":
            result = store.create(entity, user_id, payload, params=params, request_id=request_id or None)
            return {"success": True, "data": result}
        if op == "update":
            if not entity_id:
                return {"success": False, "error": "id is required for update"}
            result = store.update(entity, user_id, entity_id, payload, params=params, request_id=request_id or None)
            return {"success": True, "data": result}
        if op == "upsert":
            if not entity_id:
                return {"success": False, "error": "id is required for upsert"}
            result = store.upsert(entity, user_id, entity_id, payload, params=params, request_id=request_id or None)
            return {"success": True, "data": result}
        if op == "delete":
            if not entity_id:
                return {"success": False, "error": "id is required for delete"}
            ok = store.delete(entity, user_id, entity_id, params=params, request_id=request_id or None)
            return {"success": True} if ok else {"success": False, "error": "not found"}

        return {"success": False, "error": f"Unknown action op: {op}"}

    except (KeyError, PermissionError, NotImplementedError) as e:
        return {"success": False, "error": str(e)}
    except Exception as e:
        logger.error("[WS] Action dispatch error: %s.%s %s", entity, op, e)
        return {"success": False, "error": str(e)}


async def _dispatch_action(
    store: EntityStore, user_id: str,
    entity: str, op: str, entity_id: Optional[str],
    params: dict, payload: dict, request_id: str,
) -> dict:
    """Route action to the correct executor (async for custom actions, thread for CRUD)."""
    if op not in ("create", "update", "delete", "upsert"):
        # Custom EntityDef action — runs on the asyncio loop
        try:
            store.get_def(entity)
            result = await store.action(entity, user_id, op, params, payload)
            return {"success": True, "data": result}
        except (KeyError, PermissionError, NotImplementedError) as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            logger.error("[WS] Action dispatch error: %s.%s %s", entity, op, e)
            return {"success": False, "error": str(e)}

    return await asyncio.to_thread(
        _dispatch_action_blocking, store, user_id, entity, op, entity_id, params, payload, request_id,
    )


# ── Request (deprecated — use "action" for mutations) ────────────
#
# The "request" message type is retained for backward compatibility.
# New clients should use "action" for mutations and read from local
# cache (populated by entangle sync) instead of WS reads.

async def handle_request(ws: WsSender, store: EntityStore, user_id: str, msg: dict) -> None:
    """[DEPRECATED] Generic RPC dispatch — prefer ``action`` for mutations."""
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


def _dispatch_entity_ops_blocking(
    store: EntityStore, user_id: str, data: dict, request_id: str
) -> dict:
    """[DEPRECATED] list/get/create/update/… — runs in ``asyncio.to_thread``.

    Read ops (list, get, list_stream, list_all) are **deprecated** on the WS
    protocol.  Clients should entangle with the entity and read from their
    local cache.  These ops remain for backward compatibility only.

    For mutations, prefer the first-class ``action`` message type.
    """
    op = data.get("op", "")
    entity = data.get("entity", "")
    entity_id = data.get("id")
    params = data.get("params") or {}
    payload = data.get("data") or {}

    try:
        store.get_def(entity)

        _MAX_LIST_ENTRIES = 5000

        # ── Read ops (DEPRECATED — use local cache after entangle) ──
        if op == "list":
            entries = store.list(entity, user_id, params=params)
            if len(entries) > _MAX_LIST_ENTRIES:
                entries = entries[:_MAX_LIST_ENTRIES]
            return {"success": True, "entries": entries}
        if op == "list_stream":
            before_id = data.get("before_id") or data.get("id_lt")
            after_id = data.get("after_id") or data.get("id_gt")
            limit = min(int(data.get("limit", 50)), 500)
            entries = store.list_stream(
                entity,
                user_id,
                params=params,
                before_id=before_id,
                after_id=after_id,
                limit=limit + 1,
            )
            has_more = len(entries) > limit
            if has_more:
                entries = entries[:limit]
            return {"success": True, "entries": entries, "has_more": has_more}
        if op == "list_all":
            entries = store.list(entity, user_id, params=params)
            if len(entries) > _MAX_LIST_ENTRIES:
                entries = entries[:_MAX_LIST_ENTRIES]
            return {"success": True, "entries": entries}
        if op == "get":
            if not entity_id:
                return {"success": False, "error": "id is required for get"}
            item = store.get(entity, user_id, entity_id, params=params)
            if item is None:
                return {"success": False, "error": f"{entity} {entity_id} not found"}
            return {"success": True, "data": item}

        # ── Mutation ops (DEPRECATED — use "action" message type) ──
        if op == "create":
            result = store.create(
                entity, user_id, payload, params=params, request_id=request_id or None
            )
            return {"success": True, "data": result}
        if op == "update":
            if not entity_id:
                return {"success": False, "error": "id is required for update"}
            result = store.update(
                entity,
                user_id,
                entity_id,
                payload,
                params=params,
                request_id=request_id or None,
            )
            return {"success": True, "data": result}
        if op == "upsert":
            if not entity_id:
                return {"success": False, "error": "id is required for upsert"}
            result = store.upsert(
                entity,
                user_id,
                entity_id,
                payload,
                params=params,
                request_id=request_id or None,
            )
            return {"success": True, "data": result}
        if op == "delete":
            if not entity_id:
                return {"success": False, "error": "id is required for delete"}
            ok = store.delete(
                entity, user_id, entity_id, params=params, request_id=request_id or None
            )
            return {"success": True} if ok else {"success": False, "error": "not found"}
        if op == "action":
            return {"success": False, "error": "internal: action must run on asyncio loop"}
        return {"success": False, "error": f"Unknown op: {op}"}

    except (KeyError, PermissionError, NotImplementedError) as e:
        return {"success": False, "error": str(e)}
    except Exception as e:
        logger.error("[WS] Dispatch error: %s.%s %s", entity, op, e)
        return {"success": False, "error": str(e)}


async def _dispatch(store: EntityStore, user_id: str, data: dict, request_id: str = "") -> dict:
    """[DEPRECATED] Route request ops — kept for backward compatibility."""
    op = data.get("op", "")
    entity = data.get("entity", "")
    params = data.get("params") or {}
    payload = data.get("data") or {}

    if not entity:
        return {"success": False, "error": "entity is required"}
    if not op:
        return {"success": False, "error": "op is required"}

    if op == "action":
        action_name = data.get("action_name", "")
        if not action_name:
            return {"success": False, "error": "action_name is required"}
        try:
            store.get_def(entity)
            result = await store.action(
                entity, user_id, action_name, params, payload
            )
            return {"success": True, "data": result}
        except (KeyError, PermissionError, NotImplementedError) as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            logger.error(
                "[WS] Dispatch error: %s.action %s %s", entity, action_name, e
            )
            return {"success": False, "error": str(e)}

    return await asyncio.to_thread(
        _dispatch_entity_ops_blocking, store, user_id, data, request_id
    )
