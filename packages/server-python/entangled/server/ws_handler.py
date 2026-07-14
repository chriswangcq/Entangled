"""
entangled/server/ws_handler.py — WebSocket handler for Entangled protocol.

Three-operation model (quantum entanglement ideal):
  1. Entangle/Disentangle: establish/break entity entanglement
  2. Action: first-class mutation verb (create/update/delete/upsert/custom)
  3. Passive sync: server pushes sync frames automatically

Infrastructure:
  - Heartbeat: server-side dead-connection detection
  - Connect: register client, push schema
"""

import asyncio
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

from .notifier import (
    get_sync_registry,
    is_user_owned,
    register_client,
    set_store,
    unregister_client,
)
from .protocol import (
    build_ack_frame,
    build_error_frame,
    build_error_ack_frame,
    build_page_sync_frame,
    build_push_frame,
    build_schema_push_frame,
    parse_action_frame,
    parse_disentangle_frame,
    parse_entangle_frame,
)
from .store import EntityStore
from .sync import _pk_value_from_row, resolve_sync, snapshot_for_resolve

logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────

PUSH_QUEUE_MAX_SIZE = 1000       # Backpressure: drop oldest when full
HEARTBEAT_INTERVAL_S = 30        # Server → client heartbeat interval
HEARTBEAT_TIMEOUT_S = 90         # Close connection if no message in this time

# Sync Contract: advertised on direct Entangled WS schema push.
SYNC_CONTRACT_VERSION = 2


def _stream_head_order_by(defn: Any) -> Optional[str]:
    raw_order = str(getattr(defn, "default_order", "") or "").strip()
    if not raw_order:
        return None

    terms: List[str] = []
    for raw_term in raw_order.split(","):
        tokens = raw_term.strip().split()
        if not tokens:
            continue
        if tokens[-1].upper() in {"ASC", "DESC"}:
            tokens = tokens[:-1]
        expression = " ".join(tokens).strip()
        if expression:
            terms.append(f"{expression} DESC")
    return ", ".join(terms) or None


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

    # Compute schema frame once (recomputed on entity registration changes)
    _schema_cache: dict = {"frame": None}

    def _get_schema_push_frame() -> dict:
        """Return the explicit schema push frame."""
        schema = store.get_schema()
        frame = build_schema_push_frame(schema, SYNC_CONTRACT_VERSION)
        _schema_cache["frame"] = frame
        return frame

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
            msg = build_push_frame(event, data)
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
            await websocket.send_json(_get_schema_push_frame())
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

                if msg_type == "entangle":
                    await handle_entangle(
                        websocket, store, user_id, client_id, msg,
                    )
                elif msg_type == "disentangle":
                    handle_disentangle(
                        client_id,
                        msg,
                        store=store,
                        user_id=user_id,
                    )
                elif msg_type == "action":
                    await handle_action(websocket, store, user_id, client_id, msg)
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
    sync_user_id = user_id if is_user_owned(entity, store=store) else None
    registry.entangle(client_id, entity, params, user_id=sync_user_id)

    state = registry.get_state(entity, params, user_id=sync_user_id)
    # Copy op-log/version before any await, then run resolve_sync + DB in a worker thread.
    snap = snapshot_for_resolve(state)

    stream_head_order_by = _stream_head_order_by(defn) if defn.sync_type == "stream" else None

    def fetch_data(limit=None):
        if defn.sync_type == "stream":
            kwargs = {
                "params": params or {},
                "limit": limit or defn.sync_limit or 50,
            }
            if stream_head_order_by:
                kwargs["order_by"] = stream_head_order_by
            return store.list_stream(entity, user_id, **kwargs)
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
            data_order="desc" if defn.sync_type == "stream" else getattr(defn, 'data_order', 'desc'),
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
    fresh = registry.get_state(entity, params, user_id=sync_user_id)
    sync_result["version"] = fresh.current_version
    if sync_result.get("mode") == "delta" and client_version is not None:
        ops2 = fresh.get_ops_since(client_version)
        if ops2 is None:
            raise RuntimeError(
                f"delta base version {client_version} is no longer available for {entity}"
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
    try:
        frame = parse_entangle_frame(msg)
    except ValueError as e:
        await ws.send_json(build_error_frame(error=str(e)))
        return

    if not frame.entity:
        await ws.send_json(build_error_frame(error="entity is required"))
        return

    try:
        store.get_def(frame.entity)
    except KeyError:
        await ws.send_json(build_error_frame(error=f"Unknown entity: {frame.entity}"))
        return

    # Deepen window: entangle with before_id → fetch older page
    if frame.before_id is not None:
        await _entangle_deepen(
            ws,
            store,
            user_id,
            frame.entity,
            frame.params,
            frame.before_id,
            frame.limit,
            frame.request_id,
        )
        return

    # Standard entangle
    await _entangle_one(
        ws, store, user_id, client_id, frame.entity, frame.params,
        frame.version, frame.head, frame.depth,
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

        await ws.send_json(build_page_sync_frame(
            entity=entity,
            params=params,
            entries=entries,
            has_more=has_more,
            request_id=request_id,
        ))
    except Exception as e:
        logger.error("[WS] entangle deepen %s failed: %s\n%s", entity, e, traceback.format_exc())
        await ws.send_json(build_error_frame(entity=entity, error=str(e), request_id=request_id))

# ── Disentangle ──────────────────────────────────────────────────

def handle_disentangle(
    client_id: str,
    msg: dict,
    store: Optional[EntityStore] = None,
    *,
    user_id: Optional[str] = None,
) -> None:
    entity, params = parse_disentangle_frame(msg)
    if not entity:
        return

    registry = get_sync_registry()
    sync_user_id = user_id if is_user_owned(entity, store=store) else None
    registry.disentangle(client_id, entity, params, user_id=sync_user_id)
    logger.debug("[WS] %s disentangled from %s", client_id[:8], entity)

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
    try:
        frame = parse_action_frame(msg)
    except ValueError as e:
        await ws.send_json(build_error_ack_frame("", str(e)))
        return

    if not frame.entity:
        await ws.send_json(build_error_ack_frame(frame.request_id, "entity is required"))
        return
    if not frame.op:
        await ws.send_json(build_error_ack_frame(frame.request_id, "op is required"))
        return

    try:
        result = await _dispatch_action(
            store,
            user_id,
            frame.entity,
            frame.op,
            frame.entity_id,
            frame.params,
            frame.payload,
            frame.request_id,
        )
        await ws.send_json(build_ack_frame(frame.request_id, result))
    except Exception as e:
        logger.error("[WS] Action %s.%s failed: %s\n%s", frame.entity, frame.op, e, traceback.format_exc())
        await ws.send_json(build_error_ack_frame(frame.request_id, str(e)))


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
