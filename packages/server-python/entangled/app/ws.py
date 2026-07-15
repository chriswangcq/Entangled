"""WebSocket Sync endpoint — fronts Entangled's sync engine.

Clients connect with a JWT token, entangle with entities, and receive
real-time delta/snapshot pushes.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Any, Optional

from starlette.websockets import WebSocket, WebSocketDisconnect

from ..server.notifier import (
    register_client,
    set_store,
    unregister_client,
)
from ..server.sync import SyncRegistry
from ..server.ws_handler import (
    HEARTBEAT_INTERVAL_S,
    HEARTBEAT_TIMEOUT_S,
    PUSH_QUEUE_MAX_SIZE,
    SYNC_CONTRACT_VERSION,
    handle_action,
    handle_entangle,
    handle_disentangle,
)
from ..server.protocol import build_push_frame, build_schema_push_frame

from .auth import decode_jwt_from_raw
from .state import get_store

logger = logging.getLogger(__name__)

_sync_registry: Optional[SyncRegistry] = None
_initialized = False

# 用户存在性检查(Xiaoniu 跨环境事故,2026-07-07 纵深防御):factory 注入一个
# checker(user_id) -> bool | None。True=存在;False=本环境无此用户,拒连;
# None=无法判定(users 表尚未建等 bootstrap 场景),放行并由 checker 侧记警告。
_user_existence_checker = None


def set_user_existence_checker(checker) -> None:
    global _user_existence_checker
    _user_existence_checker = checker


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

    # 纵深防御:签名有效但本环境不存在的用户,拒连(Xiaoniu 事故里 prod 曾为一个
    # prod 用户表中不存在的 staging 用户建了孤儿 agent)。checker 返回 None(无法
    # 判定,如 users 表未建)时放行 —— fail-open 只对 bootstrap,fail-closed 对已知不存在。
    if _user_existence_checker is not None:
        exists = _user_existence_checker(user_id)
        if exists is False:
            logger.warning("[WS] rejected unknown user %s (not in this environment)", user_id)
            await websocket.close(code=4403, reason="Unknown user in this environment")
            return

    await websocket.accept()

    client_id = f"ws_{uuid.uuid4().hex[:12]}"
    sender = _WsSender(websocket)
    last_activity = time.monotonic()

    push_queue: asyncio.Queue = asyncio.Queue(maxsize=PUSH_QUEUE_MAX_SIZE)

    async def push_consumer() -> None:
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

    def sync_push(event: str, data: Any) -> None:
        msg = build_push_frame(event, data)
        if push_queue.full():
            try:
                push_queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
        push_queue.put_nowait(msg)

    consumer_task = asyncio.ensure_future(push_consumer())
    register_client(client_id, user_id, sync_push)
    logger.info("[WS] Client %s connected (user=%s)", client_id, user_id)

    store = get_store()

    async def heartbeat() -> None:
        try:
            while True:
                await asyncio.sleep(HEARTBEAT_INTERVAL_S)
                elapsed = time.monotonic() - last_activity
                if elapsed > HEARTBEAT_TIMEOUT_S:
                    logger.warning(
                        "[WS] Client %s heartbeat timeout (%.0fs), closing",
                        client_id,
                        elapsed,
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

    # 3. Push schema on connect — same frame as Gateway /app/ws and ws_handler.create_ws_handler
    try:
        schema = store.get_schema()
        await sender.send_json(build_schema_push_frame(schema, SYNC_CONTRACT_VERSION))
    except Exception as e:
        logger.warning("[WS] Failed to push schema to %s: %s", client_id, e)

    # 4. Message loop (protocol matches gateway/api/app_client.py + ws_handler)
    try:
        while True:
            data = await websocket.receive_json()
            last_activity = time.monotonic()
            msg_type = data.get("type")

            if msg_type == "entangle":
                await handle_entangle(sender, store, user_id, client_id, data)
            elif msg_type == "disentangle":
                handle_disentangle(
                    client_id,
                    data,
                    store=store,
                    user_id=user_id,
                )
            elif msg_type == "action":
                await handle_action(sender, store, user_id, client_id, data)
            elif msg_type == "ping":
                await sender.send_json({"type": "pong"})
            elif msg_type in ("pong", "heartbeat"):
                pass
            else:
                logger.debug("[WS] Unknown message type from %s: %s", client_id, msg_type)

    except WebSocketDisconnect:
        logger.info("[WS] Client %s disconnected", client_id)
    except Exception as e:
        logger.warning("[WS] Error for client %s: %s", client_id, e)
    finally:
        unregister_client(client_id)
        try:
            push_queue.put_nowait(None)
        except Exception:
            pass
        consumer_task.cancel()
        heartbeat_task.cancel()
