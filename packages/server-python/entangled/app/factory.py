"""FastAPI app factory for Entangled standalone service.

Usage:
    from entangled.app import create_app, ServiceConfig

    config = ServiceConfig.from_env()
    app = create_app(config)

    # Or run directly:
    import uvicorn
    uvicorn.run(app, host=config.host, port=config.port)
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

from .config import ServiceConfig
from .auth import configure_auth
from .state import init_database, close_database, init_store
from ..metrics import render_metrics
from ..sql.persistence import ensure_sync_versions_table, load_all_sync_versions, make_version_bump_handler
from ..sql.state_transitions import ensure_state_transitions_schema

logger = logging.getLogger(__name__)


def _make_user_existence_checker(db):
    """构造 WS 建连用的用户存在性 checker(纵深防御,Xiaoniu 事故 2026-07-07)。

    返回 checker(user_id) -> bool | None:
    * True/False:users 表可查,按行存在与否判定(False → ws 拒连 4403);
    * None:无法判定(users 表未建等 bootstrap 场景)→ 放行,记一次警告。
    ``users`` 表由 novaic business 的 auth 流维护;app 层本就是 novaic 胶水,
    表名在此硬编码与 auth.py 的 "Gateway signs HS256 JWTs" 同一耦合层级。
    """
    warned = {"missing_table": False}

    def _checker(user_id: str):
        try:
            row = db.fetchone("SELECT 1 FROM users WHERE id = %s", (user_id,))
            return row is not None
        except Exception as e:  # noqa: BLE001 — 表未建/查询失败:fail-open 并警告
            if not warned["missing_table"]:
                warned["missing_table"] = True
                logger.warning("[Auth] user existence check unavailable (%s) — allowing", e)
            return None

    return _checker


def create_app(config: ServiceConfig) -> FastAPI:

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        logger.info("Entangled Service starting on %s:%s", config.host, config.port)

        # 1. Database
        db = init_database(
            postgres_dsn=config.postgres_dsn,
            postgres_dsn_file=config.postgres_dsn_file,
        )
        logger.info("Database initialized: backend=postgres")

        # 2. Sync version persistence table
        ensure_sync_versions_table(db)

        # 2b. PR-31 state-transition log tables. Dynamic schema registration
        # via POST /v1/schema/register only covers SqlEntityDef-backed tables;
        # the append-only history tables are independent of that flow and
        # must be created eagerly at startup so the first transition() call
        # never races a missing table.
        ensure_state_transitions_schema(db)

        # 3. EntityStore
        store = init_store(db=db)
        store._service_token = config.service_token or ""
        logger.info("EntityStore ready (0 entities — waiting for schema registration)")

        # 4. Auth(环境绑定 + 用户存在性:Xiaoniu 跨环境事故,2026-07-07)
        configure_auth(
            jwt_secret=config.jwt_secret,
            service_token=config.service_token,
            expected_namespace=config.namespace,
        )
        if config.enforce_user_exists:
            # opt-in(见 config 注释):Entangled 自库 users 今天非权威源,默认不装。
            from .ws import set_user_existence_checker

            set_user_existence_checker(_make_user_existence_checker(db))

        # 5. Sync engine (with version persistence)
        from .ws import init_sync_engine

        registry = init_sync_engine(on_version_bump=make_version_bump_handler(db))
        load_all_sync_versions(db, registry)
        logger.info("Sync engine initialized")

        logger.info("Entangled Service ready")
        yield

        close_database()
        logger.info("Entangled Service shutdown")

    app = FastAPI(
        title="Entangled Service",
        description="Standalone entity engine — CRUD, sync, and real-time push",
        version="0.1.0",
        lifespan=lifespan,
    )

    # Routes
    from .health import router as health_router
    from .schema import router as schema_router
    from .crud import router as crud_router
    from .state_transitions import router as state_transitions_router
    from .subagent_state import router as subagent_state_router
    from .ws import ws_sync_handler

    app.include_router(health_router)
    app.include_router(schema_router)
    app.include_router(crud_router)
    # PR-168E — chat-message lifecycle HTTP routes were retired with the
    # Environment notification queue cutover. Active agent-loop state is stored
    # in Environment notifications.
    app.include_router(state_transitions_router)
    # PR-31b — single chokepoint for subagents.status transitions.
    # Business's transition() helper delegates to
    # POST /v1/subagents/{agent_id}/{subagent_id}/transition, which does
    # the status UPDATE + subagent_state_transitions INSERT in one global-lock
    # transaction.
    app.include_router(subagent_state_router)

    # PR-32 — Prometheus exposition endpoint. Deliberately unauthenticated
    # so ops can scrape without wiring service tokens into the scraper;
    # bind the Entangled port to a private network interface if that's a
    # concern. Body is small (tens of kB at steady state) and the render
    # path holds the metrics lock only for microseconds — safe to hit on
    # any interval. See ``entangled.metrics`` for the backing store.
    @app.get("/metrics")
    def metrics_endpoint():
        return PlainTextResponse(render_metrics(), media_type="text/plain; version=0.0.4")

    add_ws = getattr(app, "add_api_websocket_route", None) or app.add_websocket_route
    add_ws("/v1/sync", ws_sync_handler)

    return app
