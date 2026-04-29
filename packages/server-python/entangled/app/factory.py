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


def create_app(config: ServiceConfig) -> FastAPI:

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        logger.info("Entangled Service starting on %s:%s", config.host, config.port)

        # 1. Database
        db = init_database(config.db_path)
        logger.info("Database initialized: %s", config.db_path)

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

        # 4. Auth
        configure_auth(jwt_secret=config.jwt_secret, service_token=config.service_token)

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
    from .outbox import router as outbox_router
    from .message_state import router as message_state_router
    from .orphans import router as orphans_router
    from .stuck_claimed import router as stuck_claimed_router
    from .state_transitions import router as state_transitions_router
    from .subagent_state import router as subagent_state_router
    from .ws import ws_sync_handler

    app.include_router(health_router)
    app.include_router(schema_router)
    app.include_router(crud_router)
    app.include_router(outbox_router)
    # PR-21 — single chokepoint for chat_messages.lifecycle transitions.
    # All writes to the lifecycle column must route through this router;
    # scripts/ci/lint_lifecycle.sh enforces that.
    app.include_router(message_state_router)
    # PR-26 — orphan listing endpoint. Read-only; consumed by HealthWorker
    # (orphan scan + re-dispatch in PR-27) and by Business's ops-facing
    # /internal/messages/orphaned proxy.
    app.include_router(orphans_router)
    # PR-51 Part 2 (2026-04-23) — stuck-claimed listing endpoint.
    # Companion to /v1/orphans but for the other half of the lifecycle
    # ladder: rows that got claimed but never moved to consumed. Consumed
    # by HealthWorker's claimed-age scan and by Business's ops-facing
    # /internal/messages/stuck-claimed proxy.
    app.include_router(stuck_claimed_router)
    # PR-31 — append-only history for message + subagent state machines.
    # Message transitions are populated co-transactionally inside
    # message_state.transition; the same property now holds for subagent
    # transitions (PR-31b promoted the state machine server-side).
    # Both entity types share read endpoints so ops can reconstruct a
    # full lifecycle in one curl. Subagent transition writes now go through
    # the PR-31b router below.
    app.include_router(state_transitions_router)
    # PR-31b — single chokepoint for subagents.status transitions.
    # Business's transition() helper delegates to
    # POST /v1/subagents/{agent_id}/{subagent_id}/transition, which does
    # the status UPDATE + subagent_state_transitions INSERT in one
    # global-lock transaction. Same shape as message_state_router.
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
