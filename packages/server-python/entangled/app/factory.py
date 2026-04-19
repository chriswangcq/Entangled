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

from .config import ServiceConfig
from .auth import configure_auth
from .state import init_database, close_database, init_store
from ..sql.persistence import ensure_sync_versions_table, load_all_sync_versions, make_version_bump_handler

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
    from .ws import ws_sync_handler

    app.include_router(health_router)
    app.include_router(schema_router)
    app.include_router(crud_router)
    app.include_router(outbox_router)
    add_ws = getattr(app, "add_api_websocket_route", None) or app.add_websocket_route
    add_ws("/v1/sync", ws_sync_handler)

    return app
