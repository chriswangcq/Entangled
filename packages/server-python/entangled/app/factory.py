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
import asyncio
import hmac
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

from .config import ServiceConfig
from .auth import (
    AuthConfigurationError,
    configure_auth,
    deployment_auth_is_strict,
    validate_auth_configuration,
)
from .connection_registry import AuthenticatedConnectionRegistry
from .account_deletion import (
    AccountDeletionWriteBarrier,
    EntangledDeletionDomain,
    EntangledDeletionService,
    EntangledDeletionTopology,
    PostgresDeletionLedger,
    create_account_deletion_router,
    ensure_account_deletion_schema,
)
from .state import init_database, close_database, init_store
from ..metrics import render_metrics
from ..sql.persistence import ensure_sync_versions_table, load_all_sync_versions, make_version_bump_handler
from ..sql.state_transitions import ensure_state_transitions_schema

logger = logging.getLogger(__name__)


async def _allow_principal(_principal) -> bool:
    """Development-only authority used when revocation enforcement is disabled."""

    return True


def _make_user_existence_checker(db):
    """构造 WS 建连用的用户存在性 checker(纵深防御,Xiaoniu 事故 2026-07-07)。

    返回 checker(user_id) -> bool | None:
    * True/False:users 表可查,按行存在与否判定(False → ws 拒连 4403);
    * None:无法判定(users 表未建等 bootstrap 场景)→ 放行,记一次警告。
    这是遗留的 opt-in checker:它查询 Entangled 本地的非权威 ``users`` 表,
    且查询异常时 fail-open,因此不满足生产启用条件。启用前置条件见 config.py。
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
    strict_auth = config.strict_auth or deployment_auth_is_strict(config.namespace)
    # Fail before opening a database or serving a health check. A production
    # process with collapsed/missing auth domains must never become partially
    # ready.
    access_jwt_secret, service_token, namespace = validate_auth_configuration(
        access_jwt_secret=config.access_jwt_secret,
        service_token=config.service_token,
        expected_namespace=config.namespace,
        strict=strict_auth,
    )
    revocation_parts = (
        config.revocation_redis_url,
        config.revocation_authority_url,
        config.revocation_authority_service_token,
    )
    if any(revocation_parts) and not all(revocation_parts):
        raise AuthConfigurationError(
            "revocation Redis, Gateway URL, and authority token must be configured together"
        )
    if config.require_revocation_stream and not all(revocation_parts):
        raise AuthConfigurationError(
            "required revocation Redis, Gateway URL, or authority token is missing"
        )
    if config.revocation_authority_service_token and (
        hmac.compare_digest(
            config.revocation_authority_service_token,
            access_jwt_secret,
        )
        or hmac.compare_digest(
            config.revocation_authority_service_token,
            service_token,
        )
    ):
        raise AuthConfigurationError(
            "Gateway authority token must be independent from access and Entangled tokens"
        )
    if config.revocation_authority_response_max_age_seconds <= 0:
        raise AuthConfigurationError(
            "revocation authority response max age must be positive"
        )
    account_deletion_token = config.account_deletion_service_token.strip()
    account_deletion_topology = EntangledDeletionTopology(
        replica_count=config.account_deletion_replica_count,
        attestation=config.account_deletion_topology_attestation,
    )
    if account_deletion_token:
        account_deletion_topology.require_single_replica()
        if len(account_deletion_token) < 32:
            raise AuthConfigurationError(
                "account deletion service token must be at least 32 characters"
            )
        for other_secret in (
            access_jwt_secret,
            service_token,
            config.revocation_authority_service_token,
        ):
            if other_secret and hmac.compare_digest(
                account_deletion_token, other_secret
            ):
                raise AuthConfigurationError(
                    "account deletion token must use an independent trust domain"
                )

    revocation_configured = all(revocation_parts)
    authenticated_connections = AuthenticatedConnectionRegistry(
        available=not revocation_configured
    )

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
        ensure_account_deletion_schema(db)

        # 2b. PR-31 state-transition log tables. Dynamic schema registration
        # via POST /v1/schema/register only covers SqlEntityDef-backed tables;
        # the append-only history tables are independent of that flow and
        # must be created eagerly at startup so the first transition() call
        # never races a missing table.
        ensure_state_transitions_schema(db)

        # 3. EntityStore
        store = init_store(db=db)
        store._service_token = service_token
        account_deletion_barrier = AccountDeletionWriteBarrier(db)
        store.configure_account_deletion_guard(account_deletion_barrier)
        logger.info("EntityStore ready (0 entities — waiting for schema registration)")

        # 4. Auth(环境绑定 + 用户存在性:Xiaoniu 跨环境事故,2026-07-07)
        configure_auth(
            access_jwt_secret=access_jwt_secret,
            service_token=service_token,
            expected_namespace=namespace,
            strict=strict_auth,
        )
        revocation_stop = asyncio.Event()
        revocation_task = None
        revocation_consumer = None
        session_authority = None
        if revocation_configured:
            from .revocation import (
                GatewaySessionAuthority,
                RedisRevocationStream,
                RevocationStreamConsumer,
            )

            stream_key = config.revocation_stream_key or (
                f"novaic:{namespace}:auth-revocations:v1"
            )
            stream = RedisRevocationStream(
                redis_url=config.revocation_redis_url,
                stream_key=stream_key,
            )
            session_authority = GatewaySessionAuthority(
                base_url=config.revocation_authority_url,
                service_token=config.revocation_authority_service_token,
                namespace=namespace,
                response_max_age_seconds=(
                    config.revocation_authority_response_max_age_seconds
                ),
            )
            revocation_consumer = RevocationStreamConsumer(
                namespace=namespace,
                stream=stream,
                authority=session_authority,
                on_event=authenticated_connections.apply,
                on_unavailable=authenticated_connections.close_everything,
                on_available=authenticated_connections.mark_available,
            )
            revocation_task = asyncio.create_task(
                revocation_consumer.run(revocation_stop),
                name="entangled-auth-revocation-stream",
            )

        from .ws import configure_connection_security

        configure_connection_security(
            registry=authenticated_connections,
            revocation_ready=(
                (lambda: revocation_consumer.ready)
                if revocation_consumer is not None
                else (lambda: not config.require_revocation_stream)
            ),
            principal_is_current=(
                session_authority.principal_is_current
                if session_authority is not None
                else _allow_principal
            ),
            account_is_active=(
                lambda user_id: not account_deletion_barrier.is_blocked(user_id)
            ),
        )
        from .ws import set_user_existence_checker

        set_user_existence_checker(None)
        if config.enforce_user_exists:
            # opt-in(见 config 注释):Entangled 自库 users 今天非权威源,默认不装。
            set_user_existence_checker(_make_user_existence_checker(db))

        # 5. Sync engine (with version persistence)
        from .ws import init_sync_engine

        registry = init_sync_engine(on_version_bump=make_version_bump_handler(db))
        load_all_sync_versions(db, registry)
        if account_deletion_token:
            app.state.account_deletion_service = EntangledDeletionService(
                ledger=PostgresDeletionLedger(db, account_deletion_barrier),
                domain=EntangledDeletionDomain(
                    db, store, account_deletion_barrier
                ),
                connections=authenticated_connections,
                sync_registry_provider=lambda: registry,
                topology=account_deletion_topology,
            )
        logger.info("Sync engine initialized")

        logger.info("Entangled Service ready")
        yield

        revocation_stop.set()
        if revocation_task is not None:
            revocation_task.cancel()
            with suppress(asyncio.CancelledError):
                await revocation_task
        await authenticated_connections.close_everything("Service shutting down")
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
    if account_deletion_token:
        app.include_router(
            create_account_deletion_router(
                service_token=account_deletion_token,
                service_provider=lambda: getattr(
                    app.state, "account_deletion_service", None
                ),
            )
        )

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
