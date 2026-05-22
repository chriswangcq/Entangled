"""Entangled Service — standalone entity engine entry point.

Usage:
    python -m entangled.app.main
    python -m entangled.app.main --port 8080 --db-path data/my.db
"""

import argparse
import logging
from pathlib import Path

from .config import ServiceConfig
from .factory import create_app


def main():
    parser = argparse.ArgumentParser(description="Entangled Service")
    parser.add_argument("--host", default=None, help="Host to bind")
    parser.add_argument("--port", type=int, default=None, help="Port to listen")
    parser.add_argument("--db-backend", choices=("sqlite", "postgres"), default=None, help="Database backend")
    parser.add_argument("--db-path", default=None, help="SQLite database path")
    parser.add_argument("--postgres-dsn", default=None, help="Postgres DSN for Entangled")
    parser.add_argument("--postgres-dsn-file", default=None, help="File containing Postgres DSN for Entangled")
    parser.add_argument("--service-token", default=None, help="Service-to-service auth token (shared JWT secret)")
    parser.add_argument("--service-token-file", default=None, help="File containing service-to-service auth token")
    parser.add_argument("--log-level", default=None, help="Log level")
    args = parser.parse_args()

    config = ServiceConfig.from_env()
    if args.host:
        config.host = args.host
    if args.port:
        config.port = args.port
    if args.db_backend:
        config.db_backend = args.db_backend
    if args.db_path:
        config.db_path = args.db_path
    if args.postgres_dsn:
        config.postgres_dsn = args.postgres_dsn
    if args.postgres_dsn_file:
        config.postgres_dsn_file = args.postgres_dsn_file
    if args.service_token:
        config.service_token = args.service_token
        config.jwt_secret = args.service_token
    if args.service_token_file:
        token = Path(args.service_token_file).read_text(encoding="utf-8").strip()
        config.service_token = token
        config.jwt_secret = token
    if args.log_level:
        config.log_level = args.log_level

    logging.basicConfig(
        level=getattr(logging, config.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    app = create_app(config)

    import uvicorn
    uvicorn.run(
        app,
        host=config.host,
        port=config.port,
        log_level=config.log_level.lower(),
        ws_ping_interval=None,
        ws_ping_timeout=None,
    )


if __name__ == "__main__":
    main()
