"""Entangled Service — standalone entity engine entry point.

Usage:
    python -m entangled.app.main
    python -m entangled.app.main --port 8080 --postgres-dsn-file /path/to/dsn
"""

import argparse
import logging
from pathlib import Path

from .config import ServiceConfig
from .factory import create_app


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Entangled Service")
    parser.add_argument("--host", default=None, help="Host to bind")
    parser.add_argument("--port", type=int, default=None, help="Port to listen")
    parser.add_argument("--postgres-dsn", default=None, help="Postgres DSN for Entangled")
    parser.add_argument("--postgres-dsn-file", default=None, help="File containing Postgres DSN for Entangled")
    jwt_secret = parser.add_mutually_exclusive_group()
    jwt_secret.add_argument(
        "--jwt-secret",
        default=None,
        help="User access JWT verifier secret (development only; prefer file)",
    )
    jwt_secret.add_argument(
        "--jwt-secret-file",
        default=None,
        help="File containing the user access JWT verifier secret",
    )
    service_token = parser.add_mutually_exclusive_group()
    service_token.add_argument(
        "--service-token",
        default=None,
        help="Independent service-to-service auth token (development only; prefer file)",
    )
    service_token.add_argument(
        "--service-token-file",
        default=None,
        help="File containing the independent service-to-service auth token",
    )
    parser.add_argument("--log-level", default=None, help="Log level")
    parser.add_argument(
        "--namespace",
        default=None,
        help="Deployment namespace (staging/prod); user JWTs carrying a "
        "mismatched ns claim are rejected (cross-environment binding)",
    )
    parser.add_argument(
        "--strict-auth",
        action="store_true",
        help="Require namespace and two distinct non-empty auth secrets. "
        "Automatically enabled whenever --namespace is set.",
    )
    parser.add_argument(
        "--enforce-user-exists",
        action="store_true",
        help="Reject WS connections for users absent from this instance's "
        "users table (opt-in: requires the table to be the authoritative "
        "user store — see app/config.py)",
    )
    parser.add_argument(
        "--revocation-redis-url",
        default=None,
        help="Namespace-local Redis URL for durable auth revocation delivery",
    )
    parser.add_argument("--revocation-stream-key", default=None)
    parser.add_argument(
        "--revocation-authority-url",
        default=None,
        help="Gateway base URL for service-authenticated session introspection",
    )
    parser.add_argument("--revocation-authority-service-token-file", default=None)
    parser.add_argument(
        "--revocation-authority-response-max-age-seconds",
        type=float,
        default=None,
    )
    parser.add_argument(
        "--require-revocation-stream",
        action="store_true",
        help="Fail startup unless the long-lived-session revocation plane is configured",
    )
    parser.add_argument(
        "--account-deletion-token-file",
        default=None,
        help="Owner-only file containing the dedicated account-deletion v2 token",
    )
    parser.add_argument(
        "--account-deletion-replica-count",
        type=int,
        default=None,
        help="Declared Entangled replica count for process-local deletion inventory",
    )
    parser.add_argument(
        "--account-deletion-topology-attestation",
        default=None,
        help="Exact versioned deployment attestation for account deletion",
    )
    return parser


def _read_secret_file(path: str, *, label: str) -> str:
    value = Path(path).read_text(encoding="utf-8").strip()
    if not value:
        raise ValueError(f"{label} file is empty: {path}")
    return value


def config_from_args(args: argparse.Namespace) -> ServiceConfig:
    """Build service configuration without coupling the two auth domains."""

    if args.namespace and (args.jwt_secret or args.service_token):
        raise ValueError(
            "named deployments require --jwt-secret-file and --service-token-file"
        )
    config = ServiceConfig.from_env()
    if args.host:
        config.host = args.host
    if args.port:
        config.port = args.port
    if args.postgres_dsn:
        config.postgres_dsn = args.postgres_dsn
    if args.postgres_dsn_file:
        config.postgres_dsn_file = args.postgres_dsn_file
    if args.jwt_secret:
        config.access_jwt_secret = args.jwt_secret.strip()
    if args.jwt_secret_file:
        config.access_jwt_secret = _read_secret_file(
            args.jwt_secret_file,
            label="access JWT secret",
        )
    if args.service_token:
        config.service_token = args.service_token
    if args.service_token_file:
        config.service_token = _read_secret_file(
            args.service_token_file,
            label="service token",
        )
    if args.log_level:
        config.log_level = args.log_level
    if args.namespace:
        config.namespace = args.namespace
    if args.strict_auth:
        config.strict_auth = True
    if args.enforce_user_exists:
        config.enforce_user_exists = True
    if args.revocation_redis_url:
        config.revocation_redis_url = args.revocation_redis_url.strip()
    if args.revocation_stream_key:
        config.revocation_stream_key = args.revocation_stream_key.strip()
    if args.revocation_authority_url:
        config.revocation_authority_url = args.revocation_authority_url.strip()
    if args.revocation_authority_service_token_file:
        config.revocation_authority_service_token = _read_secret_file(
            args.revocation_authority_service_token_file,
            label="Gateway authority service token",
        )
    if args.revocation_authority_response_max_age_seconds is not None:
        config.revocation_authority_response_max_age_seconds = (
            args.revocation_authority_response_max_age_seconds
        )
    if args.require_revocation_stream:
        config.require_revocation_stream = True
    if args.account_deletion_token_file:
        from .account_deletion import read_owner_only_secret_file

        config.account_deletion_service_token = read_owner_only_secret_file(
            args.account_deletion_token_file
        )
    if args.account_deletion_replica_count is not None:
        config.account_deletion_replica_count = args.account_deletion_replica_count
    if args.account_deletion_topology_attestation is not None:
        config.account_deletion_topology_attestation = (
            args.account_deletion_topology_attestation.strip()
        )
    return config


def main():
    args = build_parser().parse_args()
    config = config_from_args(args)

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
