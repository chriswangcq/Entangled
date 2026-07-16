"""Entangled Service configuration.

All values are set via CLI args in main.py. No environment variable reads.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ServiceConfig:
    host: str = "0.0.0.0"
    port: int = 19900
    postgres_dsn: str = ""
    postgres_dsn_file: str = ""
    access_jwt_secret: str = ""
    service_token: str = ""
    # Explicit strict mode is useful before a namespace is assigned. Any
    # non-empty deployment namespace enables it automatically in app.factory.
    strict_auth: bool = False
    log_level: str = "INFO"
    # Access JWTs always carry this exact namespace. Staging/prod reject a
    # missing namespace and cannot start without one.
    namespace: str = ""
    # Intentionally OFF: Entangled's local ``users`` table isn't authoritative;
    # each environment's Gateway Postgres is the account authority. Public WS
    # access is already fail-closed at Gateway (namespace + active user), then
    # Entangled independently validates typ/iss/aud/ns for defense in depth.
    # Enable only after the checker reads Gateway authority, or a complete
    # projection with an integrity watermark, and lookup failures fail closed.
    enforce_user_exists: bool = False

    @classmethod
    def from_env(cls) -> ServiceConfig:
        return cls()
