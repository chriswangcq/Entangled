"""Entangled Service configuration.

All values are set via CLI args in main.py. No environment variable reads.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ServiceConfig:
    host: str = "0.0.0.0"
    port: int = 19900
    db_path: str = "data/entangled.db"
    jwt_secret: str = ""
    service_token: str = ""
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> ServiceConfig:
        return cls()
