"""Entangled Service configuration."""

from __future__ import annotations

import os
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
        return cls(
            host=os.environ.get("ENTANGLED_HOST", "0.0.0.0"),
            port=int(os.environ.get("ENTANGLED_PORT", "19900")),
            db_path=os.environ.get("ENTANGLED_DB_PATH", "data/entangled.db"),
            jwt_secret=os.environ.get("JWT_SECRET", ""),
            service_token=os.environ.get("ENTANGLED_SERVICE_TOKEN", ""),
            log_level=os.environ.get("ENTANGLED_LOG_LEVEL", "INFO"),
        )
