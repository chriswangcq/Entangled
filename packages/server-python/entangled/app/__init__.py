"""
Entangled App — batteries-included standalone service.

Provides a turnkey FastAPI application with:
  - Postgres-backed entity store (from entangled.sql)
  - WebSocket sync endpoint
  - REST CRUD API
  - Schema registration API
  - JWT + service-token authentication
  - Health check

Usage:
    from entangled.app import create_app
    from entangled.app.config import ServiceConfig

    config = ServiceConfig.from_env()
    app = create_app(config)
"""

from .factory import create_app
from .config import ServiceConfig

__all__ = ["create_app", "ServiceConfig"]
