"""App-level state — database, entity store, and sync registry singletons.

These singletons are managed by the app factory (create_app) and available
to all endpoints via get_db / get_store / get_sync_registry.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from ..sql.database import Database
from ..sql.entity_store import SqlEntityStore

_database: Optional[Database] = None
_store: Optional[SqlEntityStore] = None


def init_database(db_path: str) -> Database:
    global _database
    if _database is not None:
        return _database
    p = Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    _database = Database(p)
    _database.connect()
    return _database


def get_db() -> Database:
    if _database is None:
        raise RuntimeError("Database not initialized — call init_database() first")
    return _database


def close_database():
    global _database
    if _database:
        _database.close()
        _database = None


def init_store(db: Optional[Database] = None) -> SqlEntityStore:
    global _store
    if _store is None:
        _store = SqlEntityStore(db=db or get_db())
    return _store


def get_store() -> SqlEntityStore:
    if _store is None:
        raise RuntimeError("EntityStore not initialized — call init_store() first")
    return _store
