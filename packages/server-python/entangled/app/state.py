"""App-level state — database, entity store, and sync registry singletons.

These singletons are managed by the app factory (create_app) and available
to all endpoints via get_db / get_store / get_sync_registry.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from ..sql.database import Database, PostgresDatabase, create_database
from ..sql.entity_store import SqlEntityStore

_database: Optional[Database | PostgresDatabase] = None
_store: Optional[SqlEntityStore] = None


def init_database(
    db_path: str,
    *,
    backend: str = "sqlite",
    postgres_dsn: str = "",
    postgres_dsn_file: str = "",
) -> Database | PostgresDatabase:
    global _database
    if _database is not None:
        return _database
    p = Path(db_path)
    if backend == "sqlite":
        p.parent.mkdir(parents=True, exist_ok=True)
    dsn_file = Path(postgres_dsn_file) if postgres_dsn_file else None
    db = create_database(
        backend=backend,
        db_path=p,
        postgres_dsn=postgres_dsn,
        postgres_dsn_file=dsn_file,
    )
    try:
        db.connect()
    except Exception:
        _database = None
        raise
    _database = db
    return _database


def get_db() -> Database | PostgresDatabase:
    if _database is None:
        raise RuntimeError("Database not initialized — call init_database() first")
    return _database


def close_database():
    global _database
    if _database:
        _database.close()
        _database = None


def init_store(db: Optional[Database | PostgresDatabase] = None) -> SqlEntityStore:
    global _store
    if _store is None:
        _store = SqlEntityStore(db=db or get_db())
    return _store


def get_store() -> SqlEntityStore:
    if _store is None:
        raise RuntimeError("EntityStore not initialized — call init_store() first")
    return _store
