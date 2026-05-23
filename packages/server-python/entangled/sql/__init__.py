"""
Entangled SQL storage layer.

Provides a batteries-included SQL storage layer on top of the Entangled
sync engine.  Any project can get a full CRUD + sync + real-time push
service with:

    from entangled.sql import SqlEntityDef, SqlEntityStore, PostgresDatabase, F

    db = PostgresDatabase(dsn_file=Path("/opt/novaic/postgres/secrets/novaic_entangled_dsn"))
    db.connect()

    store = SqlEntityStore(db=db)
    store.register(my_entity_def)
    store.ensure_schema(my_entity_def)
"""

from .field_def import FieldDef, FieldKind, F
from .entity_def import SqlEntityDef
from .entity_store import SqlEntityStore
from .database import PostgresDatabase, create_database
from .locks import DatabaseLockManager, FIFOLock, ShardedFIFOLock
from .persistence import load_all_sync_versions, make_version_bump_handler

__all__ = [
    # Field system
    "FieldDef",
    "FieldKind",
    "F",
    # Entity definition
    "SqlEntityDef",
    # Entity store
    "SqlEntityStore",
    # Database
    "PostgresDatabase",
    "create_database",
    "DatabaseLockManager",
    "FIFOLock",
    "ShardedFIFOLock",
    # Sync persistence
    "load_all_sync_versions",
    "make_version_bump_handler",
]
