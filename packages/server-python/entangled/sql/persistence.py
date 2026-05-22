"""Sync version persistence — save/load SyncRegistry versions to SQLite."""

from __future__ import annotations

import logging
from typing import Callable, Dict

from ..server.sync import SyncRegistry

logger = logging.getLogger(__name__)


def _is_postgres(db) -> bool:
    return getattr(db, "backend_name", "sqlite") == "postgres"


def sync_versions_create_table_sql(db) -> str:
    if _is_postgres(db):
        return """
            CREATE TABLE IF NOT EXISTS entangled_sync_versions (
                state_key text PRIMARY KEY,
                version bigint NOT NULL DEFAULT 0 CHECK (version >= 0)
            )
        """
    return """
            CREATE TABLE IF NOT EXISTS entangled_sync_versions (
                state_key TEXT PRIMARY KEY,
                version INTEGER NOT NULL DEFAULT 0
            )
        """


def sync_version_upsert_sql(db) -> str:
    if _is_postgres(db):
        return (
            "INSERT INTO entangled_sync_versions (state_key, version) "
            "VALUES (?, ?) "
            "ON CONFLICT(state_key) DO UPDATE SET "
            "version = GREATEST(entangled_sync_versions.version, excluded.version)"
        )
    return (
        "INSERT INTO entangled_sync_versions (state_key, version) "
        "VALUES (?, ?) "
        "ON CONFLICT(state_key) DO UPDATE SET version = excluded.version"
    )


def ensure_sync_versions_table(db) -> None:
    """Create the entangled_sync_versions table if it doesn't exist."""
    with db.transaction("global"):
        db.execute(sync_versions_create_table_sql(db))


def load_all_sync_versions(db, registry: SyncRegistry) -> None:
    """Hydrate SyncRegistry from the entangled_sync_versions table."""
    try:
        rows = db.fetchall("SELECT state_key, version FROM entangled_sync_versions")
        if not rows:
            return
        versions = {r["state_key"]: r["version"] for r in rows}
        registry.hydrate_versions(versions)
        logger.info("[SyncPersistence] Loaded %d version entries", len(versions))
    except Exception as e:
        logger.warning("[SyncPersistence] Failed to load versions: %s", e)


def load_all_sync_versions_dict(db) -> Dict[str, int]:
    """Load all sync versions as a dict (for Gateway compatibility)."""
    try:
        rows = db.fetchall("SELECT state_key, version FROM entangled_sync_versions")
        return {r["state_key"]: r["version"] for r in rows} if rows else {}
    except Exception as e:
        logger.warning("[SyncPersistence] Failed to load versions: %s", e)
        return {}


def make_version_bump_handler(db) -> Callable[[str, int], None]:
    """Return a callback for SyncRegistry.on_version_bump that persists to DB."""

    def _on_version_bump(state_key: str, version: int) -> None:
        try:
            with db.transaction("global"):
                db.execute(sync_version_upsert_sql(db), (state_key, version))
        except Exception as e:
            logger.warning("[SyncPersistence] Failed to persist version for %s: %s", state_key, e)

    return _on_version_bump
