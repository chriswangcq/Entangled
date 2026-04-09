"""
Database Connection and Management

Provides synchronous SQLite connection with WAL mode support.
"""

import sqlite3
import threading
import logging
from pathlib import Path
from typing import Optional, Any, List, Dict
from contextlib import contextmanager

from .locks import DatabaseLockManager

logger = logging.getLogger(__name__)


class Database:
    """
    Synchronous SQLite database wrapper with transaction support.

    Features:
    - WAL mode for concurrent access
    - Automatic connection management
    - Transaction support with context manager
    - Row factory for dict-like access
    - Thread-safe with FIFO locks (configurable)

    IMPORTANT: All database operations MUST use transaction() or get_connection()
    to ensure proper locking. Direct execute()/commit() calls are only safe
    within a transaction context.
    """

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._initialized = False
        self._local = threading.local()  # Thread-local storage for connections
        self._init_schema_func = None  # Store schema init function

        # Lock manager for coordinating concurrent access
        self.locks = DatabaseLockManager()

    def connect(self, init_schema_func=None):
        """
        Initialize database (schema setup).

        Args:
            init_schema_func: Optional function to initialize schema.
                             Should accept a sqlite3.Connection object.
        """
        if self._initialized:
            return

        # Ensure directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"[DB] Connecting to {self.db_path}")

        # Store schema init function for thread-local connections
        self._init_schema_func = init_schema_func

        # Create initial connection for schema setup
        conn = self._create_connection()

        # Initialize schema (if provided)
        if init_schema_func:
            init_schema_func(conn)

        self._initialized = True

        logger.info(f"[DB] Connected and initialized")

    def _create_connection(self) -> sqlite3.Connection:
        """Create a new SQLite connection with proper settings."""
        conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,  # We handle thread safety ourselves
            timeout=30.0  # Reasonable timeout for multi-process
        )
        conn.row_factory = sqlite3.Row

        # Enable WAL mode; FK off — referential integrity is the business layer's job
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("PRAGMA synchronous = NORMAL")
        # Fail fast on lock contention so the event loop doesn't get starved.
        # 3 s is enough for legitimate short transactions; long waits indicate a bug.
        conn.execute("PRAGMA busy_timeout = 3000")

        return conn

    def _get_thread_connection(self) -> sqlite3.Connection:
        """Get or create a connection for the current thread."""
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            self._local.conn = self._create_connection()
        return self._local.conn

    def close(self):
        """Close all database connections."""
        # Close thread-local connection if exists
        if hasattr(self._local, 'conn') and self._local.conn:
            self._local.conn.close()
            self._local.conn = None
        self._initialized = False
        logger.info("[DB] Connection closed")

    def vacuum(self):
        """Vacuum the database to reclaim space."""
        conn = self._get_thread_connection()
        conn.execute("VACUUM")
        logger.info("[DB] Database vacuumed")

    def execute(
        self,
        sql: str,
        params: tuple = ()
    ) -> sqlite3.Cursor:
        """Execute a SQL statement. Should be called within transaction() context."""
        if not self._initialized:
            raise RuntimeError("Database not connected")
        conn = self._get_thread_connection()
        return conn.execute(sql, params)

    def executemany(
        self,
        sql: str,
        params_list: List[tuple]
    ) -> sqlite3.Cursor:
        """Execute a SQL statement with multiple parameter sets."""
        if not self._initialized:
            raise RuntimeError("Database not connected")
        conn = self._get_thread_connection()
        return conn.executemany(sql, params_list)

    def fetchone(
        self,
        sql: str,
        params: tuple = ()
    ) -> Optional[Dict[str, Any]]:
        """Execute query and fetch one row as dict."""
        cursor = self.execute(sql, params)
        row = cursor.fetchone()
        return dict(row) if row else None

    def fetchall(
        self,
        sql: str,
        params: tuple = ()
    ) -> List[Dict[str, Any]]:
        """Execute query and fetch all rows as list of dicts."""
        cursor = self.execute(sql, params)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

    fetch_all = fetchall  # Alias for compatibility

    def commit(self):
        """Commit current transaction."""
        if self._initialized:
            conn = self._get_thread_connection()
            conn.commit()

    def rollback(self):
        """Rollback current transaction."""
        if self._initialized:
            conn = self._get_thread_connection()
            conn.rollback()

    @contextmanager
    def transaction(self, lock_type: str = "global", **lock_kwargs):
        """
        Context manager for database transactions with FIFO lock.

        Uses thread-local connections for true parallel execution.

        Args:
            lock_type (str): Lock type ("global", "message", "agent", "task", "saga")
            **lock_kwargs: Lock-specific parameters
                - resource_id (str): Resource identifier (for sharded locks)
                - timeout (float): Maximum wait time

        Usage:
            with db.transaction("agent", resource_id=agent_id):
                db.execute("INSERT ...")
                db.execute("UPDATE ...")
                # Auto-commits on exit, rolls back on exception
        """
        if not self._initialized:
            raise RuntimeError("Database not connected")

        conn = self._get_thread_connection()

        # Use specified lock type (thread-local connections enable true sharding)
        with self.locks.acquire(lock_type, **lock_kwargs):
            try:
                yield self
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    @contextmanager
    def get_connection(self, lock_type: str = "global", **lock_kwargs):
        """
        Get a connection context manager with FIFO lock.

        Uses thread-local connections for true parallel execution.

        Args:
            lock_type (str): Lock type ("global", "message", "agent", "task", "saga")
            **lock_kwargs: Lock-specific parameters
                - resource_id (str): Resource identifier (for sharded locks)
                - timeout (float): Maximum wait time

        Usage:
            with db.get_connection("agent", resource_id=agent_id) as conn:
                cursor = conn.execute("SELECT ...")
                conn.commit()
        """
        if not self._initialized:
            raise RuntimeError("Database not connected")

        # Use specified lock type (thread-local connections enable true sharding)
        with self.locks.acquire(lock_type, **lock_kwargs):
            yield self
