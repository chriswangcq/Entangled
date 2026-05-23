"""
Database Connection and Management

Provides the synchronous Postgres storage boundary used by Entangled server.
"""

import threading
import logging
import hashlib
from pathlib import Path
from typing import Optional, Any, List, Dict, Callable
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class PostgresDatabase:
    """Postgres implementation of Entangled's minimal database boundary.

    This adapter intentionally preserves the small surface used by
    ``SqlEntityStore`` while later migration children port generated SQL to
    first-class Postgres dialects.
    """

    backend_name = "postgres"

    def __init__(
        self,
        *,
        dsn: str = "",
        dsn_file: Path | None = None,
        min_size: int = 1,
        max_size: int = 10,
        pool_factory: Callable[..., Any] | None = None,
    ) -> None:
        self.dsn = dsn
        self.dsn_file = dsn_file
        self.min_size = min_size
        self.max_size = max_size
        self._pool_factory = pool_factory
        self._pool = None
        self._initialized = False
        self._local = threading.local()
        self._init_schema_func = None

    def _resolve_dsn(self) -> str:
        if self.dsn:
            return self.dsn
        if self.dsn_file is None:
            raise ValueError("Postgres backend requires dsn or dsn_file")
        dsn = self.dsn_file.read_text(encoding="utf-8").strip()
        if not dsn:
            raise ValueError(f"Postgres DSN file is empty: {self.dsn_file}")
        return dsn

    def _create_pool(self, dsn: str) -> Any:
        if self._pool_factory is not None:
            return self._pool_factory(
                dsn,
                min_size=self.min_size,
                max_size=self.max_size,
            )
        try:
            from psycopg.rows import dict_row
            from psycopg_pool import ConnectionPool
        except ImportError as exc:
            raise RuntimeError(
                "Postgres backend requires psycopg and psycopg_pool; install "
                "the entangled app dependencies with Postgres support."
            ) from exc
        return ConnectionPool(
            conninfo=dsn,
            kwargs={"row_factory": dict_row},
            min_size=self.min_size,
            max_size=self.max_size,
            open=True,
        )

    def connect(self, init_schema_func=None) -> None:
        if self._initialized:
            return
        dsn = self._resolve_dsn()
        logger.info("[DB] Connecting to Postgres")
        self._init_schema_func = init_schema_func
        self._pool = self._create_pool(dsn)
        self._initialized = True
        if init_schema_func:
            init_schema_func(self)
        logger.info("[DB] Postgres connected and initialized")

    def close(self) -> None:
        if hasattr(self._local, "conn") and self._local.conn is not None:
            if self._pool is not None and hasattr(self._pool, "putconn"):
                self._pool.putconn(self._local.conn)
            else:
                self._local.conn.close()
            self._local.conn = None
        if self._pool is not None:
            self._pool.close()
            self._pool = None
        self._initialized = False
        logger.info("[DB] Postgres connection pool closed")

    def _get_thread_connection(self):
        if not self._initialized or self._pool is None:
            raise RuntimeError("Database not connected")
        if not hasattr(self._local, "conn") or self._local.conn is None:
            if hasattr(self._pool, "getconn"):
                self._local.conn = self._pool.getconn()
            else:
                self._local.conn = self._pool.connection()
        return self._local.conn

    @staticmethod
    def _convert_placeholders(sql: str) -> str:
        """Convert qmark placeholders/literals into psycopg-safe SQL."""
        out: list[str] = []
        in_single = False
        in_double = False
        i = 0
        while i < len(sql):
            ch = sql[i]
            if ch == "'" and not in_double:
                out.append(ch)
                if in_single and i + 1 < len(sql) and sql[i + 1] == "'":
                    out.append(sql[i + 1])
                    i += 2
                    continue
                in_single = not in_single
                i += 1
                continue
            if ch == '"' and not in_single:
                in_double = not in_double
                out.append(ch)
                i += 1
                continue
            if ch == "?" and not in_single and not in_double:
                out.append("%s")
            elif ch == "%":
                out.append("%%")
            else:
                out.append(ch)
            i += 1
        return "".join(out)

    @staticmethod
    def advisory_lock_key(lock_type: str, resource_id: str | None = None) -> int:
        token = f"{lock_type}:{resource_id or 'global'}"
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        value = int.from_bytes(digest[:8], "big", signed=False)
        if value >= 2**63:
            value -= 2**64
        return value

    def _acquire_advisory_lock(self, conn: Any, lock_type: str, **lock_kwargs) -> None:
        timeout = lock_kwargs.get("timeout")
        if timeout is not None:
            lock_timeout_ms = max(int(float(timeout) * 1000), 1)
            conn.execute("SELECT set_config(%s, %s, true)", ("lock_timeout", f"{lock_timeout_ms}ms"))
        key = self.advisory_lock_key(lock_type, lock_kwargs.get("resource_id") or "")
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (key,))

    def execute(self, sql: str, params: tuple = ()):
        conn = self._get_thread_connection()
        return conn.execute(self._convert_placeholders(sql), params)

    def executemany(self, sql: str, params_list: List[tuple]):
        conn = self._get_thread_connection()
        cursor = conn.cursor()
        cursor.executemany(self._convert_placeholders(sql), params_list)
        return cursor

    def fetchone(self, sql: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
        cursor = self.execute(sql, params)
        row = cursor.fetchone()
        return dict(row) if row else None

    def fetchall(self, sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
        cursor = self.execute(sql, params)
        return [dict(row) for row in cursor.fetchall()]

    fetch_all = fetchall

    def commit(self) -> None:
        if self._initialized:
            self._get_thread_connection().commit()

    def rollback(self) -> None:
        if self._initialized:
            self._get_thread_connection().rollback()

    def vacuum(self) -> None:
        conn = self._get_thread_connection()
        old_autocommit = getattr(conn, "autocommit", False)
        conn.autocommit = True
        try:
            conn.execute("VACUUM")
        finally:
            conn.autocommit = old_autocommit

    def insert_returning_id(self, sql: str, params: tuple = ()) -> Any:
        cursor = self.execute(sql, params)
        row = cursor.fetchone()
        if not row:
            return None
        if isinstance(row, dict):
            return next(iter(row.values()))
        return row[0]

    def table_columns(self, table: str) -> List[str]:
        rows = self.fetchall(
            """
            SELECT column_name AS name
              FROM information_schema.columns
             WHERE table_schema = current_schema()
               AND table_name = ?
             ORDER BY ordinal_position
            """,
            (table,),
        )
        return [r["name"] for r in rows]

    @contextmanager
    def transaction(self, lock_type: str = "global", **lock_kwargs):
        conn = self._get_thread_connection()
        try:
            conn.execute("BEGIN")
            self._acquire_advisory_lock(conn, lock_type, **lock_kwargs)
            yield self
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    @contextmanager
    def get_connection(self, lock_type: str = "global", **lock_kwargs):
        with self.transaction(lock_type, **lock_kwargs):
            yield self


def create_database(
    *,
    postgres_dsn: str = "",
    postgres_dsn_file: Path | None = None,
) -> PostgresDatabase:
    return PostgresDatabase(dsn=postgres_dsn, dsn_file=postgres_dsn_file)
