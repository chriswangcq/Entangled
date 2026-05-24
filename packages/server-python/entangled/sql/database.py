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


class PostgresCursor:
    """Small cursor wrapper that releases pooled connections after reads."""

    def __init__(self, cursor: Any, cleanup: Callable[[], None] | None = None) -> None:
        self._cursor = cursor
        self._cleanup = cleanup
        self._cleaned_up = False

    @property
    def rowcount(self) -> int:
        return int(getattr(self._cursor, "rowcount", -1))

    @property
    def lastrowid(self) -> Any:
        return getattr(self._cursor, "lastrowid", None)

    def fetchone(self):
        try:
            return self._cursor.fetchone()
        finally:
            self._close_cursor_only()
            self._cleanup_once()

    def fetchall(self):
        try:
            return self._cursor.fetchall()
        finally:
            self._close_cursor_only()
            self._cleanup_once()

    def close(self) -> None:
        try:
            self._close_cursor_only()
        finally:
            self._cleanup_once()

    def __iter__(self):
        try:
            yield from self._cursor
        finally:
            self._close_cursor_only()
            self._cleanup_once()

    def _close_cursor_only(self) -> None:
        close = getattr(self._cursor, "close", None)
        if close is not None:
            close()

    def _cleanup_once(self) -> None:
        if self._cleaned_up or self._cleanup is None:
            return
        self._cleaned_up = True
        self._cleanup()


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

    def in_transaction(self) -> bool:
        return int(getattr(self._local, "transaction_depth", 0) or 0) > 0

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
        try:
            if init_schema_func:
                init_schema_func(self)
        except Exception:
            self.close()
            raise
        logger.info("[DB] Postgres connected and initialized")

    def close(self) -> None:
        if getattr(self._local, "conn", None) is not None:
            if self._pool is not None and hasattr(self._pool, "putconn"):
                self._pool.putconn(self._local.conn)
            else:
                close = getattr(self._local.conn, "close", None)
                if close is not None:
                    close()
            self._local.conn = None
        if self._pool is not None:
            self._pool.close()
            self._pool = None
        self._initialized = False
        logger.info("[DB] Postgres connection pool closed")

    def _get_thread_connection(self):
        if not self._initialized or self._pool is None:
            raise RuntimeError("Database not connected")
        conn = getattr(self._local, "conn", None)
        if conn is None or bool(getattr(conn, "closed", False)):
            self._local.conn = self._checkout_connection()
        return self._local.conn

    def _checkout_connection(self):
        if not self._initialized or self._pool is None:
            raise RuntimeError("Database not connected")
        if hasattr(self._pool, "getconn"):
            return self._pool.getconn()
        return self._pool.connection()

    def _release_thread_connection(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            return
        self._local.conn = None
        self._release_connection(conn)

    def _release_connection(self, conn: Any) -> None:
        if self._pool is not None and hasattr(self._pool, "putconn"):
            self._pool.putconn(conn)
            return
        close = getattr(conn, "close", None)
        if close is not None:
            close()

    def _release_read_connection(self, conn: Any, *, commit: bool = False) -> None:
        try:
            if commit:
                conn.commit()
            else:
                rollback = getattr(conn, "rollback", None)
                if rollback is not None:
                    rollback()
        finally:
            self._release_connection(conn)

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
        converted_sql = self._convert_placeholders(sql)
        if self.in_transaction():
            conn = self._get_thread_connection()
            return conn.execute(converted_sql, params)

        conn = self._checkout_connection()
        mutating_returning = self._is_mutating_returning(sql)
        try:
            cursor = conn.execute(converted_sql, params)
            if self._returns_rows(sql):
                return PostgresCursor(
                    cursor,
                    cleanup=lambda: self._release_read_connection(
                        conn,
                        commit=mutating_returning,
                    ),
                )
            conn.commit()
            self._release_connection(conn)
            return PostgresCursor(cursor)
        except Exception:
            rollback = getattr(conn, "rollback", None)
            if rollback is not None:
                rollback()
            self._release_connection(conn)
            raise

    def executemany(self, sql: str, params_list: List[tuple]):
        conn = self._get_thread_connection() if self.in_transaction() else self._checkout_connection()
        cursor = conn.cursor()
        try:
            cursor.executemany(self._convert_placeholders(sql), params_list)
            if not self.in_transaction():
                conn.commit()
                self._release_connection(conn)
            return PostgresCursor(cursor)
        except Exception:
            if not self.in_transaction():
                rollback = getattr(conn, "rollback", None)
                if rollback is not None:
                    rollback()
                self._release_connection(conn)
            raise

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
            conn = getattr(self._local, "conn", None)
            if conn is not None:
                conn.commit()

    def rollback(self) -> None:
        if self._initialized:
            conn = getattr(self._local, "conn", None)
            if conn is not None:
                conn.rollback()

    def vacuum(self) -> None:
        conn = self._get_thread_connection()
        old_autocommit = getattr(conn, "autocommit", False)
        conn.autocommit = True
        try:
            conn.execute("VACUUM")
        finally:
            conn.autocommit = old_autocommit

    def insert_returning_id(self, sql: str, params: tuple = ()) -> Any:
        conn = self._get_thread_connection() if self.in_transaction() else self._checkout_connection()
        try:
            cursor = conn.execute(self._convert_placeholders(sql), params)
            row = cursor.fetchone()
            if not self.in_transaction():
                conn.commit()
                self._release_connection(conn)
            if not row:
                return None
            if isinstance(row, dict):
                return next(iter(row.values()))
            return row[0]
        except Exception:
            if not self.in_transaction():
                rollback = getattr(conn, "rollback", None)
                if rollback is not None:
                    rollback()
                self._release_connection(conn)
            raise

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

    def table_column_types(self, table: str) -> Dict[str, str]:
        rows = self.fetchall(
            """
            SELECT column_name AS name, data_type, udt_name
              FROM information_schema.columns
             WHERE table_schema = current_schema()
               AND table_name = ?
            """,
            (table,),
        )
        result: Dict[str, str] = {}
        for row in rows:
            data_type = str(row.get("data_type") or "").lower()
            udt_name = str(row.get("udt_name") or "").lower()
            result[str(row["name"])] = self._normalize_column_type(data_type, udt_name)
        return result

    @contextmanager
    def transaction(self, lock_type: str = "global", **lock_kwargs):
        if not self._initialized:
            raise RuntimeError("Database not connected")
        conn = self._get_thread_connection()
        current_depth = int(getattr(self._local, "transaction_depth", 0) or 0)
        try:
            if current_depth == 0:
                conn.execute("BEGIN")
            self._local.transaction_depth = current_depth + 1
            self._acquire_advisory_lock(conn, lock_type, **lock_kwargs)
            yield self
            if current_depth == 0:
                conn.commit()
        except Exception:
            if current_depth == 0:
                conn.rollback()
            raise
        finally:
            self._local.transaction_depth = current_depth
            if current_depth == 0:
                self._release_thread_connection()

    @contextmanager
    def get_connection(self, lock_type: str = "global", **lock_kwargs):
        with self.transaction(lock_type, **lock_kwargs):
            yield self

    @staticmethod
    def _returns_rows(sql: str) -> bool:
        stripped = sql.lstrip().lower()
        return stripped.startswith(("select", "with", "show", "explain")) or " returning " in stripped

    @staticmethod
    def _is_mutating_returning(sql: str) -> bool:
        stripped = sql.lstrip().lower()
        return (
            stripped.startswith(("insert", "update", "delete"))
            and " returning " in stripped
        )

    @staticmethod
    def _normalize_column_type(data_type: str, udt_name: str = "") -> str:
        token = (data_type or udt_name or "").lower().strip()
        if token in {"integer", "bigint", "smallint"}:
            return "bigint" if token == "bigint" else token
        if token in {"character varying", "varchar", "text"}:
            return "text"
        if token in {"json", "jsonb"}:
            return token
        if token in {"boolean", "bool"}:
            return "boolean"
        if token in {"double precision", "float8"}:
            return "double precision"
        if token in {"bytea"}:
            return "bytea"
        return token


def create_database(
    *,
    postgres_dsn: str = "",
    postgres_dsn_file: Path | None = None,
) -> PostgresDatabase:
    return PostgresDatabase(dsn=postgres_dsn, dsn_file=postgres_dsn_file)
