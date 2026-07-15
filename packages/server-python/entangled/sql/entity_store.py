"""SqlEntityStore — Postgres-backed SQL storage engine.

Inherits Entangled's EntityStore (fn-pointer dispatch) and provides
concrete SQL implementations for all CRUD + advanced operations.

This is the unified implementation that was previously duplicated in
both ``entangled-service`` and ``novaic-gateway``. Any host that needs
a SQL-backed entity store should subclass this or use it directly.
"""

from __future__ import annotations

import inspect
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from ..server.store import EntityStore as BaseStore
from .entity_def import SqlEntityDef
from .field_def import FieldDef, FieldKind
from .validation import normalize_order_by, validate_field_key_with_extras, validate_field_keys

logger = logging.getLogger(__name__)

TENANT_OWNERSHIP_MIGRATION_MAX_ITEMS = 5000
_QUARANTINE_TENANT_PREFIX = "__legacy_unowned_"
_ALLOWED_QUARANTINE_SOURCE_TENANTS = frozenset({"__legacy_unowned__"})


def _iso_now_utc() -> str:
    """ISO-8601 UTC timestamp with millisecond precision and 'Z' suffix.

    Kept in sync with `common.utils.time.utc_now_iso` in the business layer so
    that *all* NOT-NULL timestamp fields end up with one canonical wire format
    regardless of which code path wrote them. Entangled is a foundation package
    and cannot import from novaic-common, so the format literal is duplicated
    here; a cross-repo format test (tests/test_timestamp_format_parity.py)
    locks the two helpers together.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class SqlEntityStore(BaseStore):
    """SQL-backed entity store.

    Dispatches CRUD operations to SQL queries, with automatic:
    - Schema management (CREATE TABLE, ALTER TABLE ADD COLUMN)
    - Field serialization (JSON, BOOL, TIMESTAMP)
    - User scoping and key_params filtering
    - Cascading ownership via parent tuples
    - Change notification via Entangled's push system

    Usage:
        from entangled.sql import SqlEntityStore, SqlEntityDef, F, PostgresDatabase

        db = PostgresDatabase(dsn_file=Path("/opt/novaic/postgres/secrets/novaic_entangled_dsn"))
        db.connect()
        store = SqlEntityStore(db=db)
        store.register(my_def)
        store.ensure_schema(my_def)
    """

    def __init__(self, db=None):
        super().__init__([])
        self._db = db

    @property
    def db(self):
        if self._db is None:
            raise RuntimeError(
                "Database not set on SqlEntityStore. Pass db= to constructor "
                "or override the db property in a subclass."
            )
        return self._db

    def _dialect(self) -> str:
        return getattr(self.db, "backend_name", "postgres")

    def _is_postgres(self) -> bool:
        return self._dialect() == "postgres"

    def _timestamp_update_expr(self) -> str:
        return "to_char(timezone('UTC', now()), 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')"

    @staticmethod
    def _should_auto_set_updated_at(defn: SqlEntityDef, row: Dict[str, Any]) -> bool:
        """Auto-touch updated_at only when the caller did not set it explicitly."""
        return defn.tracks_updated_at_column and "updated_at" not in row

    def _insert_sql(self, table: str, cols: List[str], *, returning: str = "") -> str:
        ph = ", ".join("?" for _ in cols)
        sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({ph})"
        if returning:
            sql += f" RETURNING {returning}"
        return sql

    def _insert_row(
        self,
        defn: SqlEntityDef,
        row: Dict[str, Any],
        *,
        is_auto_int: bool,
    ) -> None:
        cols = list(row.keys())
        returning = defn.id_field if is_auto_int and self._is_postgres() else ""
        sql = self._insert_sql(defn.table, cols, returning=returning)
        values = tuple(row[c] for c in cols)
        if returning and hasattr(self.db, "insert_returning_id"):
            returned_id = self.db.insert_returning_id(sql, values)
            if returned_id is not None:
                row[defn.id_field] = returned_id
            return
        cur = self.db.execute(sql, values)
        if is_auto_int and getattr(cur, "lastrowid", None):
                row[defn.id_field] = cur.lastrowid

    def _rowid_column(self) -> str:
        return "entangled_rowid"

    def _normalize_order_by(self, defn: SqlEntityDef, order_by: str | None) -> str:
        normalized = normalize_order_by(
            defn,
            order_by,
            extra_fields=[self._rowid_column()],
        )
        parts = []
        for part in normalized.split(","):
            tokens = part.strip().split()
            if tokens and tokens[0] == "rowid":
                tokens[0] = "entangled_rowid"
            parts.append(" ".join(tokens))
        return ", ".join(parts)

    # ── Registration & Schema ─────────────────────────────────────────────

    def register(self, entity_def: SqlEntityDef) -> None:
        """Register entity definition and bind canonical SQL operations.

        All fn pointers are registered so that both:
          - SqlEntityStore's overridden methods (list, list_stream, etc.) work
          - Entangled's fn-pointer dispatch (defn.list_fn, etc.) also works
        """
        defn = entity_def
        if defn.list_fn is None:
            defn.list_fn = lambda store, uid, params, **kw: self.list(
                defn.name, uid, params=params, **kw
            )
        if defn.list_stream_fn is None:
            defn.list_stream_fn = lambda store, uid, params, **kw: self.list_stream(
                defn.name, uid, params=params, **kw
            )
        if defn.exists_before_fn is None:
            defn.exists_before_fn = lambda store, uid, oid, params: self.exists_before(
                defn.name, uid, oid, params=params
            )
        if defn.get_fn is None:
            defn.get_fn = lambda store, uid, eid, params: self._sql_get(defn, uid, eid, params=params)
        if defn.create_fn is None:
            defn.create_fn = lambda store, uid, params, data: self._sql_create(defn, uid, data, params=params)
        if defn.update_fn is None:
            defn.update_fn = lambda store, uid, eid, data, params: self._sql_update(defn, uid, eid, data, params=params)
        if defn.delete_fn is None:
            defn.delete_fn = lambda store, uid, eid, params: self._sql_delete(defn, uid, eid, params=params)
        if defn.upsert_fn is None:
            defn.upsert_fn = lambda store, uid, eid, data, params: self._sql_upsert(defn, uid, eid, data, params=params)

        super().register(defn)
        logger.debug("[SqlEntityStore] registered: %s → %s", defn.name, defn.table)

    def ensure_schema(self, entity_def: SqlEntityDef) -> None:
        """Idempotent schema management: CREATE TABLE + ALTER TABLE ADD COLUMN."""
        if not entity_def.fields:
            return
        with self.db.transaction("global"):
            self.ensure_schema_unlocked(entity_def)

    def ensure_schema_unlocked(self, entity_def: SqlEntityDef) -> None:
        """Idempotent schema management inside an already-held transaction."""
        if not entity_def.fields:
            return
        dialect = getattr(self.db, "backend_name", "postgres")
        self.db.execute(entity_def.create_table_sql(dialect=dialect))
        # PR-21 (2026-04-20): ALTER MUST run before index_sqls.
        # Previously the order was reversed, which silently worked for
        # every prior migration because new columns happened to be
        # index=False. The moment someone adds a new column with
        # index=True (e.g. chat_messages.lifecycle), CREATE INDEX
        # fires against the not-yet-added column and fails with
        # ``no such column`` — the exception escapes ensure_schema,
        # the ALTER never runs, and the table is left permanently
        # half-migrated. Swap the order so every ALTER commits first
        # and index creation always sees the final column set.
        existing_cols = self.db.table_columns(entity_def.table)
        for alter_sql in entity_def.alter_add_column_sqls(existing_cols, dialect=dialect):
            logger.info("[SqlEntityStore] Migrating: %s", alter_sql)
            self.db.execute(alter_sql)
        for migrate_sql in self._type_migration_sqls(entity_def, dialect=dialect):
            logger.info("[SqlEntityStore] Reconciling column type: %s", migrate_sql)
            self.db.execute(migrate_sql)
        for idx_sql in entity_def.index_sqls(dialect=dialect):
            self.db.execute(idx_sql)

    def _type_migration_sqls(self, entity_def: SqlEntityDef, *, dialect: str) -> list[str]:
        """Generate explicit schema-drift migrations for compatible type changes."""
        normalized = dialect.lower().strip()
        if normalized != "postgres" or not hasattr(self.db, "table_column_types"):
            return []
        existing_types = self.db.table_column_types(entity_def.table)
        stmts: list[str] = []
        for field in entity_def.fields:
            actual = existing_types.get(field.name)
            if not actual:
                continue
            expected = field.sql_type_for(normalized)
            if self._column_type_matches(expected, actual):
                continue
            conversion = self._compatible_type_migration_sql(entity_def.table, field, actual, expected)
            if conversion is None:
                raise RuntimeError(
                    "Incompatible Postgres schema drift for "
                    f"{entity_def.table}.{field.name}: actual={actual}, expected={expected}"
                )
            stmts.extend(conversion)
        return stmts

    @staticmethod
    def _column_type_matches(expected: str, actual: str) -> bool:
        aliases = {
            "bigint": {"bigint"},
            "boolean": {"boolean"},
            "text": {"text", "character varying"},
            "jsonb": {"jsonb"},
            "double precision": {"double precision"},
            "bytea": {"bytea"},
        }
        return actual in aliases.get(expected, {expected})

    @staticmethod
    def _compatible_type_migration_sql(
        table: str,
        field: FieldDef,
        actual: str,
        expected: str,
    ) -> list[str] | None:
        if field.kind == FieldKind.BOOL and expected == "boolean" and actual in {"bigint", "integer", "smallint"}:
            stmts = [
                (
                    f"ALTER TABLE {table} ALTER COLUMN {field.name} TYPE boolean "
                    f"USING CASE WHEN {field.name} IS NULL THEN NULL "
                    f"WHEN {field.name} = 0 THEN false ELSE true END;"
                )
            ]
            if field.default is not None:
                default = "true" if bool(field.default) else "false"
                stmts.append(f"ALTER TABLE {table} ALTER COLUMN {field.name} SET DEFAULT {default};")
            if not field.nullable:
                stmts.append(f"ALTER TABLE {table} ALTER COLUMN {field.name} SET NOT NULL;")
            return stmts
        if field.kind == FieldKind.JSON and expected == "jsonb" and actual == "text":
            stmts = [
                (
                    f"ALTER TABLE {table} ALTER COLUMN {field.name} TYPE jsonb "
                    f"USING CASE WHEN {field.name} IS NULL OR btrim({field.name}) = '' "
                    f"THEN NULL ELSE {field.name}::jsonb END;"
                )
            ]
            if field.default is not None:
                encoded = field.default if isinstance(field.default, str) else json.dumps(field.default, ensure_ascii=False)
                escaped = encoded.replace("'", "''")
                stmts.append(f"ALTER TABLE {table} ALTER COLUMN {field.name} SET DEFAULT '{escaped}'::jsonb;")
            if not field.nullable:
                stmts.append(f"ALTER TABLE {table} ALTER COLUMN {field.name} SET NOT NULL;")
            return stmts
        return None

    def ensure_all_schemas(self) -> None:
        """Run ensure_schema for all registered entities that have fields."""
        for defn in self._defs.values():
            if defn.fields:
                self.ensure_schema(defn)
        # NOTE: PR-31 state-transition log tables are created eagerly in
        # app.factory.lifespan (not here) — ensure_all_schemas has no live
        # caller at runtime, and Entangled schema registration is driven
        # dynamically via POST /v1/schema/register by upstream services.

    def get_def(self, entity: str) -> SqlEntityDef:
        defn = self._defs.get(entity)
        if defn is None:
            raise KeyError(f"Entity '{entity}' not registered. Available: {list(self._defs.keys())}")
        return defn

    @property
    def entities(self) -> List[str]:
        return list(self._defs.keys())

    def get_all_defs(self) -> List[SqlEntityDef]:
        return list(self._defs.values())

    def get_schema(self) -> List[Dict[str, Any]]:
        result = []
        for defn in self._defs.values():
            if hasattr(defn, "to_schema_dict"):
                result.append(defn.to_schema_dict())
            else:
                result.append({
                    "name": defn.name,
                    "keyParams": defn.key_params,
                    "syncType": defn.sync_type,
                    "syncLimit": defn.sync_limit,
                    "subscriptionMode": getattr(defn, "subscription_mode", "lazy"),
                })
        return result

    # ── CRUD ─────────────────────────────────────────────────────────────

    def list(
        self,
        entity: str,
        user_id: str,
        *,
        params: Optional[Dict[str, str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
        skip_default_not_in: bool = False,
    ) -> List[Dict[str, Any]]:
        defn = self.get_def(entity)
        where, values = self._scope_where(defn, user_id, params)

        if filters:
            validate_field_keys(defn, filters.keys(), label="filter field")
            for k, v in filters.items():
                where += f" AND {k} = ?"
                values.append(v)

        if not skip_default_not_in and defn.default_not_in_filters:
            validate_field_keys(defn, defn.default_not_in_filters.keys(), label="default_not_in_filter")
            for k, vlist in defn.default_not_in_filters.items():
                if vlist:
                    placeholders = ",".join(["?"] * len(vlist))
                    where += f" AND {k} NOT IN ({placeholders})"
                    values.extend(vlist)

        order = self._normalize_order_by(defn, order_by or defn.default_order)
        sql = f"SELECT * FROM {defn.table} WHERE {where} ORDER BY {order}"
        if limit:
            sql += f" LIMIT {limit}"
        rows = self.db.fetchall(sql, tuple(values))
        return [self._out(defn, r) for r in rows]

    def list_stream(
        self,
        entity: str,
        user_id: str,
        *,
        params: Optional[Dict[str, str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        in_filters: Optional[Dict[str, List[Any]]] = None,
        not_in_filters: Optional[Dict[str, List[Any]]] = None,
        before_id: Optional[str] = None,
        after_id: Optional[str] = None,
        limit: int = 50,
        order_by: Optional[str] = None,
        cursor_field: Optional[str] = None,
        skip_default_not_in: bool = False,
    ) -> List[Dict[str, Any]]:
        """Cursor-based backward pagination for stream entities."""
        defn = self.get_def(entity)
        where, values = self._scope_where(defn, user_id, params)

        if filters:
            validate_field_keys(defn, filters.keys(), label="filter field")
            for k, v in filters.items():
                where += f" AND {k} = ?"
                values.append(v)

        if in_filters:
            validate_field_keys(defn, in_filters.keys(), label="in_filter field")
            for k, vlist in in_filters.items():
                if not vlist:
                    continue
                placeholders = ",".join(["?"] * len(vlist))
                where += f" AND {k} IN ({placeholders})"
                values.extend(vlist)

        if not_in_filters:
            validate_field_keys(defn, not_in_filters.keys(), label="not_in_filter field")
            for k, vlist in not_in_filters.items():
                if not vlist:
                    continue
                placeholders = ",".join(["?"] * len(vlist))
                where += f" AND {k} NOT IN ({placeholders})"
                values.extend(vlist)

        if not skip_default_not_in and defn.default_not_in_filters:
            validate_field_keys(defn, defn.default_not_in_filters.keys(), label="default_not_in_filter")
            for k, vlist in defn.default_not_in_filters.items():
                if vlist:
                    placeholders = ",".join(["?"] * len(vlist))
                    where += f" AND {k} NOT IN ({placeholders})"
                    values.extend(vlist)

        rowid_col = self._rowid_column()
        order_by = self._normalize_order_by(defn, order_by or defn.default_order or f"{rowid_col} DESC")
        cursor_field = cursor_field or order_by.split(",")[0].strip().split()[0]
        validate_field_key_with_extras(
            defn,
            cursor_field,
            label="cursor field",
            extra_fields=[rowid_col],
        )
        if before_id:
            ref = self.db.fetchone(
                f"SELECT {cursor_field} AS _cf, {rowid_col} AS _rid FROM {defn.table} WHERE {defn.id_field} = ?",
                (before_id,),
            )
            if ref:
                where += f" AND ({cursor_field} < ? OR ({cursor_field} = ? AND {rowid_col} < ?))"
                values.extend([ref["_cf"], ref["_cf"], ref["_rid"]])
        elif after_id:
            where += f" AND {defn.id_field} > ?"
            values.append(after_id)

        sql = f"SELECT * FROM {defn.table} WHERE {where} ORDER BY {order_by} LIMIT ?"
        values.append(limit)
        rows = self.db.fetchall(sql, tuple(values))
        return [self._out(defn, r) for r in rows]

    def exists_before(
        self,
        entity: str,
        user_id: str,
        before_id: str,
        *,
        params: Optional[Dict[str, str]] = None,
    ) -> bool:
        """Check if any row exists before the given cursor (SELECT EXISTS)."""
        defn = self.get_def(entity)
        where, values = self._scope_where(defn, user_id, params)

        if defn.default_not_in_filters:
            validate_field_keys(defn, defn.default_not_in_filters.keys(), label="default_not_in_filter")
            for k, vlist in defn.default_not_in_filters.items():
                if vlist:
                    placeholders = ",".join(["?"] * len(vlist))
                    where += f" AND {k} NOT IN ({placeholders})"
                    values.extend(vlist)

        raw_order = defn.default_order or "created_at"
        rowid_col = self._rowid_column()
        raw_order = self._normalize_order_by(defn, raw_order)
        cursor_field = raw_order.split(",")[0].strip().split()[0]
        validate_field_key_with_extras(
            defn,
            cursor_field,
            label="cursor field",
            extra_fields=[rowid_col],
        )
        ref = self.db.fetchone(
            f"SELECT {cursor_field} AS _cf, {rowid_col} AS _rid FROM {defn.table} WHERE {defn.id_field} = ?",
            (before_id,),
        )
        if not ref:
            return False
        where += f" AND ({cursor_field} < ? OR ({cursor_field} = ? AND {rowid_col} < ?))"
        values.extend([ref["_cf"], ref["_cf"], ref["_rid"]])
        row = self.db.fetchone(
            f"SELECT EXISTS(SELECT 1 FROM {defn.table} WHERE {where}) AS has_more",
            tuple(values),
        )
        return bool(row and row["has_more"])

    # ── SQL primitives ────────────────────────────────────────────────────

    def _sql_get(self, defn: SqlEntityDef, user_id: str, entity_id: str,
                 *, params: Optional[Dict[str, str]] = None,
                 include_hidden: bool = False) -> Optional[Dict[str, Any]]:
        where, values = self._scope_where(defn, user_id, params)
        where += f" AND {defn.id_field} = ?"
        values.append(entity_id)
        row = self.db.fetchone(f"SELECT * FROM {defn.table} WHERE {where}", tuple(values))
        return self._out(defn, row, include_hidden=include_hidden) if row else None

    def _sql_create(self, defn: SqlEntityDef, user_id: str, data: Dict[str, Any],
                    *, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        row = self._in(defn, data)
        if defn.user_scoped:
            row["user_id"] = user_id
        if params:
            for kp in defn.key_params:
                if kp in params and kp not in row:
                    row[kp] = params[kp]
        id_f_def = defn.field_map.get(defn.id_field) if defn.fields else None
        is_auto_int = id_f_def and id_f_def.kind.name == "INTEGER"
        res_id = row.get(defn.id_field, "")
        if not res_id and not is_auto_int:
            # PR-40 (no silent failure): this path previously had a
            # two-step fallback — (a) coerce ``params[key_params[0]]``
            # into the primary key, then (b) mint ``uuid.uuid4().hex``.
            # Both steps violate fail-fast:
            #   (a) for stream entities (``messages`` / ``subagents`` /
            #       ``agent-memory`` — ``id_field != key_params[0]``)
            #       coerced the scope key into a primary key and
            #       collided UNIQUE on every second insert (prod
            #       symptom: ``chat_reply`` stuck after one reply);
            #   (b) silent uuid minting hid "caller forgot to mint an
            #       id" bugs forever — the insert succeeded but the
            #       caller's own id-tracking logic was wrong.
            # Singleton entities (``id_field == key_params[0]``, e.g.
            # ``agent-tools`` / ``agent-state`` / ``agent-binding``)
            # already populate ``row[id_field]`` above via the
            # scope-key-copy loop, so they reach this point with
            # ``res_id`` already truthy and do NOT trip this guard.
            # Stream/list entities MUST provide their own id (see
            # ``business/message_actions._store_add_message``,
            # ``gateway/files/registry.py:register_file``, etc.).
            raise ValueError(
                f"missing required '{defn.id_field}' on entity="
                f"'{defn.name}': caller must provide a value. "
                f"Entangled does not mint ids for non-auto-int "
                f"primary keys (PR-40 fail-fast)."
            )

        self._apply_defaults(defn, row)
        self._check_required(defn, row)
        with self.db.transaction(defn.lock_type, resource_id=res_id or ""):
            self._assert_parent_owned(defn, user_id, row)
            self._insert_row(defn, row, is_auto_int=bool(is_auto_int))
            if is_auto_int and row.get(defn.id_field):
                res_id = str(row[defn.id_field])
        entity_id = str(row.get(defn.id_field, res_id))
        result = self._sql_get(defn, user_id, entity_id, params=params)
        return result or row

    def _sql_update(self, defn: SqlEntityDef, user_id: str, entity_id: str,
                    data: Dict[str, Any], *, params: Optional[Dict[str, str]] = None) -> Optional[Dict[str, Any]]:
        row = self._in(defn, data)
        self._discard_immutable_mutations(defn, row)
        if not row:
            return self._sql_get(defn, user_id, entity_id, params=params)
        set_parts, set_vals = [], []
        for k, v in row.items():
            set_parts.append(f"{k} = ?")
            set_vals.append(v)
        if self._should_auto_set_updated_at(defn, row):
            set_parts.append(f"updated_at = {self._timestamp_update_expr()}")
        where, where_vals = self._scope_where(defn, user_id, params)
        where += f" AND {defn.id_field} = ?"
        where_vals.append(entity_id)
        sql = f"UPDATE {defn.table} SET {', '.join(set_parts)} WHERE {where}"
        with self.db.transaction(defn.lock_type, resource_id=entity_id):
            self._assert_parent_move_owned(defn, user_id, row)
            self.db.execute(sql, tuple(set_vals + where_vals))
        return self._sql_get(defn, user_id, entity_id, params=params)

    def _sql_delete(self, defn: SqlEntityDef, user_id: str, entity_id: str,
                    *, params: Optional[Dict[str, str]] = None) -> bool:
        where, values = self._scope_where(defn, user_id, params)
        where += f" AND {defn.id_field} = ?"
        values.append(entity_id)
        with self.db.transaction(defn.lock_type, resource_id=entity_id):
            cur = self.db.execute(f"DELETE FROM {defn.table} WHERE {where}", tuple(values))
        return cur.rowcount > 0

    def _sql_upsert(self, defn: SqlEntityDef, user_id: str, entity_id: str,
                    data: Dict[str, Any], *, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        row = self._in(defn, data)
        row[defn.id_field] = entity_id
        if defn.user_scoped:
            row["user_id"] = user_id
        if params:
            for kp in defn.key_params:
                if kp in params:
                    row[kp] = params[kp]
        self._apply_defaults(defn, row)
        self._check_required(defn, row)
        cols = list(row.keys())
        ph = ", ".join("?" for _ in cols)
        immutable_cols = {defn.id_field}
        if defn.user_scoped:
            immutable_cols.add("user_id")
        update_parts = [f"{c} = excluded.{c}" for c in cols if c not in immutable_cols]
        if self._should_auto_set_updated_at(defn, row):
            update_parts.append(f"updated_at = {self._timestamp_update_expr()}")
        ownership_guard, ownership_values = self._upsert_ownership_guard(defn, user_id)
        target_alias = " AS target" if ownership_guard else ""
        sql = (
            f"INSERT INTO {defn.table}{target_alias} ({', '.join(cols)}) VALUES ({ph})"
            f" ON CONFLICT({defn.id_field}) DO UPDATE SET {', '.join(update_parts)}"
        )
        if ownership_guard:
            sql += f" WHERE {ownership_guard}"
        with self.db.transaction(defn.lock_type, resource_id=entity_id):
            self._assert_parent_owned(defn, user_id, row)
            cur = self.db.execute(
                sql,
                tuple(row[c] for c in cols) + tuple(ownership_values),
            )
            if ownership_guard and cur.rowcount == 0:
                raise PermissionError(
                    f"ownership conflict: {defn.name}/{entity_id} is not owned by user {user_id!r}"
                )
        result = self._sql_get(defn, user_id, entity_id, params=params)
        return result or row

    # ── Batch / advanced ops ──────────────────────────────────────────────

    def batch_update(self, entity: str, user_id: str, entity_ids: list[str],
                     data: Dict[str, Any], *, params: Optional[Dict[str, str]] = None,
                     emit_notifications: bool = True) -> int:
        defn = self.get_def(entity)
        if not entity_ids:
            return 0
        row = self._in(defn, data)
        self._discard_immutable_mutations(defn, row)
        if not row:
            return 0
        set_parts, set_vals = [], []
        for k, v in row.items():
            set_parts.append(f"{k} = ?")
            set_vals.append(v)
        if self._should_auto_set_updated_at(defn, row):
            set_parts.append(f"updated_at = {self._timestamp_update_expr()}")
        where, where_vals = self._scope_where(defn, user_id, params)
        placeholders = ",".join("?" for _ in entity_ids)
        where += f" AND {defn.id_field} IN ({placeholders})"
        where_vals.extend(entity_ids)
        sql = f"UPDATE {defn.table} SET {', '.join(set_parts)} WHERE {where}"
        res_id = entity_ids[0] if entity_ids else "batch"
        with self.db.transaction("global", resource_id=res_id, timeout=10.0):
            self._assert_parent_move_owned(defn, user_id, row)
            cur = self.db.execute(sql, tuple(set_vals + where_vals))
            rowcount = cur.rowcount
        if emit_notifications and rowcount > 0:
            notify_data = self._out(defn, row)
            for eid in entity_ids:
                self._notify_change(
                    entity,
                    "updated",
                    user_id,
                    entity_id=eid,
                    params=params,
                    data=notify_data,
                )
        return rowcount

    def migrate_quarantined_tenant_ownership(
        self,
        entity: str,
        source_user_id: str,
        items: List[Dict[str, str]],
        *,
        emit_notifications: bool = True,
    ) -> Dict[str, Any]:
        """Atomically assign quarantined rows to verified tenant parents.

        This is deliberately narrower than generic owner transfer.  Only rows
        held under a reserved legacy-quarantine owner can move, and each target
        parent must already belong to the requested tenant.  A single SQL
        statement handles the whole bounded batch, so startup migrations do not
        perform thousands of HTTP read/delete/create round trips.
        """

        defn = self.get_def(entity)
        source = str(source_user_id or "").strip()
        if source not in _ALLOWED_QUARANTINE_SOURCE_TENANTS:
            raise PermissionError("source owner is not a reserved quarantine tenant")
        if not defn.user_scoped or not defn.parent:
            raise ValueError("tenant ownership migration requires a parent-scoped entity")
        if not items:
            return {"completed_ids": [], "completed": 0, "skipped": 0}
        if len(items) > TENANT_OWNERSHIP_MIGRATION_MAX_ITEMS:
            raise ValueError(
                f"tenant ownership migration is limited to "
                f"{TENANT_OWNERSHIP_MIGRATION_MAX_ITEMS} items"
            )

        parent_name, local_parent_field, parent_id_field = defn.parent
        parent_def = self.get_def(parent_name)
        if not parent_def.user_scoped:
            raise ValueError("tenant ownership migration requires a user-scoped parent")
        if "user_id" not in defn.field_map or "user_id" not in parent_def.field_map:
            raise ValueError("tenant ownership migration requires direct user_id fields")
        if local_parent_field not in defn.field_map:
            raise ValueError("tenant ownership migration parent field is not registered")

        normalized: List[tuple[str, str, str]] = []
        seen_ids: set[str] = set()
        for item in items:
            entity_id = str(item.get("entity_id") or "").strip()
            target_user_id = str(item.get("user_id") or "").strip()
            target_parent_id = str(item.get("parent_id") or "").strip()
            if not entity_id or not target_user_id or not target_parent_id:
                raise ValueError("migration items require entity_id, user_id, and parent_id")
            if target_user_id.startswith(_QUARANTINE_TENANT_PREFIX):
                raise PermissionError("target owner cannot be a quarantine tenant")
            if entity_id in seen_ids:
                raise ValueError(f"duplicate migration entity_id: {entity_id}")
            seen_ids.add(entity_id)
            normalized.append((entity_id, target_user_id, target_parent_id))

        value_rows = ", ".join("(?, ?, ?)" for _ in normalized)
        values: List[str] = []
        for entity_id, target_user_id, target_parent_id in normalized:
            values.extend((entity_id, target_user_id, target_parent_id))
        values.append(source)

        sql = f"""
            WITH requested(entity_id, target_user_id, target_parent_id) AS (
                VALUES {value_rows}
            ), eligible AS (
                SELECT requested.entity_id,
                       requested.target_user_id,
                       requested.target_parent_id
                  FROM requested
                  JOIN {parent_def.table} AS parent
                    ON parent.{parent_id_field} = requested.target_parent_id
                   AND parent.user_id = requested.target_user_id
            )
            UPDATE {defn.table} AS target
               SET user_id = eligible.target_user_id,
                   {local_parent_field} = eligible.target_parent_id
              FROM eligible
             WHERE target.{defn.id_field} = eligible.entity_id
               AND (
                    target.user_id = ?
                    OR (
                        target.user_id = eligible.target_user_id
                        AND target.{local_parent_field} = eligible.target_parent_id
                    )
               )
            RETURNING target.{defn.id_field} AS entity_id,
                      target.user_id AS user_id
        """
        with self.db.transaction("global", resource_id=f"tenant-migration:{entity}", timeout=15.0):
            completed_rows = self.db.fetchall(sql, tuple(values))

        completed_ids = sorted(str(row["entity_id"]) for row in completed_rows)
        if emit_notifications:
            for tenant_id in sorted({str(row["user_id"]) for row in completed_rows}):
                self._notify_change(entity, "clear", tenant_id)
        return {
            "completed_ids": completed_ids,
            "completed": len(completed_ids),
            "skipped": len(normalized) - len(completed_ids),
        }

    def count(self, entity: str, user_id: str, *, params: Optional[Dict[str, str]] = None,
              filters: Optional[Dict[str, Any]] = None) -> int:
        defn = self.get_def(entity)
        where, values = self._scope_where(defn, user_id, params)
        if filters:
            validate_field_keys(defn, filters.keys(), label="filter field")
            for k, v in filters.items():
                where += f" AND {k} = ?"
                values.append(v)
        row = self.db.fetchone(f"SELECT COUNT(*) as cnt FROM {defn.table} WHERE {where}", tuple(values))
        return row["cnt"] if row else 0

    def delete_where(self, entity: str, user_id: str, *, params: Optional[Dict[str, str]] = None,
                     filters: Optional[Dict[str, Any]] = None, notify: bool = True) -> int:
        defn = self.get_def(entity)
        where, values = self._scope_where(defn, user_id, params)
        if filters:
            validate_field_keys(defn, filters.keys(), label="filter field")
            for k, v in filters.items():
                where += f" AND {k} = ?"
                values.append(v)
        sql = f"DELETE FROM {defn.table} WHERE {where}"
        res_id = (params or {}).get(defn.key_params[0], "batch") if defn.key_params else "batch"
        with self.db.transaction(defn.lock_type, resource_id=res_id):
            cur = self.db.execute(sql, tuple(values))
        rowcount = cur.rowcount
        if notify and rowcount > 0:
            # Batch deletes do not have per-row ids. Broadcast them as a scoped
            # invalidate so clients drop the affected projection and resubscribe
            # from the server snapshot instead of trying to delete id="".
            self._notify_change(entity, "clear", user_id, params=params)
        return rowcount

    def update_where(self, entity: str, user_id: str, data: Dict[str, Any],
                     *, params: Optional[Dict[str, str]] = None,
                     filters: Optional[Dict[str, Any]] = None,
                     notify: bool = True) -> int:
        defn = self.get_def(entity)
        row = self._in(defn, data)
        self._discard_immutable_mutations(defn, row)
        if not row:
            return 0
        set_parts, set_vals = [], []
        for k, v in row.items():
            set_parts.append(f"{k} = ?")
            set_vals.append(v)
        if self._should_auto_set_updated_at(defn, row):
            set_parts.append(f"updated_at = {self._timestamp_update_expr()}")
        where, where_vals = self._scope_where(defn, user_id, params)
        if filters:
            validate_field_keys(defn, filters.keys(), label="filter field")
            for k, v in filters.items():
                where += f" AND {k} = ?"
                where_vals.append(v)
        sql = f"UPDATE {defn.table} SET {', '.join(set_parts)} WHERE {where}"
        res_id = (params or {}).get(defn.key_params[0] if defn.key_params else "", "batch") or "batch"
        with self.db.transaction("global", resource_id=res_id):
            self._assert_parent_move_owned(defn, user_id, row)
            cur = self.db.execute(sql, tuple(set_vals + where_vals))
        rowcount = cur.rowcount
        if notify and rowcount > 0:
            self._notify_change(
                entity,
                "updated",
                user_id,
                params=params,
                data=self._out(defn, row),
            )
        return rowcount

    def cleanup(self, entity: str, user_id: str, keep_count: int,
                *, params: Optional[Dict[str, str]] = None,
                order_by: Optional[str] = None, notify: bool = True) -> int:
        defn = self.get_def(entity)
        rowid_col = self._rowid_column()
        order = self._normalize_order_by(defn, order_by or defn.default_order or f"{rowid_col} DESC")
        where, values = self._scope_where(defn, user_id, params)
        keep_sql = (
            f"SELECT {defn.id_field} FROM {defn.table} "
            f"WHERE {where} ORDER BY {order} LIMIT ?"
        )
        keep_values = list(values) + [keep_count]
        sql = (
            f"DELETE FROM {defn.table} WHERE {where} "
            f"AND {defn.id_field} NOT IN ({keep_sql})"
        )
        all_values = list(values) + keep_values
        res_id = (params or {}).get(defn.key_params[0], "cleanup") if defn.key_params else "cleanup"
        with self.db.transaction(defn.lock_type, resource_id=res_id):
            cur = self.db.execute(sql, tuple(all_values))
        rowcount = cur.rowcount
        if notify and rowcount > 0:
            # Cleanup is also a batch delete; clients must invalidate and
            # resubscribe rather than applying a fake per-row delete.
            self._notify_change(entity, "clear", user_id, params=params)
        return rowcount

    # ── Stream ops ────────────────────────────────────────────────────────

    def append(self, entity: str, user_id: str, data: Dict[str, Any],
               *, params: Optional[Dict[str, str]] = None,
               notify: bool = True) -> Dict[str, Any]:
        """Append data to a stream entity."""
        defn = self.get_def(entity)
        row = self._in(defn, data)
        if defn.user_scoped:
            row["user_id"] = user_id
        if params:
            for kp in defn.key_params:
                if kp in params and kp not in row:
                    row[kp] = params[kp]
        id_f_def = defn.field_map.get(defn.id_field) if defn.fields else None
        is_auto_int = id_f_def and id_f_def.kind.name == "INTEGER"
        res_id = row.get(defn.id_field, "")
        if not res_id and not is_auto_int:
            # Same fail-fast guard as _sql_create — see PR-40 rationale
            # there. Stream entities (``messages`` / ``subagents`` /
            # ``agent-memory``) MUST provide their own id.
            raise ValueError(
                f"missing required '{defn.id_field}' on entity="
                f"'{defn.name}': caller must provide a value. "
                f"Entangled does not mint ids for non-auto-int "
                f"primary keys (PR-40 fail-fast)."
            )
        lock_id = res_id or (params.get(defn.key_params[0], "") if params and defn.key_params else "") or "auto"
        self._apply_defaults(defn, row)
        self._check_required(defn, row)
        with self.db.transaction(defn.lock_type, resource_id=lock_id):
            self._assert_parent_owned(defn, user_id, row)
            self._insert_row(defn, row, is_auto_int=bool(is_auto_int))
            if is_auto_int and row.get(defn.id_field):
                res_id = str(row[defn.id_field])
        entity_id = str(row.get(defn.id_field, res_id))
        result = self.get(entity, user_id, entity_id, params=params) or row
        if notify:
            self._notify_change(entity, "stream_append", user_id, entity_id=entity_id, params=params, data=result)
        return result

    def stream_chunk(self, entity: str, user_id: str, entity_id: str,
                     chunk_delta: Any, *, params: Optional[Dict[str, str]] = None) -> None:
        """Broadcast a streaming chunk (no DB write, just push to entangled peers)."""
        self.get_def(entity)
        data_payload = {"delta": chunk_delta}
        self._notify_change(entity, "stream_chunk", user_id, entity_id=entity_id, params=params, data=data_payload)

    def cas_update(self, entity: str, user_id: str, where_condition: Dict[str, Any],
                   update_data: Dict[str, Any], *, params: Optional[Dict[str, str]] = None,
                   emit_notifications: bool = True) -> Optional[Dict[str, Any]]:
        """Atomic CAS (Compare-And-Swap) update."""
        defn = self.get_def(entity)
        row = self._in(defn, update_data)
        self._discard_immutable_mutations(defn, row)
        if not row:
            return None
        cols = list(row.keys())
        update_parts = [f"{c} = ?" for c in cols]
        values = [row[c] for c in cols]
        if self._should_auto_set_updated_at(defn, row):
            update_parts.append(f"updated_at = {self._timestamp_update_expr()}")
        where, where_values = self._scope_where(defn, user_id, params)
        validate_field_keys(defn, where_condition.keys(), label="CAS condition field")
        for k, v in where_condition.items():
            where += f" AND {k} = ?"
            where_values.append(v)
        sql = f"UPDATE {defn.table} SET {', '.join(update_parts)} WHERE {where}"
        resource_id = where_condition.get(defn.id_field, "")
        with self.db.transaction(defn.lock_type, resource_id=str(resource_id)):
            self._assert_parent_move_owned(defn, user_id, row)
            cur = self.db.execute(sql, tuple(values + where_values))
            if cur.rowcount == 0:
                return None
            id_val = row.get(defn.id_field) or resource_id
            if not id_val:
                return {"_cas_success": True, "rowcount": cur.rowcount}
        result = self.get(entity, user_id, str(id_val), params=params)
        notify_data = result if result is not None else self._out(defn, row)
        if emit_notifications:
            self._notify_change(entity, "updated", user_id, entity_id=str(id_val), params=params, data=notify_data)
        return result

    # ── Action dispatch ───────────────────────────────────────────────────

    async def action(
        self,
        entity: str,
        user_id: str,
        action_name: str,
        params: Dict[str, str],
        payload: Dict[str, Any],
    ) -> Any:
        defn = self.get_def(entity)

        if defn.actions and action_name in defn.actions:
            handler = defn.actions[action_name]
            if inspect.iscoroutinefunction(handler):
                return await handler(self, user_id, params, payload)
            else:
                res = handler(self, user_id, params, payload)
                if inspect.isawaitable(res):
                    return await res
                return res

        if defn.action_hooks and action_name in defn.action_hooks:
            return await self._call_action_hook(
                defn.action_hooks[action_name], entity, action_name, user_id, params, payload,
            )

        raise KeyError(f"No action handler for '{action_name}' on '{entity}'")

    async def _call_action_hook(
        self, url: str, entity: str, action_name: str,
        user_id: str, params: Dict[str, str], payload: Dict[str, Any],
    ) -> Any:
        """Forward a custom action to an external service (Gateway) via HTTP POST."""
        import asyncio
        import urllib.request

        body = json.dumps({
            "user_id": user_id,
            "params": params,
            "payload": payload,
        }).encode("utf-8")

        headers = {"Content-Type": "application/json"}
        service_token = getattr(self, "_service_token", None)
        if service_token:
            headers["X-Service-Token"] = service_token
            headers["X-Internal-Service"] = "entangled"
            headers["Authorization"] = f"Bearer {service_token}"

        def _do_request() -> Any:
            import urllib.error
            req = urllib.request.Request(url, data=body, headers=headers, method="POST")
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    resp_body = json.loads(resp.read())
            except urllib.error.HTTPError as he:
                try:
                    err_body = json.loads(he.read())
                    detail = err_body.get("detail", err_body.get("error", str(he)))
                except Exception:
                    detail = str(he)
                raise RuntimeError(
                    f"Action hook {entity}.{action_name} returned {he.code}: {detail}"
                ) from he
            if not resp_body.get("success"):
                raise RuntimeError(resp_body.get("error", f"Action hook failed: {entity}.{action_name}"))
            return resp_body.get("data")

        return await asyncio.to_thread(_do_request)

    # ── Internal helpers ──────────────────────────────────────────────────

    @staticmethod
    def _discard_immutable_mutations(
        defn: SqlEntityDef,
        row: Dict[str, Any],
    ) -> None:
        """Keep primary keys and direct ownership immutable on update paths.

        Full-row clients commonly echo their primary key and ``user_id`` back,
        so ignoring those fields is safer and more compatible than rejecting
        the whole update. Explicit identity/tenant transfers require a separate
        privileged workflow; generic CRUD never changes either value.
        """
        row.pop(defn.id_field, None)
        if defn.user_scoped:
            row.pop("user_id", None)

    def _assert_parent_move_owned(
        self,
        defn: SqlEntityDef,
        user_id: str,
        row: Dict[str, Any],
    ) -> None:
        """Validate a new parent FK when an update attempts to move a child."""
        if not defn.parent:
            return
        _parent_name, local_fk, _parent_pk = defn.parent
        if local_fk in row:
            self._assert_parent_owned(defn, user_id, row)

    def _ownership_where(
        self,
        defn: SqlEntityDef,
        user_id: str,
        *,
        seen: Optional[set[str]] = None,
    ) -> Tuple[Optional[str], List[Any]]:
        """Build the recursive user-ownership predicate for ``defn``.

        ``None`` means the entity is truly global. Missing definitions and
        parent cycles fail closed because silently returning ``1=1`` would turn
        malformed schema into an authorization bypass.
        """
        if not user_id:
            return None, []
        if defn.user_scoped:
            return "user_id = ?", [user_id]
        if not defn.parent:
            return None, []

        visited = set(seen or ())
        if defn.name in visited:
            raise ValueError(f"Parent ownership cycle detected at '{defn.name}'.")
        visited.add(defn.name)

        parent_name, local_fk, parent_pk = defn.parent
        try:
            parent_def = self.get_def(parent_name)
        except KeyError as exc:
            logger.error(
                "[SqlEntityStore] SECURITY: parent entity '%s' not registered for '%s'.",
                parent_name,
                defn.name,
            )
            raise ValueError(f"Parent entity '{parent_name}' not registered.") from exc

        parent_where, values = self._ownership_where(
            parent_def,
            user_id,
            seen=visited,
        )
        if parent_where is None:
            return None, []
        return (
            f"{local_fk} IN (SELECT {parent_pk} FROM {parent_def.table} "
            f"WHERE {parent_where})",
            values,
        )

    def _assert_parent_owned(
        self,
        defn: SqlEntityDef,
        user_id: str,
        row: Dict[str, Any],
    ) -> None:
        """Reject inserts/moves beneath a parent outside the caller's tenant."""
        if not defn.parent or not user_id:
            return
        parent_name, local_fk, parent_pk = defn.parent
        parent_def = self.get_def(parent_name)
        parent_where, owner_values = self._ownership_where(parent_def, user_id)
        if parent_where is None:
            return
        parent_id = row.get(local_fk)
        if parent_id is None:
            raise ValueError(
                f"missing parent key '{local_fk}' for entity '{defn.name}'"
            )
        owned = self.db.fetchone(
            f"SELECT 1 AS owned FROM {parent_def.table} "
            f"WHERE {parent_pk} = ? AND {parent_where} LIMIT 1",
            (parent_id, *owner_values),
        )
        if not owned:
            raise PermissionError(
                f"parent ownership denied: {parent_name}/{parent_id} is not owned by user {user_id!r}"
            )

    def _upsert_ownership_guard(
        self,
        defn: SqlEntityDef,
        user_id: str,
    ) -> Tuple[Optional[str], List[Any]]:
        """Return an atomic ``ON CONFLICT DO UPDATE`` ownership guard."""
        if defn.user_scoped:
            return "target.user_id = excluded.user_id", []
        if not defn.parent or not user_id:
            return None, []

        parent_name, local_fk, parent_pk = defn.parent
        parent_def = self.get_def(parent_name)
        parent_where, values = self._ownership_where(parent_def, user_id)
        if parent_where is None:
            return None, []
        return (
            f"target.{local_fk} IN (SELECT {parent_pk} FROM {parent_def.table} "
            f"WHERE {parent_where})",
            values,
        )

    def _scope_where(self, defn: SqlEntityDef, user_id: str,
                     params: Optional[Dict[str, str]]) -> Tuple[str, List[Any]]:
        clauses, values = [], []
        if defn.user_scoped and user_id:
            clauses.append("user_id = ?")
            values.append(user_id)
        if params:
            for kp in defn.key_params:
                if kp in params:
                    clauses.append(f"{kp} = ?")
                    values.append(params[kp])
        # Cascading ownership via an arbitrary registered parent chain.
        if defn.parent and not defn.user_scoped and user_id:
            ownership_where, ownership_values = self._ownership_where(defn, user_id)
            if ownership_where:
                clauses.append(ownership_where)
                values.extend(ownership_values)
        return (" AND ".join(clauses) if clauses else "1=1"), values

    def _in(self, defn: SqlEntityDef, data: Dict[str, Any]) -> Dict[str, Any]:
        """Input dict → DB-ready dict (serialize per-field, compute has_* from hidden)."""
        result = dict(data)
        if not defn.fields:
            raise ValueError(f"Entity '{defn.name}' has no fields defined.")
        fm = defn.field_map
        validate_field_keys(defn, result.keys(), label="input field")
        for h in defn.hidden_fields:
            has_key = f"has_{h}"
            if has_key in fm and h in result:
                result[has_key] = bool(result[h])
        for k in list(result.keys()):
            if k in fm:
                if self._is_postgres() and fm[k].is_bool and result[k] is not None:
                    result[k] = bool(result[k])
                else:
                    result[k] = fm[k].serialize(result[k])
        return result

    def _apply_defaults(self, defn: SqlEntityDef, row: Dict[str, Any]) -> Dict[str, Any]:
        """Fill schema-declared defaults for NOT NULL fields missing from `row`.

        Purpose: eliminate the "silent 400 on missing NOT NULL" class of bugs
        (see novaic /docs/roadmap/tickets/PR-33 §"no silent failure"). A field
        declared ``nullable=False, default=<X>`` carries an explicit intent:
        *if the caller did not provide this value, fill X*. Previously that
        intent was only honored via a SQL ``DEFAULT`` clause at CREATE TABLE
        time, which does NOT apply to existing tables and does NOT propagate
        through generic CRUD callers that don't know per-entity semantics
        (e.g. agent-runtime's ``gw.entity_create("messages", {...})``).

        Scope (deliberately narrow to avoid behaviour change for existing
        fields such as ``F.timestamp(auto=True)`` which are ``nullable=True``):

            * Only fields with ``nullable=False`` AND ``default is not None``
              AND whose name is NOT already present in ``row``.
            * ``default="NOW"``  →  filled with :func:`_iso_now_utc`.
            * Any other literal  →  filled verbatim.

        Explicit ``None`` from the caller is left untouched (the caller has
        stated an intent; we honour it and let the SQL layer fail loudly).
        """
        fm = defn.field_map
        for f in defn.fields:
            if f.nullable or f.default is None:
                continue
            if f.name in row:
                continue
            row[f.name] = _iso_now_utc() if f.default == "NOW" else f.default
        return row

    def _check_required(self, defn: SqlEntityDef, row: Dict[str, Any]) -> None:
        """Raise ``ValueError`` listing every NOT-NULL, no-default, non-primary
        field that the caller didn't provide.

        Called right after :meth:`_apply_defaults` so this only fires for
        fields the *schema* says are caller-must-provide (i.e. nullable=False
        AND default is None). Those are business invariants — we'd rather
        fail loudly at the Python layer with an actionable message than let
        the write reach the database and surface as an opaque
        integrity error
        (which an HTTP 400 hands back to the caller with no field attribution).

        PR-33 §"no silent failure" motivation: the failure already happens,
        but the caller gets *named* fields they forgot. Combined with
        ``_apply_defaults`` this closes the loop:

            * time-like defaults → filled
            * business required → ValueError with field names
            * nullable fields → None, SQL accepts

        Crossing row ``None`` values: an explicit ``None`` is considered
        "caller stated an intent (NULL)" and is *not* reported here —
        ``_apply_defaults`` already documented that contract. SQL will then
        raise the classic NOT NULL error for that case, which is loud enough
        given the caller's deliberate None.
        """
        missing: List[str] = []
        for f in defn.fields:
            if f.nullable or f.primary or f.default is not None:
                continue
            if f.name in row:
                continue
            missing.append(f.name)
        if missing:
            raise ValueError(
                f"missing required field(s) on entity='{defn.name}': "
                f"{', '.join(missing)}"
            )

    def _out(self, defn: SqlEntityDef, row: Dict[str, Any], *, include_hidden: bool = False) -> Dict[str, Any]:
        """DB row → Python dict (deserialize + strip hidden + compute has_* fields)."""
        result = dict(row)
        if defn.fields:
            fm = defn.field_map
            for k in list(result.keys()):
                if k in fm:
                    result[k] = fm[k].deserialize(result[k])
            if not include_hidden:
                for h in defn.hidden_fields:
                    has_key = f"has_{h}"
                    if has_key in fm:
                        result[has_key] = bool(result.get(h))
                    result.pop(h, None)
        return result
