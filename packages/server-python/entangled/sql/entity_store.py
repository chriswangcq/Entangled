"""SqlEntityStore — pure SQL storage engine backed by SQLite.

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
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from ..server.store import EntityStore as BaseStore
from .entity_def import SqlEntityDef

logger = logging.getLogger(__name__)


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
        from entangled.sql import SqlEntityStore, SqlEntityDef, F, Database

        db = Database(Path("data/app.db"))
        db.connect()
        store = SqlEntityStore(db=db)
        store.register(my_def)
        store.ensure_schema(my_def)
    """

    def __init__(self, db=None):
        super().__init__([])
        self._db = db
        self._outbox_schema_ensured = False

    @property
    def db(self):
        if self._db is None:
            raise RuntimeError(
                "Database not set on SqlEntityStore. Pass db= to constructor "
                "or override the db property in a subclass."
            )
        return self._db

    # ── Registration & Schema ─────────────────────────────────────────────

    def register(self, entity_def: SqlEntityDef) -> None:
        """Register entity definition and bind SQL fallback operations.

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
            self.db.execute(entity_def.create_table_sql())
            for idx_sql in entity_def.index_sqls():
                self.db.execute(idx_sql)
            existing = self.db.fetchall(f"PRAGMA table_info({entity_def.table})")
            existing_cols = [r["name"] for r in existing]
            for alter_sql in entity_def.alter_add_column_sqls(existing_cols):
                logger.info("[SqlEntityStore] Migrating: %s", alter_sql)
                self.db.execute(alter_sql)
        # Auto-create outbox infrastructure if this entity uses it
        if getattr(entity_def, 'outbox_trigger_types', None):
            self._ensure_outbox_schema()

    def ensure_all_schemas(self) -> None:
        """Run ensure_schema for all registered entities that have fields."""
        for defn in self._defs.values():
            if defn.fields:
                self.ensure_schema(defn)
        # Ensure outbox table exists if any entity uses outbox triggers
        if any(getattr(d, 'outbox_trigger_types', None) for d in self._defs.values()):
            self._ensure_outbox_schema()

    def _ensure_outbox_schema(self) -> None:
        """Idempotent creation of the message_outbox infrastructure table.

        This table is NOT a registered entity — it is an internal
        changefeed mechanism used by the dispatch subscriber (PR-15/16).
        """
        if self._outbox_schema_ensured:
            return
        with self.db.transaction("global"):
            self.db.execute("""
                CREATE TABLE IF NOT EXISTS message_outbox (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id TEXT NOT NULL UNIQUE,
                    agent_id TEXT NOT NULL,
                    trigger_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    delivered_at INTEGER,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    locked_by TEXT,
                    locked_until INTEGER
                )
            """)
            self.db.execute("""
                CREATE INDEX IF NOT EXISTS idx_outbox_undelivered
                ON message_outbox (delivered_at, locked_until, id)
                WHERE delivered_at IS NULL
            """)
        self._outbox_schema_ensured = True
        logger.info("[SqlEntityStore] message_outbox schema ensured")

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
            for k, v in filters.items():
                where += f" AND {k} = ?"
                values.append(v)

        if not skip_default_not_in and defn.default_not_in_filters:
            for k, vlist in defn.default_not_in_filters.items():
                if vlist:
                    placeholders = ",".join(["?"] * len(vlist))
                    where += f" AND {k} NOT IN ({placeholders})"
                    values.extend(vlist)

        order = order_by or defn.default_order
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
        order_by: str = "timestamp DESC, rowid DESC",
        cursor_field: str = "timestamp",
        skip_default_not_in: bool = False,
    ) -> List[Dict[str, Any]]:
        """Cursor-based backward pagination for stream entities."""
        defn = self.get_def(entity)
        where, values = self._scope_where(defn, user_id, params)

        if filters:
            for k, v in filters.items():
                where += f" AND {k} = ?"
                values.append(v)

        if in_filters:
            for k, vlist in in_filters.items():
                if not vlist:
                    continue
                placeholders = ",".join(["?"] * len(vlist))
                where += f" AND {k} IN ({placeholders})"
                values.extend(vlist)

        if not_in_filters:
            for k, vlist in not_in_filters.items():
                if not vlist:
                    continue
                placeholders = ",".join(["?"] * len(vlist))
                where += f" AND {k} NOT IN ({placeholders})"
                values.extend(vlist)

        if not skip_default_not_in and defn.default_not_in_filters:
            for k, vlist in defn.default_not_in_filters.items():
                if vlist:
                    placeholders = ",".join(["?"] * len(vlist))
                    where += f" AND {k} NOT IN ({placeholders})"
                    values.extend(vlist)

        if before_id:
            ref = self.db.fetchone(
                f"SELECT {cursor_field} AS _cf, rowid AS _rid FROM {defn.table} WHERE {defn.id_field} = ?",
                (before_id,),
            )
            if ref:
                where += f" AND ({cursor_field} < ? OR ({cursor_field} = ? AND rowid < ?))"
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
            for k, vlist in defn.default_not_in_filters.items():
                if vlist:
                    placeholders = ",".join(["?"] * len(vlist))
                    where += f" AND {k} NOT IN ({placeholders})"
                    values.extend(vlist)

        raw_order = defn.default_order or "created_at"
        cursor_field = raw_order.split(",")[0].strip().split()[0]
        ref = self.db.fetchone(
            f"SELECT {cursor_field} AS _cf, rowid AS _rid FROM {defn.table} WHERE {defn.id_field} = ?",
            (before_id,),
        )
        if not ref:
            return False
        where += f" AND ({cursor_field} < ? OR ({cursor_field} = ? AND rowid < ?))"
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
            if params and defn.key_params:
                res_id = params.get(defn.key_params[0], "")
                if res_id:
                    row[defn.id_field] = res_id
            if not res_id:
                res_id = uuid.uuid4().hex
                row[defn.id_field] = res_id

        self._apply_defaults(defn, row)
        self._check_required(defn, row)
        cols = list(row.keys())
        ph = ", ".join("?" for _ in cols)
        sql = f"INSERT INTO {defn.table} ({', '.join(cols)}) VALUES ({ph})"
        with self.db.transaction(defn.lock_type, resource_id=res_id or ""):
            cur = self.db.execute(sql, tuple(row[c] for c in cols))
            if is_auto_int and cur.lastrowid:
                row[defn.id_field] = cur.lastrowid
                res_id = str(cur.lastrowid)
        entity_id = str(row.get(defn.id_field, res_id))
        result = self._sql_get(defn, user_id, entity_id, params=params)
        return result or row

    def _sql_update(self, defn: SqlEntityDef, user_id: str, entity_id: str,
                    data: Dict[str, Any], *, params: Optional[Dict[str, str]] = None) -> Optional[Dict[str, Any]]:
        row = self._in(defn, data)
        if not row:
            return self._sql_get(defn, user_id, entity_id, params=params)
        set_parts, set_vals = [], []
        for k, v in row.items():
            set_parts.append(f"{k} = ?")
            set_vals.append(v)
        if defn.tracks_updated_at_column:
            set_parts.append("updated_at = datetime('now')")
        where, where_vals = self._scope_where(defn, user_id, params)
        where += f" AND {defn.id_field} = ?"
        where_vals.append(entity_id)
        sql = f"UPDATE {defn.table} SET {', '.join(set_parts)} WHERE {where}"
        with self.db.transaction(defn.lock_type, resource_id=entity_id):
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
        update_parts = [f"{c} = excluded.{c}" for c in cols if c != defn.id_field]
        if defn.tracks_updated_at_column:
            update_parts.append("updated_at = datetime('now')")
        sql = (
            f"INSERT INTO {defn.table} ({', '.join(cols)}) VALUES ({ph})"
            f" ON CONFLICT({defn.id_field}) DO UPDATE SET {', '.join(update_parts)}"
        )
        with self.db.transaction(defn.lock_type, resource_id=entity_id):
            self.db.execute(sql, tuple(row[c] for c in cols))
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
        if not row:
            return 0
        set_parts, set_vals = [], []
        for k, v in row.items():
            set_parts.append(f"{k} = ?")
            set_vals.append(v)
        if defn.tracks_updated_at_column:
            set_parts.append("updated_at = datetime('now')")
        where, where_vals = self._scope_where(defn, user_id, params)
        placeholders = ",".join("?" for _ in entity_ids)
        where += f" AND {defn.id_field} IN ({placeholders})"
        where_vals.extend(entity_ids)
        sql = f"UPDATE {defn.table} SET {', '.join(set_parts)} WHERE {where}"
        res_id = entity_ids[0] if entity_ids else "batch"
        with self.db.transaction("global", resource_id=res_id, timeout=10.0):
            cur = self.db.execute(sql, tuple(set_vals + where_vals))
            rowcount = cur.rowcount
        if emit_notifications and rowcount > 0:
            for eid in entity_ids:
                self._notify_change(entity, "updated", user_id, entity_id=eid, params=params, data=data)
        return rowcount

    def count(self, entity: str, user_id: str, *, params: Optional[Dict[str, str]] = None,
              filters: Optional[Dict[str, Any]] = None) -> int:
        defn = self.get_def(entity)
        where, values = self._scope_where(defn, user_id, params)
        if filters:
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
            for k, v in filters.items():
                where += f" AND {k} = ?"
                values.append(v)
        sql = f"DELETE FROM {defn.table} WHERE {where}"
        res_id = (params or {}).get(defn.key_params[0], "batch") if defn.key_params else "batch"
        with self.db.transaction(defn.lock_type, resource_id=res_id):
            cur = self.db.execute(sql, tuple(values))
        rowcount = cur.rowcount
        if notify and rowcount > 0:
            self._notify_change(entity, "deleted", user_id, params=params)
        return rowcount

    def update_where(self, entity: str, user_id: str, data: Dict[str, Any],
                     *, params: Optional[Dict[str, str]] = None,
                     filters: Optional[Dict[str, Any]] = None,
                     notify: bool = True) -> int:
        defn = self.get_def(entity)
        row = self._in(defn, data)
        if not row:
            return 0
        set_parts, set_vals = [], []
        for k, v in row.items():
            set_parts.append(f"{k} = ?")
            set_vals.append(v)
        if defn.tracks_updated_at_column:
            set_parts.append("updated_at = datetime('now')")
        where, where_vals = self._scope_where(defn, user_id, params)
        if filters:
            for k, v in filters.items():
                where += f" AND {k} = ?"
                where_vals.append(v)
        sql = f"UPDATE {defn.table} SET {', '.join(set_parts)} WHERE {where}"
        res_id = (params or {}).get(defn.key_params[0] if defn.key_params else "", "batch") or "batch"
        with self.db.transaction("global", resource_id=res_id):
            cur = self.db.execute(sql, tuple(set_vals + where_vals))
        rowcount = cur.rowcount
        if notify and rowcount > 0:
            self._notify_change(entity, "updated", user_id, params=params, data=data)
        return rowcount

    def cleanup(self, entity: str, user_id: str, keep_count: int,
                *, params: Optional[Dict[str, str]] = None,
                order_by: Optional[str] = None, notify: bool = True) -> int:
        defn = self.get_def(entity)
        order = order_by or defn.default_order or "rowid DESC"
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
            self._notify_change(entity, "deleted", user_id, params=params)
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
            if params and defn.key_params:
                res_id = params.get(defn.key_params[0], "")
                if res_id:
                    row[defn.id_field] = res_id
            if not res_id:
                res_id = uuid.uuid4().hex
                row[defn.id_field] = res_id
        lock_id = res_id or (params.get(defn.key_params[0], "") if params and defn.key_params else "") or "auto"
        self._apply_defaults(defn, row)
        self._check_required(defn, row)
        cols = list(row.keys())
        ph = ", ".join("?" for _ in cols)
        sql = f"INSERT INTO {defn.table} ({', '.join(cols)}) VALUES ({ph})"
        with self.db.transaction(defn.lock_type, resource_id=lock_id):
            cur = self.db.execute(sql, tuple(row[c] for c in cols))
            if is_auto_int and cur.lastrowid:
                row[defn.id_field] = cur.lastrowid
                res_id = str(cur.lastrowid)
            # Co-transaction outbox insert
            if defn.outbox_trigger_types:
                msg_type = row.get("type", "")
                trigger_value = defn.outbox_trigger_types.get(msg_type)
                if trigger_value:
                    entity_id_val = str(row.get(defn.id_field, res_id))
                    agent_id_val = row.get("agent_id", "")
                    # metadata may be a JSON string from _in(); decode for payload
                    raw_meta = row.get("metadata")
                    if isinstance(raw_meta, str):
                        try:
                            meta_dict = json.loads(raw_meta)
                        except (json.JSONDecodeError, TypeError):
                            meta_dict = {}
                    else:
                        meta_dict = raw_meta or {}
                    payload = {
                        "message_ids": [entity_id_val],
                        "metadata": meta_dict,
                    }
                    # Extract subagent_id from metadata if present
                    sub_id = meta_dict.get("target_subagent_id") or meta_dict.get("subagent_id")
                    if sub_id:
                        payload["subagent_id"] = sub_id
                    self.db.execute("""
                        INSERT INTO message_outbox
                            (message_id, agent_id, trigger_type, payload_json, created_at)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(message_id) DO NOTHING
                    """, (
                        entity_id_val,
                        agent_id_val,
                        trigger_value,
                        json.dumps(payload, ensure_ascii=False),
                        int(time.time() * 1000),
                    ))
                    logger.info(
                        "event=outbox_enqueue message_id=%s agent=%s trigger=%s",
                        entity_id_val, agent_id_val, trigger_value,
                    )
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
        if not row:
            return None
        cols = list(row.keys())
        update_parts = [f"{c} = ?" for c in cols]
        values = [row[c] for c in cols]
        if defn.tracks_updated_at_column:
            update_parts.append("updated_at = datetime('now')")
        where, where_values = self._scope_where(defn, user_id, params)
        for k, v in where_condition.items():
            where += f" AND {k} = ?"
            where_values.append(v)
        sql = f"UPDATE {defn.table} SET {', '.join(update_parts)} WHERE {where}"
        resource_id = where_condition.get(defn.id_field, "")
        with self.db.transaction(defn.lock_type, resource_id=str(resource_id)):
            cur = self.db.execute(sql, tuple(values + where_values))
            if cur.rowcount == 0:
                return None
            id_val = update_data.get(defn.id_field) or resource_id
            if not id_val:
                return {"_cas_success": True, "rowcount": cur.rowcount}
        result = self.get(entity, user_id, str(id_val), params=params)
        notify_data = result if result is not None else self._out(defn, update_data)
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
        # Cascading ownership via parent tuple
        if defn.parent and not defn.user_scoped and user_id:
            parent_name, local_fk, parent_pk = defn.parent
            try:
                parent_def = self.get_def(parent_name)
                if parent_def.user_scoped:
                    clauses.append(
                        f"{local_fk} IN (SELECT {parent_pk} FROM {parent_def.table} WHERE user_id = ?)"
                    )
                    values.append(user_id)
                elif parent_def.parent:
                    gp_name, gp_fk, gp_pk = parent_def.parent
                    gp_def = self.get_def(gp_name)
                    if gp_def.user_scoped:
                        clauses.append(
                            f"{local_fk} IN (SELECT {parent_pk} FROM {parent_def.table} "
                            f"WHERE {gp_fk} IN (SELECT {gp_pk} FROM {gp_def.table} WHERE user_id = ?))"
                        )
                        values.append(user_id)
            except KeyError:
                logger.error(
                    "[SqlEntityStore] SECURITY: parent entity '%s' not registered for '%s'.",
                    parent_name, defn.name,
                )
                raise ValueError(f"Parent entity '{parent_name}' not registered.")
        return (" AND ".join(clauses) if clauses else "1=1"), values

    def _in(self, defn: SqlEntityDef, data: Dict[str, Any]) -> Dict[str, Any]:
        """Input dict → DB-ready dict (serialize per-field, compute has_* from hidden)."""
        result = dict(data)
        if not defn.fields:
            raise ValueError(f"Entity '{defn.name}' has no fields defined.")
        fm = defn.field_map
        for h in defn.hidden_fields:
            has_key = f"has_{h}"
            if has_key in fm and h in result:
                result[has_key] = bool(result[h])
        for k in list(result.keys()):
            if k in fm:
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
        the write reach SQLite and surface as the opaque
        ``IntegrityError: NOT NULL constraint failed: <table>.<col>``
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
