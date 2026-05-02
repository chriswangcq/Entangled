"""SqlEntityDef — Entangled EntityDef with SQL schema (DDL) capabilities.

Extends the base EntityDef with:
  - Typed field definitions (FieldDef)
  - DDL generation (CREATE TABLE, ALTER TABLE ADD COLUMN, CREATE INDEX)
  - SQL-specific metadata (table name, id_field, user_scoped, lock_type, etc.)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from ..server.defs import EntityDef as BaseEntityDef
from .field_def import FieldDef


@dataclass(kw_only=True)
class SqlEntityDef(BaseEntityDef):
    """Entity definition with SQL schema support.

    Inherits all Entangled protocol fields (name, key_params, sync_type,
    fn-pointers, actions, relations) and adds SQL-specific capabilities.

    Attributes:
        table:          DB table name
        id_field:       Primary key column name (default: "id")
        user_scoped:    Whether rows are scoped by user_id
        fields:         Typed field definitions (FieldDef list)
        constraints:    Table-level constraints (e.g. UNIQUE, CHECK)
        default_order:  Default ORDER BY clause
        lock_type:      Transaction lock type (default: "global")
        auto_timestamps: Auto-set updated_at on UPDATE
        parent:         Cascading ownership: (parent_entity, fk_col, parent_pk_col)
        default_not_in_filters: Default NOT IN filters for list/list_stream
    """
    table: str = ""
    id_field: str = "id"
    user_scoped: bool = True
    fields: List[FieldDef] = field(default_factory=list)
    constraints: List[str] = field(default_factory=list)
    default_order: str = "created_at"
    lock_type: str = "global"
    auto_timestamps: bool = True
    # (parent_entity_name, local_fk_column, parent_pk_column)
    parent: Optional[Tuple[str, str, str]] = None
    default_not_in_filters: Dict[str, List[Any]] = field(default_factory=dict)
    # Outbox: optional mapping of message type → TriggerType.value.
    # When set, append() will co-transactionally insert a message_outbox row
    # for rows whose "type" field is a key in this dict.
    outbox_trigger_types: Optional[Dict[str, str]] = None

    # ── DDL ────────────────────────────────────────────────────────────────

    def create_table_sql(self) -> str:
        """Generate CREATE TABLE IF NOT EXISTS SQL."""
        if not self.fields:
            raise ValueError(f"SqlEntityDef '{self.name}' has no fields, cannot generate DDL")
        pk_fields = [f for f in self.fields if f.primary]
        composite_pk = len(pk_fields) > 1
        if composite_pk:
            col_defs = [f.column_ddl_no_pk() for f in self.fields]
            pk_clause = f"PRIMARY KEY({', '.join(f.name for f in pk_fields)})"
            all_parts = col_defs + [pk_clause] + self.constraints
        else:
            col_defs = [f.column_ddl() for f in self.fields]
            all_parts = col_defs + self.constraints
        cols = ",\n    ".join(all_parts)
        return f"CREATE TABLE IF NOT EXISTS {self.table} (\n    {cols}\n);"

    def index_sqls(self) -> List[str]:
        """Generate all index DDL statements."""
        stmts = []
        for f in self.fields:
            if f.index and not f.primary:
                stmts.append(
                    f"CREATE INDEX IF NOT EXISTS idx_{self.table}_{f.name} "
                    f"ON {self.table}({f.name});"
                )
        return stmts

    def alter_add_column_sqls(self, existing_cols: List[str]) -> List[str]:
        """Generate ALTER TABLE ADD COLUMN for missing columns (idempotent migration)."""
        stmts = []
        for f in self.fields:
            if f.name not in existing_cols:
                stmts.append(f"ALTER TABLE {self.table} ADD COLUMN {f.alter_column_ddl()};")
        return stmts

    # ── Field lookup ──────────────────────────────────────────────────────

    @property
    def field_map(self) -> Dict[str, FieldDef]:
        return {f.name: f for f in self.fields}

    @property
    def json_fields(self) -> List[str]:
        return [f.name for f in self.fields if f.is_json]

    @property
    def bool_fields(self) -> List[str]:
        return [f.name for f in self.fields if f.is_bool]

    @property
    def hidden_fields(self) -> List[str]:
        return [f.name for f in self.fields if f.hidden]

    @property
    def tracks_updated_at_column(self) -> bool:
        if not self.auto_timestamps or not self.fields:
            return False
        return any(f.name == "updated_at" for f in self.fields)

    # ── Spec serialization (for schema registration API) ──────────────────

    def to_spec(self) -> dict:
        """Serialize to a JSON-compatible dict for POST /v1/schema/register."""
        spec: Dict[str, Any] = {
            "name": self.name,
            "table": self.table,
            "id_field": self.id_field,
            "user_scoped": self.user_scoped,
            "key_params": list(self.key_params),
            "fields": [f.to_spec() for f in self.fields],
            "constraints": list(self.constraints),
            "default_order": self.default_order,
            "lock_type": self.lock_type,
            "auto_timestamps": self.auto_timestamps,
            "sync_type": self.sync_type,
            "sync_limit": self.sync_limit,
            "op_log_size": self.op_log_size,
            "subscription_mode": self.subscription_mode,
            "data_order": self.data_order,
            "default_not_in_filters": dict(self.default_not_in_filters),
        }
        if self.outbox_trigger_types:
            spec["outbox_trigger_types"] = dict(self.outbox_trigger_types)
        if self.parent:
            spec["parent"] = list(self.parent)
        if self.action_hooks:
            spec["action_hooks"] = dict(self.action_hooks)
        return spec

    @classmethod
    def from_spec(cls, spec: dict) -> SqlEntityDef:
        fields = [FieldDef.from_spec(f) for f in spec.get("fields", [])]
        return cls(
            name=spec["name"],
            table=spec.get("table", spec["name"].replace("-", "_")),
            id_field=spec.get("id_field", "id"),
            user_scoped=spec.get("user_scoped", True),
            key_params=spec.get("key_params", []),
            fields=fields,
            constraints=spec.get("constraints", []),
            default_order=spec.get("default_order", "created_at"),
            lock_type=spec.get("lock_type", "global"),
            auto_timestamps=spec.get("auto_timestamps", True),
            sync_type=spec.get("sync_type", "list"),
            sync_limit=spec.get("sync_limit", 50),
            op_log_size=spec.get("op_log_size", 200),
            subscription_mode=spec.get("subscription_mode", "lazy"),
            data_order=spec.get("data_order", "desc"),
            default_not_in_filters=spec.get("default_not_in_filters", {}),
            parent=tuple(spec["parent"]) if spec.get("parent") else None,
            action_hooks=spec.get("action_hooks", {}),
            outbox_trigger_types=spec.get("outbox_trigger_types"),
        )
