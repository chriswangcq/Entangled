"""Offline SQLite-to-Postgres migration planning helpers.

This module is intentionally side-effect-light: it inspects a SQLite source in
read-only mode, builds a copy/sequence plan, and renders a secret-safe report.
The executor and CLI layers live in later migration slices.
"""

from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass, field
from pathlib import Path
import re
import sqlite3
from typing import Any
from urllib.parse import quote

from .entity_def import SqlEntityDef
from .field_def import FieldDef, FieldKind
from .persistence import sync_versions_create_table_sql
from .state_transitions import subagent_transitions_create_table_sql


SYNC_VERSIONS_TABLE = "entangled_sync_versions"
TRANSITIONS_TABLE = "subagent_state_transitions"
SQLITE_INTERNAL_PREFIX = "sqlite_"
DEFAULT_SKIPPED_TABLE_PREFIXES = ("_",)


class MigrationSafetyError(ValueError):
    """Raised when migration planning would allow an unsafe operation."""


def quote_identifier(identifier: str) -> str:
    """Quote an SQLite identifier discovered from the source catalog."""
    return '"' + identifier.replace('"', '""') + '"'


def sqlite_readonly_uri(path: Path) -> str:
    """Return a SQLite URI that opens an existing DB read-only."""
    return f"file:{quote(path.resolve().as_posix(), safe='/')}?mode=ro"


def connect_sqlite_readonly(path: Path) -> sqlite3.Connection:
    """Open a SQLite source read-only without creating missing files."""
    conn = sqlite3.connect(sqlite_readonly_uri(path), uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def redact_secret(value: str) -> str:
    """Redact common DSN and key/value secret forms for reports."""
    if not value:
        return value

    redacted = re.sub(r"(://[^:/@\s]+):([^@\s]+)@", r"\1:***@", value)
    redacted = re.sub(
        r"(?i)(password|passwd|pass|token|secret|api_key)=([^&\s]+)",
        lambda m: f"{m.group(1)}=***",
        redacted,
    )
    return redacted


@dataclass(frozen=True)
class ColumnInventory:
    name: str
    declared_type: str
    not_null: bool = False
    primary_key_position: int = 0

    @classmethod
    def from_pragma_row(cls, row: sqlite3.Row) -> "ColumnInventory":
        return cls(
            name=str(row["name"]),
            declared_type=str(row["type"] or ""),
            not_null=bool(row["notnull"]),
            primary_key_position=int(row["pk"] or 0),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "declared_type": self.declared_type,
            "not_null": self.not_null,
            "primary_key_position": self.primary_key_position,
        }


@dataclass(frozen=True)
class TableInventory:
    name: str
    columns: tuple[ColumnInventory, ...]
    row_count: int
    max_rowid: int | None = None
    column_max_values: tuple[tuple[str, int], ...] = ()

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    def column_type(self, name: str) -> str:
        for column in self.columns:
            if column.name == name:
                return column.declared_type
        return ""

    def max_for_column(self, name: str) -> int:
        for column_name, max_value in self.column_max_values:
            if column_name == name:
                return int(max_value)
        return int(self.max_rowid or 0)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "columns": [c.to_dict() for c in self.columns],
            "row_count": self.row_count,
            "max_rowid": self.max_rowid,
            "column_max_values": dict(self.column_max_values),
        }


@dataclass(frozen=True)
class SkippedTable:
    table: str
    reason: str

    def to_dict(self) -> dict[str, str]:
        return {"table": self.table, "reason": self.reason}


@dataclass(frozen=True)
class TableCopyPlan:
    table: str
    kind: str
    source_columns: tuple[str, ...]
    target_columns: tuple[str, ...]
    source_count: int
    copy_rowid_to_entangled_rowid: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "kind": self.kind,
            "source_columns": list(self.source_columns),
            "target_columns": list(self.target_columns),
            "source_count": self.source_count,
            "copy_rowid_to_entangled_rowid": self.copy_rowid_to_entangled_rowid,
        }


@dataclass(frozen=True)
class SequenceResetPlan:
    table: str
    column: str
    restart_with: int
    source_max: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "column": self.column,
            "source_max": self.source_max,
            "restart_with": self.restart_with,
        }


@dataclass(frozen=True)
class MigrationPlan:
    source_path: str
    source_tables: tuple[TableInventory, ...]
    table_plans: tuple[TableCopyPlan, ...]
    skipped_tables: tuple[SkippedTable, ...]
    sequence_resets: tuple[SequenceResetPlan, ...]
    clean_target_allowed: bool = False

    def plan_for(self, table: str) -> TableCopyPlan | None:
        for table_plan in self.table_plans:
            if table_plan.table == table:
                return table_plan
        return None

    def to_report(self, *, connection_label: str = "") -> "MigrationReport":
        checks = {
            "target_counts_match": "pending",
            "sync_versions_match": "pending",
            "transition_ids_match": "pending",
            "rowid_copy_complete": "pending",
        }
        return MigrationReport(
            source_path=self.source_path,
            connection_label=redact_secret(connection_label),
            source_counts={t.name: t.row_count for t in self.source_tables},
            target_counts={p.table: None for p in self.table_plans},
            table_plans=self.table_plans,
            sequence_resets=self.sequence_resets,
            checks=checks,
            skipped_tables=self.skipped_tables,
            clean_target_allowed=self.clean_target_allowed,
        )


@dataclass(frozen=True)
class MigrationReport:
    source_path: str
    connection_label: str
    source_counts: dict[str, int]
    target_counts: dict[str, int | None]
    table_plans: tuple[TableCopyPlan, ...]
    sequence_resets: tuple[SequenceResetPlan, ...]
    checks: dict[str, str]
    skipped_tables: tuple[SkippedTable, ...] = field(default_factory=tuple)
    clean_target_allowed: bool = False
    prepared_tables: tuple[str, ...] = ()
    cleaned_tables: tuple[str, ...] = ()
    schema_statement_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_path": self.source_path,
            "connection_label": redact_secret(self.connection_label),
            "source_counts": dict(self.source_counts),
            "target_counts": dict(self.target_counts),
            "table_plans": [p.to_dict() for p in self.table_plans],
            "sequence_resets": [s.to_dict() for s in self.sequence_resets],
            "checks": dict(self.checks),
            "skipped_tables": [s.to_dict() for s in self.skipped_tables],
            "clean_target_allowed": self.clean_target_allowed,
            "prepared_tables": list(self.prepared_tables),
            "cleaned_tables": list(self.cleaned_tables),
            "schema_statement_count": self.schema_statement_count,
        }


@dataclass(frozen=True)
class TableCopyStats:
    table: str
    rows_copied: int
    target_count: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "rows_copied": self.rows_copied,
            "target_count": self.target_count,
        }


@dataclass(frozen=True)
class TargetPreparationResult:
    prepared_tables: tuple[str, ...]
    cleaned_tables: tuple[str, ...]
    schema_statements: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "prepared_tables": list(self.prepared_tables),
            "cleaned_tables": list(self.cleaned_tables),
            "schema_statement_count": len(self.schema_statements),
        }


class _PostgresDialect:
    backend_name = "postgres"


def confirm_target_cleanup(
    *,
    clean_target: bool,
    confirmation: str,
    expected_confirmation: str,
) -> bool:
    """Validate the destructive target-clean opt-in."""
    if not clean_target:
        return False
    if not expected_confirmation:
        raise MigrationSafetyError("expected target confirmation is required")
    if confirmation != expected_confirmation:
        raise MigrationSafetyError("target cleanup confirmation did not match")
    return True


def inspect_sqlite_source(path: Path) -> tuple[TableInventory, ...]:
    """Inspect user tables in a SQLite source opened read-only."""
    with connect_sqlite_readonly(path) as conn:
        table_rows = conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
            ORDER BY name
            """
        ).fetchall()

        inventories: list[TableInventory] = []
        for row in table_rows:
            table = str(row["name"])
            columns = tuple(
                ColumnInventory.from_pragma_row(column_row)
                for column_row in conn.execute(f"PRAGMA table_info({quote_identifier(table)})")
            )
            row_count = int(
                conn.execute(f"SELECT COUNT(*) AS count FROM {quote_identifier(table)}").fetchone()["count"]
            )
            max_rowid = None
            column_max_values: list[tuple[str, int]] = []
            if not table.startswith(SQLITE_INTERNAL_PREFIX):
                max_row = conn.execute(
                    f"SELECT COALESCE(MAX(rowid), 0) AS max_rowid FROM {quote_identifier(table)}"
                ).fetchone()
                max_rowid = int(max_row["max_rowid"] or 0)
                for column in columns:
                    if is_integer_declared_type(column.declared_type):
                        max_col_row = conn.execute(
                            f"SELECT COALESCE(MAX({quote_identifier(column.name)}), 0) AS max_value "
                            f"FROM {quote_identifier(table)}"
                        ).fetchone()
                        column_max_values.append((column.name, int(max_col_row["max_value"] or 0)))
            inventories.append(
                TableInventory(
                    name=table,
                    columns=columns,
                    row_count=row_count,
                    max_rowid=max_rowid,
                    column_max_values=tuple(column_max_values),
                )
            )
    return tuple(inventories)


def is_integer_declared_type(declared_type: str) -> bool:
    return "INT" in declared_type.upper()


def classify_table(table: TableInventory) -> str:
    if table.name.startswith(SQLITE_INTERNAL_PREFIX):
        return "skip"
    if table.name == SYNC_VERSIONS_TABLE:
        return "sync_versions"
    if table.name == TRANSITIONS_TABLE:
        return "transitions"
    if table.name.startswith(DEFAULT_SKIPPED_TABLE_PREFIXES):
        return "skip"
    return "dynamic"


def build_copy_plan(table: TableInventory) -> TableCopyPlan | None:
    kind = classify_table(table)
    if kind == "skip":
        return None
    source_columns = table.column_names
    target_columns = table.column_names
    copy_rowid = kind == "dynamic" and "entangled_rowid" not in table.column_names
    if copy_rowid:
        source_columns = ("rowid",) + source_columns
        target_columns = ("entangled_rowid",) + target_columns
    return TableCopyPlan(
        table=table.name,
        kind=kind,
        source_columns=source_columns,
        target_columns=target_columns,
        source_count=table.row_count,
        copy_rowid_to_entangled_rowid=copy_rowid,
    )


def build_sequence_reset_plan(table: TableInventory, copy_plan: TableCopyPlan) -> tuple[SequenceResetPlan, ...]:
    plans: list[SequenceResetPlan] = []
    if copy_plan.copy_rowid_to_entangled_rowid:
        source_max = int(table.max_rowid or 0)
        plans.append(
            SequenceResetPlan(
                table=table.name,
                column="entangled_rowid",
                source_max=source_max,
                restart_with=source_max + 1,
            )
        )

    if table.name == TRANSITIONS_TABLE and "id" in table.column_names:
        source_max = table.max_for_column("id")
        plans.append(
            SequenceResetPlan(
                table=table.name,
                column="id",
                source_max=source_max,
                restart_with=source_max + 1,
            )
        )
    elif "id" in table.column_names and is_integer_declared_type(table.column_type("id")):
        source_max = table.max_for_column("id")
        plans.append(
            SequenceResetPlan(
                table=table.name,
                column="id",
                source_max=source_max,
                restart_with=source_max + 1,
            )
        )
    return tuple(plans)


def plan_migration(
    sqlite_path: Path,
    *,
    clean_target: bool = False,
    target_confirmation: str = "",
    expected_target_confirmation: str = "",
) -> MigrationPlan:
    """Build a SQLite-to-Postgres migration plan without copying data."""
    clean_allowed = confirm_target_cleanup(
        clean_target=clean_target,
        confirmation=target_confirmation,
        expected_confirmation=expected_target_confirmation,
    )
    inventories = inspect_sqlite_source(sqlite_path)
    table_plans: list[TableCopyPlan] = []
    skipped: list[SkippedTable] = []
    sequence_resets: list[SequenceResetPlan] = []

    for table in inventories:
        copy_plan = build_copy_plan(table)
        if copy_plan is None:
            skipped.append(SkippedTable(table=table.name, reason="internal_or_ignored_table"))
            continue
        table_plans.append(copy_plan)
        sequence_resets.extend(build_sequence_reset_plan(table, copy_plan))

    return MigrationPlan(
        source_path=str(sqlite_path),
        source_tables=inventories,
        table_plans=tuple(table_plans),
        skipped_tables=tuple(skipped),
        sequence_resets=tuple(sequence_resets),
        clean_target_allowed=clean_allowed,
    )


def sqlite_declared_type_to_field_kind(column: ColumnInventory) -> FieldKind:
    declared = column.declared_type.upper()
    name = column.name.lower()
    if "INT" in declared:
        return FieldKind.INTEGER
    if any(token in declared for token in ("REAL", "FLOA", "DOUB")):
        return FieldKind.REAL
    if "BLOB" in declared:
        return FieldKind.BLOB
    if "BOOL" in declared:
        return FieldKind.BOOL
    if "JSON" in declared or name.endswith("_json"):
        return FieldKind.JSON
    if "TIMESTAMP" in declared or "DATETIME" in declared:
        return FieldKind.TIMESTAMP
    return FieldKind.TEXT


def table_inventory_to_entity_def(table: TableInventory) -> SqlEntityDef:
    fields = []
    for column in table.columns:
        primary = column.primary_key_position > 0
        fields.append(
            FieldDef(
                name=column.name,
                kind=sqlite_declared_type_to_field_kind(column),
                primary=primary,
                nullable=column.not_null is False and not primary,
            )
        )
    return SqlEntityDef(
        name=table.name,
        table=table.name,
        fields=fields,
        user_scoped=False,
    )


def table_inventory_by_name(plan: MigrationPlan) -> dict[str, TableInventory]:
    return {table.name: table for table in plan.source_tables}


def build_target_schema_sqls(table: TableInventory, table_plan: TableCopyPlan) -> tuple[str, ...]:
    if table_plan.kind == "dynamic":
        entity = table_inventory_to_entity_def(table)
        return (entity.create_table_sql(dialect="postgres"), *entity.index_sqls(dialect="postgres"))
    if table_plan.kind == "sync_versions":
        return (sync_versions_create_table_sql(_PostgresDialect()),)
    if table_plan.kind == "transitions":
        return (subagent_transitions_create_table_sql(_PostgresDialect()),)
    return ()


def build_clean_target_sql(table: str) -> str:
    return f"DELETE FROM {quote_identifier(table)}"


def execute_target_cleanup(target_db: Any, plan: MigrationPlan) -> tuple[str, ...]:
    if not plan.clean_target_allowed:
        raise MigrationSafetyError("target cleanup was not confirmed")
    cleaned: list[str] = []
    for table_plan in plan.table_plans:
        target_db.execute(build_clean_target_sql(table_plan.table))
        cleaned.append(table_plan.table)
    return tuple(cleaned)


def prepare_target_for_migration(target_db: Any, plan: MigrationPlan) -> TargetPreparationResult:
    inventories = table_inventory_by_name(plan)
    schema_statements: list[str] = []
    prepared_tables: list[str] = []
    for table_plan in plan.table_plans:
        table = inventories[table_plan.table]
        for sql in build_target_schema_sqls(table, table_plan):
            target_db.execute(sql)
            schema_statements.append(sql)
        prepared_tables.append(table_plan.table)
    cleaned_tables = execute_target_cleanup(target_db, plan) if plan.clean_target_allowed else ()
    return TargetPreparationResult(
        prepared_tables=tuple(prepared_tables),
        cleaned_tables=cleaned_tables,
        schema_statements=tuple(schema_statements),
    )


def build_source_select_sql(table_plan: TableCopyPlan) -> str:
    """Build the SQLite SELECT for one copy plan."""
    select_exprs: list[str] = []
    for source_column, target_column in zip(table_plan.source_columns, table_plan.target_columns):
        if source_column == "rowid" and target_column == "entangled_rowid":
            select_exprs.append(f"rowid AS {quote_identifier(target_column)}")
        else:
            select_exprs.append(quote_identifier(source_column))
    order_clause = " ORDER BY rowid" if table_plan.copy_rowid_to_entangled_rowid else ""
    return f"SELECT {', '.join(select_exprs)} FROM {quote_identifier(table_plan.table)}{order_clause}"


def build_target_insert_sql(table_plan: TableCopyPlan) -> str:
    """Build a placeholder-based INSERT for PostgresDatabase/fake adapters."""
    columns = ", ".join(quote_identifier(column) for column in table_plan.target_columns)
    placeholders = ", ".join("?" for _ in table_plan.target_columns)
    return f"INSERT INTO {quote_identifier(table_plan.table)} ({columns}) VALUES ({placeholders})"


def build_target_count_sql(table: str) -> str:
    return f"SELECT COUNT(*) AS count FROM {quote_identifier(table)}"


def build_target_null_count_sql(table: str, column: str) -> str:
    return f"SELECT COUNT(*) AS count FROM {quote_identifier(table)} WHERE {quote_identifier(column)} IS NULL"


def build_source_transition_stats_sql() -> str:
    return (
        f"SELECT COUNT(*) AS count, COALESCE(MAX(id), 0) AS max_id "
        f"FROM {quote_identifier(TRANSITIONS_TABLE)}"
    )


def build_target_transition_stats_sql() -> str:
    return (
        f"SELECT COUNT(*) AS count, COALESCE(MAX({quote_identifier('id')}), 0) AS max_id "
        f"FROM {quote_identifier(TRANSITIONS_TABLE)}"
    )


def build_sequence_reset_sql(sequence_plan: SequenceResetPlan) -> str:
    """Build Postgres identity restart SQL for a migrated identity column."""
    return (
        f"ALTER TABLE {quote_identifier(sequence_plan.table)} "
        f"ALTER COLUMN {quote_identifier(sequence_plan.column)} "
        f"RESTART WITH {int(sequence_plan.restart_with)}"
    )


def _row_value(row: Any, key: str) -> Any:
    if isinstance(row, dict):
        return row[key]
    return row[key]


def _int_row_value(row: Any, key: str, default: int = 0) -> int:
    if row is None:
        return default
    value = _row_value(row, key)
    return int(value or 0)


def execute_copy_plan(
    source_conn: sqlite3.Connection,
    target_db: Any,
    table_plan: TableCopyPlan,
) -> TableCopyStats:
    """Copy one planned table from SQLite into the target adapter."""
    source_conn.row_factory = sqlite3.Row
    rows = source_conn.execute(build_source_select_sql(table_plan)).fetchall()
    insert_sql = build_target_insert_sql(table_plan)
    params_list = [
        tuple(_row_value(row, target_column) for target_column in table_plan.target_columns)
        for row in rows
    ]
    if params_list:
        if hasattr(target_db, "executemany"):
            target_db.executemany(insert_sql, params_list)
        else:
            for params in params_list:
                target_db.execute(insert_sql, params)
    return TableCopyStats(table=table_plan.table, rows_copied=len(params_list))


def fetch_target_count(target_db: Any, table: str) -> int:
    row = target_db.fetchone(build_target_count_sql(table))
    return _int_row_value(row, "count")


def execute_sequence_resets(target_db: Any, sequence_resets: tuple[SequenceResetPlan, ...]) -> tuple[str, ...]:
    statements: list[str] = []
    for sequence_plan in sequence_resets:
        sql = build_sequence_reset_sql(sequence_plan)
        target_db.execute(sql)
        statements.append(sql)
    return tuple(statements)


def _source_sync_versions(source_conn: sqlite3.Connection) -> list[tuple[str, int]]:
    rows = source_conn.execute(
        f"SELECT state_key, version FROM {quote_identifier(SYNC_VERSIONS_TABLE)} ORDER BY state_key"
    ).fetchall()
    return [(str(row["state_key"]), int(row["version"])) for row in rows]


def _target_sync_versions(target_db: Any) -> list[tuple[str, int]]:
    rows = target_db.fetchall(
        f"SELECT state_key, version FROM {quote_identifier(SYNC_VERSIONS_TABLE)} ORDER BY {quote_identifier('state_key')}"
    )
    return [(str(row["state_key"]), int(row["version"])) for row in rows]


def _source_transition_stats(source_conn: sqlite3.Connection) -> tuple[int, int]:
    row = source_conn.execute(build_source_transition_stats_sql()).fetchone()
    return _int_row_value(row, "count"), _int_row_value(row, "max_id")


def _target_transition_stats(target_db: Any) -> tuple[int, int]:
    row = target_db.fetchone(build_target_transition_stats_sql())
    return _int_row_value(row, "count"), _int_row_value(row, "max_id")


def _check_rowid_copy(target_db: Any, plan: MigrationPlan, target_counts: dict[str, int]) -> str:
    for table_plan in plan.table_plans:
        if not table_plan.copy_rowid_to_entangled_rowid:
            continue
        nulls = fetch_target_null_count(target_db, table_plan.table, "entangled_rowid")
        if nulls != 0 or target_counts.get(table_plan.table) != table_plan.source_count:
            return "failed"
    return "passed"


def fetch_target_null_count(target_db: Any, table: str, column: str) -> int:
    row = target_db.fetchone(build_target_null_count_sql(table, column))
    return _int_row_value(row, "count")


def build_execution_checks(
    source_conn: sqlite3.Connection,
    target_db: Any,
    plan: MigrationPlan,
    target_counts: dict[str, int],
) -> dict[str, str]:
    counts_match = all(target_counts.get(p.table) == p.source_count for p in plan.table_plans)
    checks = {
        "target_counts_match": "passed" if counts_match else "failed",
        "sync_versions_match": "skipped",
        "transition_ids_match": "skipped",
        "rowid_copy_complete": _check_rowid_copy(target_db, plan, target_counts),
    }
    if plan.plan_for(SYNC_VERSIONS_TABLE):
        checks["sync_versions_match"] = (
            "passed" if _source_sync_versions(source_conn) == _target_sync_versions(target_db) else "failed"
        )
    if plan.plan_for(TRANSITIONS_TABLE):
        checks["transition_ids_match"] = (
            "passed" if _source_transition_stats(source_conn) == _target_transition_stats(target_db) else "failed"
        )
    return checks


def execute_migration_plan(
    sqlite_path: Path,
    target_db: Any,
    *,
    plan: MigrationPlan | None = None,
    connection_label: str = "",
) -> MigrationReport:
    """Execute a migration plan and return a redacted result report."""
    active_plan = plan or plan_migration(sqlite_path)
    target_counts: dict[str, int] = {}
    transaction = target_db.transaction("global") if hasattr(target_db, "transaction") else nullcontext(target_db)

    with connect_sqlite_readonly(sqlite_path) as source_conn:
        with transaction:
            preparation = prepare_target_for_migration(target_db, active_plan)
            for table_plan in active_plan.table_plans:
                execute_copy_plan(source_conn, target_db, table_plan)
                target_counts[table_plan.table] = fetch_target_count(target_db, table_plan.table)
            execute_sequence_resets(target_db, active_plan.sequence_resets)
            checks = build_execution_checks(source_conn, target_db, active_plan, target_counts)

    return MigrationReport(
        source_path=active_plan.source_path,
        connection_label=redact_secret(connection_label),
        source_counts={t.name: t.row_count for t in active_plan.source_tables},
        target_counts=target_counts,
        table_plans=active_plan.table_plans,
        sequence_resets=active_plan.sequence_resets,
        checks=checks,
        skipped_tables=active_plan.skipped_tables,
        clean_target_allowed=active_plan.clean_target_allowed,
        prepared_tables=preparation.prepared_tables,
        cleaned_tables=preparation.cleaned_tables,
        schema_statement_count=len(preparation.schema_statements),
    )
