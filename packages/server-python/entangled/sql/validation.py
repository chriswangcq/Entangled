"""Schema and SQL-fragment validation for Entangled SQL entities.

Entangled is the schema authority for synced entities. Validation must happen
inside Entangled before DDL, registry mutation, or sync broadcast so upstream
services cannot accidentally publish a half-valid schema.
"""

from __future__ import annotations

import re
from typing import Iterable, Mapping

from .entity_def import SqlEntityDef


class SchemaValidationError(ValueError):
    """Raised when a schema definition is invalid for Entangled SQL storage."""


_ENTITY_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*$")
_SQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Conservative SQL keyword list kept local so validation has no optional DB API dependency.
SQL_RESERVED_KEYWORDS = {
    "ABORT", "ACTION", "ADD", "AFTER", "ALL", "ALTER", "ALWAYS", "ANALYZE",
    "AND", "AS", "ASC", "ATTACH", "AUTOINCREMENT", "BEFORE", "BEGIN",
    "BETWEEN", "BY", "CASCADE", "CASE", "CAST", "CHECK", "COLLATE",
    "COLUMN", "COMMIT", "CONFLICT", "CONSTRAINT", "CREATE", "CROSS",
    "CURRENT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
    "DATABASE", "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE", "DESC",
    "DETACH", "DISTINCT", "DO", "DROP", "EACH", "ELSE", "END", "ESCAPE",
    "EXCEPT", "EXCLUDE", "EXCLUSIVE", "EXISTS", "EXPLAIN", "FAIL",
    "FILTER", "FIRST", "FOLLOWING", "FOR", "FOREIGN", "FROM", "FULL",
    "GENERATED", "GLOB", "GROUP", "GROUPS", "HAVING", "IF", "IGNORE",
    "IMMEDIATE", "IN", "INDEX", "INDEXED", "INITIALLY", "INNER", "INSERT",
    "INSTEAD", "INTERSECT", "INTO", "IS", "ISNULL", "JOIN", "KEY", "LAST",
    "LEFT", "LIKE", "LIMIT", "MATCH", "MATERIALIZED", "NATURAL", "NO",
    "NOT", "NOTHING", "NOTNULL", "NULL", "NULLS", "OF", "OFFSET", "ON",
    "OR", "ORDER", "OTHERS", "OUTER", "OVER", "PARTITION", "PLAN",
    "PRAGMA", "PRECEDING", "PRIMARY", "QUERY", "RAISE", "RANGE",
    "RECURSIVE", "REFERENCES", "REGEXP", "REINDEX", "RELEASE", "RENAME",
    "REPLACE", "RESTRICT", "RETURNING", "RIGHT", "ROLLBACK", "ROW", "ROWS",
    "SAVEPOINT", "SELECT", "SET", "TABLE", "TEMP", "TEMPORARY", "THEN",
    "TIES", "TO", "TRANSACTION", "TRIGGER", "UNBOUNDED", "UNION", "UNIQUE",
    "UPDATE", "USING", "VACUUM", "VALUES", "VIEW", "VIRTUAL", "WHEN",
    "WHERE", "WINDOW", "WITH", "WITHOUT",
}


def validate_entity_name(name: str) -> None:
    if not name or not _ENTITY_NAME_RE.match(name):
        raise SchemaValidationError(f"invalid entity name: {name!r}")


def validate_sql_identifier(name: str, *, label: str = "identifier") -> None:
    if not name or not _SQL_IDENTIFIER_RE.match(name):
        raise SchemaValidationError(f"invalid SQL {label}: {name!r}")
    if name.upper() in SQL_RESERVED_KEYWORDS:
        raise SchemaValidationError(f"reserved SQL {label}: {name!r}")


def _field_names(defn: SqlEntityDef) -> set[str]:
    return {field.name for field in defn.fields}


def validate_field_key(defn: SqlEntityDef, key: str, *, label: str = "field") -> None:
    validate_field_key_with_extras(defn, key, label=label)


def validate_field_key_with_extras(
    defn: SqlEntityDef,
    key: str,
    *,
    label: str = "field",
    extra_fields: Iterable[str] = (),
) -> None:
    validate_sql_identifier(key, label=label)
    allowed = _field_names(defn) | {"rowid"} | set(extra_fields)
    if key not in allowed:
        raise SchemaValidationError(
            f"unknown {label} {key!r} for entity {defn.name!r}"
        )


def validate_field_keys(
    defn: SqlEntityDef,
    keys: Iterable[str],
    *,
    label: str = "field",
) -> None:
    for key in keys:
        validate_field_key(defn, str(key), label=label)


def normalize_order_by(
    defn: SqlEntityDef,
    order_by: str | None,
    *,
    extra_fields: Iterable[str] = (),
) -> str:
    """Validate and return a canonical ORDER BY fragment.

    Supported shape is intentionally small: ``field`` or ``field ASC|DESC``,
    comma-separated. This covers every active schema while avoiding caller-
    provided SQL fragments.
    """
    raw = (order_by or "").strip()
    if not raw:
        raise SchemaValidationError(f"empty order_by for entity {defn.name!r}")

    normalized: list[str] = []
    for part in raw.split(","):
        tokens = part.strip().split()
        if not tokens:
            raise SchemaValidationError(f"empty order_by segment for entity {defn.name!r}")
        if len(tokens) > 2:
            raise SchemaValidationError(
                f"unsupported order_by segment {part!r} for entity {defn.name!r}"
            )
        field = tokens[0]
        validate_field_key_with_extras(
            defn,
            field,
            label="order_by field",
            extra_fields=extra_fields,
        )
        if len(tokens) == 2:
            direction = tokens[1].upper()
            if direction not in {"ASC", "DESC"}:
                raise SchemaValidationError(
                    f"unsupported order direction {tokens[1]!r} for entity {defn.name!r}"
                )
            normalized.append(f"{field} {direction}")
        else:
            normalized.append(field)
    return ", ".join(normalized)


def validate_entity_def(
    defn: SqlEntityDef,
    *,
    known_defs: Mapping[str, SqlEntityDef] | None = None,
) -> None:
    validate_entity_name(defn.name)
    validate_sql_identifier(defn.table, label=f"table for entity {defn.name!r}")
    if not defn.fields:
        raise SchemaValidationError(f"entity {defn.name!r} has no fields")

    seen_fields: set[str] = set()
    for field in defn.fields:
        validate_sql_identifier(field.name, label=f"field for entity {defn.name!r}")
        if field.name in seen_fields:
            raise SchemaValidationError(
                f"duplicate field {field.name!r} on entity {defn.name!r}"
            )
        seen_fields.add(field.name)

    validate_field_key(defn, defn.id_field, label="id_field")
    validate_field_keys(defn, defn.key_params, label="key_param")
    validate_field_keys(defn, defn.default_not_in_filters.keys(), label="default_not_in_filter")
    normalize_order_by(defn, defn.default_order)
    validate_sql_identifier(defn.lock_type, label=f"lock_type for entity {defn.name!r}")

    if defn.parent:
        if len(defn.parent) != 3:
            raise SchemaValidationError(f"invalid parent tuple on entity {defn.name!r}")
        parent_name, local_fk, parent_pk = defn.parent
        validate_entity_name(parent_name)
        validate_field_key(defn, local_fk, label="parent local field")
        if known_defs and parent_name in known_defs:
            validate_field_key(known_defs[parent_name], parent_pk, label="parent pk field")


def validate_schema_batch(
    defs: Iterable[SqlEntityDef],
    *,
    existing_defs: Mapping[str, SqlEntityDef] | None = None,
) -> None:
    existing_defs = dict(existing_defs or {})
    batch_defs = list(defs)
    candidate: dict[str, SqlEntityDef] = dict(existing_defs)

    seen_entities: set[str] = set()
    table_owner: dict[str, str] = {
        defn.table: name for name, defn in existing_defs.items()
    }
    for defn in batch_defs:
        if defn.name in seen_entities:
            raise SchemaValidationError(f"duplicate entity in schema batch: {defn.name!r}")
        seen_entities.add(defn.name)
        owner = table_owner.get(defn.table)
        if owner and owner != defn.name:
            raise SchemaValidationError(
                f"table {defn.table!r} is already owned by entity {owner!r}"
            )
        table_owner[defn.table] = defn.name
        candidate[defn.name] = defn

    for defn in batch_defs:
        validate_entity_def(defn, known_defs=candidate)
