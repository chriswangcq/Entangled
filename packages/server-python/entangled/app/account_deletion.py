"""Fail-closed account-deletion v2 boundary for Entangled.

The durable resurrection block is the authority for future writes.  Operation
rows and blocks retain only SHA-256 digests; raw account and request identifiers
exist only for the duration of one authenticated request.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import secrets
import stat
import time
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from ..server.sync import classify_state_key_owner
from ..sql.validation import validate_sql_identifier


SCHEMA_VERSION = 2
STEP_NAME = "purge_entangled"
DOMAIN = "entangled"
CALLER = "account-deletion-worker"
_CANONICAL_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$")
_SAFE_USER_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}$")
ENTANGLED_SINGLE_REPLICA_ATTESTATION = (
    "entangled-account-deletion-single-replica-v1"
)


class DeletionResource(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    domain: Literal["entangled"]
    resource_type: str = Field(min_length=1, max_length=128)
    reference: str = Field(min_length=1, max_length=512)


class EntangledDeletionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal[2]
    effect_contract: Literal["discover_effect_verify_zero"]
    request_id: str = Field(min_length=1, max_length=512)
    operation_id: str = Field(min_length=1, max_length=512)
    user_id: str = Field(min_length=1, max_length=256)
    step_name: Literal["purge_entangled"]
    resources: list[DeletionResource] = Field(max_length=10_000)

    @field_validator("request_id", "operation_id")
    @classmethod
    def _canonical_id(cls, value: str) -> str:
        if not _CANONICAL_ID.fullmatch(value):
            raise ValueError("canonical identifier required")
        return value

    @field_validator("user_id")
    @classmethod
    def _canonical_user_id(cls, value: str) -> str:
        if not _SAFE_USER_ID.fullmatch(value) or ".." in value:
            raise ValueError("canonical user identifier required")
        return value


class OperationConflict(RuntimeError):
    pass


class OperationLeaseLost(RuntimeError):
    pass


class AccountDeletedError(PermissionError):
    """A write attempted to resurrect data for a deleted account."""


@dataclass(frozen=True)
class EntangledDeletionTopology:
    """Immutable startup-owned proof for process-local deletion inventory."""

    replica_count: int
    attestation: str

    def require_single_replica(self) -> None:
        if (
            self.replica_count != 1
            or self.attestation != ENTANGLED_SINGLE_REPLICA_ATTESTATION
        ):
            raise RuntimeError(
                "Entangled account deletion requires an attested single replica"
            )


def read_owner_only_secret_file(raw_path: str | Path) -> str:
    """Read a small regular secret without following the final symlink."""

    path = Path(str(raw_path or "").strip())
    if not str(path):
        raise RuntimeError("account deletion token file is required")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise RuntimeError("account deletion token file cannot be opened") from exc
    try:
        info = os.fstat(fd)
        mode = stat.S_IMODE(info.st_mode)
        if not stat.S_ISREG(info.st_mode):
            raise RuntimeError("account deletion token file must be regular")
        if mode & 0o077 or not mode & 0o400:
            raise RuntimeError("account deletion token file must be owner-only readable")
        with os.fdopen(fd, "rb", closefd=True) as handle:
            fd = -1
            raw = handle.read(4097)
    finally:
        if fd >= 0:
            os.close(fd)
    if not raw or len(raw) > 4096:
        raise RuntimeError("account deletion token file has invalid size")
    try:
        token = raw.decode("utf-8").strip()
    except UnicodeDecodeError as exc:
        raise RuntimeError("account deletion token file must be UTF-8") from exc
    if len(token) < 32 or any(character.isspace() for character in token):
        raise RuntimeError("account deletion token has invalid format")
    return token


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _request_digest(payload: EntangledDeletionRequest) -> str:
    return _digest(_canonical_json(payload.model_dump(mode="json")))


def _opaque_reference(operation_id: str, resource_type: str, count: int) -> str:
    return "sha256:" + _digest(f"{operation_id}\0{resource_type}\0{count}")


def ensure_account_deletion_schema(db: Any) -> None:
    """Create the hash-only ledger and permanent write barrier."""

    with db.transaction("global"):
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS entangled_account_deletion_operations (
                operation_digest text PRIMARY KEY,
                request_digest text NOT NULL,
                user_digest text NOT NULL,
                step_name text NOT NULL,
                state text NOT NULL CHECK (state IN ('pending', 'running', 'completed')),
                lease_owner_digest text,
                lease_expires_at double precision NOT NULL DEFAULT 0,
                response_json text,
                updated_at double precision NOT NULL
            )
            """
        )
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS entangled_account_deletion_blocks (
                user_digest text PRIMARY KEY,
                operation_digest text NOT NULL,
                created_at double precision NOT NULL
            )
            """
        )


class AccountDeletionWriteBarrier:
    """Postgres advisory-lock barrier shared by deletion and entity writers."""

    def __init__(self, db: Any) -> None:
        self._db = db

    @staticmethod
    def user_digest(user_id: str) -> str:
        return _digest(user_id)

    def establish_in_transaction(
        self, user_id: str, operation_id: str, *, now: float
    ) -> None:
        user_digest = self.user_digest(user_id)
        with self._db.transaction("account_deletion_user", resource_id=user_digest):
            self._db.execute(
                """
                INSERT INTO entangled_account_deletion_blocks(
                    user_digest, operation_digest, created_at
                ) VALUES (?, ?, ?)
                ON CONFLICT (user_digest) DO NOTHING
                """,
                (user_digest, _digest(operation_id), now),
            )
            if self._db.fetchone(
                "SELECT 1 AS present FROM entangled_account_deletion_blocks WHERE user_digest = ?",
                (user_digest,),
            ) is None:
                raise RuntimeError("account deletion block was not durable")

    def assert_writable_in_transaction(self, user_id: str) -> None:
        if not user_id:
            return
        if not self._db.in_transaction():
            raise RuntimeError("account deletion write guard requires a transaction")
        user_digest = self.user_digest(user_id)
        with self._db.transaction("account_deletion_user", resource_id=user_digest):
            blocked = self._db.fetchone(
                "SELECT 1 AS present FROM entangled_account_deletion_blocks WHERE user_digest = ?",
                (user_digest,),
            )
        if blocked is not None:
            raise AccountDeletedError("account is permanently blocked from entity writes")

    def is_blocked(self, user_id: str) -> bool:
        if not user_id:
            return False
        return self._db.fetchone(
            "SELECT 1 AS present FROM entangled_account_deletion_blocks WHERE user_digest = ?",
            (self.user_digest(user_id),),
        ) is not None


class PostgresDeletionLedger:
    """Lease/CAS operation ledger retaining no raw lifecycle identifiers."""

    def __init__(
        self,
        db: Any,
        barrier: AccountDeletionWriteBarrier,
        *,
        lease_seconds: int = 60,
    ) -> None:
        if lease_seconds <= 0:
            raise ValueError("account deletion lease must be positive")
        self._db = db
        self._barrier = barrier
        self._lease_seconds = lease_seconds

    def claim(
        self,
        payload: EntangledDeletionRequest,
        *,
        owner: str,
        now: float,
    ) -> tuple[str, dict[str, Any] | None]:
        operation_digest = _digest(payload.operation_id)
        request_digest = _request_digest(payload)
        user_digest = _digest(payload.user_id)
        owner_digest = _digest(owner)
        with self._db.transaction(
            "account_deletion_operation", resource_id=operation_digest
        ):
            row = self._db.fetchone(
                """
                SELECT request_digest, user_digest, step_name, state,
                       lease_owner_digest, lease_expires_at, response_json
                  FROM entangled_account_deletion_operations
                 WHERE operation_digest = ?
                 FOR UPDATE
                """,
                (operation_digest,),
            )
            if row is None:
                self._db.execute(
                    """
                    INSERT INTO entangled_account_deletion_operations(
                        operation_digest, request_digest, user_digest, step_name,
                        state, lease_owner_digest, lease_expires_at, updated_at
                    ) VALUES (?, ?, ?, ?, 'running', ?, ?, ?)
                    """,
                    (
                        operation_digest,
                        request_digest,
                        user_digest,
                        STEP_NAME,
                        owner_digest,
                        now + self._lease_seconds,
                        now,
                    ),
                )
                self._barrier.establish_in_transaction(
                    payload.user_id, payload.operation_id, now=now
                )
                return ("acquired", None)
            if (
                str(row["request_digest"]) != request_digest
                or str(row["user_digest"]) != user_digest
                or str(row["step_name"]) != STEP_NAME
            ):
                raise OperationConflict("operation identity mismatch")
            if str(row["state"]) == "completed":
                response = row["response_json"]
                if not isinstance(response, str):
                    raise RuntimeError("completed account deletion receipt is missing")
                return ("completed", json.loads(response))
            if (
                str(row["state"]) == "running"
                and float(row["lease_expires_at"] or 0) > now
            ):
                return ("running", None)
            cursor = self._db.execute(
                """
                UPDATE entangled_account_deletion_operations
                   SET state = 'running', lease_owner_digest = ?,
                       lease_expires_at = ?, updated_at = ?
                 WHERE operation_digest = ?
                   AND (state = 'pending'
                        OR (state = 'running' AND lease_expires_at <= ?))
                """,
                (
                    owner_digest,
                    now + self._lease_seconds,
                    now,
                    operation_digest,
                    now,
                ),
            )
            if int(getattr(cursor, "rowcount", 0) or 0) != 1:
                return ("running", None)
            self._barrier.establish_in_transaction(
                payload.user_id, payload.operation_id, now=now
            )
            return ("acquired", None)

    def complete(
        self,
        operation_id: str,
        *,
        owner: str,
        response: dict[str, Any],
        now: float,
    ) -> None:
        with self._db.transaction(
            "account_deletion_operation", resource_id=_digest(operation_id)
        ):
            cursor = self._db.execute(
                """
                UPDATE entangled_account_deletion_operations
                   SET state = 'completed', lease_owner_digest = NULL,
                       lease_expires_at = 0, response_json = ?, updated_at = ?
                 WHERE operation_digest = ? AND state = 'running'
                   AND lease_owner_digest = ?
                """,
                (
                    _canonical_json(response),
                    now,
                    _digest(operation_id),
                    _digest(owner),
                ),
            )
            if int(getattr(cursor, "rowcount", 0) or 0) != 1:
                raise OperationLeaseLost("account deletion operation lease was lost")

    def release(self, operation_id: str, *, owner: str, now: float) -> None:
        with self._db.transaction(
            "account_deletion_operation", resource_id=_digest(operation_id)
        ):
            self._db.execute(
                """
                UPDATE entangled_account_deletion_operations
                   SET state = 'pending', lease_owner_digest = NULL,
                       lease_expires_at = 0, updated_at = ?
                 WHERE operation_digest = ? AND state = 'running'
                   AND lease_owner_digest = ?
                """,
                (now, _digest(operation_id), _digest(owner)),
            )


def _definition_is_user_owned(store: Any, defn: Any, seen: set[str] | None = None) -> bool:
    if bool(getattr(defn, "user_scoped", False)):
        return True
    parent = getattr(defn, "parent", None)
    if not parent:
        return False
    seen = set(seen or ())
    if defn.name in seen:
        raise RuntimeError("cyclic entity ownership definition")
    seen.add(defn.name)
    return _definition_is_user_owned(store, store.get_def(parent[0]), seen)


def _definition_depth(store: Any, defn: Any, seen: set[str] | None = None) -> int:
    if bool(getattr(defn, "user_scoped", False)):
        return 0
    parent = getattr(defn, "parent", None)
    if not parent:
        return 0
    seen = set(seen or ())
    if defn.name in seen:
        raise RuntimeError("cyclic entity ownership definition")
    seen.add(defn.name)
    return 1 + _definition_depth(store, store.get_def(parent[0]), seen)


def _validate_definition_identifiers(defn: Any) -> None:
    validate_sql_identifier(str(defn.table), label="entity table")
    validate_sql_identifier(str(defn.id_field), label="entity id field")
    if bool(getattr(defn, "user_scoped", False)):
        validate_sql_identifier("user_id", label="entity owner field")
    for relationship in [
        item
        for item in [getattr(defn, "parent", None)]
        if item is not None
    ] + list(getattr(defn, "ownership_refs", ()) or ()):
        _parent_name, local_fk, parent_pk = relationship
        validate_sql_identifier(str(local_fk), label="ownership local field")
        validate_sql_identifier(str(parent_pk), label="ownership target field")


def _definition_owner_expression(
    store: Any,
    defn: Any,
    alias: str,
    *,
    prefix: str,
    depth: int = 0,
    seen: set[str] | None = None,
) -> str:
    """Build a scalar canonical owner projection for one registered row."""

    _validate_definition_identifiers(defn)
    if bool(getattr(defn, "user_scoped", False)):
        return f"{alias}.user_id"
    parent = getattr(defn, "parent", None)
    if not parent:
        raise RuntimeError("entity ownership definition has no user root")
    visited = set(seen or ())
    if defn.name in visited:
        raise RuntimeError("cyclic entity ownership definition")
    visited.add(defn.name)
    parent_name, local_fk, parent_pk = parent
    parent_def = store.get_def(parent_name)
    _validate_definition_identifiers(parent_def)
    parent_alias = f"{prefix}_{depth}"
    parent_owner = _definition_owner_expression(
        store,
        parent_def,
        parent_alias,
        prefix=prefix,
        depth=depth + 1,
        seen=visited,
    )
    return (
        f"(SELECT {parent_owner} FROM {parent_def.table} AS {parent_alias} "
        f"WHERE {parent_alias}.{parent_pk} = {alias}.{local_fk} LIMIT 1)"
    )


def _query_has_row(db: Any, sql: str, params: tuple[Any, ...] = ()) -> bool:
    return db.fetchone(sql, params) is not None


def _count_unattributed_registered_state(
    db: Any,
    store: Any,
    all_definitions: list[Any],
    direct_tables: list[dict[str, Any]],
) -> int:
    """Return bounded opaque witnesses for unresolved durable ownership."""

    unattributed = 0
    registered_tables = {str(item.table) for item in all_definitions}
    for row in direct_tables:
        table = str(row["table_name"])
        validate_sql_identifier(table, label="discovered tenant table")
        if table in registered_tables:
            # Registered definitions are classified by their declared ownership
            # graph below.  A global entity may retain a nullable ``user_id``
            # compatibility column without making its shared rows unattributed.
            continue
        if _query_has_row(
            db,
            f"""
            SELECT 1 AS present
              FROM {table}
             WHERE user_id IS NULL
                OR CAST(user_id AS text) = ''
                OR CAST(user_id AS text) !~ ?
                OR POSITION('..' IN CAST(user_id AS text)) > 0
             LIMIT 1
             /* account-deletion:invalid-direct-owner */
            """,
            (_SAFE_USER_ID.pattern,),
        ):
            unattributed += 1

    definitions_by_name = {str(item.name): item for item in all_definitions}
    owned_definition_names: set[str] = set()
    for defn in all_definitions:
        try:
            if _definition_is_user_owned(store, defn):
                owned_definition_names.add(str(defn.name))
        except (KeyError, RuntimeError, ValueError):
            _validate_definition_identifiers(defn)
            if _query_has_row(
                db,
                f"""
                SELECT 1 AS present FROM {defn.table} LIMIT 1
                /* account-deletion:unresolved-definition */
                """,
            ):
                unattributed += 1

    for defn in all_definitions:
        if str(defn.name) not in owned_definition_names:
            continue
        _validate_definition_identifiers(defn)
        parent = getattr(defn, "parent", None)
        if parent and not bool(getattr(defn, "user_scoped", False)):
            parent_name, local_fk, parent_pk = parent
            parent_def = definitions_by_name.get(str(parent_name))
            if parent_def is None:
                if _query_has_row(
                    db,
                    f"""
                    SELECT 1 AS present FROM {defn.table} LIMIT 1
                    /* account-deletion:unresolved-parent */
                    """,
                ):
                    unattributed += 1
            else:
                _validate_definition_identifiers(parent_def)
                if _query_has_row(
                    db,
                    f"""
                    SELECT 1 AS present
                      FROM {defn.table} AS owned_row
                      LEFT JOIN {parent_def.table} AS parent_row
                        ON parent_row.{parent_pk} = owned_row.{local_fk}
                     WHERE owned_row.{local_fk} IS NULL
                        OR parent_row.{parent_pk} IS NULL
                     LIMIT 1
                     /* account-deletion:orphan-parent */
                    """,
                ):
                    unattributed += 1

        for parent_name, local_fk, parent_pk in list(
            getattr(defn, "ownership_refs", ()) or ()
        ):
            parent_def = definitions_by_name.get(str(parent_name))
            if parent_def is None:
                if _query_has_row(
                    db,
                    f"""
                    SELECT 1 AS present FROM {defn.table} LIMIT 1
                    /* account-deletion:unresolved-ownership-reference */
                    """,
                ):
                    unattributed += 1
                continue
            try:
                child_owner = _definition_owner_expression(
                    store,
                    defn,
                    "owned_row",
                    prefix="child_owner",
                )
                parent_owner = _definition_owner_expression(
                    store,
                    parent_def,
                    "referenced_row",
                    prefix="reference_owner",
                )
            except (KeyError, RuntimeError, ValueError):
                unattributed += 1
                continue
            if _query_has_row(
                db,
                f"""
                SELECT 1 AS present
                  FROM {defn.table} AS owned_row
                  LEFT JOIN {parent_def.table} AS referenced_row
                    ON referenced_row.{parent_pk} = owned_row.{local_fk}
                 WHERE owned_row.{local_fk} IS NULL
                    OR referenced_row.{parent_pk} IS NULL
                    OR ({child_owner}) IS DISTINCT FROM ({parent_owner})
                 LIMIT 1
                 /* account-deletion:ownership-reference */
                """,
            ):
                unattributed += 1

    transitions_exist_sql = """
        SELECT 1 AS present FROM subagent_state_transitions LIMIT 1
        /* account-deletion:orphan-transition-unresolved */
    """
    subagent_def = definitions_by_name.get("subagents")
    agent_def = definitions_by_name.get("agents")
    if subagent_def is None or agent_def is None:
        if _query_has_row(db, transitions_exist_sql):
            unattributed += 1
    else:
        _validate_definition_identifiers(subagent_def)
        _validate_definition_identifiers(agent_def)
        subagent_parent = getattr(subagent_def, "parent", None)
        if not subagent_parent or str(subagent_parent[0]) != "agents":
            if _query_has_row(db, transitions_exist_sql):
                unattributed += 1
        else:
            _parent_name, subagent_agent_fk, _parent_pk = subagent_parent
            if _query_has_row(
                db,
                f"""
                SELECT 1 AS present
                  FROM subagent_state_transitions AS transition_row
                  LEFT JOIN {subagent_def.table} AS subagent_row
                    ON subagent_row.{subagent_def.id_field} = transition_row.subagent_id
                  LEFT JOIN {agent_def.table} AS agent_row
                    ON agent_row.{agent_def.id_field} = transition_row.agent_id
                 WHERE subagent_row.{subagent_def.id_field} IS NULL
                    OR (
                        transition_row.agent_id IS NOT NULL
                        AND agent_row.{agent_def.id_field} IS NULL
                    )
                    OR (
                        transition_row.agent_id IS NOT NULL
                        AND subagent_row.{subagent_agent_fk}
                            IS DISTINCT FROM transition_row.agent_id
                    )
                 LIMIT 1
                 /* account-deletion:orphan-transition */
                """,
            ):
                unattributed += 1
    return unattributed


def _classify_sync_rows(
    rows: list[dict[str, Any]], user_id: str
) -> tuple[list[str], int]:
    owned: list[str] = []
    unattributed = 0
    seen: set[str] = set()
    for row in rows:
        state_key = row["state_key"]
        valid, owner = classify_state_key_owner(state_key)
        if not valid:
            unattributed += 1
            continue
        if owner == user_id and state_key not in seen:
            seen.add(state_key)
            owned.append(state_key)
    return owned, unattributed


class EntangledDeletionDomain:
    """Discover, purge, and verify every Entangled-owned durable row."""

    def __init__(self, db: Any, store: Any, barrier: AccountDeletionWriteBarrier) -> None:
        self._db = db
        self._store = store
        self._barrier = barrier

    def purge_user(self, user_id: str) -> tuple[int, int, int, int]:
        user_digest = self._barrier.user_digest(user_id)
        discovered = 0
        deleted = 0
        with self._db.transaction("account_deletion_user", resource_id=user_digest):
            if self._db.fetchone(
                "SELECT 1 AS present FROM entangled_account_deletion_blocks WHERE user_digest = ?",
                (user_digest,),
            ) is None:
                raise RuntimeError("account deletion block is missing")

            all_definitions = list(self._store.get_all_defs())
            definitions = []
            for item in all_definitions:
                try:
                    if _definition_is_user_owned(self._store, item):
                        definitions.append(item)
                except (KeyError, RuntimeError, ValueError):
                    continue
            definitions.sort(
                key=lambda item: (_definition_depth(self._store, item), item.name),
                reverse=True,
            )

            registered_tables = {str(item.table) for item in all_definitions}
            direct_tables = self._db.fetchall(
                """
                SELECT DISTINCT table_name
                  FROM information_schema.columns
                 WHERE table_schema = current_schema() AND column_name = 'user_id'
                """
            )
            initial_sync_rows = self._db.fetchall(
                "SELECT state_key FROM entangled_sync_versions"
            )
            _initial_sync_keys, initial_sync_unattributed = _classify_sync_rows(
                initial_sync_rows, user_id
            )
            initial_unattributed = _count_unattributed_registered_state(
                self._db,
                self._store,
                all_definitions,
                direct_tables,
            ) + initial_sync_unattributed
            if initial_unattributed:
                return 0, 0, 0, initial_unattributed

            subagent_ids: list[str] = []
            agent_ids: list[str] = []
            for entity_name, target in (("subagents", subagent_ids), ("agents", agent_ids)):
                try:
                    defn = self._store.get_def(entity_name)
                except KeyError:
                    continue
                if not _definition_is_user_owned(self._store, defn):
                    continue
                where, values = self._store._scope_where(defn, user_id, None)
                rows = self._db.fetchall(
                    f"SELECT {defn.id_field} AS entity_id FROM {defn.table} WHERE {where}",
                    tuple(values),
                )
                target.extend(str(row["entity_id"]) for row in rows)

            transition_values = subagent_ids + agent_ids
            if transition_values:
                placeholders = ",".join("?" for _ in transition_values)
                transition_row = self._db.fetchone(
                    "SELECT COUNT(*) AS cnt FROM subagent_state_transitions "
                    f"WHERE subagent_id IN ({placeholders}) OR agent_id IN ({placeholders})",
                    tuple(transition_values + transition_values),
                )
                transition_count = int((transition_row or {}).get("cnt", 0) or 0)
                discovered += transition_count
                cursor = self._db.execute(
                    "DELETE FROM subagent_state_transitions "
                    f"WHERE subagent_id IN ({placeholders}) OR agent_id IN ({placeholders})",
                    tuple(transition_values + transition_values),
                )
                deleted += int(getattr(cursor, "rowcount", 0) or 0)

            for defn in definitions:
                validate_sql_identifier(defn.table, label="entity table")
                where, values = self._store._scope_where(defn, user_id, None)
                row = self._db.fetchone(
                    f"SELECT COUNT(*) AS cnt FROM {defn.table} WHERE {where}",
                    tuple(values),
                )
                discovered += int((row or {}).get("cnt", 0) or 0)
                cursor = self._db.execute(
                    f"DELETE FROM {defn.table} WHERE {where}", tuple(values)
                )
                deleted += int(getattr(cursor, "rowcount", 0) or 0)

            for row in direct_tables:
                table = str(row["table_name"])
                validate_sql_identifier(table, label="discovered tenant table")
                if table in registered_tables:
                    continue
                count_row = self._db.fetchone(
                    f"SELECT COUNT(*) AS cnt FROM {table} WHERE user_id = ?", (user_id,)
                )
                discovered += int((count_row or {}).get("cnt", 0) or 0)
                cursor = self._db.execute(
                    f"DELETE FROM {table} WHERE user_id = ?", (user_id,)
                )
                deleted += int(getattr(cursor, "rowcount", 0) or 0)

            sync_keys, _sync_unattributed = _classify_sync_rows(
                initial_sync_rows, user_id
            )
            discovered += len(sync_keys)
            for state_key in sync_keys:
                cursor = self._db.execute(
                    "DELETE FROM entangled_sync_versions WHERE state_key = ?",
                    (state_key,),
                )
                deleted += int(getattr(cursor, "rowcount", 0) or 0)

            remaining = 0
            for defn in definitions:
                where, values = self._store._scope_where(defn, user_id, None)
                row = self._db.fetchone(
                    f"SELECT COUNT(*) AS cnt FROM {defn.table} WHERE {where}",
                    tuple(values),
                )
                remaining += int((row or {}).get("cnt", 0) or 0)
            for row in direct_tables:
                table = str(row["table_name"])
                if table in registered_tables:
                    continue
                count_row = self._db.fetchone(
                    f"SELECT COUNT(*) AS cnt FROM {table} WHERE user_id = ?", (user_id,)
                )
                remaining += int((count_row or {}).get("cnt", 0) or 0)
            if transition_values:
                placeholders = ",".join("?" for _ in transition_values)
                transition_row = self._db.fetchone(
                    "SELECT COUNT(*) AS cnt FROM subagent_state_transitions "
                    f"WHERE subagent_id IN ({placeholders}) OR agent_id IN ({placeholders})",
                    tuple(transition_values + transition_values),
                )
                remaining += int((transition_row or {}).get("cnt", 0) or 0)
            final_sync_rows = self._db.fetchall(
                "SELECT state_key FROM entangled_sync_versions"
            )
            final_sync_keys, final_sync_unattributed = _classify_sync_rows(
                final_sync_rows, user_id
            )
            remaining += len(final_sync_keys)
            final_unattributed = _count_unattributed_registered_state(
                self._db,
                self._store,
                all_definitions,
                direct_tables,
            ) + final_sync_unattributed
            unattributed = max(initial_unattributed, final_unattributed)
        return deleted, remaining, discovered, unattributed


class EntangledDeletionService:
    def __init__(
        self,
        *,
        ledger: PostgresDeletionLedger,
        domain: EntangledDeletionDomain,
        connections: Any,
        sync_registry_provider: Any,
        topology: EntangledDeletionTopology,
    ) -> None:
        self._ledger = ledger
        self._domain = domain
        self._connections = connections
        self._sync_registry_provider = sync_registry_provider
        self._topology = topology

    async def execute(self, payload: EntangledDeletionRequest) -> dict[str, Any]:
        self._topology.require_single_replica()
        owner = secrets.token_hex(16)
        state, previous = self._ledger.claim(payload, owner=owner, now=time.time())
        if state == "completed":
            assert previous is not None
            return previous
        if state == "running":
            return {
                "verified": False,
                "result_code": "entangled_operation_in_progress",
                "deleted_count": 0,
                "remaining_count": 1,
                "resources": [],
            }
        try:
            connection_count = await self._connections.close_user(payload.user_id)
            from ..server.notifier import (
                get_unattributed_client_count,
                get_user_client_count,
                unregister_user_clients,
            )

            subscription_count = unregister_user_clients(payload.user_id)
            sync_registry = self._sync_registry_provider()
            sync_state_count = sync_registry.purge_user(payload.user_id)
            (
                row_deleted,
                row_remaining,
                row_discovered,
                row_unattributed,
            ) = self._domain.purge_user(payload.user_id)
            process_unattributed = (
                await self._connections.count_unattributed_user_owners()
                + get_unattributed_client_count()
                + sync_registry.count_unattributed_user_owners()
            )
            total_unattributed = row_unattributed + process_unattributed
            remaining = row_remaining + total_unattributed
            remaining += await self._connections.count_user(payload.user_id)
            remaining += get_user_client_count(payload.user_id)
            remaining += sync_registry.count_user_states(payload.user_id)
            discovered = {
                "entity_rows": row_discovered,
                "connections": connection_count,
                "subscriptions": subscription_count + sync_state_count,
                "unattributed_state": total_unattributed,
            }
            resources = [
                {
                    "domain": DOMAIN,
                    "resource_type": resource_type,
                    "reference": _opaque_reference(
                        payload.operation_id, resource_type, count
                    ),
                }
                for resource_type, count in discovered.items()
                if count > 0
            ]
            response = {
                "verified": remaining == 0,
                "result_code": (
                    "entangled_purged" if remaining == 0 else "entangled_resources_remain"
                ),
                "deleted_count": (
                    row_deleted + connection_count + subscription_count + sync_state_count
                ),
                "remaining_count": remaining,
                "resources": resources,
            }
            if remaining:
                self._ledger.release(payload.operation_id, owner=owner, now=time.time())
                return response
            self._ledger.complete(
                payload.operation_id,
                owner=owner,
                response=response,
                now=time.time(),
            )
            return response
        except Exception:
            self._ledger.release(payload.operation_id, owner=owner, now=time.time())
            raise


def _single_header(request: Request, name: bytes) -> str | None:
    values = [
        value.decode("latin-1")
        for key, value in request.scope.get("headers", [])
        if key.lower() == name
    ]
    return values[0] if len(values) == 1 else None


def _authenticate(
    request: Request, *, service_token: str, operation_id: str
) -> None:
    authorization = _single_header(request, b"authorization")
    internal_service = _single_header(request, b"x-internal-service")
    idempotency_key = _single_header(request, b"x-idempotency-key")
    content_type = _single_header(request, b"content-type")
    if (
        authorization is None
        or not hmac.compare_digest(authorization, f"Bearer {service_token}")
        or internal_service != CALLER
        or idempotency_key is None
        or not hmac.compare_digest(idempotency_key, operation_id)
        or content_type is None
        or content_type.split(";", 1)[0].strip().lower() != "application/json"
    ):
        raise HTTPException(status_code=401, detail="invalid_internal_authority")


def create_account_deletion_router(
    *, service_token: str, service_provider: Any
) -> APIRouter:
    if len(service_token) < 32:
        raise ValueError("account deletion service token is too short")
    router = APIRouter(tags=["internal-account-deletion"])

    @router.post("/internal/account-deletion/v2/purge_entangled")
    async def purge_entangled(request: Request) -> dict[str, Any]:
        raw_body = await request.body()
        if len(raw_body) > 1024 * 1024:
            raise HTTPException(status_code=413, detail="account_deletion_request_too_large")
        try:
            payload = EntangledDeletionRequest.model_validate_json(raw_body)
        except ValidationError as exc:
            raise HTTPException(
                status_code=422, detail="invalid_account_deletion_request_shape"
            ) from exc
        _authenticate(
            request, service_token=service_token, operation_id=payload.operation_id
        )
        service = service_provider()
        if service is None:
            raise HTTPException(status_code=503, detail="entangled_effect_unavailable")
        try:
            return await service.execute(payload)
        except OperationConflict as exc:
            raise HTTPException(status_code=409, detail="idempotency_conflict") from exc
        except Exception as exc:
            raise HTTPException(status_code=503, detail="entangled_effect_failed") from exc

    return router


__all__ = [
    "AccountDeletedError",
    "AccountDeletionWriteBarrier",
    "EntangledDeletionDomain",
    "EntangledDeletionRequest",
    "EntangledDeletionService",
    "EntangledDeletionTopology",
    "ENTANGLED_SINGLE_REPLICA_ATTESTATION",
    "PostgresDeletionLedger",
    "create_account_deletion_router",
    "ensure_account_deletion_schema",
    "read_owner_only_secret_file",
]
