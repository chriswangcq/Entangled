from __future__ import annotations

from contextlib import contextmanager
import re
from pathlib import Path

import pytest

from common.account_deletion_fixture import (
    FixtureCategory,
    FixtureContractError,
    FixtureOperation,
    FixtureOwner,
    FixtureReplayLedger,
    fixture_account_user_id,
    mint_fixture_request,
)
from entangled.app.account_deletion import (
    AccountDeletionWriteBarrier,
    EntangledDeletionDomain,
)
from entangled.sql.entity_def import SqlEntityDef
from entangled.sql.entity_store import SqlEntityStore
from entangled.sql.field_def import F
from entangled.staging_fixture.relational_store import (
    EntangledFixtureStoreError,
    EntangledRelationalFixtureStore,
)


CAPABILITY_SECRET = b"entangled-fixture-capability-secret-value-01"
DERIVATION_SECRET = b"entangled-fixture-derivation-secret-value-02"
RUN_HANDLE = "1" * 64
ARTIFACT_DIGEST = "sha256:" + "2" * 64


class _Cursor:
    def __init__(self, rowcount: int = 0) -> None:
        self.rowcount = rowcount


class _FixtureDb:
    backend_name = "postgres"

    def __init__(self) -> None:
        self.depth = 0
        self.rows: dict[str, dict[str, object]] = {}
        self.blocks: set[str] = set()
        self.agent_insert_count = 0

    def in_transaction(self) -> bool:
        return self.depth > 0

    @contextmanager
    def transaction(self, *_args, **_kwargs):
        self.depth += 1
        try:
            yield self
        finally:
            self.depth -= 1

    def execute(self, sql: str, params: tuple = ()) -> _Cursor:
        normalized = " ".join(sql.split())
        if normalized.startswith("INSERT INTO entangled_account_deletion_blocks"):
            self.blocks.add(str(params[0]))
            return _Cursor(1)
        if normalized.startswith("INSERT INTO agents"):
            columns_match = re.search(r"INSERT INTO agents \(([^)]+)\)", normalized)
            assert columns_match is not None
            columns = [item.strip() for item in columns_match.group(1).split(",")]
            row = dict(zip(columns, params, strict=True))
            agent_id = str(row["id"])
            if agent_id in self.rows:
                raise RuntimeError("duplicate agent")
            self.rows[agent_id] = row
            self.agent_insert_count += 1
            return _Cursor(1)
        if normalized.startswith("DELETE FROM agents"):
            if len(params) == 2:
                user_id, agent_id = map(str, params)
                row = self.rows.get(agent_id)
                if row is None or row.get("user_id") != user_id:
                    return _Cursor(0)
                del self.rows[agent_id]
                return _Cursor(1)
            user_id = str(params[0])
            owned = [
                agent_id
                for agent_id, row in self.rows.items()
                if row.get("user_id") == user_id
            ]
            for agent_id in owned:
                del self.rows[agent_id]
            return _Cursor(len(owned))
        if normalized.startswith("DELETE FROM subagent_state_transitions"):
            return _Cursor(0)
        raise AssertionError(f"unexpected execute: {normalized}")

    def fetchone(self, sql: str, params: tuple = ()):
        normalized = " ".join(sql.split())
        if "FROM entangled_account_deletion_blocks" in normalized:
            return {"present": 1} if str(params[0]) in self.blocks else None
        if normalized.startswith("SELECT * FROM agents"):
            user_id, agent_id = map(str, params)
            row = self.rows.get(agent_id)
            return dict(row) if row is not None and row.get("user_id") == user_id else None
        if "account-deletion:invalid-direct-owner" in normalized:
            return None
        if "account-deletion:orphan-transition-unresolved" in normalized:
            return None
        if normalized.startswith("SELECT COUNT(*) AS cnt FROM subagent_state_transitions"):
            return {"cnt": 0}
        if normalized.startswith("SELECT COUNT(*) AS cnt FROM agents"):
            user_id = str(params[0])
            return {
                "cnt": sum(
                    row.get("user_id") == user_id for row in self.rows.values()
                )
            }
        raise AssertionError(f"unexpected fetchone: {normalized}")

    def fetchall(self, sql: str, params: tuple = ()):
        normalized = " ".join(sql.split())
        if "FROM information_schema.columns" in normalized:
            return [{"table_name": "agents"}]
        if normalized == "SELECT state_key FROM entangled_sync_versions":
            return []
        if normalized.startswith("SELECT id AS entity_id FROM agents"):
            user_id = str(params[0])
            return [
                {"entity_id": agent_id}
                for agent_id, row in self.rows.items()
                if row.get("user_id") == user_id
            ]
        raise AssertionError(f"unexpected fetchall: {normalized}")


def _agents_definition() -> SqlEntityDef:
    return SqlEntityDef(
        name="agents",
        table="agents",
        id_field="id",
        user_scoped=True,
        key_params=[],
        subscription_mode="eager",
        default_order="created_at",
        fields=[
            F.text("id", primary=True),
            F.text("user_id", nullable=False, default="", index=True),
            F.text("name", nullable=False),
            F.timestamp("created_at"),
            F.bool_("setup_complete", default=False),
            F.text("model_id"),
            F.timestamp("updated_at"),
        ],
    )


def _ledger(tmp_path: Path) -> FixtureReplayLedger:
    state = tmp_path / "replay"
    state.mkdir(mode=0o700, parents=True)
    return FixtureReplayLedger(
        state / "entangled.sqlite3",
        owner=FixtureOwner.ENTANGLED,
    )


def _store(
    tmp_path: Path,
) -> tuple[
    EntangledRelationalFixtureStore,
    _FixtureDb,
    AccountDeletionWriteBarrier,
    SqlEntityStore,
]:
    database = _FixtureDb()
    entity_store = SqlEntityStore(db=database)
    entity_store.register(_agents_definition())
    barrier = AccountDeletionWriteBarrier(database)
    entity_store.configure_account_deletion_guard(barrier)
    return (
        EntangledRelationalFixtureStore(
            namespace="staging",
            store=entity_store,
            capability_secret=CAPABILITY_SECRET,
            derivation_secret=DERIVATION_SECRET,
            replay_ledger=_ledger(tmp_path),
        ),
        database,
        barrier,
        entity_store,
    )


def _request(
    operation: FixtureOperation,
    *,
    capability_id: str,
    category: FixtureCategory = FixtureCategory.RELATIONAL_ROWS,
) -> dict[str, object]:
    return mint_fixture_request(
        secret=CAPABILITY_SECRET,
        owner=FixtureOwner.ENTANGLED,
        category=category,
        operation=operation,
        run_handle=RUN_HANDLE,
        artifact_digest=ARTIFACT_DIGEST,
        capability_id=capability_id,
    )


def test_seed_observe_cleanup_are_exact_idempotent_and_aggregate_only(tmp_path: Path) -> None:
    fixture, database, _barrier, _entity_store = _store(tmp_path)

    first = fixture.handle_request(_request(FixtureOperation.SEED, capability_id="3" * 64))
    second = fixture.handle_request(_request(FixtureOperation.SEED, capability_id="4" * 64))
    observed = fixture.handle_request(_request(FixtureOperation.OBSERVE, capability_id="5" * 64))
    cleaned = fixture.handle_request(_request(FixtureOperation.CLEANUP, capability_id="6" * 64))
    empty = fixture.handle_request(_request(FixtureOperation.OBSERVE, capability_id="7" * 64))

    assert [first.count, second.count, observed.count, cleaned.count, empty.count] == [1, 1, 1, 0, 0]
    assert database.agent_insert_count == 1
    assert database.rows == {}
    response = first.to_dict()
    assert set(response) == {
        "schema_version", "owner", "category", "operation", "count", "receipt_digest", "verified"
    }
    serialized = repr(response)
    assert RUN_HANDLE not in serialized
    assert fixture_account_user_id(
        derivation_secret=DERIVATION_SECRET,
        run_handle=RUN_HANDLE,
    ) not in serialized


def test_capability_replay_wrong_category_and_prod_fail_closed(tmp_path: Path) -> None:
    fixture, _database, _barrier, _entity_store = _store(tmp_path)
    request = _request(FixtureOperation.SEED, capability_id="8" * 64)
    fixture.handle_request(request)
    with pytest.raises(FixtureContractError, match="already been used"):
        fixture.handle_request(request)
    with pytest.raises(FixtureContractError):
        fixture.handle_request(
            _request(
                FixtureOperation.OBSERVE,
                capability_id="9" * 64,
                category=FixtureCategory.STORED_OBJECTS,
            )
        )

    database = _FixtureDb()
    entity_store = SqlEntityStore(db=database)
    entity_store.register(_agents_definition())
    with pytest.raises(EntangledFixtureStoreError, match="authority"):
        EntangledRelationalFixtureStore(
            namespace="prod",
            store=entity_store,
            capability_secret=CAPABILITY_SECRET,
            derivation_secret=DERIVATION_SECRET,
            replay_ledger=_ledger(tmp_path / "prod"),
        )


def test_exact_agents_definition_is_required(tmp_path: Path) -> None:
    database = _FixtureDb()
    entity_store = SqlEntityStore(db=database)
    definition = _agents_definition()
    definition.table = "not_agents"
    entity_store.register(definition)
    with pytest.raises(EntangledFixtureStoreError, match="schema is invalid"):
        EntangledRelationalFixtureStore(
            namespace="staging",
            store=entity_store,
            capability_secret=CAPABILITY_SECRET,
            derivation_secret=DERIVATION_SECRET,
            replay_ledger=_ledger(tmp_path),
        )


def test_permanent_deletion_barrier_rejects_fixture_resurrection(tmp_path: Path) -> None:
    fixture, database, barrier, entity_store = _store(tmp_path)
    fixture.handle_request(_request(FixtureOperation.SEED, capability_id="a" * 64))
    user_id = fixture_account_user_id(
        derivation_secret=DERIVATION_SECRET,
        run_handle=RUN_HANDLE,
    )
    with database.transaction("test"):
        barrier.establish_in_transaction(user_id, "fixture-deletion", now=1)
    domain = EntangledDeletionDomain(database, entity_store, barrier)
    assert domain.purge_user(user_id) == (1, 0, 1, 0)
    assert fixture.handle_request(
        _request(FixtureOperation.OBSERVE, capability_id="b" * 64)
    ).count == 0

    with pytest.raises(EntangledFixtureStoreError, match="seed is unavailable"):
        fixture.handle_request(_request(FixtureOperation.SEED, capability_id="c" * 64))
    assert database.agent_insert_count == 1
    assert database.rows == {}


def test_fixture_source_has_no_generic_or_remote_execution_surface() -> None:
    source = Path(
        "entangled/staging_fixture/relational_store.py"
    ).read_text(encoding="utf-8")
    for forbidden in (
        "subprocess",
        "urllib",
        "http://",
        "https://",
        "target_table",
        "target_entity",
        "command",
    ):
        assert forbidden not in source
