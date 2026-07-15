from contextlib import contextmanager

import pytest

from entangled.sql.entity_def import SqlEntityDef
from entangled.sql.entity_store import SqlEntityStore
from entangled.sql.field_def import F
from entangled.sql.validation import SchemaValidationError, validate_entity_def


class _Cursor:
    rowcount = 1
    lastrowid = None


class _OwnershipDb:
    def __init__(self, owned_ids=(), *, backend_name="postgres"):
        self.backend_name = backend_name
        self.owned_ids = set(owned_ids)
        self.executed = []
        self.events = []

    @contextmanager
    def transaction(self, lock_type="global", resource_id="", timeout=None):
        self.events.append(("transaction_begin", lock_type, resource_id))
        try:
            yield self
        finally:
            self.events.append(("transaction_end", lock_type, resource_id))

    def execute(self, sql, params=()):
        self.executed.append((sql, params))
        self.events.append(("execute", sql, params))
        return _Cursor()

    def fetchone(self, sql, params=()):
        self.executed.append((sql, params))
        self.events.append(("fetchone", sql, params))
        if " AS owned" in sql and params and params[0] in self.owned_ids:
            return {"owned": 1}
        return None


def _tenant_entity(name: str) -> SqlEntityDef:
    return SqlEntityDef(
        name=name,
        table=name.replace("-", "_"),
        id_field="id",
        user_scoped=True,
        fields=[
            F.text("id", primary=True),
            F.text("user_id", nullable=False),
        ],
    )


def _binding_def() -> SqlEntityDef:
    return SqlEntityDef(
        name="agent-binding",
        table="agent_device_bindings",
        id_field="agent_id",
        user_scoped=False,
        key_params=["agent_id"],
        parent=("agents", "agent_id", "id"),
        ownership_refs=[("devices", "device_id", "id")],
        default_order="agent_id",
        fields=[
            F.text("agent_id", primary=True),
            F.text("device_id", nullable=False),
            F.json("mounted_tools", default="{}"),
        ],
    )


def _store(
    *,
    owned_ids=("agent-owned",),
    register_devices=True,
    backend_name="postgres",
):
    db = _OwnershipDb(owned_ids, backend_name=backend_name)
    store = SqlEntityStore(db=db)
    store.register(_tenant_entity("agents"))
    if register_devices:
        store.register(_tenant_entity("devices"))
    binding = _binding_def()
    store.register(binding)
    return store, binding, db


@pytest.mark.parametrize(
    "operation",
    ("create", "upsert", "append", "update", "batch", "update_where", "cas"),
)
def test_all_insert_and_reference_update_paths_reject_foreign_owner(operation):
    store, binding, db = _store()
    row = {
        "agent_id": "agent-owned",
        "device_id": "device-foreign",
        "mounted_tools": {},
    }

    with pytest.raises(PermissionError, match="ownership denied"):
        if operation == "create":
            store._sql_create(
                binding,
                "tenant-a",
                row,
                params={"agent_id": "agent-owned"},
            )
        elif operation == "upsert":
            store._sql_upsert(
                binding,
                "tenant-a",
                "agent-owned",
                row,
                params={"agent_id": "agent-owned"},
            )
        elif operation == "append":
            store.append(
                "agent-binding",
                "tenant-a",
                row,
                params={"agent_id": "agent-owned"},
                notify=False,
            )
        elif operation == "update":
            store._sql_update(
                binding,
                "tenant-a",
                "agent-owned",
                {"device_id": "device-foreign"},
                params={"agent_id": "agent-owned"},
            )
        elif operation == "batch":
            store.batch_update(
                "agent-binding",
                "tenant-a",
                ["agent-owned"],
                {"device_id": "device-foreign"},
                params={"agent_id": "agent-owned"},
                emit_notifications=False,
            )
        elif operation == "update_where":
            store.update_where(
                "agent-binding",
                "tenant-a",
                {"device_id": "device-foreign"},
                params={"agent_id": "agent-owned"},
                notify=False,
            )
        else:
            store.cas_update(
                "agent-binding",
                "tenant-a",
                {"agent_id": "agent-owned"},
                {"device_id": "device-foreign"},
                params={"agent_id": "agent-owned"},
                emit_notifications=False,
            )

    assert not any(
        sql.startswith(("INSERT INTO agent_device_bindings", "UPDATE agent_device_bindings"))
        for sql, _params in db.executed
    )


def test_owned_agent_and_device_allow_binding_upsert():
    store, binding, db = _store(owned_ids=("agent-owned", "device-owned"))

    store._sql_upsert(
        binding,
        "tenant-a",
        "agent-owned",
        {"device_id": "device-owned", "mounted_tools": {}},
        params={"agent_id": "agent-owned"},
    )

    assert any(
        sql.startswith("INSERT INTO agent_device_bindings")
        for sql, _params in db.executed
    )


@pytest.mark.parametrize("backend_name", ("postgres", "sqlite"))
def test_ownership_checks_lock_postgres_targets_before_child_mutation(
    backend_name,
):
    store, binding, db = _store(
        owned_ids=("agent-owned", "device-owned"),
        backend_name=backend_name,
    )

    store._sql_upsert(
        binding,
        "tenant-a",
        "agent-owned",
        {"device_id": "device-owned", "mounted_tools": {}},
        params={"agent_id": "agent-owned"},
    )

    ownership_reads = [
        (index, event[1])
        for index, event in enumerate(db.events)
        if event[0] == "fetchone" and " AS owned" in event[1]
    ]
    mutation_index = next(
        index
        for index, event in enumerate(db.events)
        if event[0] == "execute"
        and event[1].startswith("INSERT INTO agent_device_bindings")
    )
    transaction_begin = next(
        index for index, event in enumerate(db.events) if event[0] == "transaction_begin"
    )
    transaction_end = next(
        index for index, event in enumerate(db.events) if event[0] == "transaction_end"
    )

    assert len(ownership_reads) == 2
    if backend_name == "postgres":
        assert all(sql.endswith(" FOR KEY SHARE") for _index, sql in ownership_reads)
    else:
        assert all("FOR KEY SHARE" not in sql for _index, sql in ownership_reads)
    assert transaction_begin < ownership_reads[0][0] < ownership_reads[1][0]
    assert ownership_reads[1][0] < mutation_index < transaction_end


def test_ownership_guard_requires_tenant_identity():
    store, binding, db = _store(owned_ids=("agent-owned", "device-owned"))

    with pytest.raises(PermissionError, match="tenant identity required"):
        store._sql_upsert(
            binding,
            "",
            "agent-owned",
            {"device_id": "device-owned"},
            params={"agent_id": "agent-owned"},
        )

    assert not any(
        sql.startswith("INSERT INTO agent_device_bindings")
        for sql, _params in db.executed
    )


def test_unknown_ownership_entity_fails_closed_at_write_time():
    store, binding, db = _store(register_devices=False)

    with pytest.raises(ValueError, match="is not registered"):
        store._sql_upsert(
            binding,
            "tenant-a",
            "agent-owned",
            {"device_id": "device-owned"},
            params={"agent_id": "agent-owned"},
        )

    assert not any(
        sql.startswith("INSERT INTO agent_device_bindings")
        for sql, _params in db.executed
    )


def test_quarantine_migration_rejects_additional_ownership_refs_before_sql():
    store, _binding, db = _store()
    guarded = SqlEntityDef(
        name="guarded-migration",
        table="guarded_migration",
        id_field="id",
        user_scoped=True,
        parent=("agents", "agent_id", "id"),
        ownership_refs=[("devices", "device_id", "id")],
        fields=[
            F.text("id", primary=True),
            F.text("user_id", nullable=False),
            F.text("agent_id", nullable=False),
            F.text("device_id", nullable=False),
        ],
    )
    store.register(guarded)

    with pytest.raises(ValueError, match="additional ownership references"):
        store.migrate_quarantined_tenant_ownership(
            "guarded-migration",
            "__legacy_unowned__",
            [
                {
                    "entity_id": "row-1",
                    "user_id": "tenant-a",
                    "parent_id": "agent-owned",
                }
            ],
            emit_notifications=False,
        )

    assert db.executed == []


def test_ownership_refs_round_trip_through_schema_spec():
    original = _binding_def()

    restored = SqlEntityDef.from_spec(original.to_spec())

    assert restored.ownership_refs == [("devices", "device_id", "id")]
    validate_entity_def(
        restored,
        known_defs={
            "agents": _tenant_entity("agents"),
            "devices": _tenant_entity("devices"),
        },
    )


@pytest.mark.parametrize(
    "ownership_refs",
    (
        "devices",
        [["devices", "device_id"]],
        [["devices", "", "id"]],
    ),
)
def test_malformed_ownership_ref_specs_are_rejected(ownership_refs):
    spec = _binding_def().to_spec()
    spec["ownership_refs"] = ownership_refs

    with pytest.raises(ValueError, match="ownership_refs"):
        SqlEntityDef.from_spec(spec)


def test_missing_ownership_local_field_is_rejected_by_schema_validation():
    binding = _binding_def()
    binding.ownership_refs = [("devices", "missing_device_id", "id")]

    with pytest.raises(SchemaValidationError, match="ownership reference local field"):
        validate_entity_def(binding)


def test_missing_ownership_parent_field_is_rejected_when_parent_is_known():
    binding = _binding_def()
    binding.ownership_refs = [("devices", "device_id", "missing_id")]

    with pytest.raises(SchemaValidationError, match="ownership reference parent field"):
        validate_entity_def(
            binding,
            known_defs={"devices": _tenant_entity("devices")},
        )
