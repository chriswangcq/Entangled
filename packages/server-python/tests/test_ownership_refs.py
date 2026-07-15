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
    backend_name = "postgres"

    def __init__(self, owned_ids=()):
        self.owned_ids = set(owned_ids)
        self.executed = []

    @contextmanager
    def transaction(self, lock_type="global", resource_id="", timeout=None):
        yield self

    def execute(self, sql, params=()):
        self.executed.append((sql, params))
        return _Cursor()

    def fetchone(self, sql, params=()):
        self.executed.append((sql, params))
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


def _store(*, owned_ids=("agent-owned",), register_devices=True):
    db = _OwnershipDb(owned_ids)
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
