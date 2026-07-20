import asyncio
from contextlib import contextmanager
import json
import os

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from entangled.app import account_deletion
from entangled.app.account_deletion import (
    AccountDeletedError,
    AccountDeletionWriteBarrier,
    ENTANGLED_SINGLE_REPLICA_ATTESTATION,
    EntangledDeletionService,
    EntangledDeletionTopology,
    EntangledDeletionRequest,
    OperationConflict,
    OperationLeaseLost,
    PostgresDeletionLedger,
    create_account_deletion_router,
    ensure_account_deletion_schema,
    read_owner_only_secret_file,
)
from entangled.app.main import build_parser, config_from_args
from entangled.app.auth import SessionPrincipal
from entangled.app.connection_registry import (
    AuthenticatedConnection,
    AuthenticatedConnectionRegistry,
)
from entangled.app.config import ServiceConfig
from entangled.app.factory import create_app
from entangled.server import notifier
from entangled.server.sync import SyncRegistry
from entangled.sql.entity_def import SqlEntityDef
from entangled.sql.entity_store import SqlEntityStore
from entangled.sql.field_def import F


TOKEN = "d" * 48


def _single_replica_topology():
    return EntangledDeletionTopology(
        replica_count=1,
        attestation=ENTANGLED_SINGLE_REPLICA_ATTESTATION,
    )


def _payload(**overrides):
    value = {
        "schema_version": 2,
        "effect_contract": "discover_effect_verify_zero",
        "request_id": "request-1",
        "operation_id": "operation-1",
        "user_id": "user-1",
        "step_name": "purge_entangled",
        "resources": [],
    }
    value.update(overrides)
    return value


def _headers(**overrides):
    value = {
        "Authorization": f"Bearer {TOKEN}",
        "X-Internal-Service": "account-deletion-worker",
        "X-Idempotency-Key": "operation-1",
    }
    value.update(overrides)
    return value


class _Service:
    def __init__(self):
        self.payloads = []

    async def execute(self, payload):
        self.payloads.append(payload)
        return {
            "verified": True,
            "result_code": "entangled_purged",
            "deleted_count": 3,
            "remaining_count": 0,
            "resources": [
                {
                    "domain": "entangled",
                    "resource_type": "entity_rows",
                    "reference": "sha256:" + "a" * 64,
                }
            ],
        }


def _client(service=None):
    service = service or _Service()
    app = FastAPI()
    app.include_router(
        create_account_deletion_router(
            service_token=TOKEN, service_provider=lambda: service
        )
    )
    return TestClient(app), service


def test_http_contract_accepts_exact_gateway_shape_and_returns_minimal_receipt():
    client, service = _client()
    response = client.post(
        "/internal/account-deletion/v2/purge_entangled",
        json=_payload(),
        headers=_headers(),
    )
    assert response.status_code == 200
    assert set(response.json()) == {
        "verified",
        "result_code",
        "deleted_count",
        "remaining_count",
        "resources",
    }
    assert service.payloads[0].resources == []


@pytest.mark.parametrize(
    "payload",
    [
        _payload(schema_version="2"),
        _payload(effect_contract="account-deletion-effect-v2"),
        _payload(step_name="purge_device"),
        _payload(extra="provider-secret"),
        _payload(account_deletion_replica_count=1),
        _payload(
            resources=[
                {
                    "domain": "device",
                    "resource_type": "row",
                    "reference": "opaque",
                }
            ]
        ),
    ],
)
def test_http_contract_rejects_non_exact_shapes_without_echo(payload):
    client, _service = _client()
    response = client.post(
        "/internal/account-deletion/v2/purge_entangled",
        json=payload,
        headers=_headers(),
    )
    assert response.status_code == 422
    assert response.json() == {"detail": "invalid_account_deletion_request_shape"}
    assert "provider-secret" not in response.text


@pytest.mark.parametrize(
    "headers",
    [
        {},
        _headers(Authorization="Bearer wrong"),
        _headers(**{"X-Internal-Service": "device"}),
        _headers(**{"X-Idempotency-Key": "different"}),
    ],
)
def test_http_contract_rejects_missing_or_mismatched_authority(headers):
    client, _service = _client()
    response = client.post(
        "/internal/account-deletion/v2/purge_entangled",
        json=_payload(),
        headers=headers,
    )
    assert response.status_code == 401
    assert response.json() == {"detail": "invalid_internal_authority"}


def test_http_contract_rejects_duplicate_security_header():
    client, _service = _client()
    headers = list(_headers().items()) + [("Authorization", f"Bearer {TOKEN}")]
    response = client.post(
        "/internal/account-deletion/v2/purge_entangled",
        json=_payload(),
        headers=headers,
    )
    assert response.status_code == 401


def test_owner_only_token_file_rejects_group_access(tmp_path):
    path = tmp_path / "token"
    path.write_text(TOKEN)
    os.chmod(path, 0o600)
    assert read_owner_only_secret_file(path) == TOKEN
    os.chmod(path, 0o640)
    with pytest.raises(RuntimeError, match="owner-only"):
        read_owner_only_secret_file(path)


class _Cursor:
    def __init__(self, rowcount=0):
        self.rowcount = rowcount


class _DefinitionStore:
    def __init__(self, definitions):
        self.definitions = {item.name: item for item in definitions}

    def get_all_defs(self):
        return list(self.definitions.values())

    def get_def(self, name):
        return self.definitions[name]


class _InventoryProbeDb:
    def __init__(self, active_markers=()):
        self.active_markers = set(active_markers)
        self.queries = []

    def fetchone(self, sql, params=()):
        normalized = " ".join(sql.split())
        self.queries.append((normalized, params))
        if any(marker in normalized for marker in self.active_markers):
            return {"present": 1}
        return None


def _definition(
    name,
    *,
    user_scoped,
    parent=None,
    ownership_refs=None,
):
    return SqlEntityDef(
        name=name,
        table=name.replace("-", "_"),
        id_field="id",
        user_scoped=user_scoped,
        parent=parent,
        ownership_refs=list(ownership_refs or ()),
    )


@pytest.mark.parametrize(
    ("marker", "definitions", "direct_tables"),
    [
        (
            "account-deletion:invalid-direct-owner",
            [],
            [{"table_name": "unregistered_rows"}],
        ),
        (
            "account-deletion:orphan-parent",
            [
                _definition("agents", user_scoped=True),
                _definition(
                    "subagents",
                    user_scoped=False,
                    parent=("agents", "agent_id", "id"),
                ),
            ],
            [],
        ),
        (
            "account-deletion:ownership-reference",
            [
                _definition("agents", user_scoped=True),
                _definition("devices", user_scoped=True),
                _definition(
                    "agent-binding",
                    user_scoped=False,
                    parent=("agents", "agent_id", "id"),
                    ownership_refs=[("devices", "device_id", "id")],
                ),
            ],
            [],
        ),
        (
            "account-deletion:orphan-transition",
            [
                _definition("agents", user_scoped=True),
                _definition(
                    "subagents",
                    user_scoped=False,
                    parent=("agents", "agent_id", "id"),
                ),
            ],
            [],
        ),
    ],
)
def test_durable_inventory_counts_one_opaque_witness_per_ambiguity_category(
    marker, definitions, direct_tables
):
    db = _InventoryProbeDb([marker])
    store = _DefinitionStore(definitions)

    count = account_deletion._count_unattributed_registered_state(
        db, store, definitions, direct_tables
    )

    assert count == 1
    assert all(query.startswith("SELECT") for query, _params in db.queries)


def test_durable_inventory_blocks_extant_rows_with_unresolved_parent_definition():
    definition = _definition(
        "orphan-child",
        user_scoped=False,
        parent=("missing-parent", "parent_id", "id"),
    )
    db = _InventoryProbeDb(["account-deletion:unresolved-definition"])
    store = _DefinitionStore([definition])

    count = account_deletion._count_unattributed_registered_state(
        db, store, [definition], []
    )

    assert count == 1


def test_ownership_reference_probe_compares_recursive_canonical_owners():
    definitions = [
        _definition("agents", user_scoped=True),
        _definition("devices", user_scoped=True),
        _definition(
            "agent-binding",
            user_scoped=False,
            parent=("agents", "agent_id", "id"),
            ownership_refs=[("devices", "device_id", "id")],
        ),
    ]
    db = _InventoryProbeDb(["account-deletion:ownership-reference"])
    store = _DefinitionStore(definitions)

    account_deletion._count_unattributed_registered_state(
        db, store, definitions, []
    )

    query = next(
        sql
        for sql, _params in db.queries
        if "account-deletion:ownership-reference" in sql
    )
    assert "FROM agents AS child_owner_0" in query
    assert "owned_row.agent_id" in query
    assert "referenced_row.user_id" in query
    assert "IS DISTINCT FROM" in query


def test_direct_user_scoped_definition_is_a_root_even_with_external_parent():
    definition = _definition(
        "user-preferences",
        user_scoped=True,
        parent=("users", "user_id", "id"),
    )
    store = _DefinitionStore([definition])

    assert account_deletion._definition_depth(store, definition) == 0


def test_durable_sync_inventory_preserves_global_keys_and_blocks_malformed_owners():
    rows = [
        {"state_key": "models"},
        {"state_key": 'models:[["kind","chat"]]'},
        {"state_key": 'messages:{"params":[],"user_id":"user-1"}'},
        {"state_key": 'messages:{"params":[],"user_id":"user-2"}'},
        {"state_key": "messages:{bad-json"},
        {"state_key": 'messages:{"params":[]}'},
        {"state_key": 'messages:{"params":[],"user_id":"../owner"}'},
    ]

    owned, unattributed = account_deletion._classify_sync_rows(rows, "user-1")

    assert owned == ['messages:{"params":[],"user_id":"user-1"}']
    assert unattributed == 3


class _AmbiguousDomainDb:
    backend_name = "postgres"

    def __init__(self):
        self.executed = []

    @contextmanager
    def transaction(self, *_args, **_kwargs):
        yield self

    def fetchall(self, sql, _params=()):
        normalized = " ".join(sql.split())
        if "information_schema.columns" in normalized:
            return [{"table_name": "unregistered_rows"}]
        if "SELECT state_key FROM entangled_sync_versions" in normalized:
            return []
        raise AssertionError(f"unexpected fetchall: {normalized}")

    def fetchone(self, sql, _params=()):
        normalized = " ".join(sql.split())
        if "FROM entangled_account_deletion_blocks" in normalized:
            return {"present": 1}
        if "account-deletion:invalid-direct-owner" in normalized:
            return {"present": 1}
        if "account-deletion:orphan-transition-unresolved" in normalized:
            return None
        if normalized.startswith("SELECT COUNT(*) AS cnt"):
            return {"cnt": 0}
        raise AssertionError(f"unexpected fetchone: {normalized}")

    def execute(self, sql, params=()):
        self.executed.append((" ".join(sql.split()), params))
        raise AssertionError("durable effects must not run with ambiguous ownership")


def test_domain_preserves_ambiguous_row_and_returns_only_opaque_blocker():
    db = _AmbiguousDomainDb()
    barrier = AccountDeletionWriteBarrier(db)
    domain = account_deletion.EntangledDeletionDomain(
        db,
        _DefinitionStore([]),
        barrier,
    )

    deleted, remaining, discovered, unattributed = domain.purge_user("user-1")

    assert (deleted, remaining, discovered, unattributed) == (0, 0, 0, 1)
    assert db.executed == []


class _LedgerDb:
    backend_name = "postgres"

    def __init__(self):
        self.depth = 0
        self.operations = {}
        self.blocks = {}
        self.sql = []

    def in_transaction(self):
        return self.depth > 0

    @contextmanager
    def transaction(self, _lock_type="global", **_kwargs):
        self.depth += 1
        try:
            yield self
        finally:
            self.depth -= 1

    def execute(self, sql, params=()):
        normalized = " ".join(sql.split())
        self.sql.append((normalized, params))
        if normalized.startswith("CREATE TABLE"):
            return _Cursor()
        if normalized.startswith("INSERT INTO entangled_account_deletion_blocks"):
            self.blocks.setdefault(params[0], params[1:])
            return _Cursor(1)
        if normalized.startswith("INSERT INTO entangled_account_deletion_operations"):
            self.operations[params[0]] = {
                "request_digest": params[1],
                "user_digest": params[2],
                "step_name": params[3],
                "state": "running",
                "lease_owner_digest": params[4],
                "lease_expires_at": params[5],
                "response_json": None,
                "updated_at": params[6],
            }
            return _Cursor(1)
        if normalized.startswith("UPDATE entangled_account_deletion_operations"):
            if "SET state = 'running'" in normalized:
                owner, lease, updated, operation, now = params
                row = self.operations[operation]
                if not (
                    row["state"] == "pending"
                    or (
                        row["state"] == "running"
                        and row["lease_expires_at"] <= now
                    )
                ):
                    return _Cursor(0)
                row.update(
                    state="running",
                    lease_owner_digest=owner,
                    lease_expires_at=lease,
                    updated_at=updated,
                )
                return _Cursor(1)
            if "SET state = 'completed'" in normalized:
                response, updated, operation, owner = params
                row = self.operations[operation]
                if row["state"] != "running" or row["lease_owner_digest"] != owner:
                    return _Cursor(0)
                row.update(
                    state="completed",
                    lease_owner_digest=None,
                    lease_expires_at=0,
                    response_json=response,
                    updated_at=updated,
                )
                return _Cursor(1)
            if "SET state = 'pending'" in normalized:
                updated, operation, owner = params
                row = self.operations[operation]
                if row["state"] == "running" and row["lease_owner_digest"] == owner:
                    row.update(
                        state="pending",
                        lease_owner_digest=None,
                        lease_expires_at=0,
                        updated_at=updated,
                    )
                    return _Cursor(1)
                return _Cursor(0)
        raise AssertionError(f"unexpected SQL: {normalized}")

    def fetchone(self, sql, params=()):
        normalized = " ".join(sql.split())
        self.sql.append((normalized, params))
        if "FROM entangled_account_deletion_operations" in normalized:
            row = self.operations.get(params[0])
            return dict(row) if row is not None else None
        if "FROM entangled_account_deletion_blocks" in normalized:
            return {"present": 1} if params[0] in self.blocks else None
        raise AssertionError(f"unexpected SQL: {normalized}")


def test_postgres_ledger_reclaims_expired_lease_with_cas_and_replays_receipt():
    db = _LedgerDb()
    ensure_account_deletion_schema(db)
    barrier = AccountDeletionWriteBarrier(db)
    ledger = PostgresDeletionLedger(db, barrier, lease_seconds=10)
    payload = EntangledDeletionRequest.model_validate(_payload())

    assert ledger.claim(payload, owner="owner-a", now=100) == ("acquired", None)
    assert ledger.claim(payload, owner="owner-b", now=105) == ("running", None)
    assert ledger.claim(payload, owner="owner-b", now=111) == ("acquired", None)
    receipt = {
        "verified": True,
        "result_code": "entangled_purged",
        "deleted_count": 0,
        "remaining_count": 0,
        "resources": [],
    }
    with pytest.raises(OperationLeaseLost):
        ledger.complete("operation-1", owner="owner-a", response=receipt, now=112)
    ledger.complete("operation-1", owner="owner-b", response=receipt, now=112)
    assert ledger.claim(payload, owner="owner-c", now=113) == ("completed", receipt)
    assert "user-1" not in repr(db.operations)
    assert "request-1" not in repr(db.operations)
    assert "operation-1" not in repr(db.operations)
    assert any(
        "lease_expires_at <=" in sql and "state = 'pending'" in sql
        for sql, _params in db.sql
    )


def test_postgres_ledger_rejects_cross_user_idempotency_key_reuse():
    db = _LedgerDb()
    barrier = AccountDeletionWriteBarrier(db)
    ledger = PostgresDeletionLedger(db, barrier)
    first = EntangledDeletionRequest.model_validate(_payload())
    second = EntangledDeletionRequest.model_validate(_payload(user_id="user-2"))
    ledger.claim(first, owner="owner-a", now=1)
    with pytest.raises(OperationConflict):
        ledger.claim(second, owner="owner-b", now=100)


def test_write_barrier_is_permanent_and_hash_only():
    db = _LedgerDb()
    barrier = AccountDeletionWriteBarrier(db)
    with db.transaction("test"):
        barrier.establish_in_transaction("user-1", "operation-1", now=1)
    assert barrier.is_blocked("user-1") is True
    assert barrier.is_blocked("user-2") is False
    with db.transaction("write"):
        with pytest.raises(AccountDeletedError):
            barrier.assert_writable_in_transaction("user-1")
    assert "user-1" not in repr(db.blocks)


class _ServiceLedger:
    def __init__(self, state, response=None):
        self.state = state
        self.response = response
        self.claims = 0
        self.completions = 0
        self.releases = 0

    def claim(self, _payload, *, owner, now):
        assert owner
        assert now > 0
        self.claims += 1
        return self.state, self.response

    def complete(self, *_args, **_kwargs):
        self.completions += 1

    def release(self, *_args, **_kwargs):
        self.releases += 1


class _ServiceEffects:
    def __init__(self):
        self.calls = 0

    async def close_user(self, _user_id):
        self.calls += 1
        return 0

    async def count_user(self, _user_id):
        self.calls += 1
        return 0


class _UnusedDomain:
    def purge_user(self, _user_id):
        raise AssertionError("completed replay must not repeat domain effects")


@pytest.mark.parametrize(
    ("replica_count", "attestation"),
    [
        (0, ENTANGLED_SINGLE_REPLICA_ATTESTATION),
        (2, ENTANGLED_SINGLE_REPLICA_ATTESTATION),
        (1, ""),
        (1, "entangled-account-deletion-single-replica-v0"),
    ],
)
def test_service_rejects_unattested_topology_before_claim_or_replay(
    replica_count, attestation
):
    async def scenario():
        receipt = {
            "verified": True,
            "result_code": "entangled_purged",
            "deleted_count": 0,
            "remaining_count": 0,
            "resources": [],
        }
        ledger = _ServiceLedger("completed", receipt)
        effects = _ServiceEffects()
        service = EntangledDeletionService(
            ledger=ledger,
            domain=_UnusedDomain(),
            connections=effects,
            sync_registry_provider=lambda: None,
            topology=EntangledDeletionTopology(replica_count, attestation),
        )

        with pytest.raises(RuntimeError, match="attested single replica"):
            await service.execute(EntangledDeletionRequest.model_validate(_payload()))

        assert ledger.claims == 0
        assert effects.calls == 0

    asyncio.run(scenario())


def test_attested_completed_replay_is_exact_and_skips_all_effects():
    async def scenario():
        receipt = {
            "verified": True,
            "result_code": "entangled_purged",
            "deleted_count": 17,
            "remaining_count": 0,
            "resources": [
                {
                    "domain": "entangled",
                    "resource_type": "entity_rows",
                    "reference": "sha256:" + "a" * 64,
                }
            ],
        }
        ledger = _ServiceLedger("completed", receipt)
        effects = _ServiceEffects()
        service = EntangledDeletionService(
            ledger=ledger,
            domain=_UnusedDomain(),
            connections=effects,
            sync_registry_provider=lambda: None,
            topology=_single_replica_topology(),
        )

        replay = await service.execute(
            EntangledDeletionRequest.model_validate(_payload())
        )

        assert replay == receipt
        assert ledger.claims == 1
        assert ledger.completions == ledger.releases == 0
        assert effects.calls == 0

    asyncio.run(scenario())


def test_cli_topology_defaults_fail_closed_and_requires_exact_explicit_values():
    default_config = config_from_args(build_parser().parse_args([]))
    assert default_config.account_deletion_replica_count == 0
    assert default_config.account_deletion_topology_attestation == ""

    configured = config_from_args(
        build_parser().parse_args(
            [
                "--account-deletion-replica-count",
                "1",
                "--account-deletion-topology-attestation",
                ENTANGLED_SINGLE_REPLICA_ATTESTATION,
            ]
        )
    )
    topology = EntangledDeletionTopology(
        configured.account_deletion_replica_count,
        configured.account_deletion_topology_attestation,
    )
    topology.require_single_replica()


def test_app_factory_requires_topology_when_account_deletion_is_enabled():
    with pytest.raises(RuntimeError, match="attested single replica"):
        create_app(ServiceConfig(account_deletion_service_token=TOKEN))

    app = create_app(
        ServiceConfig(
            account_deletion_service_token=TOKEN,
            account_deletion_replica_count=1,
            account_deletion_topology_attestation=(
                ENTANGLED_SINGLE_REPLICA_ATTESTATION
            ),
        )
    )
    assert any(
        route.path == "/internal/account-deletion/v2/purge_entangled"
        for route in app.routes
    )


def test_sync_registry_purges_only_target_user_state_and_subscriptions():
    registry = SyncRegistry()
    registry.entangle("client-1", "messages", user_id="user-1")
    registry.entangle("client-2", "messages", user_id="user-2")
    registry.record_op("messages", "insert", "m1", user_id="user-1")
    registry.record_op("messages", "insert", "m2", user_id="user-2")

    assert registry.count_user_states("user-1") == 1
    assert registry.purge_user("user-1") == 1
    assert registry.count_user_states("user-1") == 0
    assert registry.count_user_states("user-2") == 1
    assert registry.get_entangled_clients("messages", user_id="user-2") == [
        "client-2"
    ]


def test_sync_registry_audits_malformed_state_and_subscription_owners():
    registry = SyncRegistry()
    registry.entangle("bad-state", "messages", user_id="../state-owner")
    registry._client_subs["bad-subscription"] = {
        'messages:{"params":[],"user_id":"../subscription-owner"}'
    }

    assert registry.count_unattributed_user_owners() == 2
    assert registry.purge_user("user-1") == 0
    assert registry.count_unattributed_user_owners() == 2


def test_sync_registry_purges_valid_owned_dangling_subscription():
    registry = SyncRegistry()
    target_key = 'messages:{"params":[],"user_id":"user-1"}'
    registry._client_subs["dangling-client"] = {target_key}

    assert registry.count_user_states("user-1") == 1
    assert registry.purge_user("user-1") == 1
    assert registry.count_user_states("user-1") == 0
    assert "dangling-client" not in registry._client_subs


def test_connection_registry_closes_only_deleted_users_sockets():
    async def scenario():
        registry = AuthenticatedConnectionRegistry()
        closed = []

        async def add(connection_id, user_id):
            async def close(code, reason):
                closed.append((connection_id, code, reason))

            await registry.register(
                AuthenticatedConnection(
                    connection_id,
                    SessionPrincipal(user_id, connection_id, 1, 999, "staging"),
                    close,
                )
            )

        await add("socket-a", "user-1")
        await add("socket-b", "user-1")
        await add("socket-c", "user-2")
        assert await registry.close_user("user-1") == 2
        assert await registry.count_user("user-1") == 0
        assert await registry.count_user("user-2") == 1
        assert {item[0] for item in closed} == {"socket-a", "socket-b"}
        assert all(item[1:] == (4403, "Account deleted") for item in closed)

    asyncio.run(scenario())


def test_connection_registry_audits_noncanonical_principal_owner():
    async def scenario():
        registry = AuthenticatedConnectionRegistry()

        async def close(_code, _reason):
            return None

        await registry.register(
            AuthenticatedConnection(
                "socket-bad",
                SessionPrincipal("../connection-owner", "sid", 1, 999, "staging"),
                close,
            )
        )

        assert await registry.count_unattributed_user_owners() == 1
        assert await registry.close_user("user-1") == 0
        assert await registry.count_unattributed_user_owners() == 1

    asyncio.run(scenario())


def test_notifier_audits_noncanonical_client_owner():
    notifier.reset_state()
    try:
        notifier.set_store(_DefinitionStore([]), sync_registry=SyncRegistry())
        notifier.register_client("client-bad", "../notifier-owner", lambda _item: None)

        assert notifier.get_unattributed_client_count() == 1
        assert notifier.unregister_user_clients("user-1") == 0
        assert notifier.get_unattributed_client_count() == 1
    finally:
        notifier.reset_state()


class _BlockedDomain:
    def __init__(self):
        self.calls = 0

    def purge_user(self, _user_id):
        self.calls += 1
        return 0, 0, 0, 1


def test_service_composes_opaque_process_blockers_and_never_caches_zero():
    async def scenario():
        notifier.reset_state()
        try:
            sync_registry = SyncRegistry()
            notifier.set_store(
                _DefinitionStore([]),
                sync_registry=sync_registry,
            )
            notifier.register_client(
                "client-bad",
                "../notifier-owner",
                lambda _item: None,
            )
            sync_registry.entangle(
                "sync-bad",
                "messages",
                user_id="../sync-owner",
            )

            connections = AuthenticatedConnectionRegistry()

            async def close(_code, _reason):
                return None

            await connections.register(
                AuthenticatedConnection(
                    "socket-bad",
                    SessionPrincipal(
                        "../connection-owner",
                        "sid",
                        1,
                        999,
                        "staging",
                    ),
                    close,
                )
            )
            ledger = _ServiceLedger("acquired")
            domain = _BlockedDomain()
            service = EntangledDeletionService(
                ledger=ledger,
                domain=domain,
                connections=connections,
                sync_registry_provider=lambda: sync_registry,
                topology=_single_replica_topology(),
            )
            payload = EntangledDeletionRequest.model_validate(_payload())

            first = await service.execute(payload)
            second = await service.execute(payload)

            assert first == second
            assert first == {
                "verified": False,
                "result_code": "entangled_resources_remain",
                "deleted_count": 0,
                "remaining_count": 4,
                "resources": [
                    {
                        "domain": "entangled",
                        "resource_type": "unattributed_state",
                        "reference": first["resources"][0]["reference"],
                    }
                ],
            }
            assert first["resources"][0]["reference"].startswith("sha256:")
            assert len(first["resources"][0]["reference"]) == 71
            assert "../" not in json.dumps(first)
            assert ledger.claims == ledger.releases == domain.calls == 2
            assert ledger.completions == 0
            assert await connections.count_unattributed_user_owners() == 1
            assert notifier.get_unattributed_client_count() == 1
            assert sync_registry.count_unattributed_user_owners() == 1
        finally:
            notifier.reset_state()

    asyncio.run(scenario())


def test_sql_entity_create_hits_resurrection_guard_before_insert():
    class Db:
        backend_name = "postgres"

        def __init__(self):
            self.executed = []

        @contextmanager
        def transaction(self, *_args, **_kwargs):
            yield self

        def execute(self, sql, params=()):
            self.executed.append((sql, params))
            return _Cursor(1)

    class Guard:
        def assert_writable_in_transaction(self, user_id):
            assert user_id == "user-1"
            raise AccountDeletedError("blocked")

    db = Db()
    store = SqlEntityStore(db=db)
    store.register(
        SqlEntityDef(
            name="notes",
            table="notes",
            id_field="id",
            user_scoped=True,
            fields=[
                F.text("id", primary=True),
                F.text("user_id", nullable=False),
                F.text("body", default=""),
            ],
        )
    )
    store.configure_account_deletion_guard(Guard())
    with pytest.raises(AccountDeletedError):
        store.create(
            "notes", "user-1", {"id": "note-1", "body": "late write"}
        )
    assert db.executed == []
