import asyncio
from contextlib import contextmanager
import json
import os

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from entangled.app.account_deletion import (
    AccountDeletedError,
    AccountDeletionWriteBarrier,
    EntangledDeletionRequest,
    OperationConflict,
    OperationLeaseLost,
    PostgresDeletionLedger,
    create_account_deletion_router,
    ensure_account_deletion_schema,
    read_owner_only_secret_file,
)
from entangled.app.auth import SessionPrincipal
from entangled.app.connection_registry import (
    AuthenticatedConnection,
    AuthenticatedConnectionRegistry,
)
from entangled.server.sync import SyncRegistry
from entangled.sql.entity_def import SqlEntityDef
from entangled.sql.entity_store import SqlEntityStore
from entangled.sql.field_def import F


TOKEN = "d" * 48


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
