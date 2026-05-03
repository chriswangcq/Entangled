"""Schema idField + notifier delta frames (protocol v1)."""

import sqlite3

import pytest

from entangled.server.defs import EntityDef
from entangled.sql.entity_def import SqlEntityDef
from entangled.sql.entity_store import SqlEntityStore
from entangled.sql.field_def import F


class _FakeDatabase:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def execute(self, sql, params=()):
        return self._conn.execute(sql, params)

    def fetchone(self, sql, params=()):
        cur = self._conn.execute(sql, params)
        row = cur.fetchone()
        return dict(row) if row else None

    def fetchall(self, sql, params=()):
        cur = self._conn.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]

    class _Tx:
        def __init__(self, conn):
            self._conn = conn

        def __enter__(self):
            return self

        def __exit__(self, exc_type, *_):
            if exc_type:
                self._conn.rollback()
            else:
                self._conn.commit()

    def transaction(self, lock_type="global", resource_id="", timeout=None):
        return self._Tx(self._conn)


def _make_sql_store():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return SqlEntityStore(db=_FakeDatabase(conn))


def _widget_def(**overrides):
    data = {
        "name": "widgets",
        "table": "widgets",
        "id_field": "id",
        "user_scoped": True,
        "default_order": "created_at DESC",
        "fields": [
            F.text("id", primary=True),
            F.text("user_id", nullable=False, default="", index=True),
            F.text("name", nullable=False),
            F.timestamp("created_at"),
        ],
    }
    data.update(overrides)
    return SqlEntityDef(**data)


def test_to_schema_dict_id_field_defaults_to_id():
    d = EntityDef(name="todos", key_params=["project_id"])
    s = d.to_schema_dict()
    assert s["name"] == "todos"
    assert s["idField"] == "id"


def test_notifier_delta_includes_id_field(monkeypatch):
    from entangled.server import notifier
    from entangled.server.sync import SyncRegistry

    notifier.reset_state()

    class FakeDefn:
        name = "widgets"
        id_field = "widget_id"
        op_log_size = 1000
        relations = []
        user_scoped = True

    class FakeStore:
        def get_all_defs(self):
            return [FakeDefn()]

        def get_def(self, entity):
            if entity == "widgets":
                return FakeDefn()
            raise KeyError(entity)

    registry = SyncRegistry()
    notifier.set_store(FakeStore(), sync_registry=registry)

    pushed = []

    def push_fn(_event, payload):
        pushed.append(payload)

    notifier.register_client("c1", "u1", push_fn)
    registry.entangle("c1", "widgets", None)

    notifier.notify_entity_change("u1", "widgets", "created", entity_id="w1", data={"widget_id": "w1"})

    notifier.unregister_client("c1")
    notifier.reset_state()

    assert len(pushed) == 1
    assert pushed[0]["idField"] == "widget_id"
    assert pushed[0]["mode"] == "delta"


def test_scoped_delta_also_pushes_to_unscoped_user_subscription(monkeypatch):
    from entangled.server import notifier
    from entangled.server.sync import SyncRegistry

    notifier.reset_state()

    class FakeDefn:
        name = "messages"
        id_field = "msg_id"
        op_log_size = 1000
        relations = []
        user_scoped = True

    class FakeStore:
        def get_all_defs(self):
            return [FakeDefn()]

        def get_def(self, entity):
            if entity == "messages":
                return FakeDefn()
            raise KeyError(entity)

    registry = SyncRegistry()
    notifier.set_store(FakeStore(), sync_registry=registry)

    scoped_pushed = []
    unscoped_pushed = []

    notifier.register_client("scoped", "u1", lambda _event, payload: scoped_pushed.append(payload))
    notifier.register_client("unscoped", "u1", lambda _event, payload: unscoped_pushed.append(payload))
    registry.entangle("scoped", "messages", {"agent_id": "a1"})
    registry.entangle("unscoped", "messages", None)

    notifier.notify_entity_change(
        "u1",
        "messages",
        "created",
        entity_id="m1",
        params={"agent_id": "a1"},
        data={"msg_id": "m1", "agent_id": "a1", "text": "hello"},
    )

    notifier.unregister_client("scoped")
    notifier.unregister_client("unscoped")
    notifier.reset_state()

    assert len(scoped_pushed) == 1
    assert scoped_pushed[0]["params"] == {"agent_id": "a1"}
    assert scoped_pushed[0]["ops"][0]["data"]["text"] == "hello"

    assert len(unscoped_pushed) == 1
    assert unscoped_pushed[0]["params"] is None
    assert unscoped_pushed[0]["ops"][0]["data"]["agent_id"] == "a1"


def test_sync_push_port_override_routes_notify():
    from entangled.server import notifier
    from entangled.server.push_port import set_sync_push_port
    from entangled.server.sync import SyncRegistry

    notifier.reset_state()

    calls = []

    class StubPort:
        def notify_entity_change(self, *args, **kwargs):
            calls.append((args, kwargs))

    set_sync_push_port(StubPort())
    notifier.notify_entity_change("u1", "x", "updated", entity_id="1")
    notifier.reset_state()

    assert len(calls) == 1
    assert calls[0][0][0] == "u1"


def test_schema_registration_broadcasts_schema_update(monkeypatch):
    from entangled.app import schema as schema_module
    from entangled.app.schema import RegisterRequest

    store = _make_sql_store()
    broadcasts = []

    monkeypatch.setattr(schema_module, "get_store", lambda: store)
    monkeypatch.setattr(schema_module, "notify_all", lambda event, data: broadcasts.append((event, data)))

    result = schema_module.register_schema(RegisterRequest(entities=[_widget_def().to_spec()]))

    assert result["registered"] == ["widgets"]
    assert result["errors"] == []
    assert store.entities == ["widgets"]
    assert len(broadcasts) == 1
    event, data = broadcasts[0]
    assert event == "schema"
    assert data["entities"][0]["name"] == "widgets"
    assert data["entities"][0]["idField"] == "id"
    assert data["hash"]
    assert data["syncContractVersion"] == schema_module.SYNC_CONTRACT_VERSION


def test_schema_registration_rejects_reserved_field_without_broadcast(monkeypatch):
    from fastapi import HTTPException
    from entangled.app import schema as schema_module
    from entangled.app.schema import RegisterRequest

    store = _make_sql_store()
    broadcasts = []
    bad = _widget_def(fields=[
        F.text("id", primary=True),
        F.text("order"),
    ])

    monkeypatch.setattr(schema_module, "get_store", lambda: store)
    monkeypatch.setattr(schema_module, "notify_all", lambda event, data: broadcasts.append((event, data)))

    with pytest.raises(HTTPException) as exc:
        schema_module.register_schema(RegisterRequest(entities=[bad.to_spec()]))

    assert exc.value.status_code == 422
    assert "reserved SQL" in str(exc.value.detail)
    assert store.entities == []
    assert broadcasts == []


def test_schema_registration_batch_is_all_or_nothing(monkeypatch):
    from fastapi import HTTPException
    from entangled.app import schema as schema_module
    from entangled.app.schema import RegisterRequest

    store = _make_sql_store()
    broadcasts = []
    good = _widget_def()
    bad = _widget_def(
        name="bad-widgets",
        table="bad_widgets",
        fields=[F.text("id", primary=True), F.text("select")],
    )

    monkeypatch.setattr(schema_module, "get_store", lambda: store)
    monkeypatch.setattr(schema_module, "notify_all", lambda event, data: broadcasts.append((event, data)))

    with pytest.raises(HTTPException) as exc:
        schema_module.register_schema(RegisterRequest(entities=[good.to_spec(), bad.to_spec()]))

    assert exc.value.status_code == 422
    assert store.entities == []
    assert broadcasts == []


def test_runtime_filters_reject_unknown_sql_fields():
    from entangled.sql.validation import SchemaValidationError

    store = _make_sql_store()
    defn = _widget_def()
    store.ensure_schema(defn)
    store.register(defn)

    with pytest.raises(SchemaValidationError) as exc:
        store.list("widgets", "user-1", filters={"not_a_column": "x"})

    assert "unknown filter field" in str(exc.value)


def test_runtime_order_by_rejects_sql_fragments():
    from entangled.sql.validation import SchemaValidationError

    store = _make_sql_store()
    defn = _widget_def()
    store.ensure_schema(defn)
    store.register(defn)

    with pytest.raises(SchemaValidationError):
        store.list("widgets", "user-1", order_by="created_at DESC; DROP TABLE widgets")
