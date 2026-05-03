"""Schema idField + notifier delta frames (protocol v1)."""

from entangled.server.defs import EntityDef


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

    class FakeDefn:
        name = "widgets"
        table = "widgets"

    class FakeStore:
        def __init__(self):
            self.registered = []
            self.ensured = []

        def register(self, defn):
            self.registered.append(defn.name)

        def ensure_schema(self, defn):
            self.ensured.append(defn.name)

        def get_schema(self):
            return [{"name": "widgets", "idField": "id"}]

    store = FakeStore()
    broadcasts = []

    monkeypatch.setattr(schema_module, "get_store", lambda: store)
    monkeypatch.setattr(schema_module.SqlEntityDef, "from_spec", lambda spec: FakeDefn())
    monkeypatch.setattr(schema_module, "notify_all", lambda event, data: broadcasts.append((event, data)))

    result = schema_module.register_schema(RegisterRequest(entities=[{"name": "widgets"}]))

    assert result["registered"] == ["widgets"]
    assert result["errors"] == []
    assert store.registered == ["widgets"]
    assert store.ensured == ["widgets"]
    assert len(broadcasts) == 1
    event, data = broadcasts[0]
    assert event == "schema"
    assert data["entities"] == [{"name": "widgets", "idField": "id"}]
    assert data["hash"]
    assert data["syncContractVersion"] == schema_module.SYNC_CONTRACT_VERSION
