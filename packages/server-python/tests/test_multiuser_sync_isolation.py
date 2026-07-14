import asyncio

from entangled.server.defs import EntityDef
from entangled.server.notifier import (
    notify_entity_change,
    register_client,
    reset_state,
    set_store,
)
from entangled.server.sync import SyncRegistry
from entangled.server.ws_handler import _entangle_one, handle_disentangle


class _Ws:
    def __init__(self):
        self.sent = []

    async def send_json(self, data):
        self.sent.append(data)


class _Store:
    def __init__(self, defs):
        self.defs = {defn.name: defn for defn in defs}

    def get_all_defs(self):
        return list(self.defs.values())

    def get_def(self, entity):
        return self.defs[entity]

    def list(self, entity, user_id, *, params=None, limit=None):
        return [{"id": f"{user_id}-snapshot"}]


def _def(name, *, user_scoped, parent=None):
    defn = EntityDef(name=name, sync_type="list")
    defn.user_scoped = user_scoped
    defn.parent = parent
    defn.id_field = "id"
    return defn


def _reconnect_frame(store, *, entity, user_id, client_version=0):
    ws = _Ws()
    asyncio.run(
        _entangle_one(
            ws,
            store,
            user_id=user_id,
            client_id=f"client-{user_id}-{entity}",
            entity=entity,
            params=None,
            client_version=client_version,
            client_head=None,
            depth=None,
        )
    )
    return ws.sent[-1]


def test_user_scoped_reconnect_cannot_replay_another_users_delta():
    store = _Store([_def("notes", user_scoped=True)])
    registry = SyncRegistry()
    set_store(store, sync_registry=registry)
    try:
        notify_entity_change(
            "user-1",
            "notes",
            "created",
            entity_id="private-1",
            data={"id": "private-1", "secret": "user-1-only"},
        )

        frame = _reconnect_frame(store, entity="notes", user_id="user-2")

        assert frame["mode"] == "snapshot"
        assert frame["data"] == [{"id": "user-2-snapshot"}]
        assert "user-1-only" not in repr(frame)
    finally:
        reset_state()


def test_parent_scoped_reconnect_cannot_replay_another_users_delta():
    agents = _def("agents", user_scoped=True)
    messages = _def(
        "messages",
        user_scoped=False,
        parent=("agents", "agent_id", "id"),
    )
    store = _Store([agents, messages])
    registry = SyncRegistry()
    set_store(store, sync_registry=registry)
    try:
        notify_entity_change(
            "user-1",
            "messages",
            "created",
            entity_id="message-1",
            data={"id": "message-1", "body": "user-1-only"},
        )

        frame = _reconnect_frame(store, entity="messages", user_id="user-2")

        assert frame["mode"] == "snapshot"
        assert frame["data"] == [{"id": "user-2-snapshot"}]
        assert "user-1-only" not in repr(frame)
    finally:
        reset_state()


def test_user_scoped_live_push_reaches_only_owning_users_client():
    store = _Store([_def("notes", user_scoped=True)])
    registry = SyncRegistry()
    set_store(store, sync_registry=registry)
    user_1_pushes = []
    user_2_pushes = []
    try:
        register_client(
            "client-user-1",
            "user-1",
            lambda event, frame: user_1_pushes.append((event, frame)),
        )
        register_client(
            "client-user-2",
            "user-2",
            lambda event, frame: user_2_pushes.append((event, frame)),
        )
        registry.entangle("client-user-1", "notes", user_id="user-1")
        registry.entangle("client-user-2", "notes", user_id="user-2")

        notify_entity_change(
            "user-1",
            "notes",
            "created",
            entity_id="private-1",
            data={"id": "private-1", "secret": "user-1-only"},
        )

        assert len(user_1_pushes) == 1
        assert user_1_pushes[0][1]["ops"][0]["data"]["secret"] == "user-1-only"
        assert user_2_pushes == []
    finally:
        reset_state()


def test_user_scoped_disentangle_removes_only_that_scoped_subscription():
    store = _Store([_def("notes", user_scoped=True)])
    registry = SyncRegistry()
    set_store(store, sync_registry=registry)
    pushes = []
    params = {"folder_id": "folder-1"}
    try:
        register_client(
            "client-user-1",
            "user-1",
            lambda event, frame: pushes.append((event, frame)),
        )
        registry.entangle(
            "client-user-1",
            "notes",
            params,
            user_id="user-1",
        )

        handle_disentangle(
            "client-user-1",
            {"entity": "notes", "params": params},
            store=store,
            user_id="user-1",
        )
        notify_entity_change(
            "user-1",
            "notes",
            "updated",
            entity_id="private-1",
            params=params,
            data={"id": "private-1", "name": "after-disentangle"},
        )

        assert pushes == []
    finally:
        reset_state()


def test_global_entity_reconnect_still_replays_shared_delta():
    store = _Store([_def("models", user_scoped=False)])
    registry = SyncRegistry()
    set_store(store, sync_registry=registry)
    try:
        notify_entity_change(
            "user-1",
            "models",
            "updated",
            entity_id="model-1",
            data={"id": "model-1", "name": "shared-model"},
        )

        frame = _reconnect_frame(store, entity="models", user_id="user-2")

        assert frame["mode"] == "delta"
        assert frame["ops"][0]["data"]["name"] == "shared-model"
    finally:
        reset_state()


def test_entity_beneath_global_parent_still_uses_shared_partition():
    catalogs = _def("catalogs", user_scoped=False)
    entries = _def(
        "catalog-entries",
        user_scoped=False,
        parent=("catalogs", "catalog_id", "id"),
    )
    store = _Store([catalogs, entries])
    registry = SyncRegistry()
    set_store(store, sync_registry=registry)
    try:
        notify_entity_change(
            "user-1",
            "catalog-entries",
            "updated",
            entity_id="entry-1",
            data={"id": "entry-1", "name": "shared-entry"},
        )

        frame = _reconnect_frame(store, entity="catalog-entries", user_id="user-2")

        assert frame["mode"] == "delta"
        assert frame["ops"][0]["data"]["name"] == "shared-entry"
    finally:
        reset_state()
