import asyncio

from entangled.server.defs import EntityDef
from entangled.server.ws_handler import _entangle_one, _stream_head_order_by


class _Ws:
    def __init__(self):
        self.sent = []

    async def send_json(self, data):
        self.sent.append(data)


class _StreamStore:
    def __init__(self, rows):
        self.rows = rows
        self.list_calls = []
        self.list_stream_calls = []
        self.defn = EntityDef(
            name="activity-stream",
            key_params=["agent_id"],
            sync_type="stream",
            sync_limit=3,
        )
        self.defn.id_field = "id"
        self.defn.default_order = "sort_order ASC, created_at ASC"

    def get_def(self, entity):
        assert entity == self.defn.name
        return self.defn

    def list(self, entity, user_id, *, params=None, limit=None):
        self.list_calls.append((entity, user_id, params, limit))
        return list(self.rows[: limit or len(self.rows)])

    def list_stream(
        self,
        entity,
        user_id,
        *,
        params=None,
        before_id=None,
        after_id=None,
        limit=50,
        order_by=None,
        cursor_field=None,
    ):
        self.list_stream_calls.append(
            {
                "entity": entity,
                "user_id": user_id,
                "params": params,
                "before_id": before_id,
                "after_id": after_id,
                "limit": limit,
                "order_by": order_by,
                "cursor_field": cursor_field,
            }
        )
        agent_id = (params or {}).get("agent_id")
        scoped = [row for row in self.rows if row["agent_id"] == agent_id]
        newest_first = sorted(scoped, key=lambda row: row["sort_order"], reverse=True)
        return newest_first[:limit]

    def exists_before(self, entity, user_id, oldest_id, *, params=None):
        agent_id = (params or {}).get("agent_id")
        oldest = next(row for row in self.rows if row["id"] == oldest_id)
        return any(
            row["agent_id"] == agent_id and row["sort_order"] < oldest["sort_order"]
            for row in self.rows
        )


def test_stream_head_order_by_forces_latest_window_order():
    defn = EntityDef(name="activity-stream")
    defn.default_order = "sort_order ASC, created_at ASC"

    assert _stream_head_order_by(defn) == "sort_order DESC, created_at DESC"


def test_entangle_stream_first_subscribe_uses_latest_scoped_window():
    rows = [
        {"id": "a1-old", "agent_id": "a1", "sort_order": 1, "created_at": "t1"},
        {"id": "a1-mid", "agent_id": "a1", "sort_order": 2, "created_at": "t2"},
        {"id": "a1-newer", "agent_id": "a1", "sort_order": 3, "created_at": "t3"},
        {"id": "a1-newest", "agent_id": "a1", "sort_order": 4, "created_at": "t4"},
        {"id": "a2-newest", "agent_id": "a2", "sort_order": 99, "created_at": "t99"},
    ]
    store = _StreamStore(rows)
    ws = _Ws()

    asyncio.run(
        _entangle_one(
            ws,
            store,
            user_id="user-1",
            client_id="client-stream-head-test",
            entity="activity-stream",
            params={"agent_id": "a1"},
            client_version=None,
            client_head=None,
            depth=3,
        )
    )

    assert store.list_calls == []
    assert store.list_stream_calls == [
        {
            "entity": "activity-stream",
            "user_id": "user-1",
            "params": {"agent_id": "a1"},
            "before_id": None,
            "after_id": None,
            "limit": 3,
            "order_by": "sort_order DESC, created_at DESC",
            "cursor_field": None,
        }
    ]
    frame = ws.sent[-1]
    assert frame["mode"] == "head_n"
    assert frame["hasMore"] is True
    assert [row["id"] for row in frame["data"]] == ["a1-mid", "a1-newer", "a1-newest"]
