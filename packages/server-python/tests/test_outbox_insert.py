"""Tests for message_outbox co-transaction insert in SqlEntityStore.append().

Verifies that:
1. USER_MESSAGE appends → outbox row created
2. ASSISTANT_MESSAGE appends → no outbox row
3. payload_json contains decoded metadata (not double-encoded)
4. ON CONFLICT(message_id) DO NOTHING → duplicate insert is silent
"""
import json
import sqlite3
import time
import pytest

from entangled.sql.entity_store import SqlEntityStore
from entangled.sql.entity_def import SqlEntityDef
from entangled.sql.field_def import F


def _make_test_db():
    """Create an in-memory SQLite DB with a minimal messages table + outbox."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


class FakeDatabase:
    """Minimal db shim for SqlEntityStore that wraps a sqlite3.Connection."""
    def __init__(self, conn):
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

    class _TxCtx:
        def __init__(self, conn):
            self._conn = conn
        def __enter__(self):
            return self
        def __exit__(self, *args):
            self._conn.commit()

    def transaction(self, lock_type="global", resource_id="", timeout=None):
        return self._TxCtx(self._conn)


MESSAGES_DEF = SqlEntityDef(
    name="messages",
    table="chat_messages",
    id_field="id",
    user_scoped=False,
    key_params=["agent_id"],
    default_order="timestamp DESC",
    sync_type="list",
    fields=[
        F.text("id", primary=True),
        F.text("agent_id", nullable=False),
        F.text("type", nullable=False),
        F.json("content"),
        F.json("metadata", default="{}"),
        F.text("timestamp", nullable=False),
        F.int_("read", default=0),
    ],
    outbox_trigger_types={
        "USER_MESSAGE": "user_message",
        "SUBAGENT_SEND": "subagent_send",
        "SPAWN_SUBAGENT": "spawn_subagent",
    },
)


@pytest.fixture
def store():
    conn = _make_test_db()
    db = FakeDatabase(conn)
    s = SqlEntityStore(db=db)
    s.register(MESSAGES_DEF)
    s.ensure_schema(MESSAGES_DEF)
    s._ensure_outbox_schema()
    return s


def test_user_message_creates_outbox_row(store):
    """Writing a USER_MESSAGE should co-transactionally insert an outbox row."""
    result = store.append("messages", "", {
        "id": "msg-001",
        "agent_id": "agent-abc",
        "type": "USER_MESSAGE",
        "content": json.dumps({"text": "hello"}),
        "metadata": json.dumps({"model": "gpt-4"}),
        "timestamp": "2026-04-18T00:00:00Z",
        "read": 0,
    }, params={"agent_id": "agent-abc"}, notify=False)

    assert result["id"] == "msg-001"

    # Verify outbox row exists
    row = store.db.fetchone(
        "SELECT * FROM message_outbox WHERE message_id = ?", ("msg-001",)
    )
    assert row is not None
    assert row["agent_id"] == "agent-abc"
    assert row["trigger_type"] == "user_message"
    assert row["delivered_at"] is None

    # Verify payload_json is properly formed
    payload = json.loads(row["payload_json"])
    assert payload["message_ids"] == ["msg-001"]
    assert payload["metadata"] == {"model": "gpt-4"}  # decoded, not double-encoded


def test_assistant_message_no_outbox_row(store):
    """Writing an ASSISTANT_MESSAGE (not in outbox_trigger_types) should NOT create an outbox row."""
    store.append("messages", "", {
        "id": "msg-002",
        "agent_id": "agent-abc",
        "type": "ASSISTANT_MESSAGE",
        "content": json.dumps({"text": "hi there"}),
        "metadata": "{}",
        "timestamp": "2026-04-18T00:00:01Z",
        "read": 0,
    }, params={"agent_id": "agent-abc"}, notify=False)

    row = store.db.fetchone(
        "SELECT * FROM message_outbox WHERE message_id = ?", ("msg-002",)
    )
    assert row is None


def test_duplicate_message_id_silent(store):
    """ON CONFLICT(message_id) DO NOTHING should silently skip duplicates."""
    data = {
        "id": "msg-dup",
        "agent_id": "agent-abc",
        "type": "USER_MESSAGE",
        "content": "{}",
        "metadata": "{}",
        "timestamp": "2026-04-18T00:00:02Z",
        "read": 0,
    }
    store.append("messages", "", dict(data), params={"agent_id": "agent-abc"}, notify=False)

    # Attempt to write a second message with same ID — the chat_messages INSERT
    # will fail (PRIMARY KEY), but let's test the outbox protection separately
    # by directly inserting into outbox
    store.db.execute("""
        INSERT INTO message_outbox (message_id, agent_id, trigger_type, payload_json, created_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(message_id) DO NOTHING
    """, ("msg-dup", "agent-abc", "user_message", "{}", int(time.time() * 1000)))

    # Should still have exactly 1 outbox row
    count = store.db.fetchone(
        "SELECT COUNT(*) as cnt FROM message_outbox WHERE message_id = ?", ("msg-dup",)
    )
    assert count["cnt"] == 1


def test_subagent_send_extracts_subagent_id(store):
    """SUBAGENT_SEND should extract subagent_id from metadata into payload."""
    store.append("messages", "", {
        "id": "msg-sub",
        "agent_id": "agent-abc",
        "type": "SUBAGENT_SEND",
        "content": "{}",
        "metadata": json.dumps({"target_subagent_id": "sub-xyz", "foo": "bar"}),
        "timestamp": "2026-04-18T00:00:03Z",
        "read": 0,
    }, params={"agent_id": "agent-abc"}, notify=False)

    row = store.db.fetchone(
        "SELECT * FROM message_outbox WHERE message_id = ?", ("msg-sub",)
    )
    assert row is not None
    assert row["trigger_type"] == "subagent_send"

    payload = json.loads(row["payload_json"])
    assert payload["subagent_id"] == "sub-xyz"
    assert payload["metadata"]["foo"] == "bar"
