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
        # PR-41 mirror of the real MESSAGES_DEF so the lifecycle-at-
        # insert logic in entity_store.append has a column to write.
        F.text("lifecycle", default="pending"),
        F.int_("lifecycle_updated_at"),
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


def test_duplicate_outbox_insert_silent(store):
    """ON CONFLICT(message_id) DO NOTHING protects against subscriber retries.
    NOTE: Not triggered through append() in normal flow (chat_messages PK fires first).
    Simulated here by direct SQL insert to verify the constraint semantics for PR-15/16.
    """
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


# ── PR-41: born-consumed for non-trigger types ────────────────────────────────

def test_trigger_type_keeps_pending_lifecycle(store):
    """Regression guard for PR-41's write-side rule. A ``USER_MESSAGE``
    must stay ``lifecycle='pending'`` at insert — subscribers rely on
    picking it up from there. If PR-41's logic accidentally clamped
    everything to consumed, the whole main path would break."""
    store.append("messages", "", {
        "id": "msg-trigger",
        "agent_id": "agent-abc",
        "type": "USER_MESSAGE",
        "content": "{}",
        "metadata": "{}",
        "timestamp": "2026-04-21T00:00:00Z",
        "read": 0,
    }, params={"agent_id": "agent-abc"}, notify=False)

    row = store.db.fetchone(
        "SELECT lifecycle, lifecycle_updated_at FROM chat_messages WHERE id = ?",
        ("msg-trigger",),
    )
    assert row["lifecycle"] == "pending"
    # lifecycle_updated_at stays NULL until transition() flips it.
    assert row["lifecycle_updated_at"] is None


def test_non_trigger_type_born_consumed(store):
    """PR-41 (2026-04-21) — an ``AGENT_REPLY`` has no registered
    subscriber, so it must NOT enter the pending state. If it did,
    PR-26's orphan scanner + PR-27's RECOVERED re-dispatch would
    pick it up at the 5-minute mark and wake the agent in a self-
    loop. Fix: stamp ``lifecycle='consumed'`` at INSERT time."""
    store.append("messages", "", {
        "id": "msg-reply",
        "agent_id": "agent-abc",
        "type": "AGENT_REPLY",
        "content": json.dumps({"text": "hi"}),
        "metadata": "{}",
        "timestamp": "2026-04-21T00:00:01Z",
        "read": 0,
    }, params={"agent_id": "agent-abc"}, notify=False)

    row = store.db.fetchone(
        "SELECT lifecycle, lifecycle_updated_at FROM chat_messages WHERE id = ?",
        ("msg-reply",),
    )
    assert row["lifecycle"] == "consumed", (
        "non-trigger types must be born consumed to avoid orphan-scan "
        "false positives (PR-41 self-loop fix)"
    )
    # lifecycle_updated_at stamped with the insert wall clock — orphan
    # scan's COALESCE prefers this column, so a stray query that still
    # matched pending rows would see a fresh timestamp anyway.
    assert row["lifecycle_updated_at"] is not None
    assert row["lifecycle_updated_at"] > 0

    # No outbox row — reinforces "no consumer exists" invariant.
    outbox = store.db.fetchone(
        "SELECT * FROM message_outbox WHERE message_id = ?", ("msg-reply",),
    )
    assert outbox is None


def test_caller_provided_lifecycle_is_not_overridden(store):
    """Respect caller intent. If someone explicitly sets
    ``lifecycle='pending'`` on an AGENT_REPLY (backfill scripts,
    tests, someone debugging the orphan path on purpose), PR-41 must
    NOT silently rewrite it to consumed — that would hide the bug
    the caller is trying to reproduce.

    The gate is ``"lifecycle" not in data`` — only rewrite when the
    caller said nothing about it."""
    store.append("messages", "", {
        "id": "msg-explicit",
        "agent_id": "agent-abc",
        "type": "AGENT_REPLY",
        "content": "{}",
        "metadata": "{}",
        "timestamp": "2026-04-21T00:00:02Z",
        "read": 0,
        "lifecycle": "pending",
    }, params={"agent_id": "agent-abc"}, notify=False)

    row = store.db.fetchone(
        "SELECT lifecycle FROM chat_messages WHERE id = ?", ("msg-explicit",),
    )
    assert row["lifecycle"] == "pending"


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
