"""Test that message_outbox table is auto-created via the production path.

Production path: SqlEntityDef.from_spec() → store.register() → store.ensure_schema()
This test deliberately does NOT call store._ensure_outbox_schema() manually.
If this test fails, it means production will crash with
'sqlite3.OperationalError: no such table: message_outbox' on first append().
"""
import json
import sqlite3
import pytest

from entangled.sql.entity_store import SqlEntityStore
from entangled.sql.entity_def import SqlEntityDef
from entangled.sql.field_def import F


class FakeDatabase:
    """Minimal db shim for SqlEntityStore."""
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
    },
)


def test_outbox_schema_created_via_ensure_schema_path():
    """Simulate production path: register + ensure_schema, NOT manual _ensure_outbox_schema().

    Production: POST /v1/schema/register → SqlEntityDef.from_spec() → register() → ensure_schema()
    If ensure_schema() does not auto-create message_outbox when outbox_trigger_types is set,
    the first append() will crash with 'no such table: message_outbox'.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    db = FakeDatabase(conn)
    store = SqlEntityStore(db=db)

    store.register(MESSAGES_DEF)
    store.ensure_schema(MESSAGES_DEF)
    # NOT calling store._ensure_outbox_schema() — that's the point

    # 1. Table must exist in sqlite_master
    row = store.db.fetchone(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='message_outbox'"
    )
    assert row is not None, "message_outbox table should be auto-created via ensure_schema"

    # 2. append() must work without crashing
    result = store.append("messages", "", {
        "id": "msg-bootstrap",
        "agent_id": "agent-a",
        "type": "USER_MESSAGE",
        "content": "{}",
        "metadata": "{}",
        "timestamp": "2026-04-18T00:00:00Z",
        "read": 0,
    }, params={"agent_id": "agent-a"}, notify=False)

    assert result["id"] == "msg-bootstrap"

    # 3. outbox row must exist
    outbox = store.db.fetchone(
        "SELECT * FROM message_outbox WHERE message_id = ?", ("msg-bootstrap",)
    )
    assert outbox is not None
    assert outbox["trigger_type"] == "user_message"
