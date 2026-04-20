"""PR-25 (2026-04-15) — chat_messages + message_outbox trace query.

Isolated from test_message_state.py because the trace read joins
``message_outbox`` — which isn't a registered entity and therefore isn't
in that file's MESSAGES_DEF harness. Matching the orphan-scan test
fixture (tests/test_orphans.py) lets both tests share a mental model of
"what the prod schema actually looks like".
"""

from __future__ import annotations

import sqlite3

import pytest

from entangled.app.message_state import (
    MessageTraceNotFound,
    MessageTraceRow,
    query_message_trace,
)


class _FakeDb:
    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=()):
        return self._conn.execute(sql, params)

    class _Tx:
        def __init__(self, conn):
            self._conn = conn

        def __enter__(self):
            return self

        def __exit__(self, *a):
            self._conn.commit()

    def transaction(self, *a, **kw):
        return self._Tx(self._conn)


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    # Mirrors the live chat_messages + message_outbox shape — kept in
    # sync with entangled/sql/entity_store._ensure_outbox_schema and
    # novaic-business/business/schema_push.py::MESSAGES_DEF.
    conn.executescript(
        """
        CREATE TABLE chat_messages (
            id TEXT PRIMARY KEY,
            agent_id TEXT NOT NULL,
            type TEXT NOT NULL,
            sender TEXT DEFAULT '',
            timestamp TEXT NOT NULL,
            created_at TEXT NOT NULL,
            lifecycle TEXT NOT NULL DEFAULT 'pending',
            claimed_by_scope TEXT,
            lifecycle_updated_at INTEGER
        );
        CREATE TABLE message_outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT NOT NULL UNIQUE,
            agent_id TEXT NOT NULL,
            trigger_type TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            delivered_at INTEGER,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            locked_by TEXT,
            locked_until INTEGER,
            permanent_failure INTEGER NOT NULL DEFAULT 0
        );
        """
    )
    return _FakeDb(conn), conn


def _insert_msg(conn, mid: str, **overrides):
    row = {
        "id": mid,
        "agent_id": "agent-1",
        "type": "USER_MESSAGE",
        "sender": "user-1",
        "timestamp": "2026-04-15T00:00:00Z",
        "created_at": "2026-04-15 00:00:00",
        "lifecycle": "pending",
        "claimed_by_scope": None,
        "lifecycle_updated_at": None,
    }
    row.update(overrides)
    cols = ", ".join(row.keys())
    placeholders = ", ".join(["?"] * len(row))
    conn.execute(
        f"INSERT INTO chat_messages ({cols}) VALUES ({placeholders})",
        tuple(row.values()),
    )
    conn.commit()


def _insert_outbox(conn, mid: str, **overrides):
    row = {
        "message_id": mid,
        "agent_id": "agent-1",
        "trigger_type": "user_message",
        "payload_json": "{}",
        "created_at": 1_700_000_000_000,
        "delivered_at": None,
        "attempts": 0,
        "last_error": None,
        "locked_by": None,
        "locked_until": None,
    }
    row.update(overrides)
    cols = ", ".join(row.keys())
    placeholders = ", ".join(["?"] * len(row))
    conn.execute(
        f"INSERT INTO message_outbox ({cols}) VALUES ({placeholders})",
        tuple(row.values()),
    )
    conn.commit()


# ── 404 ──────────────────────────────────────────────────────────────────────

def test_unknown_message_raises_not_found(db):
    """404 at the SQL layer via MessageTraceNotFound — the route layer
    maps that to HTTP 404. Tests the explicit exception (not a generic
    AttributeError on a None row) so the mapping can't silently drift."""
    fdb, _ = db
    with pytest.raises(MessageTraceNotFound):
        query_message_trace(fdb, "nope")


# ── Happy path ───────────────────────────────────────────────────────────────

def test_trace_returns_row_with_outbox(db):
    fdb, conn = db
    _insert_msg(
        conn, "m1",
        lifecycle="claimed",
        claimed_by_scope="scope-abc",
        lifecycle_updated_at=1_700_000_005_000,
    )
    _insert_outbox(
        conn, "m1",
        delivered_at=1_700_000_003_000,
        attempts=1,
        last_error=None,
    )
    res = query_message_trace(fdb, "m1")
    assert isinstance(res, MessageTraceRow)
    assert res.message_id == "m1"
    assert res.agent_id == "agent-1"
    assert res.lifecycle == "claimed"
    assert res.claimed_by_scope == "scope-abc"
    assert res.lifecycle_updated_at == 1_700_000_005_000
    assert res.outbox_trigger_type == "user_message"
    assert res.outbox_delivered_at == 1_700_000_003_000
    assert res.outbox_attempts == 1
    assert res.outbox_last_error is None


# ── Missing outbox (signal, not error) ───────────────────────────────────────

def test_trace_without_outbox_row_returns_zero_attempts(db):
    """A chat_messages row with no matching outbox sibling is itself
    diagnostic (PR-15 co-insert failed OR row predates PR-14). Must
    surface as HTTP 200 so the ops caller sees the absence, with
    attempts coalesced to 0 and all outbox_* fields None."""
    fdb, conn = db
    _insert_msg(conn, "orphan-msg", lifecycle="pending")
    res = query_message_trace(fdb, "orphan-msg")
    assert res.lifecycle == "pending"
    assert res.outbox_attempts == 0
    assert res.outbox_trigger_type is None
    assert res.outbox_created_at is None
    assert res.outbox_delivered_at is None
    assert res.outbox_last_error is None


# ── All four lifecycle states surface correctly ──────────────────────────────

@pytest.mark.parametrize(
    "lc,scope,ts",
    [
        ("pending", None, None),
        ("claimed", "s-1", 1_700_000_001_000),
        ("consumed", "s-1", 1_700_000_002_000),
        ("orphaned", "s-1", 1_700_000_003_000),
    ],
)
def test_trace_preserves_lifecycle_and_scope(db, lc, scope, ts):
    fdb, conn = db
    _insert_msg(
        conn, "m1",
        lifecycle=lc,
        claimed_by_scope=scope,
        lifecycle_updated_at=ts,
    )
    res = query_message_trace(fdb, "m1")
    assert res.lifecycle == lc
    assert res.claimed_by_scope == scope
    assert res.lifecycle_updated_at == ts


# ── Failure diagnostics (outbox last_error) ──────────────────────────────────

def test_trace_surfaces_outbox_last_error(db):
    """Ops SOP: 'did the subscriber try and fail?' must arrive in one
    hop. last_error is specifically the tell."""
    fdb, conn = db
    _insert_msg(conn, "m-bad", lifecycle="pending")
    _insert_outbox(
        conn, "m-bad",
        attempts=5,
        last_error="bad_argument: missing agent_id",
    )
    res = query_message_trace(fdb, "m-bad")
    assert res.outbox_attempts == 5
    assert res.outbox_last_error == "bad_argument: missing agent_id"
    assert res.outbox_delivered_at is None
