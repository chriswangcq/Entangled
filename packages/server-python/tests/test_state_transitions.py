"""PR-31 (2026-04-15) — state_transitions log table tests.

Covers:

1. The new tables + indexes come up clean via
   ``ensure_state_transitions_schema`` (idempotent across two runs).
2. ``append_message_transition`` + ``append_subagent_transition`` write
   the exact fields ``list_*_transitions`` reads back, including
   metadata_json round-trip and ``scope_id=None``.
3. The integration hook in ``message_state.transition``: a non-noop
   transition appends exactly one row co-transactionally; a self-loop
   noop (PR-23 idempotency) writes ZERO rows (otherwise every
   subscriber retry would flood the log).
4. ``list_message_transitions`` returns oldest-first so callers can
   render a lifecycle timeline directly.
"""
from __future__ import annotations

import sqlite3

import pytest

from entangled.sql.message_state import transition
from entangled.sql.state_transitions import (
    append_message_transition,
    append_subagent_transition,
    ensure_state_transitions_schema,
    list_message_transitions,
    list_subagent_transitions,
)


class FakeDatabase:
    """Same shim the PR-21 tests use — ``transition()`` only needs
    ``execute(...).fetchone()`` and a ``transaction()`` CM."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def execute(self, sql, params=()):
        return self._conn.execute(sql, params)

    def fetchone(self, sql, params=()):
        row = self._conn.execute(sql, params).fetchone()
        return dict(row) if row else None

    def fetchall(self, sql, params=()):
        return [dict(r) for r in self._conn.execute(sql, params).fetchall()]

    class _TxCtx:
        def __init__(self, conn):
            self._conn = conn

        def __enter__(self):
            return self

        def __exit__(self, *a):
            self._conn.commit()

    def transaction(self, lock_type="global", resource_id="", timeout=None):
        return self._TxCtx(self._conn)


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return FakeDatabase(conn)


# ── Schema bring-up ───────────────────────────────────────────────────────────

def test_ensure_schema_creates_both_tables(db):
    ensure_state_transitions_schema(db)
    tables = {r["name"] for r in db.fetchall(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}
    assert "message_state_transitions" in tables
    assert "subagent_state_transitions" in tables


def test_ensure_schema_is_idempotent(db):
    """Running the migration twice must not raise (the ensure_schema
    hook fires on every ensure_all_schemas pass, which fires on every
    service startup)."""
    ensure_state_transitions_schema(db)
    ensure_state_transitions_schema(db)  # must not raise
    tables = {r["name"] for r in db.fetchall(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}
    assert "message_state_transitions" in tables


def test_ensure_schema_creates_indexes(db):
    ensure_state_transitions_schema(db)
    indexes = {r["name"] for r in db.fetchall(
        "SELECT name FROM sqlite_master WHERE type='index'"
    )}
    assert "idx_message_state_transitions_msg" in indexes
    assert "idx_subagent_state_transitions_sub" in indexes


# ── Raw append + read ─────────────────────────────────────────────────────────

def test_append_and_list_message_transition_roundtrip(db):
    ensure_state_transitions_schema(db)
    with db.transaction("global"):
        append_message_transition(
            db,
            message_id="m1",
            from_state="pending",
            to_state="claimed",
            reason="subscriber_dispatch",
            actor="entangled",
            scope_id="s1",
            metadata={"attempt": 1},
        )
    rows = list_message_transitions(db, "m1")
    assert len(rows) == 1
    r = rows[0]
    assert r["message_id"] == "m1"
    assert r["from_state"] == "pending"
    assert r["to_state"] == "claimed"
    assert r["reason"] == "subscriber_dispatch"
    assert r["actor"] == "entangled"
    assert r["scope_id"] == "s1"
    assert r["metadata"] == {"attempt": 1}
    assert isinstance(r["created_at"], int) and r["created_at"] > 0


def test_list_messages_is_oldest_first(db):
    ensure_state_transitions_schema(db)
    with db.transaction("global"):
        append_message_transition(db, message_id="m1",
                                  from_state="pending", to_state="claimed",
                                  reason="a", actor="x")
        append_message_transition(db, message_id="m1",
                                  from_state="claimed", to_state="consumed",
                                  reason="b", actor="x")
    rows = list_message_transitions(db, "m1")
    assert [r["to_state"] for r in rows] == ["claimed", "consumed"]


def test_append_subagent_transition_with_null_scope(db):
    ensure_state_transitions_schema(db)
    append_subagent_transition(
        db,
        subagent_id="sa1",
        agent_id="a1",
        from_state="sleeping",
        to_state="awake",
        reason="scheduled_wake",
        actor="scheduler",
        scope_id=None,
    )
    rows = list_subagent_transitions(db, "sa1")
    assert len(rows) == 1
    assert rows[0]["scope_id"] is None
    assert rows[0]["metadata"] is None


def test_list_limit_truncates(db):
    ensure_state_transitions_schema(db)
    with db.transaction("global"):
        for i in range(5):
            append_message_transition(
                db,
                message_id="m1",
                from_state="pending",
                to_state="claimed",
                reason=f"r{i}",
                actor="x",
            )
    rows = list_message_transitions(db, "m1", limit=3)
    assert len(rows) == 3


# ── Integration with message_state.transition ────────────────────────────────

@pytest.fixture
def message_db(db):
    """Full chat_messages + state_transitions schema so we can drive
    ``transition()`` end-to-end and assert on the log row that commits
    alongside the UPDATE."""
    db._conn.executescript(
        """
        CREATE TABLE chat_messages (
            id                    TEXT PRIMARY KEY,
            agent_id              TEXT NOT NULL,
            type                  TEXT NOT NULL,
            timestamp             TEXT NOT NULL,
            lifecycle             TEXT DEFAULT 'pending',
            claimed_by_scope      TEXT,
            lifecycle_updated_at  INTEGER
        );
        """
    )
    ensure_state_transitions_schema(db)
    db._conn.execute(
        "INSERT INTO chat_messages (id, agent_id, type, timestamp) "
        "VALUES (?, ?, ?, ?)",
        ("m1", "a1", "USER_MESSAGE", "2026-04-15T00:00:00Z"),
    )
    db._conn.commit()
    return db


def test_message_transition_appends_history_row(message_db):
    """Real transition() writes one row per non-noop state change."""
    transition(message_db, "m1", to="claimed", scope_id="s1",
               reason="subscriber_dispatch")

    rows = list_message_transitions(message_db, "m1")
    assert len(rows) == 1
    assert rows[0]["from_state"] == "pending"
    assert rows[0]["to_state"] == "claimed"
    assert rows[0]["scope_id"] == "s1"
    assert rows[0]["actor"] == "entangled"
    assert rows[0]["reason"] == "subscriber_dispatch"


def test_message_transition_noop_does_not_log(message_db):
    """PR-23 idempotency: re-transitioning into the current state is
    a noop — and an unlogged one, so retries of the subscriber don't
    flood the table with duplicate rows. This is the single most
    important invariant from PR-31's design notes: log volume tracks
    real state changes, not caller behavior."""
    transition(message_db, "m1", to="claimed", scope_id="s1",
               reason="first_dispatch")
    transition(message_db, "m1", to="claimed", scope_id="s1",
               reason="retry_after_outbox_redeliver")
    transition(message_db, "m1", to="claimed", scope_id="s1",
               reason="retry_again")

    rows = list_message_transitions(message_db, "m1")
    assert len(rows) == 1, (
        "Self-loop retries must not append extra history rows "
        f"(got {len(rows)})"
    )


def test_message_transition_full_lifecycle_is_replayable(message_db):
    """End-to-end: pending → claimed → consumed should yield exactly
    the oldest-first path ops can render in a timeline view."""
    transition(message_db, "m1", to="claimed", scope_id="s1", reason="dispatch")
    transition(message_db, "m1", to="consumed", scope_id="s1", reason="scope_end")

    rows = list_message_transitions(message_db, "m1")
    assert [(r["from_state"], r["to_state"]) for r in rows] == [
        ("pending", "claimed"),
        ("claimed", "consumed"),
    ]
