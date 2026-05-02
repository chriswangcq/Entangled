"""Subagent state-transition log table tests."""

from __future__ import annotations

import sqlite3

import pytest

from entangled.sql.state_transitions import (
    append_subagent_transition,
    ensure_state_transitions_schema,
    list_subagent_transitions,
)


class FakeDatabase:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def execute(self, sql, params=()):
        return self._conn.execute(sql, params)

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


def test_ensure_schema_creates_subagent_table_only(db):
    ensure_state_transitions_schema(db)
    tables = {
        r["name"]
        for r in db.fetchall("SELECT name FROM sqlite_master WHERE type='table'")
    }
    assert "subagent_state_transitions" in tables
    assert "message_state_transitions" not in tables


def test_ensure_schema_is_idempotent(db):
    ensure_state_transitions_schema(db)
    ensure_state_transitions_schema(db)
    tables = {
        r["name"]
        for r in db.fetchall("SELECT name FROM sqlite_master WHERE type='table'")
    }
    assert "subagent_state_transitions" in tables


def test_ensure_schema_creates_subagent_index(db):
    ensure_state_transitions_schema(db)
    indexes = {
        r["name"]
        for r in db.fetchall("SELECT name FROM sqlite_master WHERE type='index'")
    }
    assert "idx_subagent_state_transitions_sub" in indexes
    assert "idx_message_state_transitions_msg" not in indexes


def test_append_subagent_transition_roundtrip(db):
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
        metadata={"attempt": 1},
    )
    rows = list_subagent_transitions(db, "sa1")
    assert len(rows) == 1
    row = rows[0]
    assert row["subagent_id"] == "sa1"
    assert row["agent_id"] == "a1"
    assert row["from_state"] == "sleeping"
    assert row["to_state"] == "awake"
    assert row["reason"] == "scheduled_wake"
    assert row["actor"] == "scheduler"
    assert row["scope_id"] is None
    assert row["metadata"] == {"attempt": 1}
    assert isinstance(row["created_at"], int) and row["created_at"] > 0


def test_list_subagents_is_oldest_first_and_limit_truncates(db):
    ensure_state_transitions_schema(db)
    with db.transaction("global"):
        for i in range(5):
            append_subagent_transition(
                db,
                subagent_id="sa1",
                agent_id="a1",
                from_state="sleeping",
                to_state="awake",
                reason=f"r{i}",
                actor="test",
            )
    rows = list_subagent_transitions(db, "sa1", limit=3)
    assert [r["reason"] for r in rows] == ["r0", "r1", "r2"]
