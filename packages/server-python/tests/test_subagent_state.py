"""PR-31b — ``entangled.sql.subagent_state.transition`` unit tests.

Covers:

* Schema bring-up dependency (the ``subagents`` table must already
  exist — we build a minimal one inside the fixture so we're not
  coupled to ``SqlEntityStore``).
* Happy-path transition: status UPDATE + ``subagent_state_transitions``
  INSERT commit atomically.
* Idempotent self-loop: no history row, but ancillary extras still
  land.
* Invalid transition raises ``InvalidTransition`` without mutating
  state.
* Missing row raises ``SubagentNotFound``.
* ``extra`` is intersected with ``EXTRA_ALLOWLIST`` (typo'd key is
  silently dropped, real keys are written).
* ``extra`` cannot carry a second ``status`` value.
"""
from __future__ import annotations

import sqlite3

import pytest

from entangled.sql.state_transitions import ensure_state_transitions_schema
from entangled.sql.subagent_state import (
    EXTRA_ALLOWLIST,
    InvalidTransition,
    SubagentNotFound,
    transition,
)


class FakeDatabase:
    """Same shim used by ``test_state_transitions.py`` so the two files
    can share a fixture shape once the PR-31b rollout bakes. Only
    ``execute`` + ``transaction`` are exercised by the code under test."""

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


# ── Fixtures ────────────────────────────────────────────────────────────────

# We build a minimal ``subagents`` table directly rather than bringing in
# SqlEntityStore because the state machine never touches the entity
# store layer — it talks raw SQL through the ``db`` handle. Keeping the
# test surface small means the ALLOWED_TRANSITIONS matrix stays the only
# interesting unit under test.
_SUBAGENTS_DDL = """
CREATE TABLE subagents (
    subagent_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    status TEXT DEFAULT 'sleeping',
    need_rest INTEGER DEFAULT 0,
    progress TEXT,
    error TEXT,
    result TEXT,
    updated_at TEXT
)
"""


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    fake = FakeDatabase(conn)
    conn.execute(_SUBAGENTS_DDL)
    ensure_state_transitions_schema(fake)
    return fake


def _insert_subagent(db, sid="s1", aid="a1", status="sleeping"):
    db.execute(
        "INSERT INTO subagents (subagent_id, agent_id, status) VALUES (?, ?, ?)",
        (sid, aid, status),
    )


# ── Happy path ──────────────────────────────────────────────────────────────

def test_transition_updates_status_and_appends_history(db):
    _insert_subagent(db)

    result = transition(db, "s1", "a1", to="awake", reason="wake", actor="scheduler")

    assert result["from"] == "sleeping"
    assert result["to"] == "awake"
    assert result["noop"] is False

    row = db.fetchone("SELECT status FROM subagents WHERE subagent_id='s1'")
    assert row["status"] == "awake"

    rows = db.fetchall(
        "SELECT from_state, to_state, reason, actor, agent_id "
        "FROM subagent_state_transitions ORDER BY id ASC"
    )
    assert rows == [
        {"from_state": "sleeping", "to_state": "awake",
         "reason": "wake", "actor": "scheduler", "agent_id": "a1"}
    ]


def test_transition_with_extras_writes_allowlisted_columns(db):
    _insert_subagent(db, status="running")

    transition(
        db, "s1", "a1",
        to="failed", reason="timeout", actor="worker",
        extra={"error": "deadline", "progress": None, "bogus_col": "ignore_me"},
    )

    row = db.fetchone(
        "SELECT status, error, progress FROM subagents WHERE subagent_id='s1'"
    )
    assert row["status"] == "failed"
    assert row["error"] == "deadline"
    assert row["progress"] is None
    # bogus_col never existed — the intersection with EXTRA_ALLOWLIST
    # protected the UPDATE from hitting a "no such column" error.
    assert "bogus_col" not in EXTRA_ALLOWLIST


# ── Self-loop / idempotency ─────────────────────────────────────────────────

def test_self_loop_returns_noop_without_history_row(db):
    _insert_subagent(db, status="failed")

    result = transition(db, "s1", "a1", to="failed", reason="retry", actor="worker")

    assert result["noop"] is True
    rows = db.fetchall("SELECT id FROM subagent_state_transitions")
    assert rows == [], "self-loop must not emit a history row"


def test_self_loop_still_applies_extras(db):
    """Recovery workers re-write ``error`` on an already-failed subagent;
    PR-28 behavior preserved."""
    _insert_subagent(db, status="failed")

    transition(
        db, "s1", "a1", to="failed", reason="retry", actor="recovery",
        extra={"error": "new context"},
    )

    row = db.fetchone("SELECT status, error FROM subagents WHERE subagent_id='s1'")
    assert row["status"] == "failed"
    assert row["error"] == "new context"


# ── Error paths ─────────────────────────────────────────────────────────────

def test_missing_subagent_raises_not_found(db):
    with pytest.raises(SubagentNotFound):
        transition(db, "missing", "a1", to="awake", reason="r", actor="a")


def test_illegal_transition_raises(db):
    _insert_subagent(db, status="completed")

    with pytest.raises(InvalidTransition):
        transition(db, "s1", "a1", to="running", reason="r", actor="a")

    # Status must not have shifted
    row = db.fetchone("SELECT status FROM subagents WHERE subagent_id='s1'")
    assert row["status"] == "completed"
    # And no history row leaked.
    assert db.fetchall("SELECT id FROM subagent_state_transitions") == []


def test_extra_with_status_key_rejected(db):
    _insert_subagent(db)
    with pytest.raises(InvalidTransition):
        transition(
            db, "s1", "a1", to="awake", reason="r", actor="a",
            extra={"status": "awake"},
        )


def test_unknown_target_status_rejected(db):
    _insert_subagent(db)
    with pytest.raises(InvalidTransition):
        transition(db, "s1", "a1", to="not-a-status", reason="r", actor="a")


# ── Atomicity ───────────────────────────────────────────────────────────────

def test_history_row_carries_agent_id_and_timestamp(db):
    _insert_subagent(db)

    transition(db, "s1", "a1", to="awake", reason="wake", actor="scheduler")

    row = db.fetchone(
        "SELECT subagent_id, agent_id, created_at FROM subagent_state_transitions"
    )
    assert row["subagent_id"] == "s1"
    assert row["agent_id"] == "a1"
    # created_at is ms since epoch — sanity check that it's in the ballpark.
    assert isinstance(row["created_at"], int)
    assert row["created_at"] > 0
