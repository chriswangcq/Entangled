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
    historical_summary TEXT,
    last_scope_id TEXT,
    last_scope_archived_at TEXT,
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


# ── PR-53: continuity columns must pass through EXTRA_ALLOWLIST ─────────────
#
# Before PR-53, Business-side ``internal_update_entity`` force-routed any
# ``subagents`` PATCH containing ``status`` through ``subagent_state.transition``,
# which intersected ``extra`` with a 4-key allowlist and silently dropped
# anything else. That took out PR-42 (``handoff_notes``), PR-45
# (``historical_summary``) and PR-43 Wave A (``last_scope_id`` /
# ``last_scope_archived_at``) in one go — continuity never worked for a
# single wake on prod. These tests lock the fix: the three columns land
# in the UPDATE, and unlisted keys still get dropped *with* a WARN.

def test_transition_writes_historical_summary(db):
    """PR-45: summary piggybacks on the terminal ``awake -> sleeping`` flip."""
    _insert_subagent(db, status="awake")

    transition(
        db, "s1", "a1", to="sleeping", reason="rest", actor="runtime",
        extra={"historical_summary": "user asked the time; replied 22:36."},
    )

    row = db.fetchone(
        "SELECT status, historical_summary FROM subagents WHERE subagent_id='s1'"
    )
    assert row["status"] == "sleeping"
    assert row["historical_summary"] == "user asked the time; replied 22:36."


def test_transition_writes_last_scope_fields(db):
    """PR-43 Wave A: root-scope chain pointer + its archive time."""
    _insert_subagent(db, status="awake")

    transition(
        db, "s1", "a1", to="sleeping", reason="rest", actor="runtime",
        extra={
            "last_scope_id": "scope-abc-123",
            "last_scope_archived_at": "2026-04-25T10:00:00Z",
            "need_rest": 0,
        },
    )

    row = db.fetchone(
        "SELECT status, last_scope_id, last_scope_archived_at, need_rest "
        "FROM subagents WHERE subagent_id='s1'"
    )
    assert row["status"] == "sleeping"
    assert row["last_scope_id"] == "scope-abc-123"
    assert row["last_scope_archived_at"] == "2026-04-25T10:00:00Z"
    assert row["need_rest"] == 0


def test_continuity_fields_are_in_allowlist():
    """Defensive unit: if someone deletes an entry from EXTRA_ALLOWLIST
    during a future refactor, this test flips red immediately instead of
    continuity silently breaking in prod the next wake."""
    for key in ("historical_summary", "last_scope_id", "last_scope_archived_at"):
        assert key in EXTRA_ALLOWLIST, (
            f"{key!r} dropped from EXTRA_ALLOWLIST — "
            "runtime handlers and PR-43 Wave A/B/C will regress silently"
        )


def test_self_loop_also_writes_continuity_fields(db):
    """The saga can fire a no-op ``sleeping -> sleeping`` when a wake aborted
    early but we still want the summary update to land. The self-loop branch
    goes through ``_apply_extras`` rather than ``_apply_status_and_extras``;
    PR-53 checks both paths honor the same allowlist."""
    _insert_subagent(db, status="sleeping")

    transition(
        db, "s1", "a1", to="sleeping", reason="retry", actor="recovery",
        extra={
            "historical_summary": "retry wrote fresh summary",
            "last_scope_id": "scope-xyz",
        },
    )

    row = db.fetchone(
        "SELECT historical_summary, last_scope_id FROM subagents WHERE subagent_id='s1'"
    )
    assert row["historical_summary"] == "retry wrote fresh summary"
    assert row["last_scope_id"] == "scope-xyz"


def test_dropped_extra_keys_emit_warn(db, caplog):
    """PR-53 observability: any extra key outside the allowlist must leave
    a single WARN behind so the next-unlisted-column regression is visible
    within the first write attempt (not "users complain weeks later")."""
    import logging

    _insert_subagent(db)

    with caplog.at_level(logging.WARNING, logger="entangled.sql.subagent_state"):
        transition(
            db, "s1", "a1", to="awake", reason="wake", actor="scheduler",
            extra={"bogus_col": "drop me", "another_bogus": 42},
        )

    warns = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert len(warns) == 1, f"expected exactly one WARN, got {len(warns)}"
    msg = warns[0].getMessage()
    # Deterministic sort — test shouldn't flake on dict iteration order.
    assert "another_bogus,bogus_col" in msg
    assert "s1" in msg and "a1" in msg


def test_allowlisted_extras_do_not_emit_warn(db, caplog):
    """Negative regression: the happy path must stay quiet so prod log
    volume doesn't balloon."""
    import logging

    _insert_subagent(db, status="awake")

    with caplog.at_level(logging.WARNING, logger="entangled.sql.subagent_state"):
        transition(
            db, "s1", "a1", to="sleeping", reason="rest", actor="runtime",
            extra={"historical_summary": "ok", "last_scope_id": "s", "need_rest": 0},
        )

    warns = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert warns == [], f"unexpected WARN: {[r.getMessage() for r in warns]}"


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
