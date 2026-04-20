"""PR-21 (2026-04-20) — chat_messages lifecycle state machine tests.

What this file guards
---------------------
1. The allowed-transition matrix matches the RFC state diagram. If a new
   state is added or an edge changes, this test must change with it; a
   silent change would break the PR-26 orphan scanner or PR-25 message
   trace without warning.
2. ``transition()`` rejects unknown message_ids with MessageNotFound,
   not with a silent 0-row-affected UPDATE (pre-PR-21 code did the
   silent path and that's exactly how the "hihi" incident surfaced
   three days late).
3. ``backfill_lifecycle()`` is idempotent — running it twice against
   the same DB makes no further changes on the second pass, and a row
   inserted after PR-21 deploy (``lifecycle='pending'``, no legacy
   flags) is correctly left alone.
4. The ``ensure_schema`` hook wires the backfill — so production will
   actually run the migration on first deploy instead of leaving
   millions of legacy rows forever stuck at ``lifecycle='pending'``.
"""

from __future__ import annotations

import sqlite3

import pytest

from entangled.sql.entity_def import SqlEntityDef
from entangled.sql.entity_store import SqlEntityStore
from entangled.sql.field_def import F
from entangled.sql.message_state import (
    ALLOWED_TRANSITIONS,
    InvalidTransition,
    MessageNotFound,
    VALID_STATES,
    backfill_lifecycle,
    transition,
)


# ── Test harness ──────────────────────────────────────────────────────────────
#
# FakeDatabase mirrors the shim used by the existing outbox tests
# (test_outbox_schema_bootstrap.py) so new PR-21 tests don't drag in a
# whole Database/Connection setup. The transition() function only needs
# ``execute(...).fetchone()`` and a ``transaction()`` context manager —
# the fake provides both.


class FakeDatabase:
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


# Minimal MESSAGES_DEF with PR-21 columns included. Kept in sync with
# novaic-business/business/schema_push.py::MESSAGES_DEF on the lifecycle
# columns; the rest of the schema is trimmed for test speed.
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
        F.text("timestamp", nullable=False),
        F.int_("read", default=0),
        F.text("claimed_by"),  # legacy
        F.int_("processed", default=0),  # legacy
        F.text("status", default="sent"),
        # PR-21 columns
        F.text("lifecycle", default="pending"),
        F.text("claimed_by_scope"),
        F.int_("lifecycle_updated_at"),
    ],
)


@pytest.fixture
def store():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    db = FakeDatabase(conn)
    store = SqlEntityStore(db=db)
    store.register(MESSAGES_DEF)
    store.ensure_schema(MESSAGES_DEF)
    return store, db, conn


def _insert(conn, msg_id: str, **overrides):
    """Insert a minimal chat_messages row; overrides let each test dial
    exact legacy-state combinations for backfill tests."""
    row = {
        "id": msg_id,
        "agent_id": "a1",
        "type": "USER_MESSAGE",
        "timestamp": "2026-04-20T00:00:00Z",
        "read": 0,
        "claimed_by": None,
        "processed": 0,
        "status": "sent",
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


# ── State-machine shape ───────────────────────────────────────────────────────

def test_allowed_transitions_matches_rfc():
    """RFC mandated edges — any change must explicitly update this test
    AND docs/roadmap/tickets/PR-21-message-lifecycle-enum.md. Drift
    between them is exactly how the orphan-scan PR (PR-26) would start
    missing rows."""
    assert ALLOWED_TRANSITIONS == {
        "pending":   {"claimed", "deduped"},
        "claimed":   {"consumed", "orphaned"},
        "consumed":  set(),
        "orphaned":  {"claimed"},
        "deduped":   set(),
    }


def test_terminal_states_have_no_outbound_edges():
    """consumed/deduped are sinks. A future PR that adds an edge out of
    either must also add a migration for rows already in that state."""
    for terminal in ("consumed", "deduped"):
        assert ALLOWED_TRANSITIONS[terminal] == set()


def test_valid_states_matches_allowed_keys():
    assert VALID_STATES == frozenset(ALLOWED_TRANSITIONS.keys())


# ── Happy-path transitions ────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "path",
    [
        ("pending", "claimed", "consumed"),
        ("pending", "deduped"),
        ("pending", "claimed", "orphaned", "claimed", "consumed"),
    ],
    ids=["happy_dispatch", "deduped_at_entry", "orphan_recovery"],
)
def test_full_transition_path(store, path):
    _, db, conn = store
    _insert(conn, "m1")

    scope = "scope-1"
    for target in path[1:]:
        result = transition(db, "m1", to=target, scope_id=scope, reason="test")
        assert result["to"] == target
        row = conn.execute(
            "SELECT lifecycle, claimed_by_scope, lifecycle_updated_at "
            "FROM chat_messages WHERE id='m1'"
        ).fetchone()
        assert row["lifecycle"] == target
        # COALESCE keeps the scope set from the first claim even after
        # orphaned->claimed cycles.
        assert row["claimed_by_scope"] == scope
        assert row["lifecycle_updated_at"] is not None


# ── Rejection paths ───────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "current,to",
    [
        ("pending", "consumed"),   # skipped claimed
        ("pending", "orphaned"),   # can't orphan what wasn't claimed
        ("consumed", "claimed"),   # terminal
        ("consumed", "orphaned"),  # terminal
        ("deduped", "claimed"),    # terminal
        ("claimed", "deduped"),    # dedup only at entry
    ],
)
def test_invalid_transitions_rejected(store, current, to):
    _, db, conn = store
    _insert(conn, "m1", lifecycle=current)
    with pytest.raises(InvalidTransition):
        transition(db, "m1", to=to)
    row = conn.execute(
        "SELECT lifecycle FROM chat_messages WHERE id='m1'"
    ).fetchone()
    assert row["lifecycle"] == current  # unchanged on failure


def test_unknown_target_state_rejected_fast(store):
    """Bad ``to`` is caught before we even SELECT the row, so a typo in
    a caller doesn't cost a DB roundtrip."""
    _, db, conn = store
    _insert(conn, "m1")
    with pytest.raises(InvalidTransition, match="is not a valid lifecycle state"):
        transition(db, "m1", to="bogus")


def test_missing_message_raises_not_found(store):
    _, db, _ = store
    with pytest.raises(MessageNotFound, match="message not found"):
        transition(db, "does-not-exist", to="claimed")


def test_missing_message_not_confused_with_invalid_transition(store):
    """MessageNotFound is a distinct exception type from InvalidTransition;
    the HTTP layer maps the two to 404 vs 409 respectively, so callers
    can tell 'wrong id' apart from 'wrong state'."""
    _, db, _ = store
    try:
        transition(db, "missing", to="claimed")
    except MessageNotFound:
        pass
    except InvalidTransition:
        pytest.fail("MessageNotFound must not be caught as InvalidTransition")


# ── Backfill migration ────────────────────────────────────────────────────────

def test_backfill_processed_rows_to_consumed(store):
    _, db, conn = store
    _insert(conn, "m1", processed=1, lifecycle="pending")
    _insert(conn, "m2", processed=1, lifecycle="pending")

    updated = backfill_lifecycle(db)
    assert updated == 2

    rows = conn.execute(
        "SELECT id, lifecycle FROM chat_messages ORDER BY id"
    ).fetchall()
    assert [dict(r) for r in rows] == [
        {"id": "m1", "lifecycle": "consumed"},
        {"id": "m2", "lifecycle": "consumed"},
    ]


def test_backfill_claimed_by_rows_to_claimed(store):
    _, db, conn = store
    _insert(conn, "m1", claimed_by="worker-7", lifecycle="pending")
    backfill_lifecycle(db)
    row = conn.execute(
        "SELECT lifecycle, claimed_by_scope FROM chat_messages WHERE id='m1'"
    ).fetchone()
    assert row["lifecycle"] == "claimed"
    # Legacy claimed_by is copied into claimed_by_scope so the new
    # orphan-scan query has a value to filter on without a JOIN.
    assert row["claimed_by_scope"] == "worker-7"


def test_backfill_processed_wins_over_claimed_by(store):
    """Defensive: pre-PR-21 we saw rows where the worker set
    claimed_by at dispatch and processed=1 at completion without
    clearing claimed_by. Those rows are genuinely consumed; don't
    regress them to claimed."""
    _, db, conn = store
    _insert(conn, "m1", processed=1, claimed_by="worker-7", lifecycle="pending")
    backfill_lifecycle(db)
    row = conn.execute(
        "SELECT lifecycle FROM chat_messages WHERE id='m1'"
    ).fetchone()
    assert row["lifecycle"] == "consumed"


def test_backfill_leaves_fresh_pending_rows_alone(store):
    """A post-PR-21 row (no processed flag, no claimed_by) legitimately
    sits at 'pending'. The backfill must NOT falsely promote it to any
    other state."""
    _, db, conn = store
    _insert(conn, "m1", lifecycle="pending")
    backfill_lifecycle(db)
    row = conn.execute(
        "SELECT lifecycle, lifecycle_updated_at FROM chat_messages WHERE id='m1'"
    ).fetchone()
    assert row["lifecycle"] == "pending"
    assert row["lifecycle_updated_at"] is None  # untouched


def test_backfill_is_idempotent(store):
    _, db, conn = store
    _insert(conn, "m1", processed=1, lifecycle="pending")
    first = backfill_lifecycle(db)
    second = backfill_lifecycle(db)
    assert first == 1
    assert second == 0  # second run finds nothing to do


def test_ensure_schema_triggers_backfill_on_chat_messages(store):
    """End-to-end: inserting legacy rows and then calling ensure_schema
    again (simulating a service restart after PR-21 deploy) does run
    the backfill — no manual migration scripts required."""
    store_obj, db, conn = store
    _insert(conn, "m1", processed=1, lifecycle="pending")
    store_obj.ensure_schema(MESSAGES_DEF)
    row = conn.execute(
        "SELECT lifecycle FROM chat_messages WHERE id='m1'"
    ).fetchone()
    assert row["lifecycle"] == "consumed"
