"""chat_messages lifecycle state machine tests (PR-21).

What this file guards
---------------------
1. The allowed-transition matrix matches the RFC state diagram. If a new
   state is added or an edge changes, this test must change with it; a
   silent change would break the orphan scanner or message trace
   without warning.
2. ``transition()`` rejects unknown message_ids with MessageNotFound,
   not with a silent 0-row-affected UPDATE (the pre-PR-21 silent path
   is exactly how the "hihi" incident surfaced three days late).
3. Self-transitions (``current == to``) return noop=True so retries
   from the subscriber (PR-22) and scope_end (PR-23) don't explode.
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
    transition,
)


# ── Test harness ──────────────────────────────────────────────────────────────

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
        F.text("lifecycle", default="pending"),
        F.text("claimed_by_scope"),
        F.int_("lifecycle_updated_at"),
    ],
)


@pytest.fixture
def store():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE chat_messages (
            id        TEXT PRIMARY KEY,
            agent_id  TEXT NOT NULL,
            type      TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            read      INTEGER DEFAULT 0,
            lifecycle TEXT DEFAULT 'pending',
            claimed_by_scope TEXT,
            lifecycle_updated_at INTEGER
        );
        """
    )
    db = FakeDatabase(conn)
    # ``transition`` writes to ``message_state_transitions`` co-transactionally
    # (PR-31). Ensure the log table is up before any test body runs.
    from entangled.sql.state_transitions import ensure_state_transitions_schema
    ensure_state_transitions_schema(db)
    store = SqlEntityStore(db=db)
    store.register(MESSAGES_DEF)
    return store, db, conn


def _insert(conn, msg_id: str, **overrides):
    row = {
        "id": msg_id,
        "agent_id": "a1",
        "type": "USER_MESSAGE",
        "timestamp": "2026-04-20T00:00:00Z",
        "read": 0,
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
    """Any change to this map must also update
    docs/roadmap/tickets/PR-21-message-lifecycle-enum.md; drift between
    the two is exactly how the orphan scanner would start missing rows."""
    assert ALLOWED_TRANSITIONS == {
        "pending":   {"claimed", "deduped"},
        "claimed":   {"consumed", "orphaned"},
        "consumed":  set(),
        "orphaned":  {"claimed"},
        "deduped":   set(),
    }


def test_terminal_states_have_no_outbound_edges():
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


def test_self_transition_is_idempotent_noop(store):
    """transition(to=current) returns noop=True without error.
    Subscriber (PR-22) and scope_end (PR-23) both get retried; the
    retry must not blow up with InvalidTransition."""
    _, db, conn = store
    _insert(conn, "m1", lifecycle="claimed", claimed_by_scope="sc-1")
    result = transition(db, "m1", to="claimed", scope_id="sc-1", reason="retry")
    assert result["noop"] is True
    assert result["from"] == "claimed"
    assert result["to"] == "claimed"
    assert result["scope_id"] == "sc-1"
    # Row untouched (lifecycle_updated_at NOT bumped on a noop so
    # the message trace doesn't see a phantom transition timestamp).
    row = conn.execute(
        "SELECT lifecycle, claimed_by_scope, lifecycle_updated_at "
        "FROM chat_messages WHERE id='m1'"
    ).fetchone()
    assert row["lifecycle"] == "claimed"
    assert row["claimed_by_scope"] == "sc-1"
    assert row["lifecycle_updated_at"] is None


def test_self_transition_noop_preserves_existing_scope(store):
    """When the retry omits scope_id, noop still returns the stored one."""
    _, db, conn = store
    _insert(conn, "m2", lifecycle="consumed", claimed_by_scope="sc-2")
    result = transition(db, "m2", to="consumed")
    assert result["noop"] is True
    assert result["scope_id"] == "sc-2"


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
