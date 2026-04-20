"""PR-26 (2026-04-20) — orphan listing endpoint tests.

Covers the query logic only (no FastAPI TestClient) — the route is a
thin Pydantic wrapper; the interesting logic is the SQL shape, the
age→severity threshold, and the LEFT JOIN edge case where outbox is
missing. Using a plain sqlite3 harness keeps these tests fast and
independent of the auth / app factory wiring.
"""

from __future__ import annotations

import sqlite3
import time

import pytest

from entangled.app.orphans import query_orphans, DEFAULT_CRIT_AGE_SEC


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
    # Minimal schema that matches the real JOIN columns. Keep in sync
    # with novaic-business/business/schema_push.py::MESSAGES_DEF and
    # entity_store._ensure_outbox_schema on column names / types.
    conn.executescript(
        """
        CREATE TABLE chat_messages (
            id TEXT PRIMARY KEY,
            agent_id TEXT NOT NULL,
            user_id TEXT,
            created_at INTEGER NOT NULL,
            lifecycle TEXT NOT NULL DEFAULT 'pending'
        );
        CREATE TABLE message_outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT NOT NULL UNIQUE,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            delivered_at INTEGER
        );
        """
    )
    return _FakeDb(conn), conn


def _insert_msg(conn, mid: str, age_sec: float, *, lifecycle="pending",
                agent="a1", user="u1"):
    now_ms = int(time.time() * 1000)
    conn.execute(
        "INSERT INTO chat_messages (id, agent_id, user_id, created_at, lifecycle)"
        " VALUES (?, ?, ?, ?, ?)",
        (mid, agent, user, now_ms - int(age_sec * 1000), lifecycle),
    )
    conn.commit()


def _insert_outbox(conn, mid: str, *, attempts=0, last_error=None, delivered_at=None):
    conn.execute(
        "INSERT INTO message_outbox (message_id, attempts, last_error, delivered_at)"
        " VALUES (?, ?, ?, ?)",
        (mid, attempts, last_error, delivered_at),
    )
    conn.commit()


# ── Basic shape ───────────────────────────────────────────────────────────────

def test_empty_db_returns_zero_counts(db):
    """Healthy system = no orphans. The endpoint must NOT 404 here — every
    caller (scanner, ops dashboard) assumes a 200 with count=0."""
    fdb, _ = db
    resp = query_orphans(fdb, min_age_sec=30)
    assert resp.count == 0
    assert resp.warn_count == 0
    assert resp.crit_count == 0
    assert resp.orphans == []


def test_too_young_is_excluded(db):
    """min_age_sec=30 must filter out a 5s-old row so the scanner's first
    tick after a message lands doesn't immediately scream orphan."""
    fdb, conn = db
    _insert_msg(conn, "fresh", age_sec=5)
    resp = query_orphans(fdb, min_age_sec=30)
    assert resp.count == 0


def test_pending_over_threshold_is_warn(db):
    fdb, conn = db
    _insert_msg(conn, "warn1", age_sec=60)   # > 30 < 300
    resp = query_orphans(fdb, min_age_sec=30)
    assert resp.count == 1
    assert resp.warn_count == 1
    assert resp.crit_count == 0
    row = resp.orphans[0]
    assert row.message_id == "warn1"
    assert row.severity == "warn"
    # age_seconds is real-time; allow ±2s jitter for slow CI.
    assert 58 <= row.age_seconds <= 62


def test_pending_over_crit_threshold_is_crit(db):
    """Severity is computed against DEFAULT_CRIT_AGE_SEC regardless of
    caller's min_age_sec, so an ops query with min_age_sec=30 still sees
    crit rows classified correctly."""
    fdb, conn = db
    _insert_msg(conn, "crit1", age_sec=DEFAULT_CRIT_AGE_SEC + 10)
    resp = query_orphans(fdb, min_age_sec=30)
    assert resp.count == 1
    assert resp.crit_count == 1
    assert resp.warn_count == 0
    assert resp.orphans[0].severity == "crit"


# ── Outbox JOIN ───────────────────────────────────────────────────────────────

def test_outbox_columns_surface_via_join(db):
    """Ops SOP: 'look at last_error before escalating' — must arrive in
    one hop, no follow-up call."""
    fdb, conn = db
    _insert_msg(conn, "m1", age_sec=90)
    _insert_outbox(conn, "m1", attempts=3, last_error="bad_argument: ...")
    resp = query_orphans(fdb, min_age_sec=30)
    r = resp.orphans[0]
    assert r.outbox_attempts == 3
    assert r.outbox_last_error == "bad_argument: ..."
    assert r.outbox_delivered_at is None


def test_missing_outbox_row_coalesces_to_zero(db):
    """A pending message with NO outbox row is itself a signal (see
    endpoint docstring). It MUST surface — don't hide it behind an
    INNER JOIN. attempts should coalesce to 0, not None."""
    fdb, conn = db
    _insert_msg(conn, "orphan_no_outbox", age_sec=120)
    # no outbox row inserted
    resp = query_orphans(fdb, min_age_sec=30)
    assert resp.count == 1
    assert resp.orphans[0].outbox_attempts == 0
    assert resp.orphans[0].outbox_last_error is None


def test_delivered_but_still_pending_is_surfaced_by_default(db):
    """PR-22 wiring bug signal: outbox.delivered_at set but lifecycle
    never moved from 'pending'. Default include_delivered_pending=True
    must keep this row visible — suppressing it would hide the exact
    class of bugs PR-26 exists to catch."""
    fdb, conn = db
    _insert_msg(conn, "wired_wrong", age_sec=100)
    _insert_outbox(conn, "wired_wrong", attempts=1,
                   delivered_at=int(time.time() * 1000) - 5000)
    resp = query_orphans(fdb, min_age_sec=30)
    assert resp.count == 1
    assert resp.orphans[0].outbox_delivered_at is not None


def test_delivered_but_still_pending_can_be_filtered(db):
    """Opt-out path: ops dashboard can hide these while debugging."""
    fdb, conn = db
    _insert_msg(conn, "wired_wrong", age_sec=100)
    _insert_outbox(conn, "wired_wrong", delivered_at=1)
    _insert_msg(conn, "real_orphan", age_sec=100)
    resp = query_orphans(fdb, min_age_sec=30, include_delivered_pending=False)
    assert {o.message_id for o in resp.orphans} == {"real_orphan"}


# ── Non-pending rows are not surfaced ─────────────────────────────────────────

@pytest.mark.parametrize("lc", ["claimed", "consumed", "deduped", "orphaned"])
def test_non_pending_lifecycles_never_returned(db, lc):
    """Only lifecycle='pending' is an orphan candidate. The other four
    are explicitly someone's responsibility and surface via PR-27's
    separate metrics, not here."""
    fdb, conn = db
    _insert_msg(conn, "skipme", age_sec=1000, lifecycle=lc)
    resp = query_orphans(fdb, min_age_sec=30)
    assert resp.count == 0


# ── Ordering / limit ──────────────────────────────────────────────────────────

def test_oldest_first_ordering(db):
    """SOP 'deal with most stuck first' demands oldest-first. Subtle
    regression: ORDER BY id ASC would LOOK right in dev because ids
    happen to match insertion order, but breaks when rows are imported
    out-of-order (migrations, backfills)."""
    fdb, conn = db
    _insert_msg(conn, "new", age_sec=40)
    _insert_msg(conn, "old", age_sec=400)
    _insert_msg(conn, "mid", age_sec=120)
    resp = query_orphans(fdb, min_age_sec=30)
    assert [o.message_id for o in resp.orphans] == ["old", "mid", "new"]


def test_limit_caps_returned_rows(db):
    fdb, conn = db
    for i in range(5):
        _insert_msg(conn, f"m{i}", age_sec=100 + i)
    resp = query_orphans(fdb, min_age_sec=30, limit=3)
    assert resp.count == 3
