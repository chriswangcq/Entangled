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
    # Schema mirrors the live chat_messages shape after PR-21:
    #   * no user_id column (Business's MESSAGES_DEF uses sender only)
    #   * created_at is TEXT 'YYYY-MM-DD HH:MM:SS' (SQLite datetime('now'))
    #   * lifecycle_updated_at is INTEGER ms, NULL for pre-PR-21 rows that
    #     were never touched by a transition()
    # Keep in sync with novaic-business/business/schema_push.py::MESSAGES_DEF.
    conn.executescript(
        """
        CREATE TABLE chat_messages (
            id TEXT PRIMARY KEY,
            agent_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            lifecycle TEXT NOT NULL DEFAULT 'pending',
            lifecycle_updated_at INTEGER
        );
        CREATE TABLE message_outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT NOT NULL UNIQUE,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            delivered_at INTEGER,
            permanent_failure INTEGER NOT NULL DEFAULT 0
        );
        """
    )
    return _FakeDb(conn), conn


def _insert_msg(conn, mid: str, age_sec: float, *, lifecycle="pending",
                agent="a1", use_lifecycle_ts=True):
    """Insert a row age_sec seconds in the past.

    ``use_lifecycle_ts=True`` simulates the common case where something has
    already transitioned the row (so lifecycle_updated_at is populated).
    ``use_lifecycle_ts=False`` simulates a pre-PR-21 pending row where the
    query has to fall back to strftime(created_at).
    """
    now_s = int(time.time())
    past_s = now_s - int(age_sec)
    created_at_iso = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(past_s))
    lifecycle_ts = past_s * 1000 if use_lifecycle_ts else None
    conn.execute(
        "INSERT INTO chat_messages (id, agent_id, created_at, lifecycle, lifecycle_updated_at)"
        " VALUES (?, ?, ?, ?, ?)",
        (mid, agent, created_at_iso, lifecycle, lifecycle_ts),
    )
    conn.commit()


def _insert_outbox(
    conn, mid: str, *, attempts=0, last_error=None, delivered_at=None,
    permanent_failure=0,
):
    conn.execute(
        "INSERT INTO message_outbox (message_id, attempts, last_error, delivered_at, permanent_failure)"
        " VALUES (?, ?, ?, ?, ?)",
        (mid, attempts, last_error, delivered_at, permanent_failure),
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


def test_fallback_to_created_at_when_lifecycle_updated_at_is_null(db):
    """Pre-PR-21 rows that were never touched by transition() carry
    lifecycle_updated_at=NULL. The orphan query must still see them via
    the strftime(created_at) fallback — otherwise the oldest, most-stuck
    messages in the system (exactly the ones we care about) disappear."""
    fdb, conn = db
    _insert_msg(conn, "ancient", age_sec=600, use_lifecycle_ts=False)
    resp = query_orphans(fdb, min_age_sec=30)
    assert resp.count == 1
    assert resp.orphans[0].message_id == "ancient"
    assert resp.orphans[0].severity == "crit"


def test_limit_caps_returned_rows(db):
    fdb, conn = db
    for i in range(5):
        _insert_msg(conn, f"m{i}", age_sec=100 + i)
    resp = query_orphans(fdb, min_age_sec=30, limit=3)
    assert resp.count == 3


# ── TD-6: permanent_failure surfaces (no more 999999 sentinel) ───────────────

def test_permanent_failure_flag_surfaces_through_orphan_view(db):
    """TD-6 (2026-04-21): a subscriber's permanent mark_failed (no_owner /
    bad_argument) used to spray ``attempts = 999999``; now it flips
    ``permanent_failure = 1`` and keeps attempts truthful. HealthWorker's
    PR-27 re-dispatch reads this flag to short-circuit to
    PERMANENT_ORPHAN instead of retrying no-op forever."""
    fdb, conn = db
    _insert_msg(conn, "dead_on_arrival", age_sec=600)
    _insert_outbox(
        conn, "dead_on_arrival",
        attempts=1,
        last_error="no_owner: agent has no owner",
        permanent_failure=1,
    )
    resp = query_orphans(fdb, min_age_sec=30)
    assert resp.count == 1
    r = resp.orphans[0]
    assert r.outbox_permanent_failure is True
    assert r.outbox_attempts == 1, (
        "attempts must remain truthful; the 999999 sentinel is retired."
    )


def test_permanent_failure_default_false_when_outbox_missing(db):
    """A pending message with no outbox row coalesces to permanent_failure=False
    — missing row is already its own signal (outbox_attempts=0)."""
    fdb, conn = db
    _insert_msg(conn, "orphan_no_outbox", age_sec=120)
    resp = query_orphans(fdb, min_age_sec=30)
    assert resp.count == 1
    assert resp.orphans[0].outbox_permanent_failure is False
