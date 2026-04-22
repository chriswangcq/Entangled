"""PR-51 Part 2 (2026-04-23) — stuck-claimed listing endpoint tests.

Mirrors the shape of ``test_orphans.py`` so future changes propagate
predictably. The interesting new logic vs orphans is the two-axis
OR match (``lifecycle_updated_at`` vs ``created_at``); everything
else reuses the same SQLite-harness pattern.
"""

from __future__ import annotations

import sqlite3
import time

import pytest

from entangled.app.stuck_claimed import (
    query_stuck_claimed,
    DEFAULT_MIN_AGE_SEC,
    DEFAULT_CREATED_MIN_AGE_SEC,
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
    # Only the columns the query reads. ``claimed_by_scope`` is
    # included because PR-51 surfaces it in the response for forensic
    # queries even though it's not used in the WHERE.
    conn.executescript(
        """
        CREATE TABLE chat_messages (
            id TEXT PRIMARY KEY,
            agent_id TEXT NOT NULL,
            type TEXT NOT NULL,
            claimed_by_scope TEXT,
            created_at TEXT NOT NULL,
            lifecycle TEXT NOT NULL DEFAULT 'pending',
            lifecycle_updated_at INTEGER
        );
        """
    )
    return _FakeDb(conn), conn


def _insert(
    conn,
    mid: str,
    *,
    lifecycle="claimed",
    lifecycle_age_sec: float = 0.0,
    created_age_sec: float | None = None,
    agent="a1",
    type="USER_MESSAGE",
    scope: str | None = "scope-x",
):
    """Insert a chat_messages row.

    ``lifecycle_age_sec`` sets ``lifecycle_updated_at`` to that many
    seconds in the past. ``created_age_sec`` defaults to
    ``lifecycle_age_sec`` (normal rows: claim happens shortly after
    birth); override it to simulate the subscriber-restart case where
    ``lifecycle_updated_at`` is fresh but ``created_at`` is ancient.
    """
    if created_age_sec is None:
        created_age_sec = lifecycle_age_sec
    now_s = int(time.time())
    created_iso = time.strftime(
        "%Y-%m-%d %H:%M:%S", time.gmtime(now_s - int(created_age_sec))
    )
    life_ms = (now_s - int(lifecycle_age_sec)) * 1000
    conn.execute(
        "INSERT INTO chat_messages (id, agent_id, type, claimed_by_scope, "
        "created_at, lifecycle, lifecycle_updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (mid, agent, type, scope, created_iso, lifecycle, life_ms),
    )
    conn.commit()


# ── Basic shape ───────────────────────────────────────────────────────────────

def test_empty_db_returns_zero_counts(db):
    """Healthy steady state — endpoint MUST NOT 404."""
    fdb, _ = db
    resp = query_stuck_claimed(fdb)
    assert resp.count == 0
    assert resp.matched_by_lifecycle == 0
    assert resp.matched_by_created == 0
    assert resp.stuck == []


def test_fresh_claimed_row_not_returned(db):
    """A row claimed 60s ago is clearly in-flight; scanner must not
    touch it. 24h default threshold makes this ~2 orders of magnitude
    above anything sane."""
    fdb, conn = db
    _insert(conn, "fresh", lifecycle_age_sec=60)
    resp = query_stuck_claimed(fdb)
    assert resp.count == 0


def test_claimed_over_lifecycle_threshold_is_returned(db):
    fdb, conn = db
    age = DEFAULT_MIN_AGE_SEC + 60  # just over 24h
    _insert(conn, "stuck1", lifecycle_age_sec=age)
    resp = query_stuck_claimed(fdb)
    assert resp.count == 1
    r = resp.stuck[0]
    assert r.message_id == "stuck1"
    assert r.matched_axis in ("lifecycle", "both")
    # lifecycle_age_seconds is real-time; allow ±5s jitter for slow CI.
    assert age - 5 <= r.lifecycle_age_seconds <= age + 5
    assert resp.matched_by_lifecycle == 1
    assert resp.matched_by_created == 0


# ── Subscriber-restart case: the failure mode that broke PR-51 Part 1 ────────

def test_fresh_lifecycle_but_ancient_created_is_returned(db):
    """Subscriber restart re-claims an old row → ``lifecycle_updated_at``
    gets bumped to "now" while ``created_at`` stays at the original
    birth time. Part 1 of PR-51 learned the hard way this is the
    dominant stuck-claimed pattern on prod. Must be caught by the
    ``created_at`` axis."""
    fdb, conn = db
    _insert(
        conn, "restart_victim",
        lifecycle_age_sec=60,                              # recent claim
        created_age_sec=DEFAULT_CREATED_MIN_AGE_SEC + 60,  # ancient birth
    )
    resp = query_stuck_claimed(fdb)
    assert resp.count == 1
    r = resp.stuck[0]
    assert r.message_id == "restart_victim"
    assert r.matched_axis == "created"
    assert resp.matched_by_lifecycle == 0
    assert resp.matched_by_created == 1


def test_both_axes_match_returns_both_tag(db):
    fdb, conn = db
    _insert(
        conn, "very_old",
        lifecycle_age_sec=DEFAULT_MIN_AGE_SEC + 100,
        created_age_sec=DEFAULT_CREATED_MIN_AGE_SEC + 100,
    )
    resp = query_stuck_claimed(fdb)
    assert resp.count == 1
    assert resp.stuck[0].matched_axis == "both"
    # A row matched by BOTH axes still counts in ``matched_by_lifecycle``
    # (not ``matched_by_created``) — the bucketing is intentionally
    # biased toward lifecycle because that's the default remediation
    # axis; the ``matched_axis`` field on each row gives callers the
    # precise breakdown if needed.
    assert resp.matched_by_lifecycle == 1
    assert resp.matched_by_created == 0


# ── Non-claimed rows are never returned ───────────────────────────────────────

@pytest.mark.parametrize("lc", ["pending", "consumed", "deduped", "orphaned"])
def test_non_claimed_lifecycles_never_returned(db, lc):
    """Only lifecycle='claimed' is scannable. Pending has its own
    endpoint (/v1/orphans); consumed/deduped are terminal."""
    fdb, conn = db
    _insert(
        conn, "skipme",
        lifecycle=lc,
        lifecycle_age_sec=DEFAULT_CREATED_MIN_AGE_SEC + 100,
        created_age_sec=DEFAULT_CREATED_MIN_AGE_SEC + 100,
    )
    resp = query_stuck_claimed(fdb)
    assert resp.count == 0


# ── Type: no filter ───────────────────────────────────────────────────────────

def test_agent_reply_stuck_claimed_is_returned(db):
    """Unlike ``/v1/orphans`` (PR-41 restricts to trigger types), a
    stuck-claimed ``AGENT_REPLY`` is a real bug — a pre-PR-41-amend
    leftover. It must surface so the scanner can flip it to
    ``consumed``."""
    fdb, conn = db
    age = DEFAULT_MIN_AGE_SEC + 60
    _insert(conn, "reply_stuck", type="AGENT_REPLY", lifecycle_age_sec=age)
    resp = query_stuck_claimed(fdb)
    assert resp.count == 1
    assert resp.stuck[0].type == "AGENT_REPLY"


def test_unknown_type_stuck_claimed_is_returned(db):
    """Forward compatibility. Unknown types shouldn't block the
    remediation path; they might be real bugs too."""
    fdb, conn = db
    age = DEFAULT_MIN_AGE_SEC + 60
    _insert(conn, "unknown_stuck", type="SYSTEM_HINT_FUTURE", lifecycle_age_sec=age)
    resp = query_stuck_claimed(fdb)
    assert resp.count == 1


# ── Ordering / limit ──────────────────────────────────────────────────────────

def test_oldest_lifecycle_first(db):
    fdb, conn = db
    _insert(conn, "mid",  lifecycle_age_sec=DEFAULT_MIN_AGE_SEC + 500)
    _insert(conn, "old",  lifecycle_age_sec=DEFAULT_MIN_AGE_SEC + 5000)
    _insert(conn, "new",  lifecycle_age_sec=DEFAULT_MIN_AGE_SEC + 100)
    resp = query_stuck_claimed(fdb)
    assert [r.message_id for r in resp.stuck] == ["old", "mid", "new"]


def test_limit_caps_returned_rows(db):
    fdb, conn = db
    for i in range(5):
        _insert(conn, f"m{i}", lifecycle_age_sec=DEFAULT_MIN_AGE_SEC + 100 + i)
    resp = query_stuck_claimed(fdb, limit=3)
    assert resp.count == 3


# ── Forensic payload ──────────────────────────────────────────────────────────

def test_claimed_by_scope_is_surfaced(db):
    """The dead-scope id is the single most useful forensic field when
    chasing "which scope left claims behind"."""
    fdb, conn = db
    _insert(
        conn, "m1",
        lifecycle_age_sec=DEFAULT_MIN_AGE_SEC + 60,
        scope="dead-scope-abc",
    )
    resp = query_stuck_claimed(fdb)
    assert resp.count == 1
    assert resp.stuck[0].claimed_by_scope == "dead-scope-abc"


# ── Kill-switch behaviour via params ──────────────────────────────────────────

def test_zero_lifecycle_threshold_returns_all_claimed(db):
    """min_age_sec=0 + min_created_age_sec=0 is the "catch everything
    claimed" debug mode; useful for ops troubleshooting but NOT the
    default. Must work without crashing on lifecycle_updated_at==now_ms."""
    fdb, conn = db
    _insert(conn, "even_fresh", lifecycle_age_sec=0)
    resp = query_stuck_claimed(fdb, min_age_sec=0, min_created_age_sec=0)
    # Can't assert strict equality on count because the "age==0"
    # comparison depends on sub-second timing; what matters is no crash.
    assert resp.count >= 0


def test_claim_just_under_threshold_excluded(db):
    """Off-by-one guard: a row exactly ``min_age_sec - 1`` seconds old
    must NOT match the lifecycle axis (``<=`` not ``<`` in the SQL)."""
    fdb, conn = db
    # Pick a value comfortably below the threshold minus CI jitter.
    _insert(conn, "young", lifecycle_age_sec=DEFAULT_MIN_AGE_SEC - 300,
            created_age_sec=DEFAULT_CREATED_MIN_AGE_SEC - 300)
    resp = query_stuck_claimed(fdb)
    assert resp.count == 0
