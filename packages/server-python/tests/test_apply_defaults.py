"""Tests for SqlEntityStore._apply_defaults — schema-declared runtime fill.

Goal: eliminate the "silent 400 on missing NOT NULL" class of bugs. A field
declared with ``nullable=False`` AND ``default=<X>`` carries an explicit
intent: *if the caller did not provide this value, fill X*. Previously that
intent was only honoured via a SQL DEFAULT at CREATE TABLE time, which:

    1. Does not apply to existing tables (no ALTER TABLE SET DEFAULT path).
    2. Does not propagate through generic CRUD callers that don't know
       per-entity semantics (e.g. agent-runtime's ``gw.entity_create``).

Here we verify the runtime fill honours the narrow, predictable contract:

    ``nullable=False`` AND ``default is not None`` AND name not in row
        → fill happens.

Nothing else.
"""
from __future__ import annotations

import re
import sqlite3

import pytest

from entangled.sql.entity_store import SqlEntityStore, _iso_now_utc
from entangled.sql.entity_def import SqlEntityDef
from entangled.sql.field_def import F


# ── Test rig ─────────────────────────────────────────────────────────────────

class _FakeDatabase:
    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=()):
        return self._conn.execute(sql, params)

    def fetchone(self, sql, params=()):
        cur = self._conn.execute(sql, params)
        row = cur.fetchone()
        return dict(row) if row else None

    def fetchall(self, sql, params=()):
        cur = self._conn.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]

    class _TxCtx:
        def __init__(self, conn):
            self._conn = conn
        def __enter__(self):
            return self
        def __exit__(self, *_):
            self._conn.commit()

    def transaction(self, lock_type="global", resource_id="", timeout=None):
        return self._TxCtx(self._conn)


# Schemas used across tests. Constructed fresh per-test via factories because
# ``SqlEntityStore.register()`` installs fn-pointer lambdas onto the def that
# close over the store instance — reusing a module-level def across tests
# causes the second test's calls to dispatch to the first test's store.

def _make_messages_def() -> SqlEntityDef:
    """Models the production `messages` entity: `timestamp` is NOT NULL with
    default="NOW". The rest (created_at/updated_at as F.timestamp(auto=True))
    stay nullable=True as in production — they must NOT be affected by the
    new runtime fill."""
    return SqlEntityDef(
        name="messages",
        table="chat_messages",
        id_field="id",
        user_scoped=False,
        key_params=["agent_id"],
        default_order="timestamp DESC",
        sync_type="stream",
        fields=[
            F.text("id", primary=True),
            F.text("agent_id", nullable=False),
            F.text("type", nullable=False),
            F.json("content"),
            F.text("timestamp", nullable=False, default="NOW"),
            F.timestamp("created_at"),
            F.timestamp("updated_at"),
        ],
    )


def _make_users_def() -> SqlEntityDef:
    """Minimal user-scoped entity for _sql_create / _sql_upsert paths."""
    return SqlEntityDef(
        name="users",
        table="users",
        id_field="id",
        user_scoped=False,
        key_params=[],
        default_order="created_at DESC",
        sync_type="list",
        fields=[
            F.text("id", primary=True),
            F.text("name", nullable=False),
            # NOT NULL with a literal (non-NOW) default — should be filled verbatim.
            F.text("status", nullable=False, default="pending"),
            # Nullable-with-default — must NOT be filled (would widen
            # behaviour for F.timestamp(auto=True) style fields).
            F.timestamp("created_at"),
        ],
    )


def _make_agent_state_def(*, include_sleep_started_at: bool) -> SqlEntityDef:
    fields = [
        F.text("agent_id", primary=True),
        F.text("state", nullable=False, default="awake"),
    ]
    if include_sleep_started_at:
        fields.append(F.timestamp("sleep_started_at"))
    return SqlEntityDef(
        name="agent-state",
        table="agent_state",
        id_field="agent_id",
        user_scoped=False,
        key_params=[],
        default_order="agent_id",
        sync_type="list",
        fields=fields,
    )


def _make_store(defn: SqlEntityDef) -> SqlEntityStore:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    db = _FakeDatabase(conn)
    s = SqlEntityStore(db=db)
    s.register(defn)
    s.ensure_schema(defn)
    return s


_ISO_Z_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")


# ── `default="NOW"` runtime fill ─────────────────────────────────────────────

def test_iso_now_utc_format_matches_business_helper() -> None:
    """_iso_now_utc() must match the `common.utils.time.utc_now_iso` format
    used by the business layer so historical and new timestamps collate."""
    assert _ISO_Z_RE.match(_iso_now_utc()), _iso_now_utc()


def test_append_fills_timestamp_when_missing() -> None:
    """PR-34 root-cause: agent-runtime's chat_reply calls entity_create()
    without `timestamp`. With runtime fill, this no longer fails."""
    store = _make_store(_make_messages_def())
    result = store.append("messages", "", {
        "id": "msg-001",
        "agent_id": "agent-a",
        "type": "AGENT_REPLY",
        "content": {"text": "hi"},
    }, params={"agent_id": "agent-a"}, notify=False)

    assert result["id"] == "msg-001"
    ts = result["timestamp"]
    assert isinstance(ts, str) and _ISO_Z_RE.match(ts), (
        f"Expected ISO-UTC-with-Z timestamp, got: {ts!r}"
    )


def test_append_respects_explicit_timestamp() -> None:
    """Caller-supplied timestamp must win over the default."""
    store = _make_store(_make_messages_def())
    result = store.append("messages", "", {
        "id": "msg-002",
        "agent_id": "agent-a",
        "type": "AGENT_REPLY",
        "content": {"text": "hi"},
        "timestamp": "2026-04-18T00:00:00.000Z",
    }, params={"agent_id": "agent-a"}, notify=False)

    assert result["timestamp"] == "2026-04-18T00:00:00.000Z"


# ── literal default fill ─────────────────────────────────────────────────────

def test_create_fills_literal_default_when_missing() -> None:
    """Non-NOW defaults (e.g. status="pending") are filled verbatim."""
    store = _make_store(_make_users_def())
    result = store.create("users", "", {"id": "u1", "name": "Alice"})
    assert result["status"] == "pending"


def test_create_respects_explicit_literal() -> None:
    store = _make_store(_make_users_def())
    result = store.create("users", "", {
        "id": "u1", "name": "Alice", "status": "active",
    })
    assert result["status"] == "active"


# ── scope limit: nullable fields are NOT auto-filled ─────────────────────────

def test_nullable_fields_with_defaults_are_not_runtime_filled() -> None:
    """`F.timestamp(auto=True)` produces nullable=True, default="NOW" — these
    fields MUST continue to rely solely on the SQL DEFAULT (or be left NULL
    on old tables), so that runtime-fill does NOT silently widen behaviour
    for existing timestamp columns (created_at/updated_at)."""
    # Use an in-memory DB that we manually create WITHOUT a SQL DEFAULT for
    # created_at (simulating an old production table).
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL, "
        "status TEXT NOT NULL DEFAULT 'pending', created_at TEXT)"
    )
    db = _FakeDatabase(conn)
    s = SqlEntityStore(db=db)
    s.register(_make_users_def())

    # Create without created_at. Runtime fill must NOT populate it because
    # its field is nullable=True; the column ends up NULL.
    s.create("users", "", {"id": "u1", "name": "Alice"})
    row = conn.execute("SELECT created_at FROM users WHERE id='u1'").fetchone()
    assert row["created_at"] is None, (
        "Runtime fill leaked into a nullable field; this would change the "
        "observed wire format of existing F.timestamp(auto=True) columns."
    )


def test_alter_add_column_omits_non_constant_now_default() -> None:
    """SQLite rejects ADD COLUMN with DEFAULT (datetime('now')).

    Existing online tables may be missing nullable timestamp columns after a
    schema owner adds one. ALTER must add the column without the non-constant
    SQL default, while CREATE TABLE keeps the default for fresh tables.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    db = _FakeDatabase(conn)
    store = SqlEntityStore(db=db)

    old_def = _make_agent_state_def(include_sleep_started_at=False)
    store.register(old_def)
    store.ensure_schema(old_def)
    conn.execute(
        "INSERT INTO agent_state (agent_id, state) VALUES (?, ?)",
        ("agent-a", "sleeping"),
    )

    new_def = _make_agent_state_def(include_sleep_started_at=True)
    assert "DEFAULT (datetime('now'))" in new_def.create_table_sql()
    alter_sqls = new_def.alter_add_column_sqls(["agent_id", "state"])
    assert alter_sqls == ["ALTER TABLE agent_state ADD COLUMN sleep_started_at TEXT;"]

    store.ensure_schema(new_def)
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(agent_state)")]
    assert "sleep_started_at" in cols
    row = conn.execute(
        "SELECT sleep_started_at FROM agent_state WHERE agent_id='agent-a'"
    ).fetchone()
    assert row["sleep_started_at"] is None


# ── upsert path ──────────────────────────────────────────────────────────────

def test_upsert_fills_missing_required_default_on_insert() -> None:
    """Upsert's INSERT branch must also honour the runtime fill."""
    store = _make_store(_make_users_def())
    result = store.upsert("users", "", "u1", {"name": "Alice"})
    assert result["status"] == "pending"


# ── _check_required: caller-must-provide fields ──────────────────────────────
#
# Companion to _apply_defaults. After the runtime fill runs, any field that is
# still (a) NOT NULL, (b) non-primary, (c) has no default, and (d) missing from
# the row must be attributed and reported loudly by name — rather than letting
# the write reach SQLite and surfacing as
#   IntegrityError: NOT NULL constraint failed: <table>.<col>
# which the HTTP layer hands back as an opaque 400. See PR-35.


def test_check_required_raises_on_missing_caller_required_field() -> None:
    """messages.agent_id is NOT NULL with no default — caller must provide.
    Missing it should raise ValueError naming the field, BEFORE SQL."""
    store = _make_store(_make_messages_def())
    with pytest.raises(ValueError) as ei:
        store.append("messages", "", {
            "id": "m1",
            # agent_id omitted — the classic class-of-bug this guards against.
            "type": "AGENT_REPLY",
            "content": {"text": "hi"},
        }, params={}, notify=False)
    msg = str(ei.value)
    assert "messages" in msg and "agent_id" in msg, msg


def test_check_required_lists_all_missing_fields() -> None:
    """Multiple missing fields are all reported in a single error (not just
    the first one). This matters for caller ergonomics — fix once, not
    once-per-SQL-roundtrip."""
    store = _make_store(_make_messages_def())
    with pytest.raises(ValueError) as ei:
        store.append("messages", "", {
            "id": "m1",
            "content": {"text": "hi"},
            # both agent_id AND type are missing
        }, params={}, notify=False)
    msg = str(ei.value)
    assert "agent_id" in msg and "type" in msg, msg


def test_check_required_passes_when_apply_defaults_filled_missing() -> None:
    """Sanity: _check_required must NOT fire on fields that _apply_defaults
    filled in (timestamp with default="NOW"). The guard only covers fields
    the schema says the caller owns."""
    store = _make_store(_make_messages_def())
    result = store.append("messages", "", {
        "id": "m1",
        "agent_id": "a",
        "type": "AGENT_REPLY",
        "content": {"text": "hi"},
        # timestamp missing — _apply_defaults fills it from default="NOW"
    }, params={"agent_id": "a"}, notify=False)
    assert result["id"] == "m1"
    assert _ISO_Z_RE.match(result["timestamp"])


def test_check_required_does_not_fire_on_explicit_none() -> None:
    """An explicit ``None`` is a stated caller intent (write NULL). Per the
    documented contract of _apply_defaults, we don't overwrite it; SQL will
    then raise the classic NOT NULL error with the column name attached,
    which is loud enough given the caller's deliberate None."""
    store = _make_store(_make_messages_def())
    # agent_id is present (value=None) → _check_required sees `in row` → pass.
    # SQLite NOT NULL then surfaces — we only assert _check_required does NOT
    # swallow this case into a ValueError, to keep the "caller intent" contract.
    with pytest.raises(sqlite3.IntegrityError):
        store.append("messages", "", {
            "id": "m1",
            "agent_id": None,
            "type": "AGENT_REPLY",
            "content": {"text": "hi"},
        }, params={}, notify=False)


def test_check_required_passes_on_upsert_too() -> None:
    """Upsert's INSERT branch runs _check_required; missing caller-required
    field must also raise there."""
    store = _make_store(_make_messages_def())
    with pytest.raises(ValueError) as ei:
        store.upsert("messages", "", "m1", {
            # agent_id & type missing on insert branch
            "content": {"text": "hi"},
        }, params={})
    assert "agent_id" in str(ei.value) and "type" in str(ei.value)


def test_check_required_noop_when_everything_provided() -> None:
    """Happy path: all caller-required fields present → no raise, row lands."""
    store = _make_store(_make_messages_def())
    result = store.append("messages", "", {
        "id": "m1",
        "agent_id": "a",
        "type": "AGENT_REPLY",
        "content": {"text": "hi"},
        "timestamp": "2026-04-19T20:50:00.000Z",
    }, params={"agent_id": "a"}, notify=False)
    assert result["id"] == "m1"
    assert result["agent_id"] == "a"
    assert result["timestamp"] == "2026-04-19T20:50:00.000Z"
