"""PR-40 regression — ID-fallback must not use scope-key when it is not the
primary key.

Root-cause summary (see ``docs/roadmap/tickets/PR-40-*`` in the novaic repo):
``SqlEntityStore._sql_create`` / ``append`` used to fall back to
``params[key_params[0]]`` as ``id`` whenever the caller omitted ``id``. That is
correct for **singleton-per-scope** entities (``id_field == key_params[0]``,
e.g. ``agent-tools``), but for **stream/list** entities
(``id_field != key_params[0]``, e.g. ``messages``) every second insert
collides on ``UNIQUE constraint failed``.

PR-40 narrows the fallback to ``defn.key_params[0] == defn.id_field``;
otherwise the id is minted from ``uuid.uuid4().hex``.
"""
import sqlite3

import pytest

from entangled.sql.entity_store import SqlEntityStore
from entangled.sql.entity_def import SqlEntityDef
from entangled.sql.field_def import F


# ── Fake DB plumbing (copied shape from test_outbox_insert.py) ─────────────


class _FakeDatabase:
    def __init__(self, conn: sqlite3.Connection):
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

    class _Tx:
        def __init__(self, conn):
            self._conn = conn

        def __enter__(self):
            return self

        def __exit__(self, *a):
            self._conn.commit()

    def transaction(self, lock_type="global", resource_id="", timeout=None):
        return self._Tx(self._conn)


def _make_store():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return SqlEntityStore(db=_FakeDatabase(conn))


# ── Entity defs exercising the four shapes ────────────────────────────────


# (1) stream entity: id_field="id" ≠ key_params[0]="agent_id" — PR-40 target
STREAM_DEF = SqlEntityDef(
    name="messages",
    table="chat_messages_t40",
    id_field="id",
    user_scoped=False,
    key_params=["agent_id"],
    default_order="timestamp ASC",
    sync_type="stream",
    fields=[
        F.text("id", primary=True),
        F.text("agent_id", nullable=False),
        F.text("type", nullable=False),
        F.text("timestamp", nullable=False),
    ],
)


# (2) singleton entity: id_field=="agent_id"==key_params[0] — fallback still valid
SINGLETON_DEF = SqlEntityDef(
    name="agent-tools",
    table="agent_tools_t40",
    id_field="agent_id",
    user_scoped=False,
    key_params=["agent_id"],
    fields=[
        F.text("agent_id", primary=True),
        F.text("personality", default="{}"),
    ],
)


# (3) INTEGER autoincrement id — fallback always bypassed
AUTOINT_DEF = SqlEntityDef(
    name="exec-logs",
    table="exec_logs_t40",
    id_field="id",
    user_scoped=False,
    key_params=["agent_id"],
    fields=[
        F.int_("id", primary=True),
        F.text("agent_id", nullable=False),
        F.text("timestamp", nullable=False),
    ],
)


@pytest.fixture
def store():
    s = _make_store()
    for defn in (STREAM_DEF, SINGLETON_DEF, AUTOINT_DEF):
        s.register(defn)
        s.ensure_schema(defn)
    return s


# ── Tests ──────────────────────────────────────────────────────────────────


def test_stream_entity_gets_unique_uuid_not_scope_key(store):
    """Two appends without ``id``: both rows must get a uuid hex, neither
    equals the scope-key, and they differ from each other.

    Pre-PR-40 this test would fail on the second append with
    ``sqlite3.IntegrityError: UNIQUE constraint failed: chat_messages_t40.id``.
    """
    agent_id = "415f6cfd4e5b4a04911b66cb8ab2cad7"

    r1 = store.append(
        "messages",
        "",
        {"type": "AGENT_REPLY", "timestamp": "2026-04-21T00:00:00Z"},
        params={"agent_id": agent_id},
        notify=False,
    )
    r2 = store.append(
        "messages",
        "",
        {"type": "AGENT_REPLY", "timestamp": "2026-04-21T00:00:01Z"},
        params={"agent_id": agent_id},
        notify=False,
    )

    assert r1["id"] != agent_id, "scope-key must NOT be used as primary key"
    assert r2["id"] != agent_id
    assert r1["id"] != r2["id"], "each append must mint a fresh id"
    assert len(r1["id"]) == 32, "uuid4().hex is 32 chars"
    assert len(r2["id"]) == 32


def test_singleton_entity_still_uses_scope_key_as_id(store):
    """``agent-tools`` has id_field==key_params[0]=='agent_id' — the fallback
    is intentional here (one row per agent). Must keep working."""
    agent_id = "agent-singleton-xyz"

    row = store.create(
        "agent-tools",
        "",
        {"personality": "{\"mode\": \"friendly\"}"},
        params={"agent_id": agent_id},
        notify=False,
    )

    assert row["agent_id"] == agent_id
    # id_field *is* agent_id for this entity, so the stored id equals agent_id
    assert row.get("agent_id") == agent_id


def test_caller_provided_id_wins_over_fallback(store):
    """Explicit ``id`` in payload is always respected, regardless of
    key_params shape."""
    agent_id = "agent-explicit"
    explicit_id = "msg-explicit-123"

    r = store.append(
        "messages",
        "",
        {
            "id": explicit_id,
            "type": "USER_MESSAGE",
            "timestamp": "2026-04-21T00:00:00Z",
        },
        params={"agent_id": agent_id},
        notify=False,
    )

    assert r["id"] == explicit_id


def test_integer_autoincrement_id_unaffected(store):
    """INTEGER id entities take the ``is_auto_int`` branch before reaching the
    fallback logic; scope-key coercion must never apply."""
    agent_id = "agent-autoint"

    r1 = store.append(
        "exec-logs",
        "",
        {"agent_id": agent_id, "timestamp": "2026-04-21T00:00:00Z"},
        params={"agent_id": agent_id},
        notify=False,
    )
    r2 = store.append(
        "exec-logs",
        "",
        {"agent_id": agent_id, "timestamp": "2026-04-21T00:00:01Z"},
        params={"agent_id": agent_id},
        notify=False,
    )

    assert isinstance(r1["id"], int)
    assert isinstance(r2["id"], int)
    assert r2["id"] > r1["id"]
    assert r1["id"] != agent_id  # trivially true — different types — but explicit
