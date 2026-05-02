"""PR-40 regression — id-fallback removed, fail-fast when caller omits id.

Historical context (see ``docs/roadmap/tickets/PR-40-*`` in the novaic repo):
``SqlEntityStore._sql_create`` / ``append`` used to have a two-step fallback
when the caller omitted ``id``:

  (a) coerce ``params[key_params[0]]`` into the primary key;
  (b) else mint ``uuid.uuid4().hex``.

Both variants violated "no silent failure":

  (a) for stream entities (``messages``, ``subagents``, ``agent-memory`` —
      ``id_field != key_params[0]``) the scope key became the primary key
      and every second insert collided on ``UNIQUE constraint failed``.
      Prod symptom: ``chat_reply`` stuck after one reply.
  (b) silent uuid minting hid "caller forgot to mint an id" bugs forever.

PR-40 drops the whole fallback. Singleton entities still work because the
``row[id_field] = params[key_params[0]]`` copy happens *earlier*, via the
scope-key-copy loop, and the caller's intent (``id_field == key_params[0]``)
is explicit in the schema definition.
"""
import sqlite3

import pytest

from entangled.sql.entity_store import SqlEntityStore
from entangled.sql.entity_def import SqlEntityDef
from entangled.sql.field_def import F


# ── Fake DB plumbing ───────────────────────────────────────────────────────


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


# (1) stream entity: id_field="id" ≠ key_params[0]="agent_id".
#     Caller MUST mint id; omitting raises ValueError.
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


# (2) singleton entity: id_field=="agent_id"==key_params[0].
#     Scope-key-copy loop fills row[id_field] from params → fallback never
#     fires → no ValueError. Caller can continue to omit id in payload.
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


# (3) INTEGER autoincrement id: is_auto_int branch short-circuits.
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


def test_stream_entity_without_id_raises_value_error(store):
    """PR-40: stream entity with no ``id`` in payload must fail-fast.

    Before PR-40 this call would either (a) use ``agent_id`` as the primary
    key (UNIQUE collision on next insert) or (b) mint a silent uuid. Both
    behaviors violated "no silent failure".
    """
    agent_id = "415f6cfd4e5b4a04911b66cb8ab2cad7"

    with pytest.raises(ValueError) as excinfo:
        store.append(
            "messages",
            "",
            {"type": "AGENT_REPLY", "timestamp": "2026-04-21T00:00:00Z"},
            params={"agent_id": agent_id},
            notify=False,
        )
    msg = str(excinfo.value)
    assert "missing required 'id'" in msg
    assert "entity='messages'" in msg
    assert "PR-40" in msg, "error must reference the decision for future grep-ability"


def test_stream_entity_with_caller_minted_id_succeeds(store):
    """Caller-minted id is the ONLY correct way for stream entities."""
    agent_id = "agent-happy-path"

    r1 = store.append(
        "messages",
        "",
        {
            "id": "msg-001",
            "type": "AGENT_REPLY",
            "timestamp": "2026-04-21T00:00:00Z",
        },
        params={"agent_id": agent_id},
        notify=False,
    )
    r2 = store.append(
        "messages",
        "",
        {
            "id": "msg-002",
            "type": "AGENT_REPLY",
            "timestamp": "2026-04-21T00:00:01Z",
        },
        params={"agent_id": agent_id},
        notify=False,
    )
    assert r1["id"] == "msg-001"
    assert r2["id"] == "msg-002"
    assert r1["id"] != agent_id
    assert r2["id"] != agent_id


def test_singleton_entity_still_uses_scope_key_as_id(store):
    """``agent-tools`` has ``id_field == key_params[0] == 'agent_id'``.

    The scope-key-copy loop at the top of ``_sql_create`` / ``append``
    fills ``row['agent_id']`` from params BEFORE the id-check. So even
    though the caller's payload omits ``id``, ``res_id`` is already truthy
    and the PR-40 fail-fast guard does not fire. This test locks in that
    we did NOT break singleton semantics.
    """
    agent_id = "agent-singleton-xyz"
    row = store.create(
        "agent-tools",
        "",
        {"personality": "{\"mode\": \"friendly\"}"},
        params={"agent_id": agent_id},
        notify=False,
    )
    assert row["agent_id"] == agent_id


def test_integer_autoincrement_id_unaffected(store):
    """INTEGER id takes the ``is_auto_int`` branch before the fail-fast
    guard. Caller continues to omit id; SQLite auto-assigns via rowid."""
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


def test_error_message_names_the_entity_and_field(store):
    """Regression guard for the error shape — ops / on-call grep this
    string when triaging a new "caller forgot id" bug."""
    with pytest.raises(ValueError) as excinfo:
        store.append(
            "messages",
            "",
            {"type": "USER_MESSAGE", "timestamp": "2026-04-21T00:00:00Z"},
            params={"agent_id": "some-agent"},
            notify=False,
        )
    msg = str(excinfo.value)
    # Format contract: must contain id_field name AND entity name AND
    # mention that Entangled does NOT mint ids (so the reader doesn't go
    # looking for a broken uuid fallback).
    assert "id" in msg
    assert "messages" in msg
    assert "does not mint" in msg.lower()
