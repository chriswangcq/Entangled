from contextlib import contextmanager

from entangled.sql.entity_def import SqlEntityDef
from entangled.sql.entity_store import SqlEntityStore
from entangled.sql.field_def import F


class _Cursor:
    def __init__(self, rowcount=1, rows=None):
        self.rowcount = rowcount
        self.lastrowid = None
        self._rows = rows or []

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _FakePostgresDb:
    backend_name = "postgres"

    def __init__(self):
        self.executed = []
        self.returning = []

    @contextmanager
    def transaction(self, lock_type="global", resource_id="", timeout=None):
        yield self

    def execute(self, sql, params=()):
        self.executed.append((sql, params))
        return _Cursor(rowcount=1)

    def fetchone(self, sql, params=()):
        self.executed.append((sql, params))
        return None

    def fetchall(self, sql, params=()):
        self.executed.append((sql, params))
        return []

    def insert_returning_id(self, sql, params=()):
        self.returning.append((sql, params))
        return 42


def _auto_def() -> SqlEntityDef:
    return SqlEntityDef(
        name="auto-widgets",
        table="auto_widgets",
        id_field="id",
        user_scoped=False,
        fields=[
            F.int_("id", primary=True),
            F.text("name", nullable=False),
            F.timestamp("updated_at"),
        ],
    )


def _widget_def() -> SqlEntityDef:
    return SqlEntityDef(
        name="widgets",
        table="widgets",
        id_field="id",
        user_scoped=True,
        fields=[
            F.text("id", primary=True),
            F.text("user_id", nullable=False, index=True),
            F.text("name", nullable=False),
            F.timestamp("updated_at"),
        ],
    )


def test_postgres_auto_integer_create_uses_returning():
    db = _FakePostgresDb()
    store = SqlEntityStore(db=db)
    row = store._sql_create(_auto_def(), "u1", {"name": "alpha"})

    assert row["id"] == 42
    assert db.returning
    sql, params = db.returning[0]
    assert sql == "INSERT INTO auto_widgets (name) VALUES (?) RETURNING id"
    assert params == ("alpha",)


def test_postgres_update_uses_postgres_timestamp_expression():
    db = _FakePostgresDb()
    store = SqlEntityStore(db=db)

    store._sql_update(_widget_def(), "u1", "w1", {"name": "beta"})

    update_sql = db.executed[0][0]
    assert "UPDATE widgets SET name = ?" in update_sql
    assert "datetime('now')" not in update_sql
    assert "to_char(timezone('UTC', now())" in update_sql


def test_postgres_upsert_uses_postgres_timestamp_expression():
    db = _FakePostgresDb()
    store = SqlEntityStore(db=db)

    store._sql_upsert(_widget_def(), "u1", "w1", {"name": "gamma"})

    upsert_sql = db.executed[0][0]
    assert "ON CONFLICT(id) DO UPDATE SET" in upsert_sql
    assert "excluded.name" in upsert_sql
    assert "datetime('now')" not in upsert_sql
    assert "to_char(timezone('UTC', now())" in upsert_sql


def test_postgres_delete_and_cas_preserve_rowcount_paths():
    db = _FakePostgresDb()
    store = SqlEntityStore(db=db)
    store.register(_widget_def())

    assert store._sql_delete(_widget_def(), "u1", "w1") is True
    cas = store.cas_update("widgets", "u1", {"id": "w1"}, {"name": "delta"}, emit_notifications=False)

    assert cas is None
    assert any(sql.startswith("DELETE FROM widgets") for sql, _params in db.executed)
    assert any(sql.startswith("UPDATE widgets SET name = ?") for sql, _params in db.executed)


def test_postgres_bool_input_keeps_python_bool_for_boolean_columns():
    db = _FakePostgresDb()
    store = SqlEntityStore(db=db)
    defn = SqlEntityDef(
        name="flags",
        table="flags",
        id_field="id",
        user_scoped=False,
        fields=[
            F.int_("id", primary=True),
            F.bool_("is_enabled"),
        ],
    )

    row = store._in(defn, {"is_enabled": False})

    assert row["is_enabled"] is False
