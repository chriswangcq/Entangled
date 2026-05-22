from contextlib import contextmanager

import pytest

from entangled.sql.entity_def import SqlEntityDef
from entangled.sql.entity_store import SqlEntityStore
from entangled.sql.field_def import F
from entangled.sql.validation import SchemaValidationError, normalize_order_by


class _Cursor:
    rowcount = 1


class _FakeDb:
    def __init__(self, backend_name="postgres"):
        self.backend_name = backend_name
        self.fetchone_rows = []
        self.fetchall_rows = []
        self.executed = []
        self.fetchone_calls = []
        self.fetchall_calls = []

    @contextmanager
    def transaction(self, lock_type="global", resource_id="", timeout=None):
        yield self

    def execute(self, sql, params=()):
        self.executed.append((sql, params))
        return _Cursor()

    def fetchone(self, sql, params=()):
        self.fetchone_calls.append((sql, params))
        return self.fetchone_rows.pop(0) if self.fetchone_rows else None

    def fetchall(self, sql, params=()):
        self.fetchall_calls.append((sql, params))
        return self.fetchall_rows.pop(0) if self.fetchall_rows else []


def _stream_def(default_order="created_at DESC") -> SqlEntityDef:
    return SqlEntityDef(
        name="messages",
        table="messages",
        id_field="id",
        user_scoped=True,
        fields=[
            F.text("id", primary=True),
            F.text("user_id", nullable=False),
            F.text("body"),
            F.timestamp("created_at"),
        ],
        default_order=default_order,
    )


def test_postgres_list_stream_uses_entangled_rowid_for_before_cursor():
    db = _FakeDb("postgres")
    db.fetchone_rows.append({"_cf": "2026-05-22T00:00:00.000Z", "_rid": 9})
    store = SqlEntityStore(db=db)
    store.register(_stream_def())

    store.list_stream("messages", "u1", before_id="m9", limit=5)

    ref_sql, ref_params = db.fetchone_calls[0]
    page_sql, page_params = db.fetchall_calls[0]
    assert ref_sql == "SELECT created_at AS _cf, entangled_rowid AS _rid FROM messages WHERE id = ?"
    assert ref_params == ("m9",)
    assert "entangled_rowid < ?" in page_sql
    assert "rowid" not in page_sql.replace("entangled_rowid", "")
    assert page_params == ("u1", "2026-05-22T00:00:00.000Z", "2026-05-22T00:00:00.000Z", 9, 5)


def test_sqlite_list_stream_still_uses_rowid():
    db = _FakeDb("sqlite")
    db.fetchone_rows.append({"_cf": "2026-05-22T00:00:00.000Z", "_rid": 9})
    store = SqlEntityStore(db=db)
    store.register(_stream_def())

    store.list_stream("messages", "u1", before_id="m9", limit=5)

    ref_sql, _ref_params = db.fetchone_calls[0]
    page_sql, _page_params = db.fetchall_calls[0]
    assert "rowid AS _rid" in ref_sql
    assert "rowid < ?" in page_sql


def test_postgres_exists_before_uses_entangled_rowid():
    db = _FakeDb("postgres")
    db.fetchone_rows.extend([
        {"_cf": "2026-05-22T00:00:00.000Z", "_rid": 9},
        {"has_more": True},
    ])
    store = SqlEntityStore(db=db)
    store.register(_stream_def())

    assert store.exists_before("messages", "u1", "m9") is True

    ref_sql, _ = db.fetchone_calls[0]
    exists_sql, _ = db.fetchone_calls[1]
    assert "entangled_rowid AS _rid" in ref_sql
    assert "entangled_rowid < ?" in exists_sql


def test_postgres_cleanup_fallback_order_uses_entangled_rowid():
    db = _FakeDb("postgres")
    store = SqlEntityStore(db=db)
    store.register(_stream_def(default_order=""))

    store.cleanup("messages", "u1", keep_count=10, notify=False)

    sql, params = db.executed[0]
    assert "ORDER BY entangled_rowid DESC LIMIT ?" in sql
    assert params == ("u1", "u1", 10)


def test_unknown_order_field_still_rejected():
    with pytest.raises(SchemaValidationError):
        normalize_order_by(_stream_def(), "not_a_field DESC", extra_fields=["entangled_rowid"])
