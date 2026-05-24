from pathlib import Path

import pytest

from entangled.app import state
from entangled.sql.database import PostgresDatabase, create_database


class _FakeCursor:
    rowcount = 1

    def __init__(self, rows=None):
        self._rows = rows or []

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def executemany(self, _sql, _params):
        return None


class _FakeConnection:
    autocommit = False

    def __init__(self):
        self.commands = []
        self.commits = 0
        self.rollbacks = 0

    def execute(self, sql, params=()):
        self.commands.append((sql, params))
        return _FakeCursor([{"id": 7}])

    def cursor(self):
        return self

    def executemany(self, sql, params_list):
        self.commands.append((sql, list(params_list)))
        return self

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        return None


class _FakePool:
    def __init__(self):
        self.conn = _FakeConnection()
        self.closed = False
        self.returned = []

    def getconn(self):
        return self.conn

    def putconn(self, conn):
        self.returned.append(conn)

    def close(self):
        self.closed = True


def test_create_database_returns_postgres_boundary():
    postgres_db = create_database(postgres_dsn="dbname=test")
    assert isinstance(postgres_db, PostgresDatabase)


def test_postgres_connect_requires_dsn():
    db = PostgresDatabase()
    with pytest.raises(ValueError, match="requires dsn"):
        db.connect()


def test_postgres_dsn_file_and_transaction_boundary(tmp_path: Path):
    dsn_file = tmp_path / "dsn"
    dsn_file.write_text("dbname=entangled_test", encoding="utf-8")
    fake_pool = _FakePool()
    captured = {}

    def pool_factory(dsn, min_size, max_size):
        captured["dsn"] = dsn
        captured["sizes"] = (min_size, max_size)
        return fake_pool

    db = PostgresDatabase(dsn_file=dsn_file, pool_factory=pool_factory)
    db.connect()

    with db.transaction("agent", resource_id="agent-1", timeout=0.5):
        db.execute("INSERT INTO widgets(name, literal) VALUES (?, '?')", ("n1",))

    assert captured["dsn"] == "dbname=entangled_test"
    assert captured["sizes"] == (1, 10)
    assert ("BEGIN", ()) in fake_pool.conn.commands
    assert any(command[0] == "SELECT pg_advisory_xact_lock(%s)" for command in fake_pool.conn.commands)
    assert any("VALUES (%s, '?')" in command[0] for command in fake_pool.conn.commands)
    assert fake_pool.conn.commits == 1
    assert fake_pool.returned == [fake_pool.conn]

    db.close()
    assert fake_pool.closed is True
    assert fake_pool.returned == [fake_pool.conn]


def test_postgres_non_transaction_fetch_releases_connection(tmp_path: Path):
    dsn_file = tmp_path / "dsn"
    dsn_file.write_text("dbname=entangled_test", encoding="utf-8")
    fake_pool = _FakePool()
    db = PostgresDatabase(dsn_file=dsn_file, pool_factory=lambda *_args, **_kwargs: fake_pool)
    db.connect()

    rows = db.fetchall("SELECT id FROM widgets WHERE name = ?", ("alpha",))

    assert rows == [{"id": 7}]
    assert fake_pool.conn.rollbacks == 1
    assert fake_pool.returned == [fake_pool.conn]


def test_postgres_convert_placeholders_escapes_literal_percent_for_psycopg():
    sql = "CREATE TABLE blobs (locator text CHECK(locator LIKE 'blob://%'), owner text DEFAULT '%')"

    converted = PostgresDatabase._convert_placeholders(sql)

    assert converted == "CREATE TABLE blobs (locator text CHECK(locator LIKE 'blob://%%'), owner text DEFAULT '%%')"


def test_postgres_convert_placeholders_preserves_parameter_conversion():
    sql = "INSERT INTO blobs(locator, label, query) VALUES (?, '?', \"%?\")"

    converted = PostgresDatabase._convert_placeholders(sql)

    assert converted == "INSERT INTO blobs(locator, label, query) VALUES (%s, '?', \"%%?\")"


def test_postgres_transaction_rolls_back_on_exception(tmp_path: Path):
    dsn_file = tmp_path / "dsn"
    dsn_file.write_text("dbname=entangled_test", encoding="utf-8")
    fake_pool = _FakePool()
    db = PostgresDatabase(dsn_file=dsn_file, pool_factory=lambda *_args, **_kwargs: fake_pool)
    db.connect()

    with pytest.raises(RuntimeError):
        with db.transaction("global"):
            raise RuntimeError("boom")

    assert fake_pool.conn.rollbacks == 1


def test_state_postgres_misconfig_does_not_poison_singleton(tmp_path: Path):
    state.close_database()
    with pytest.raises(ValueError):
        state.init_database()

    dsn_file = tmp_path / "dsn"
    dsn_file.write_text("dbname=entangled_test", encoding="utf-8")
    fake_pool = _FakePool()
    db = PostgresDatabase(dsn_file=dsn_file, pool_factory=lambda *_args, **_kwargs: fake_pool)
    db.connect()
    try:
        assert isinstance(db, PostgresDatabase)
    finally:
        db.close()
