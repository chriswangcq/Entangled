import re
import sqlite3
from contextlib import contextmanager

from entangled.sql.migration import (
    build_source_select_sql,
    build_target_insert_sql,
    execute_target_cleanup,
    execute_copy_plan,
    execute_migration_plan,
    plan_migration,
    prepare_target_for_migration,
)
from entangled.sql.migration import MigrationSafetyError


def _fixture_db(tmp_path):
    path = tmp_path / "entangled.sqlite3"
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE agents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            updated_at TEXT
        )
        """
    )
    conn.execute("INSERT INTO agents (name, updated_at) VALUES ('one', '2026-01-01T00:00:00Z')")
    conn.execute("INSERT INTO agents (name, updated_at) VALUES ('two', '2026-01-01T00:00:01Z')")
    conn.execute(
        """
        CREATE TABLE entangled_sync_versions (
            state_key TEXT PRIMARY KEY,
            version INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute("INSERT INTO entangled_sync_versions VALUES ('agents:user:demo', 42)")
    conn.execute(
        """
        CREATE TABLE subagent_state_transitions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subagent_id TEXT NOT NULL,
            agent_id TEXT,
            from_state TEXT NOT NULL,
            to_state TEXT NOT NULL,
            reason TEXT,
            actor TEXT,
            scope_id TEXT,
            metadata_json TEXT,
            created_at INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        INSERT INTO subagent_state_transitions
            (subagent_id, agent_id, from_state, to_state, reason, actor, created_at)
        VALUES ('s1', 'a1', 'sleeping', 'awake', 'wake', 'test', 123)
        """
    )
    conn.commit()
    conn.close()
    return path


class _FakeTargetDb:
    def __init__(self):
        self.rows = {}
        self.executemany_calls = []
        self.executed = []

    @contextmanager
    def transaction(self, lock_type="global"):
        self.executed.append(("transaction", lock_type))
        yield self

    def executemany(self, sql, params_list):
        self.executemany_calls.append((sql, list(params_list)))
        match = re.search(r'INSERT INTO "([^"]+)" \((.*?)\) VALUES', sql)
        assert match is not None
        table = match.group(1)
        columns = re.findall(r'"([^"]+)"', match.group(2))
        self.rows.setdefault(table, [])
        for params in params_list:
            self.rows[table].append(dict(zip(columns, params)))

    def execute(self, sql, params=()):
        self.executed.append((sql, params))

    def fetchone(self, sql, params=()):
        null_match = re.search(r'SELECT COUNT\(\*\) AS count FROM "([^"]+)" WHERE "([^"]+)" IS NULL', sql)
        if null_match:
            table, column = null_match.groups()
            return {"count": sum(1 for row in self.rows.get(table, []) if row.get(column) is None)}

        stats_match = re.search(r'SELECT COUNT\(\*\) AS count, COALESCE\(MAX\("id"\), 0\) AS max_id FROM "([^"]+)"', sql)
        if stats_match:
            table = stats_match.group(1)
            rows = self.rows.get(table, [])
            return {
                "count": len(rows),
                "max_id": max((int(row["id"]) for row in rows), default=0),
            }

        count_match = re.search(r'SELECT COUNT\(\*\) AS count FROM "([^"]+)"', sql)
        if count_match:
            table = count_match.group(1)
            return {"count": len(self.rows.get(table, []))}

        raise AssertionError(f"unexpected fetchone SQL: {sql}")

    def fetchall(self, sql, params=()):
        if 'FROM "entangled_sync_versions"' in sql:
            return sorted(
                self.rows.get("entangled_sync_versions", []),
                key=lambda row: row["state_key"],
            )
        raise AssertionError(f"unexpected fetchall SQL: {sql}")


def test_copy_sql_builders_use_explicit_columns(tmp_path):
    plan = plan_migration(_fixture_db(tmp_path))
    agents = plan.plan_for("agents")

    assert build_source_select_sql(agents) == (
        'SELECT rowid AS "entangled_rowid", "id", "name", "updated_at" '
        'FROM "agents" ORDER BY rowid'
    )
    assert build_target_insert_sql(agents) == (
        'INSERT INTO "agents" ("entangled_rowid", "id", "name", "updated_at") '
        "VALUES (?, ?, ?, ?)"
    )


def test_execute_copy_plan_copies_sqlite_rowid_to_entangled_rowid(tmp_path):
    path = _fixture_db(tmp_path)
    plan = plan_migration(path)
    agents = plan.plan_for("agents")
    target = _FakeTargetDb()

    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    stats = execute_copy_plan(conn, target, agents)
    conn.close()

    assert stats.rows_copied == 2
    assert target.rows["agents"][0] == {
        "entangled_rowid": 1,
        "id": 1,
        "name": "one",
        "updated_at": "2026-01-01T00:00:00Z",
    }
    assert target.rows["agents"][1]["entangled_rowid"] == 2


def test_execute_migration_plan_copies_support_tables_and_resets_sequences(tmp_path):
    path = _fixture_db(tmp_path)
    target = _FakeTargetDb()

    report = execute_migration_plan(
        path,
        target,
        plan=plan_migration(path),
        connection_label="postgresql://user:secret@localhost/db",
    )

    assert target.rows["entangled_sync_versions"] == [
        {"state_key": "agents:user:demo", "version": 42}
    ]
    assert target.rows["subagent_state_transitions"][0]["id"] == 1

    executed_sql = [entry[0] for entry in target.executed if isinstance(entry[0], str)]
    assert 'ALTER TABLE "agents" ALTER COLUMN "entangled_rowid" RESTART WITH 3' in executed_sql
    assert 'ALTER TABLE "agents" ALTER COLUMN "id" RESTART WITH 3' in executed_sql
    assert 'ALTER TABLE "subagent_state_transitions" ALTER COLUMN "id" RESTART WITH 2' in executed_sql

    report_dict = report.to_dict()
    assert "secret" not in repr(report_dict)
    assert report_dict["target_counts"]["agents"] == 2
    assert report_dict["target_counts"]["entangled_sync_versions"] == 1
    assert report_dict["target_counts"]["subagent_state_transitions"] == 1
    assert report_dict["checks"]["target_counts_match"] == "passed"
    assert report_dict["checks"]["sync_versions_match"] == "passed"
    assert report_dict["checks"]["transition_ids_match"] == "passed"
    assert report_dict["checks"]["rowid_copy_complete"] == "passed"


def test_prepare_target_creates_dynamic_and_support_schemas(tmp_path):
    path = _fixture_db(tmp_path)
    plan = plan_migration(path)
    target = _FakeTargetDb()

    result = prepare_target_for_migration(target, plan)

    executed_sql = [entry[0] for entry in target.executed if isinstance(entry[0], str)]
    assert any(
        sql.startswith('CREATE TABLE IF NOT EXISTS agents') and "entangled_rowid" in sql
        for sql in executed_sql
    )
    assert any("CREATE TABLE IF NOT EXISTS entangled_sync_versions" in sql for sql in executed_sql)
    assert any("CREATE TABLE IF NOT EXISTS subagent_state_transitions" in sql for sql in executed_sql)
    assert set(result.prepared_tables) == {
        "agents",
        "entangled_sync_versions",
        "subagent_state_transitions",
    }
    assert result.cleaned_tables == ()


def test_target_cleanup_refuses_without_confirmation(tmp_path):
    plan = plan_migration(_fixture_db(tmp_path))

    try:
        execute_target_cleanup(_FakeTargetDb(), plan)
    except MigrationSafetyError as exc:
        assert "not confirmed" in str(exc)
    else:
        raise AssertionError("target cleanup should require confirmation")


def test_prepare_target_cleans_planned_tables_when_confirmed(tmp_path):
    path = _fixture_db(tmp_path)
    plan = plan_migration(
        path,
        clean_target=True,
        target_confirmation="novaic_entangled_staging",
        expected_target_confirmation="novaic_entangled_staging",
    )
    target = _FakeTargetDb()

    result = prepare_target_for_migration(target, plan)

    executed_sql = [entry[0] for entry in target.executed if isinstance(entry[0], str)]
    assert 'DELETE FROM "agents"' in executed_sql
    assert 'DELETE FROM "entangled_sync_versions"' in executed_sql
    assert 'DELETE FROM "subagent_state_transitions"' in executed_sql
    assert set(result.cleaned_tables) == {
        "agents",
        "entangled_sync_versions",
        "subagent_state_transitions",
    }


def test_execute_migration_plan_report_includes_preparation_evidence(tmp_path):
    path = _fixture_db(tmp_path)
    plan = plan_migration(
        path,
        clean_target=True,
        target_confirmation="novaic_entangled_staging",
        expected_target_confirmation="novaic_entangled_staging",
    )

    report = execute_migration_plan(path, _FakeTargetDb(), plan=plan)
    report_dict = report.to_dict()

    assert set(report_dict["prepared_tables"]) == {
        "agents",
        "entangled_sync_versions",
        "subagent_state_transitions",
    }
    assert set(report_dict["cleaned_tables"]) == {
        "agents",
        "entangled_sync_versions",
        "subagent_state_transitions",
    }
    assert report_dict["schema_statement_count"] >= 3
