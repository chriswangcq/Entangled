import sqlite3

import pytest

from entangled.sql.migration import (
    MigrationSafetyError,
    confirm_target_cleanup,
    inspect_sqlite_source,
    plan_migration,
    redact_secret,
)


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
    conn.execute("CREATE TABLE _scratch (id INTEGER PRIMARY KEY, note TEXT)")
    conn.commit()
    conn.close()
    return path


def test_inspect_sqlite_source_opens_existing_db_readonly(tmp_path):
    path = _fixture_db(tmp_path)

    tables = inspect_sqlite_source(path)

    agents = next(t for t in tables if t.name == "agents")
    assert agents.row_count == 2
    assert agents.max_rowid == 2
    assert agents.column_names == ("id", "name", "updated_at")


def test_inspect_sqlite_source_does_not_create_missing_db(tmp_path):
    missing = tmp_path / "missing.sqlite3"

    with pytest.raises(sqlite3.OperationalError):
        inspect_sqlite_source(missing)

    assert not missing.exists()


def test_plan_classifies_tables_and_marks_rowid_copy(tmp_path):
    plan = plan_migration(_fixture_db(tmp_path))

    agents = plan.plan_for("agents")
    assert agents is not None
    assert agents.kind == "dynamic"
    assert agents.copy_rowid_to_entangled_rowid is True
    assert agents.source_columns == ("rowid", "id", "name", "updated_at")
    assert agents.target_columns == ("entangled_rowid", "id", "name", "updated_at")

    sync_versions = plan.plan_for("entangled_sync_versions")
    assert sync_versions is not None
    assert sync_versions.kind == "sync_versions"
    assert sync_versions.copy_rowid_to_entangled_rowid is False

    transitions = plan.plan_for("subagent_state_transitions")
    assert transitions is not None
    assert transitions.kind == "transitions"
    assert transitions.copy_rowid_to_entangled_rowid is False

    skipped = {s.table: s.reason for s in plan.skipped_tables}
    assert "_scratch" in skipped
    assert "sqlite_sequence" in skipped


def test_plan_includes_sequence_resets(tmp_path):
    plan = plan_migration(_fixture_db(tmp_path))

    resets = {(r.table, r.column): r for r in plan.sequence_resets}
    assert resets[("agents", "entangled_rowid")].restart_with == 3
    assert resets[("agents", "id")].restart_with == 3
    assert resets[("subagent_state_transitions", "id")].restart_with == 2


def test_target_cleanup_requires_explicit_confirmation():
    assert (
        confirm_target_cleanup(
            clean_target=False,
            confirmation="",
            expected_confirmation="novaic_entangled_staging",
        )
        is False
    )

    with pytest.raises(MigrationSafetyError):
        confirm_target_cleanup(
            clean_target=True,
            confirmation="novaic_entangled",
            expected_confirmation="novaic_entangled_staging",
        )

    assert (
        confirm_target_cleanup(
            clean_target=True,
            confirmation="novaic_entangled_staging",
            expected_confirmation="novaic_entangled_staging",
        )
        is True
    )


def test_report_redacts_connection_label_and_has_pending_checks(tmp_path):
    plan = plan_migration(
        _fixture_db(tmp_path),
        clean_target=True,
        target_confirmation="novaic_entangled_staging",
        expected_target_confirmation="novaic_entangled_staging",
    )

    report = plan.to_report(
        connection_label="postgresql://novaic:supersecret@127.0.0.1/db?password=alsosecret"
    ).to_dict()

    rendered = repr(report)
    assert "supersecret" not in rendered
    assert "alsosecret" not in rendered
    assert report["connection_label"] == "postgresql://novaic:***@127.0.0.1/db?password=***"
    assert report["clean_target_allowed"] is True
    assert report["source_counts"]["agents"] == 2
    assert report["target_counts"]["agents"] is None
    assert report["checks"]["sync_versions_match"] == "pending"


def test_redact_secret_handles_key_value_tokens():
    value = redact_secret("host=localhost password=open-sesame token=abc123")

    assert "open-sesame" not in value
    assert "abc123" not in value
    assert "password=***" in value
    assert "token=***" in value
