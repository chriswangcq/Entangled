import json
import sqlite3

import pytest

from entangled.sql import migration_cli


def _fixture_db(tmp_path):
    path = tmp_path / "entangled.sqlite3"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE agents (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
    conn.execute("INSERT INTO agents (name) VALUES ('one')")
    conn.execute(
        """
        CREATE TABLE entangled_sync_versions (
            state_key TEXT PRIMARY KEY,
            version INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute("INSERT INTO entangled_sync_versions VALUES ('agents:user:demo', 1)")
    conn.commit()
    conn.close()
    return path


def test_cli_dry_run_writes_redacted_report(tmp_path):
    source = _fixture_db(tmp_path)
    report_path = tmp_path / "report.json"

    status = migration_cli.main(
        [
            "--sqlite-path",
            str(source),
            "--report",
            str(report_path),
            "--target-label",
            "novaic_entangled_staging",
            "--postgres-dsn",
            "postgresql://user:supersecret@localhost/db?password=alsosecret",
            "--dry-run",
        ]
    )

    assert status == 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    rendered = repr(report)
    assert "supersecret" not in rendered
    assert "alsosecret" not in rendered
    assert report["connection_label"] == "novaic_entangled_staging"
    assert report["source_counts"]["agents"] == 1
    assert report["checks"]["target_counts_match"] == "pending"


def test_cli_refuses_clean_target_without_matching_confirmation(tmp_path, capsys):
    source = _fixture_db(tmp_path)
    report_path = tmp_path / "report.json"

    status = migration_cli.main(
        [
            "--sqlite-path",
            str(source),
            "--report",
            str(report_path),
            "--target-label",
            "novaic_entangled_staging",
            "--clean-target",
            "--target-confirmation",
            "novaic_entangled",
            "--postgres-dsn",
            "postgresql://user:supersecret@localhost/db",
            "--dry-run",
        ]
    )

    captured = capsys.readouterr()
    assert status == 2
    assert "supersecret" not in captured.err
    assert "confirmation did not match" in captured.err
    assert not report_path.exists()


def test_cli_non_dry_run_uses_dsn_file_without_writing_secret(tmp_path, monkeypatch, capsys):
    source = _fixture_db(tmp_path)
    report_path = tmp_path / "report.json"
    dsn_file = tmp_path / "dsn.txt"
    dsn_file.write_text("postgresql://user:file-secret@localhost/db", encoding="utf-8")
    seen = {}

    class FakePostgres:
        def __init__(self, *, dsn="", dsn_file=None):
            self.dsn = dsn
            self.dsn_file = dsn_file

        def connect(self):
            seen["dsn_file_secret"] = self.dsn_file.read_text(encoding="utf-8")

        def close(self):
            seen["closed"] = True

    def fake_execute(sqlite_path, target, *, plan, connection_label):
        seen["sqlite_path"] = sqlite_path
        seen["target"] = target
        return plan.to_report(connection_label=connection_label)

    monkeypatch.setattr(migration_cli, "PostgresDatabase", FakePostgres)
    monkeypatch.setattr(migration_cli, "execute_migration_plan", fake_execute)

    status = migration_cli.main(
        [
            "--sqlite-path",
            str(source),
            "--report",
            str(report_path),
            "--target-label",
            "novaic_entangled_staging",
            "--postgres-dsn-file",
            str(dsn_file),
        ]
    )

    captured = capsys.readouterr()
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert status == 0
    assert seen["dsn_file_secret"] == "postgresql://user:file-secret@localhost/db"
    assert seen["closed"] is True
    assert "file-secret" not in repr(report)
    assert "file-secret" not in captured.out
    assert "file-secret" not in captured.err


def test_cli_non_dry_run_requires_dsn(tmp_path):
    source = _fixture_db(tmp_path)

    with pytest.raises(SystemExit):
        migration_cli.main(
            [
                "--sqlite-path",
                str(source),
                "--report",
                str(tmp_path / "report.json"),
                "--target-label",
                "novaic_entangled_staging",
            ]
        )
