import json
import re
import sqlite3
from contextlib import contextmanager

from entangled.sql.field_def import FieldDef, FieldKind
from entangled.sql.migration import execute_migration_plan, plan_migration


def _semantic_fixture_db(tmp_path):
    path = tmp_path / "semantic.sqlite3"
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE semantic_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload_json TEXT,
            is_enabled INTEGER,
            blob_data BLOB,
            item_count INTEGER,
            score REAL,
            created_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO semantic_events
            (payload_json, is_enabled, blob_data, item_count, score, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            json.dumps({"kind": "fixture", "n": 1}),
            1,
            b"\x00\x01fixture",
            7,
            3.5,
            "2026-05-22T12:00:00Z",
        ),
    )
    conn.execute(
        """
        CREATE TABLE entangled_sync_versions (
            state_key TEXT PRIMARY KEY,
            version INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute("INSERT INTO entangled_sync_versions VALUES ('semantic-events:user:fixture', 9)")
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
        VALUES ('s1', 'a1', 'sleeping', 'awake', 'fixture', 'test', 123456)
        """
    )
    conn.commit()
    conn.close()
    return path


class _ValidationTargetDb:
    def __init__(self):
        self.rows = {}
        self.executed = []

    @contextmanager
    def transaction(self, lock_type="global"):
        self.executed.append(("transaction", lock_type))
        yield self

    def execute(self, sql, params=()):
        self.executed.append((sql, params))

    def executemany(self, sql, params_list):
        self.executed.append((sql, list(params_list)))
        match = re.search(r'INSERT INTO "([^"]+)" \((.*?)\) VALUES', sql)
        assert match is not None
        table = match.group(1)
        columns = re.findall(r'"([^"]+)"', match.group(2))
        self.rows.setdefault(table, [])
        for params in params_list:
            self.rows[table].append(dict(zip(columns, params)))

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
            return {"count": len(self.rows.get(count_match.group(1), []))}

        raise AssertionError(f"unexpected fetchone SQL: {sql}")

    def fetchall(self, sql, params=()):
        if 'FROM "entangled_sync_versions"' in sql:
            return sorted(
                self.rows.get("entangled_sync_versions", []),
                key=lambda row: row["state_key"],
            )
        raise AssertionError(f"unexpected fetchall SQL: {sql}")


def test_fixture_backed_migration_semantics_cover_value_shapes(tmp_path):
    path = _semantic_fixture_db(tmp_path)
    target = _ValidationTargetDb()

    report = execute_migration_plan(
        path,
        target,
        plan=plan_migration(
            path,
            clean_target=True,
            target_confirmation="fixture-target",
            expected_target_confirmation="fixture-target",
        ),
        connection_label="fixture-target",
    ).to_dict()

    migrated = target.rows["semantic_events"][0]
    assert migrated["entangled_rowid"] == 1
    assert FieldDef("payload_json", FieldKind.JSON).deserialize(migrated["payload_json"]) == {
        "kind": "fixture",
        "n": 1,
    }
    assert FieldDef("is_enabled", FieldKind.BOOL).deserialize(migrated["is_enabled"]) is True
    assert migrated["blob_data"] == b"\x00\x01fixture"
    assert migrated["item_count"] == 7
    assert migrated["score"] == 3.5
    assert migrated["created_at"] == "2026-05-22T12:00:00Z"
    assert report["target_counts"]["semantic_events"] == 1
    assert report["checks"]["sync_versions_match"] == "passed"
    assert report["checks"]["transition_ids_match"] == "passed"
    assert report["checks"]["rowid_copy_complete"] == "passed"
    assert report["cleaned_tables"] == [
        "entangled_sync_versions",
        "semantic_events",
        "subagent_state_transitions",
    ]
