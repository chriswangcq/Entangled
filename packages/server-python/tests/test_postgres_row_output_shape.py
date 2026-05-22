from entangled.sql.entity_def import SqlEntityDef
from entangled.sql.entity_store import SqlEntityStore
from entangled.sql.field_def import F


class _FakePostgresDb:
    backend_name = "postgres"

    def __init__(self, rows):
        self.rows = rows
        self.fetchall_calls = []

    def fetchall(self, sql, params=()):
        self.fetchall_calls.append((sql, params))
        return list(self.rows)


def _shape_def() -> SqlEntityDef:
    return SqlEntityDef(
        name="widgets",
        table="widgets",
        id_field="id",
        user_scoped=True,
        fields=[
            F.text("id", primary=True),
            F.text("user_id", nullable=False),
            F.json("payload"),
            F.bool_("enabled"),
            F.timestamp("created_at", auto=False),
            F.text("secret", hidden=True),
            F.bool_("has_secret"),
        ],
        default_order="created_at DESC",
    )


def test_out_preserves_native_postgres_json_bool_and_timestamp_shape():
    store = SqlEntityStore(db=_FakePostgresDb([]))
    defn = _shape_def()

    out = store._out(
        defn,
        {
            "id": "w1",
            "user_id": "u1",
            "payload": {"items": [1, 2]},
            "enabled": True,
            "created_at": "2026-05-22T00:00:00.000Z",
            "secret": "value",
            "has_secret": False,
        },
    )

    assert out["payload"] == {"items": [1, 2]}
    assert out["enabled"] is True
    assert out["created_at"] == "2026-05-22T00:00:00.000Z"
    assert "secret" not in out
    assert out["has_secret"] is True


def test_out_still_decodes_legacy_json_strings_and_integer_bools():
    store = SqlEntityStore(db=_FakePostgresDb([]))
    defn = _shape_def()

    out = store._out(
        defn,
        {
            "id": "w1",
            "user_id": "u1",
            "payload": "{\"ok\": true}",
            "enabled": 1,
            "created_at": "2026-05-22T00:00:00.000Z",
            "has_secret": 0,
        },
    )

    assert out["payload"] == {"ok": True}
    assert out["enabled"] is True
    assert out["has_secret"] is False


def test_in_sets_hidden_marker_and_serializes_json_bool_inputs():
    store = SqlEntityStore(db=_FakePostgresDb([]))
    row = store._in(
        _shape_def(),
        {
            "id": "w1",
            "payload": {"ok": True},
            "enabled": False,
            "secret": "value",
        },
    )

    assert row["payload"] == "{\"ok\": true}"
    assert row["enabled"] == 0
    assert row["has_secret"] == 1


def test_list_applies_output_shape_to_fake_postgres_rows():
    row = {
        "id": "w1",
        "user_id": "u1",
        "payload": {"ok": True},
        "enabled": False,
        "created_at": "2026-05-22T00:00:00.000Z",
        "secret": "value",
        "has_secret": False,
    }
    db = _FakePostgresDb([row])
    store = SqlEntityStore(db=db)
    store.register(_shape_def())

    rows = store.list("widgets", "u1")

    assert rows == [
        {
            "id": "w1",
            "user_id": "u1",
            "payload": {"ok": True},
            "enabled": False,
            "created_at": "2026-05-22T00:00:00.000Z",
            "has_secret": True,
        }
    ]
    assert "ORDER BY created_at DESC" in db.fetchall_calls[0][0]
