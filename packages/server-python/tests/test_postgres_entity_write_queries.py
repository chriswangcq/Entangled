from contextlib import contextmanager

import pytest
from fastapi import HTTPException

from entangled.app import crud
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

    def __init__(self, *, rowcount=1, owned_parent_users=None, fetchall_rows=None):
        self.executed = []
        self.returning = []
        self.rowcount = rowcount
        self.owned_parent_users = set(owned_parent_users or [])
        self.fetchall_rows = list(fetchall_rows or [])

    @contextmanager
    def transaction(self, lock_type="global", resource_id="", timeout=None):
        yield self

    def execute(self, sql, params=()):
        self.executed.append((sql, params))
        return _Cursor(rowcount=self.rowcount)

    def fetchone(self, sql, params=()):
        self.executed.append((sql, params))
        if " AS owned" in sql and any(
            value in self.owned_parent_users for value in params
        ):
            return {"owned": 1}
        return None

    def fetchall(self, sql, params=()):
        self.executed.append((sql, params))
        return list(self.fetchall_rows)

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


def _shaped_widget_def() -> SqlEntityDef:
    return SqlEntityDef(
        name="shaped-widgets",
        table="shaped_widgets",
        id_field="id",
        user_scoped=True,
        fields=[
            F.text("id", primary=True),
            F.text("user_id", nullable=False, index=True),
            F.json("payload"),
            F.text("secret", hidden=True),
            F.bool_("has_secret"),
        ],
    )


def _user_preferences_def() -> SqlEntityDef:
    return SqlEntityDef(
        name="user-preferences",
        table="user_preferences",
        id_field="user_id",
        user_scoped=True,
        fields=[
            F.text("user_id", primary=True),
            F.text("theme"),
        ],
    )


def _project_def() -> SqlEntityDef:
    return SqlEntityDef(
        name="projects",
        table="projects",
        id_field="id",
        user_scoped=True,
        fields=[
            F.text("id", primary=True),
            F.text("user_id", nullable=False, index=True),
            F.text("name", nullable=False),
        ],
    )


def _task_def(*, sync_type="list") -> SqlEntityDef:
    return SqlEntityDef(
        name="tasks",
        table="tasks",
        id_field="id",
        user_scoped=False,
        parent=("projects", "project_id", "id"),
        key_params=["project_id"],
        sync_type=sync_type,
        fields=[
            F.text("id", primary=True),
            F.text("project_id", nullable=False, index=True),
            F.text("name", nullable=False),
        ],
    )


def _api_key_def() -> SqlEntityDef:
    return SqlEntityDef(
        name="api-keys",
        table="api_keys",
        id_field="id",
        user_scoped=True,
        fields=[
            F.text("id", primary=True),
            F.text("user_id", nullable=False, index=True),
        ],
    )


def _model_def() -> SqlEntityDef:
    return SqlEntityDef(
        name="models",
        table="models",
        id_field="model_id",
        user_scoped=True,
        parent=("api-keys", "api_key_id", "id"),
        fields=[
            F.text("model_id", primary=True),
            F.text("user_id", nullable=False, index=True),
            F.text("api_key_id", nullable=False, index=True),
            F.text("name", nullable=False),
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


def test_postgres_update_does_not_duplicate_explicit_updated_at():
    db = _FakePostgresDb()
    store = SqlEntityStore(db=db)

    store._sql_update(
        _widget_def(),
        "u1",
        "w1",
        {"name": "beta", "updated_at": "2026-05-25T00:00:00.000Z"},
    )

    update_sql, params = db.executed[0]
    assert update_sql.startswith("UPDATE widgets SET name = ?, updated_at = ?")
    assert "to_char(timezone('UTC', now())" not in update_sql
    assert update_sql.count("updated_at =") == 1
    assert params[:2] == ("beta", "2026-05-25T00:00:00.000Z")


def test_postgres_upsert_uses_postgres_timestamp_expression():
    db = _FakePostgresDb()
    store = SqlEntityStore(db=db)

    store._sql_upsert(_widget_def(), "u1", "w1", {"name": "gamma"})

    upsert_sql = db.executed[0][0]
    assert "ON CONFLICT(id) DO UPDATE SET" in upsert_sql
    assert "excluded.name" in upsert_sql
    assert "datetime('now')" not in upsert_sql
    assert "to_char(timezone('UTC', now())" in upsert_sql
    assert "WHERE target.user_id = excluded.user_id" in upsert_sql


def test_user_scoped_upsert_rejects_conflict_owned_by_another_user():
    db = _FakePostgresDb(rowcount=0)
    store = SqlEntityStore(db=db)

    with pytest.raises(PermissionError, match="ownership"):
        store._sql_upsert(_widget_def(), "attacker", "victim-id", {"name": "stolen"})


@pytest.mark.parametrize("operation", ["create", "append", "upsert"])
def test_parent_scoped_writes_require_owned_parent(operation):
    db = _FakePostgresDb(owned_parent_users=[])
    store = SqlEntityStore(db=db)
    store.register(_project_def())
    task_def = _task_def(sync_type="stream" if operation == "append" else "list")
    store.register(task_def)

    with pytest.raises(PermissionError, match="parent"):
        if operation == "create":
            store._sql_create(
                task_def,
                "attacker",
                {"id": "task-1", "name": "bad"},
                params={"project_id": "victim-project"},
            )
        elif operation == "append":
            store.append(
                "tasks",
                "attacker",
                {"id": "task-1", "name": "bad"},
                params={"project_id": "victim-project"},
                notify=False,
            )
        else:
            store._sql_upsert(
                task_def,
                "attacker",
                "task-1",
                {"name": "bad"},
                params={"project_id": "victim-project"},
            )

    assert not any(sql.startswith("INSERT INTO tasks") for sql, _ in db.executed)


def test_parent_scoped_upsert_has_atomic_existing_owner_guard():
    db = _FakePostgresDb(owned_parent_users=["owner"])
    store = SqlEntityStore(db=db)
    store.register(_project_def())
    task_def = _task_def()
    store.register(task_def)

    store._sql_upsert(
        task_def,
        "owner",
        "task-1",
        {"name": "allowed"},
        params={"project_id": "project-1"},
    )

    upsert_sql, upsert_params = next(
        (sql, params)
        for sql, params in db.executed
        if sql.startswith("INSERT INTO tasks")
    )
    assert "WHERE target.project_id IN (SELECT id FROM projects WHERE user_id = ?)" in upsert_sql
    assert upsert_params[-1] == "owner"


def test_global_upsert_remains_shared_and_has_no_owner_guard():
    db = _FakePostgresDb()
    store = SqlEntityStore(db=db)

    store._sql_upsert(_auto_def(), "service-user", "7", {"name": "global"})

    upsert_sql, _ = db.executed[0]
    assert " AS target" not in upsert_sql
    assert " WHERE " not in upsert_sql


def test_quarantined_tenant_migration_is_one_atomic_parent_verified_update():
    db = _FakePostgresDb(
        fetchall_rows=[{"entity_id": "model-1", "user_id": "owner"}]
    )
    store = SqlEntityStore(db=db)
    store.register(_api_key_def())
    store.register(_model_def())

    result = store.migrate_quarantined_tenant_ownership(
        "models",
        "__legacy_unowned__",
        [
            {
                "entity_id": "model-1",
                "user_id": "owner",
                "parent_id": "owner-key",
            },
            {
                "entity_id": "model-2",
                "user_id": "other-owner",
                "parent_id": "other-key",
            },
        ],
        emit_notifications=False,
    )

    assert result == {
        "completed_ids": ["model-1"],
        "completed": 1,
        "skipped": 1,
    }
    assert len(db.executed) == 1
    sql, params = db.executed[0]
    assert "WITH requested" in sql
    assert "JOIN api_keys AS parent" in sql
    assert "parent.user_id = requested.target_user_id" in sql
    assert "UPDATE models AS target" in sql
    assert "target.user_id = ?" in sql
    assert params == (
        "model-1",
        "owner",
        "owner-key",
        "model-2",
        "other-owner",
        "other-key",
        "__legacy_unowned__",
    )


@pytest.mark.parametrize(
    ("source_user_id", "items", "message"),
    [
        (
            "victim-user",
            [{"entity_id": "m1", "user_id": "owner", "parent_id": "key"}],
            "reserved quarantine",
        ),
        (
            "__legacy_unowned__",
            [
                {"entity_id": "m1", "user_id": "owner", "parent_id": "key"},
                {"entity_id": "m1", "user_id": "owner", "parent_id": "key"},
            ],
            "duplicate",
        ),
        (
            "__legacy_unowned__",
            [
                {
                    "entity_id": "m1",
                    "user_id": "__legacy_unowned_attacker__",
                    "parent_id": "key",
                }
            ],
            "quarantine tenant",
        ),
    ],
)
def test_quarantined_tenant_migration_rejects_owner_transfer_expansion(
    source_user_id,
    items,
    message,
):
    db = _FakePostgresDb()
    store = SqlEntityStore(db=db)
    store.register(_api_key_def())
    store.register(_model_def())

    with pytest.raises((PermissionError, ValueError), match=message):
        store.migrate_quarantined_tenant_ownership(
            "models",
            source_user_id,
            items,
            emit_notifications=False,
        )

    assert db.executed == []


@pytest.mark.parametrize(
    "operation",
    ["create", "append", "upsert", "update", "batch", "update_where", "cas"],
)
def test_parent_ownership_denial_is_exposed_as_http_403(monkeypatch, operation):
    class _DenyStore:
        def create(self, *args, **kwargs):
            raise PermissionError("parent ownership denied")

        def append(self, *args, **kwargs):
            raise PermissionError("parent ownership denied")

        def upsert(self, *args, **kwargs):
            raise PermissionError("ownership conflict")

        def update(self, *args, **kwargs):
            raise PermissionError("parent ownership denied")

        def batch_update(self, *args, **kwargs):
            raise PermissionError("parent ownership denied")

        def update_where(self, *args, **kwargs):
            raise PermissionError("parent ownership denied")

        def cas_update(self, *args, **kwargs):
            raise PermissionError("parent ownership denied")

    monkeypatch.setattr(crud, "get_store", lambda: _DenyStore())

    with pytest.raises(HTTPException) as exc:
        if operation == "create":
            crud.create_entity("tasks", {}, "attacker", None, None, False)
        elif operation == "append":
            crud.append_entity("tasks", {}, "attacker", None, False)
        elif operation == "upsert":
            crud.upsert_entity("tasks", "task-1", {}, "attacker", None, False)
        elif operation == "update":
            crud.update_entity("tasks", "task-1", {}, "attacker", None, False)
        elif operation == "batch":
            crud.batch_update_entities(
                "tasks",
                crud.BatchUpdateBody(ids=["task-1"], data={}),
                "attacker",
                None,
                False,
            )
        elif operation == "update_where":
            crud.update_where_entity(
                "tasks",
                crud.UpdateWhereBody(data={}),
                "attacker",
                None,
                False,
            )
        else:
            crud.cas_update_entity(
                "tasks",
                crud.CasUpdateBody(where={"id": "task-1"}, data={}),
                "attacker",
                None,
                False,
            )

    assert exc.value.status_code == 403


@pytest.mark.parametrize("operation", ["update", "batch", "update_where", "cas"])
def test_update_paths_never_mutate_direct_user_owner(operation):
    db = _FakePostgresDb()
    store = SqlEntityStore(db=db)
    widget_def = _widget_def()
    store.register(widget_def)
    data = {"id": "victim-id", "name": "safe", "user_id": "victim"}

    if operation == "update":
        store._sql_update(widget_def, "owner", "w1", data)
    elif operation == "batch":
        store.batch_update(
            "widgets", "owner", ["w1"], data, emit_notifications=False
        )
    elif operation == "update_where":
        store.update_where("widgets", "owner", data, notify=False)
    else:
        store.cas_update(
            "widgets",
            "owner",
            {"id": "w1"},
            data,
            emit_notifications=False,
        )

    update_sql = next(sql for sql, _ in db.executed if sql.startswith("UPDATE widgets"))
    set_clause = update_sql.split(" WHERE ", 1)[0]
    assert "id =" not in set_clause
    assert "user_id =" not in set_clause


def test_cas_with_owner_as_primary_key_uses_authenticated_owner_for_delta():
    db = _FakePostgresDb()
    store = SqlEntityStore(db=db)
    store.register(_user_preferences_def())
    notifications = []
    store._notify_change = lambda *args, **kwargs: notifications.append((args, kwargs))

    store.cas_update(
        "user-preferences",
        "owner",
        {"user_id": "owner"},
        {"user_id": "victim", "theme": "dark"},
    )

    update_sql = next(sql for sql, _ in db.executed if sql.startswith("UPDATE user_preferences"))
    assert "SET user_id =" not in update_sql
    assert len(notifications) == 1
    notify_args, notify_kwargs = notifications[0]
    assert notify_args[:3] == ("user-preferences", "updated", "owner")
    assert notify_kwargs["entity_id"] == "owner"
    assert notify_kwargs["data"] == {"theme": "dark"}


@pytest.mark.parametrize("operation", ["batch", "update_where"])
def test_bulk_update_notifications_use_public_api_shape(operation):
    db = _FakePostgresDb()
    store = SqlEntityStore(db=db)
    store.register(_shaped_widget_def())
    notifications = []
    store._notify_change = lambda *args, **kwargs: notifications.append((args, kwargs))
    data = {
        "payload": {"ok": True},
        "secret": "do-not-push",
        "user_id": "victim",
    }

    if operation == "batch":
        store.batch_update("shaped-widgets", "owner", ["w1"], data)
    else:
        store.update_where("shaped-widgets", "owner", data)

    assert len(notifications) == 1
    notify_data = notifications[0][1]["data"]
    assert notify_data["payload"] == {"ok": True}
    assert notify_data["has_secret"] is True
    assert "secret" not in notify_data
    assert "user_id" not in notify_data


@pytest.mark.parametrize("operation", ["update", "batch", "update_where", "cas"])
def test_parent_scoped_update_paths_reject_foreign_parent_move(operation):
    db = _FakePostgresDb(owned_parent_users=[])
    store = SqlEntityStore(db=db)
    store.register(_project_def())
    task_def = _task_def()
    store.register(task_def)
    data = {"project_id": "victim-project", "name": "bad"}

    with pytest.raises(PermissionError, match="parent"):
        if operation == "update":
            store._sql_update(task_def, "attacker", "task-1", data)
        elif operation == "batch":
            store.batch_update(
                "tasks", "attacker", ["task-1"], data, emit_notifications=False
            )
        elif operation == "update_where":
            store.update_where("tasks", "attacker", data, notify=False)
        else:
            store.cas_update(
                "tasks",
                "attacker",
                {"id": "task-1"},
                data,
                emit_notifications=False,
            )

    assert not any(sql.startswith("UPDATE tasks") for sql, _ in db.executed)


@pytest.mark.parametrize("operation", ["update", "batch", "update_where", "cas"])
def test_parent_scoped_update_paths_allow_owned_parent_move(operation):
    db = _FakePostgresDb(owned_parent_users=["owner"])
    store = SqlEntityStore(db=db)
    store.register(_project_def())
    task_def = _task_def()
    store.register(task_def)
    data = {"project_id": "owned-project", "name": "safe"}

    if operation == "update":
        store._sql_update(task_def, "owner", "task-1", data)
    elif operation == "batch":
        store.batch_update(
            "tasks", "owner", ["task-1"], data, emit_notifications=False
        )
    elif operation == "update_where":
        store.update_where("tasks", "owner", data, notify=False)
    else:
        store.cas_update(
            "tasks",
            "owner",
            {"id": "task-1"},
            data,
            emit_notifications=False,
        )

    assert any(sql.startswith("UPDATE tasks") for sql, _ in db.executed)


def test_recursive_parent_scope_reaches_direct_user_owner():
    organizations = SqlEntityDef(
        name="organizations",
        table="organizations",
        id_field="id",
        user_scoped=True,
        fields=[
            F.text("id", primary=True),
            F.text("user_id", nullable=False),
        ],
    )
    projects = SqlEntityDef(
        name="nested-projects",
        table="nested_projects",
        id_field="id",
        user_scoped=False,
        parent=("organizations", "organization_id", "id"),
        fields=[
            F.text("id", primary=True),
            F.text("organization_id", nullable=False),
        ],
    )
    tasks = SqlEntityDef(
        name="nested-tasks",
        table="nested_tasks",
        id_field="id",
        user_scoped=False,
        parent=("nested-projects", "project_id", "id"),
        fields=[
            F.text("id", primary=True),
            F.text("project_id", nullable=False),
        ],
    )
    store = SqlEntityStore(db=_FakePostgresDb())
    for defn in (organizations, projects, tasks):
        store.register(defn)

    where, values = store._scope_where(tasks, "owner", None)

    assert where == (
        "project_id IN (SELECT id FROM nested_projects WHERE organization_id IN "
        "(SELECT id FROM organizations WHERE user_id = ?))"
    )
    assert values == ["owner"]


def test_postgres_upsert_does_not_duplicate_explicit_updated_at():
    db = _FakePostgresDb()
    store = SqlEntityStore(db=db)

    store._sql_upsert(
        _widget_def(),
        "u1",
        "w1",
        {"name": "gamma", "updated_at": "2026-05-25T00:00:00.000Z"},
    )

    upsert_sql, params = db.executed[0]
    assert "updated_at = excluded.updated_at" in upsert_sql
    assert "to_char(timezone('UTC', now())" not in upsert_sql
    assert upsert_sql.count("updated_at =") == 1
    assert params[1] == "2026-05-25T00:00:00.000Z"


def test_postgres_delete_and_cas_preserve_rowcount_paths():
    db = _FakePostgresDb()
    store = SqlEntityStore(db=db)
    store.register(_widget_def())

    assert store._sql_delete(_widget_def(), "u1", "w1") is True
    cas = store.cas_update("widgets", "u1", {"id": "w1"}, {"name": "delta"}, emit_notifications=False)

    assert cas is None
    assert any(sql.startswith("DELETE FROM widgets") for sql, _params in db.executed)
    assert any(sql.startswith("UPDATE widgets SET name = ?") for sql, _params in db.executed)


def test_postgres_batch_update_does_not_duplicate_explicit_updated_at():
    db = _FakePostgresDb()
    store = SqlEntityStore(db=db)
    store.register(_widget_def())

    store.batch_update(
        "widgets",
        "u1",
        ["w1", "w2"],
        {"name": "delta", "updated_at": "2026-05-25T00:00:00.000Z"},
        emit_notifications=False,
    )

    update_sql, params = db.executed[0]
    assert update_sql.startswith("UPDATE widgets SET name = ?, updated_at = ?")
    assert "to_char(timezone('UTC', now())" not in update_sql
    assert update_sql.count("updated_at =") == 1
    assert params[:2] == ("delta", "2026-05-25T00:00:00.000Z")


def test_postgres_update_where_does_not_duplicate_explicit_updated_at():
    db = _FakePostgresDb()
    store = SqlEntityStore(db=db)
    store.register(_widget_def())

    store.update_where(
        "widgets",
        "u1",
        {"name": "epsilon", "updated_at": "2026-05-25T00:00:00.000Z"},
        notify=False,
    )

    update_sql, params = db.executed[0]
    assert update_sql.startswith("UPDATE widgets SET name = ?, updated_at = ?")
    assert "to_char(timezone('UTC', now())" not in update_sql
    assert update_sql.count("updated_at =") == 1
    assert params[:2] == ("epsilon", "2026-05-25T00:00:00.000Z")


def test_postgres_cas_update_does_not_duplicate_explicit_updated_at():
    db = _FakePostgresDb()
    store = SqlEntityStore(db=db)
    store.register(_widget_def())

    store.cas_update(
        "widgets",
        "u1",
        {"id": "w1"},
        {"name": "zeta", "updated_at": "2026-05-25T00:00:00.000Z"},
        emit_notifications=False,
    )

    update_sql, params = db.executed[0]
    assert update_sql.startswith("UPDATE widgets SET name = ?, updated_at = ?")
    assert "to_char(timezone('UTC', now())" not in update_sql
    assert update_sql.count("updated_at =") == 1
    assert params[:2] == ("zeta", "2026-05-25T00:00:00.000Z")


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
