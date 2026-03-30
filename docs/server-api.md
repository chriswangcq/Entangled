# Server API Reference

Complete reference for the `entangled-server` Python package.

## Installation

```bash
pip install entangled-server
# or: add to requirements.txt / pyproject.toml
```

**Dependencies**: Python 3.10+, `starlette` (optional, for `create_ws_handler`)

---

## Module: `entangled.server.defs`

### `EntityDef`

The core declaration that defines an entity in the Entangled system.

```python
from entangled.server import EntityDef

@dataclass(kw_only=True)
class EntityDef:
    # ── Identity ──────────────────────────────────────────────
    name: str                           # Unique entity name, e.g. "todos"

    # ── Scoping ───────────────────────────────────────────────
    key_params: List[str] = []          # Parameters that scope subscriptions
                                        # e.g. ["project_id"] → each project has
                                        # its own independent sync state

    # ── Sync Strategy ─────────────────────────────────────────
    sync_type: str = "list"             # "list" = mutable CRUD collection
                                        # "stream" = append-only (messages, logs)
    sync_limit: Optional[int] = None    # Default head_n window for streams
    op_log_size: int = 1000             # Max retained ops for delta sync
    data_order: str = "desc"            # Order of data from list_fn/list_stream_fn
                                        # "desc" = newest first, "asc" = oldest first
                                        # Sync engine normalizes to ASC for clients

    # ── Subscription Behavior ─────────────────────────────────
    subscription_mode: str = "lazy"     # "lazy" = subscribe on hook mount
                                        # "eager" = subscribe at app startup
    subscription_cascade: List[str] = []  # Auto-subscribe these entities too
                                          # (server-side expansion, client sends 1 msg)

    # ── CRUD Handlers ─────────────────────────────────────────
    list_fn: Optional[Callable] = None
    get_fn: Optional[Callable] = None
    create_fn: Optional[Callable] = None
    update_fn: Optional[Callable] = None
    delete_fn: Optional[Callable] = None
    upsert_fn: Optional[Callable] = None
    list_stream_fn: Optional[Callable] = None
    exists_before_fn: Optional[Callable] = None

    # ── Custom Actions ────────────────────────────────────────
    actions: Dict[str, Callable] = {}   # name → handler(store, user_id, params, payload)

    # ── Relations ─────────────────────────────────────────────
    relations: List[EntityRelation] = []  # Server-side cascade pointers

    # ── Access Control ────────────────────────────────────────
    check_access: Optional[Callable] = None  # (user_id, op, entity_id, params) → bool
```

#### Handler Signatures

```python
# List: return all items for user + params
def list_fn(store: EntityStore, user_id: str, params: dict, *, limit: int = None) -> list[dict]:
    ...

# List Stream: cursor-based backward pagination (for streams)
def list_stream_fn(store: EntityStore, user_id: str, params: dict,
                   *, before_id: str = None, after_id: str = None, limit: int = 50) -> list[dict]:
    ...

# Get: return single item by ID
def get_fn(store: EntityStore, user_id: str, entity_id: str, params: dict) -> dict | None:
    ...

# Create: insert new item, return created item (must include "id")
def create_fn(store: EntityStore, user_id: str, params: dict, data: dict) -> dict:
    ...

# Update: modify existing item, return updated item
def update_fn(store: EntityStore, user_id: str, entity_id: str, data: dict, params: dict) -> dict:
    ...

# Delete: remove item, return True if deleted
def delete_fn(store: EntityStore, user_id: str, entity_id: str, params: dict) -> bool:
    ...

# Upsert: insert-or-update, return result
def upsert_fn(store: EntityStore, user_id: str, entity_id: str, data: dict, params: dict) -> dict:
    ...

# Exists Before: check if items exist before cursor (for hasMore)
def exists_before_fn(store: EntityStore, user_id: str, oldest_id: str, params: dict) -> bool:
    ...

# Custom Action: any business logic
def action_fn(store: EntityStore, user_id: str, params: dict, payload: dict) -> dict:
    ...
# Note: action handlers can be async (the engine awaits if isawaitable)
```

#### Schema Serialization

```python
defn.to_schema_dict()
# Returns:
{
    "name": "todos",
    "keyParams": ["project_id"],
    "pushEvents": ["entity_change:todos"],
    "syncType": "list",
    "syncLimit": None,
    "subscriptionMode": "lazy",
    "dataOrder": "desc",
    "capabilities": {
        "listStream": False,
        "existsBefore": False,
        "upsert": False,
        "actions": ["archive", "bulk_delete"],
    },
}
```

### `EntityRelation`

Defines a cascade pointer between entities.

```python
from entangled.server import EntityRelation

EntityRelation(
    target="todo-items",              # Target entity name
    param_map={"id": "todo_id"},      # Source param → target param mapping
    on_actions=["created", "deleted"],  # Only cascade on these (None = all)
)
```

---

## Module: `entangled.server.store`

### `EntityStoreProtocol` (ABC)

Abstract base class defining the minimal store interface:

```python
class EntityStoreProtocol(ABC):
    def list(self, entity, user_id, *, params=None, limit=None) -> list[dict]: ...
    def get(self, entity, user_id, entity_id, *, params=None) -> dict | None: ...
    def create(self, entity, user_id, data, *, params=None, request_id=None, notify=True) -> dict: ...
    def update(self, entity, user_id, entity_id, data, *, params=None, request_id=None, notify=True) -> dict: ...
    def delete(self, entity, user_id, entity_id, *, params=None, request_id=None, notify=True) -> bool: ...
    def list_stream(self, entity, user_id, *, params=None, before_id=None, after_id=None, limit=50) -> list[dict]: ...
    def exists_before(self, entity, user_id, oldest_id, *, params=None) -> bool: ...
    def upsert(self, entity, user_id, entity_id, data, *, params=None, request_id=None) -> dict: ...
    async def action(self, entity, user_id, action_name, params, payload) -> Any: ...
    def get_def(self, entity) -> EntityDef: ...
    def get_all_defs(self) -> list[EntityDef]: ...
    def get_schema(self) -> list[dict]: ...
```

### `EntityStore`

Concrete implementation that dispatches to `EntityDef` handlers:

```python
from entangled.server import EntityStore

store = EntityStore([todos_def, users_def, settings_def])

# All operations dispatch to the corresponding EntityDef handler
result = store.create("todos", "user-1", {"title": "Buy milk"}, params={"project_id": "p1"})
# → calls todos_def.create_fn(store, "user-1", {"project_id": "p1"}, {"title": "Buy milk"})
# → _notify_change() → SyncRegistry records op → push delta to subscribers

# Access control is checked automatically
store.delete("todos", "user-2", "t1", params={"project_id": "p1"})
# → calls todos_def.check_access("user-2", "delete", "t1", {"project_id": "p1"})
# → raises PermissionError if denied

# Custom actions
result = await store.action("todos", "user-1", "archive", {"project_id": "p1"}, {"older_than": 30})
# → calls todos_def.actions["archive"](store, "user-1", {"project_id": "p1"}, {"older_than": 30})
```

### Subclassing

For custom storage backends (e.g., SQL with auto-schema):

```python
class SqlEntityStore(EntityStore):
    def __init__(self, db, defs):
        super().__init__(defs)
        self.db = db

    def list(self, entity, user_id, *, params=None, limit=None):
        defn = self.get_def(entity)
        sql = f"SELECT * FROM {entity} WHERE user_id = ?"
        args = [user_id]
        if params:
            for k, v in params.items():
                sql += f" AND {k} = ?"
                args.append(v)
        if limit:
            sql += f" LIMIT {limit}"
        return self.db.query(sql, args)
```

---

## Module: `entangled.server.sync`

### `SyncRegistry`

Manages per-(entity, params) sync state:

```python
from entangled.server import SyncRegistry

# Create with optional persistence callback
registry = SyncRegistry(
    on_version_bump=lambda key, version: db.upsert("sync_versions", key, version)
)

# Configure op-log sizes per entity
registry.set_op_log_size("messages", 5000)  # keep more ops for high-throughput entities

# Hydrate versions from persistent storage (after server restart)
saved = db.query("SELECT key, version FROM sync_versions")
registry.hydrate_versions({row["key"]: row["version"] for row in saved})

# Subscribe a client
registry.subscribe("client-abc", "todos", {"project_id": "p1"})

# Record a mutation
state, op = registry.record_op(
    entity="todos",
    op="insert",
    entity_id="t1",
    params={"project_id": "p1"},
    data={"id": "t1", "title": "Buy milk"},
    request_id="r1",  # correlates with client's write request
)

# Get subscribed clients for push
clients = registry.get_subscribed_clients("todos", {"project_id": "p1"})
```

### `resolve_sync()`

The core sync decision function:

```python
from entangled.server import resolve_sync

result = resolve_sync(
    state=registry.get_state("todos", params),
    client_version=38,              # None = first subscribe
    client_head=None,               # reserved for stream cursors
    depth=50,                       # head_n window size
    fetch_data_fn=lambda limit=None: store.list("todos", uid, params=params, limit=limit),
    sync_type="list",               # or "stream"
    default_stream_depth=50,        # from EntityDef.sync_limit
    exists_before_fn=None,          # optional cursor-based hasMore
    data_order="desc",              # order of fetch_data_fn results
)

# result: { "mode": "delta"|"snapshot"|"head_n"|"up_to_date", "version": 42, ... }
```

---

## Module: `entangled.server.ws_handler`

### `create_ws_handler()`

Creates a complete Starlette WebSocket handler:

```python
from entangled.server import create_ws_handler

handler = create_ws_handler(
    store,
    auth_fn=lambda ws: ws.headers.get("x-user-id"),  # sync or async
)

# Starlette / FastAPI
app.add_websocket_route("/ws", handler)
```

**What it handles automatically:**
- Client authentication via `auth_fn`
- Schema push (with hash for dedup)
- Subscribe/Unsubscribe with cascade expansion
- CRUD request dispatch
- Load more (backward pagination)
- Heartbeat (30s interval, 90s timeout)
- Bounded push queue (1000 items, backpressure)
- Graceful disconnect cleanup

### `WsSender` Protocol

Any object implementing `async send_json(data)` works:

```python
from entangled.server import WsSender

class AppBridgeClient:
    """Custom transport — multiplexes Entangled over existing WS."""
    async def send_json(self, data: dict):
        await self.connection.send(json.dumps(data))

# Use with handler functions
await handle_subscribe(client, store, user_id, client_id, msg)
```

### Individual Handlers

For hosts that manage their own WS and multiplex Entangled messages:

```python
from entangled.server import handle_subscribe, handle_unsubscribe, handle_request, handle_load_more

# In your WS message loop:
async def on_message(ws_sender, msg):
    if msg["type"] == "subscribe":
        await handle_subscribe(ws_sender, store, user_id, client_id, msg)
    elif msg["type"] == "unsubscribe":
        handle_unsubscribe(client_id, msg, store=store)  # sync, not async
    elif msg["type"] == "request":
        await handle_request(ws_sender, store, user_id, msg)
    elif msg["type"] == "load_more":
        await handle_load_more(ws_sender, store, user_id, msg)
```

---

## Module: `entangled.server.notifier`

### `set_store()`

Initialize the notifier (call once at startup):

```python
from entangled.server.notifier import set_store

set_store(store, sync_registry=my_registry)
```

### `notify_entity_change()`

Notify subscribers of a mutation:

```python
from entangled.server import notify_entity_change

notify_entity_change(
    user_id="user-1",
    entity="todos",
    action="created",              # "created" | "updated" | "deleted" | "stream_append" | "clear"
    entity_id="t1",
    params={"project_id": "p1"},
    data={"id": "t1", "title": "Buy milk"},
    request_id="r1",               # correlates with client's optimistic update
)
```

> **Note:** `EntityStore` calls `_notify_change()` automatically on create/update/delete when `notify=True` (default). You only need to call `notify_entity_change()` directly when bypassing the store (e.g., external DB trigger).

### Client Management

```python
from entangled.server.notifier import register_client, unregister_client, get_connected_count

register_client("client-abc", "user-1", push_callback)
unregister_client("client-abc")
count = get_connected_count()
```

---

## Complete Example

```python
"""Minimal Entangled server with FastAPI."""
import uuid
from fastapi import FastAPI
from entangled.server import EntityDef, EntityStore, SyncRegistry, create_ws_handler
from entangled.server.notifier import set_store

app = FastAPI()

# In-memory storage (replace with your DB)
_todos: dict[str, dict] = {}

def list_todos(store, user_id, params):
    pid = params.get("project_id")
    return [t for t in _todos.values() if t["project_id"] == pid]

def create_todo(store, user_id, params, data):
    todo = {"id": str(uuid.uuid4()), "project_id": params["project_id"], **data}
    _todos[todo["id"]] = todo
    return todo

def update_todo(store, user_id, entity_id, data, params):
    _todos[entity_id].update(data)
    return _todos[entity_id]

def delete_todo(store, user_id, entity_id, params):
    return _todos.pop(entity_id, None) is not None

todos_def = EntityDef(
    name="todos",
    key_params=["project_id"],
    list_fn=list_todos,
    create_fn=create_todo,
    update_fn=update_todo,
    delete_fn=delete_todo,
)

# Setup
registry = SyncRegistry()
store = EntityStore([todos_def])

async def auth(ws):
    return ws.query_params.get("user_id", "anonymous")

app.add_websocket_route("/ws", create_ws_handler(store, auth_fn=auth))
```

Run with: `uvicorn server:app --host 0.0.0.0 --port 8000`
