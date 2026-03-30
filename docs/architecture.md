# Architecture Guide

This document explains Entangled's internal architecture, data flow, and design decisions.

## Table of Contents

- [Overview](#overview)
- [Data Flow](#data-flow)
- [Sync Protocol Deep Dive](#sync-protocol-deep-dive)
- [Server Architecture](#server-architecture)
- [Client Architecture](#client-architecture)
- [Subscription Lifecycle](#subscription-lifecycle)
- [Cascade Invalidation](#cascade-invalidation)
- [Optimistic Updates](#optimistic-updates)
- [Reconnection Strategy](#reconnection-strategy)

---

## Overview

Entangled is a **three-layer middleware** sitting between your server-side database and client-side UI:

```
                          ┌──────────────────────────────────────────────────┐
                          │                 Server Process                   │
                          │                                                  │
                          │  ┌──────────┐   ┌──────────────┐   ┌─────────┐  │
                          │  │ EntityDef│──▶│  EntityStore  │──▶│   DB    │  │
                          │  │ (schema) │   │  (CRUD disp.) │   │ (yours) │  │
                          │  └──────────┘   └──────┬───────┘   └─────────┘  │
                          │                        │                         │
                          │  ┌──────────────────────▼───────────────────────┐│
                          │  │            SyncRegistry                      ││
                          │  │  per-(entity,params) version + op-log       ││
                          │  └──────────────────────┬───────────────────────┘│
                          │                         │                        │
                          │  ┌──────────────────────▼───────────────────────┐│
                          │  │          Notifier + WS Handler              ││
                          │  │  subscribe / sync / push / cascade          ││
                          │  └──────────────────────┬───────────────────────┘│
                          └─────────────────────────┼────────────────────────┘
                                                    │ WebSocket
                          ┌─────────────────────────┼────────────────────────┐
                          │                 Client Process                    │
                          │                         │                        │
                          │  ┌──────────────────────▼───────────────────────┐│
                          │  │           EntangledClient                    ││
                          │  │  subscribe / sync frame handler             ││
                          │  └──────────────────────┬───────────────────────┘│
                          │                         │                        │
                          │  ┌──────────────────────▼───────────────────────┐│
                          │  │              Cache (SQLite)                  ││
                          │  │  versioned store + delta apply + GC         ││
                          │  └──────────────────────┬───────────────────────┘│
                          │                         │ events                 │
                          │  ┌──────────────────────▼───────────────────────┐│
                          │  │              UI Layer (React/etc.)           ││
                          │  │  reads cache → renders                      ││
                          │  └─────────────────────────────────────────────┘│
                          └──────────────────────────────────────────────────┘
```

## Data Flow

### Write Path (Mutation)

```
1. UI calls entangledMethod("todos", "create", { data: { title: "Buy milk" } })
2. Client sends WS request: { type: "request", request_id: "r1", data: { op: "create", entity: "todos", ... } }
3. Server EntityStore dispatches to TodoDef.create_fn → writes to DB
4. Server _notify_change() → SyncRegistry.record_op() → version++ → creates SyncOp
5. Server pushes delta to ALL subscribers: { type: "sync", mode: "delta", ops: [{insert, ...}] }
6. Client Cache applies delta: insert into SQLite
7. Client emits entities_changed event → UI re-renders
```

### Read Path (Query)

```
1. UI calls entity_list("todos", { project_id: "p1" })
2. Client reads directly from local SQLite cache
3. Returns immediately — zero network, zero latency
```

There is **no network call on the read path**. All reads are local SQLite queries. The cache is kept in sync by the subscription + delta push mechanism.

### Subscribe Path (Initial Load)

```
1. Client sends: { type: "subscribe", entity: "todos", params: { project_id: "p1" } }
2. Server handles cascade: subscribe to "todos" + any subscription_cascade targets
3. Server calls resolve_sync() with client_version:
   - null → snapshot (full data)
   - stale but in op-log → delta (changed ops only)
   - too old → snapshot (op-log gc'd)
   - current → up_to_date (zero data)
4. Server sends sync frame(s) back
5. Client cache stores data + version
6. Future changes arrive as delta pushes automatically
```

---

## Sync Protocol Deep Dive

### Version Model

Every `(entity, params)` pair has an independent, monotonically increasing version counter. Think of it as a Git branch:

```
todos:project_id=p1  →  v0  v1  v2  v3  v4  v5  ...
todos:project_id=p2  →  v0  v1  v2  ...
settings             →  v0  v1  ...
```

### Op-Log

Each mutation is recorded as a `SyncOp` in a bounded deque (default: 1000 entries):

```python
SyncOp(
    version=42,              # monotonic version
    op="insert",             # insert | update | delete | invalidate
    id="todo-abc",           # entity item ID
    data={"title": "..."},   # full data for insert, patch for update, None for delete
    ts=1711234567.89,        # timestamp
    request_id="r1",         # correlates with client's write request
)
```

### Four Sync Modes

| Mode | When | What's sent | Analogy |
|------|------|-------------|---------|
| `snapshot` | First subscribe or op-log gap | Full entity data | `git clone` |
| `delta` | Client has recent version | Only changed ops | `git pull` (fast-forward) |
| `head_n` | Stream entity (messages) | Latest N items + hasMore | `git clone --depth N` |
| `up_to_date` | Client already current | Nothing (empty frame) | `Already up-to-date` |

### Automatic Degradation

```
Client subscribes with version=38:
  ├─ Op-log has v39, v40, v41, v42  → delta (4 ops)
  ├─ Op-log starts at v40 (gap!)    → snapshot (full reload)
  └─ Server at v38                  → up_to_date (no data)
```

The server **never** fails a sync. It always falls back to a safe mode.

---

## Server Architecture

### EntityDef — The Single Source of Truth

An `EntityDef` declares everything about an entity:

```python
EntityDef(
    # Identity
    name="todos",
    key_params=["project_id"],      # scoping parameters

    # Sync strategy
    sync_type="list",               # "list" or "stream"
    sync_limit=50,                  # stream: default window size
    op_log_size=1000,               # max ops retained for delta sync

    # CRUD handlers (all receive: store, user_id, params, ...)
    list_fn=...,
    get_fn=...,
    create_fn=...,
    update_fn=...,
    delete_fn=...,
    upsert_fn=...,                  # optional insert-or-update

    # Stream-specific handlers
    list_stream_fn=...,             # cursor-based pagination
    exists_before_fn=...,           # efficient hasMore check

    # Custom actions (business logic beyond CRUD)
    actions={
        "archive": archive_action,
        "bulk_delete": bulk_delete_action,
    },

    # Relations (server-side cascade)
    relations=[
        EntityRelation(target="todo-items", param_map={"id": "todo_id"}),
    ],

    # Subscription behavior
    subscription_mode="lazy",       # "lazy" (on-demand) or "eager" (at connect)
    subscription_cascade=["todo-items"],  # auto-subscribe related entities

    # Access control
    check_access=lambda uid, op, eid, params: is_authorized(uid, op),
)
```

### EntityStore — Generic CRUD Router

`EntityStore` holds all `EntityDef`s and routes operations:

```python
store = EntityStore([todos_def, users_def, settings_def])

# All CRUD is dispatched to EntityDef handlers
store.create("todos", "user-1", {"title": "Buy milk"}, params={"project_id": "p1"})
# → calls todos_def.create_fn(store, "user-1", {"project_id": "p1"}, {"title": "Buy milk"})
# → records op in SyncRegistry
# → pushes delta to subscribers
```

For SQL-backed storage, subclass `EntityStore` or `EntityStoreProtocol`:

```python
class SqlEntityStore(EntityStore):
    def __init__(self, db_conn, defs):
        super().__init__(defs)
        self.db = db_conn

    def list(self, entity, user_id, *, params=None, limit=None):
        defn = self.get_def(entity)
        # Custom SQL logic here
        return self.db.query(f"SELECT * FROM {entity} WHERE user_id=?", user_id)
```

### SyncRegistry — Version Tracking

The `SyncRegistry` manages per-(entity, params) sync state:

```python
registry = SyncRegistry(
    on_version_bump=lambda key, ver: db.upsert("sync_versions", key, ver)
)

# Hydrate from persistent storage after restart
registry.hydrate_versions({"todos": 42, "settings": 7})
```

### WS Handler — Protocol Implementation

Two usage modes:

```python
# Mode 1: Starlette handler (Entangled owns the route)
app.add_websocket_route("/ws", create_ws_handler(store, auth_fn=my_auth))

# Mode 2: Handler functions (host owns the WS, multiplexes Entangled)
from entangled.server import handle_subscribe, handle_request, handle_load_more

async def my_ws_handler(ws, msg):
    if msg["type"] == "subscribe":
        await handle_subscribe(ws, store, user_id, client_id, msg)
    elif msg["type"] == "request":
        await handle_request(ws, store, user_id, msg)
    elif msg["type"] == "load_more":
        await handle_load_more(ws, store, user_id, msg)
```

The `WsSender` protocol allows any object with `async send_json(data)` to work as a transport:

```python
class MyTransport:
    async def send_json(self, data):
        await self.ws.send(json.dumps(data))
```

---

## Client Architecture

### Cache (SQLite)

The Rust client maintains a local SQLite database with:

- **Entity data**: versioned rows per `(entity, params)` combination
- **Version tracking**: current server version per subscription
- **Op-log**: pending optimistic operations awaiting server confirmation
- **TTL garbage collection**: `last_accessed` timestamp, auto-cleanup of stale entries

### Two Client Modes

**Standalone** — Entangled owns the WebSocket:
```rust
let client = EntangledClient::connect(config).await;
// client.subscribe/unsubscribe/method work over its own WS
```

**Embedded** — Host owns the WebSocket (e.g., Tauri app with shared AppBridge):
```rust
let client = EntangledClient::embedded(db_dir, user_id);
// Host feeds incoming sync frames:
client.handle_sync_frame(json_value);
// Host sends outgoing subscribe/request frames through its own WS
```

### Tauri Integration

The `commands` module (feature-gated behind `tauri`) exposes `EntangledClient` as Tauri IPC commands:

```
React → invoke("entangled_subscribe")     → Rust commands.rs → SubscriptionRegistry
React → invoke("entity_list")            → Rust commands.rs → Cache (local SQLite)
React → invoke("entangled_method")        → Rust commands.rs → WS → Server → delta back
React ← listen("entities_changed")        ← Rust push.rs    ← Server sync frame
```

---

## Subscription Lifecycle

```
Mount          Subscribe         Sync            Live                  Unmount
  │               │                │               │                      │
  │  subscribe()  │  ────WS────▶   │               │                      │
  │───────────────▶  Server        │               │                      │
  │               │  resolve_sync  │               │                      │
  │               │  ◀────sync──── │               │                      │
  │               │                │  apply cache  │                      │
  │               │                │───────────────▶                      │
  │               │                │               │  delta pushes...     │
  │               │                │               │ ◀── sync(delta) ──── │
  │               │                │               │  apply + emit        │
  │               │                │               │                      │
  │               │                │               │     unsubscribe()    │
  │               │                │               │──────────────────────▶
```

### Ref-Counting

Multiple components can subscribe to the same `(entity, params)`. The `SubscriptionRegistry` (Rust) tracks reference counts:

```
Component A subscribes to "todos:{p1}" → ref_count = 1 → send WS subscribe
Component B subscribes to "todos:{p1}" → ref_count = 2 → no WS (already active)
Component B unmounts                   → ref_count = 1 → no WS
Component A unmounts                   → ref_count = 0 → send WS unsubscribe
```

---

## Cascade Invalidation

When entity A changes and has a relation to entity B, subscribers of B are automatically notified:

```python
# Server-side definition
todos = EntityDef(
    name="todos",
    relations=[
        EntityRelation(
            target="todo-items",
            param_map={"id": "todo_id"},
            on_actions=["created", "deleted"],  # only cascade on these actions
        ),
    ],
)
```

**Flow:**
1. User creates a todo → `notify_entity_change("todos", "created", entity_id="t1")`
2. Notifier sees `todos.relations[0]` → target `"todo-items"`, maps `{"id": "t1"}` → `{"todo_id": "t1"}`
3. Records `invalidate` op on `todo-items:{todo_id=t1}`
4. Pushes invalidation delta to all subscribers of `todo-items:{todo_id=t1}`
5. Client receives invalidation → triggers re-subscribe to get fresh data

**Cascade is recursive** — if `todo-items` has relations to `todo-comments`, those cascade too (with cycle detection via `visited` set).

---

## Optimistic Updates

When using `entangled_method_optimistic` (Tauri command):

```
1. Client generates temp ID + request_id
2. Client writes PENDING item to local cache
3. Client emits entities_changed → UI shows item immediately
4. Client sends WS request to server
5. Server processes → records op with request_id → pushes delta
6. Client receives delta → matches request_id → promotes PENDING → CONFIRMED
7. If server fails → rollback PENDING item → emit entities_changed with FAILED status
```

The UI can distinguish states via `EntangledMeta`:
```typescript
interface EntangledMeta {
  _status: 'confirmed' | 'pending' | 'failed';
  _op?: 'create' | 'update' | 'delete';
  _tempId?: string;
  _error?: string;
  _retry?: () => void;
}
```

---

## Reconnection Strategy

On WebSocket disconnect:

1. **Client preserves all cache data** (SQLite is persistent)
2. **On reconnect**, client re-subscribes to all active subscriptions with their last known `version`
3. **Server resolves sync** — if op-log covers the gap, sends delta; otherwise sends fresh snapshot
4. **Client applies** and emits `entities_changed` — UI updates

This means users see **zero data loss** on reconnection, and the re-sync transfers **minimum data** (delta when possible).

### Server-side Heartbeat

The WS handler sends heartbeats every 30s and closes connections idle for 90s:

```python
HEARTBEAT_INTERVAL_S = 30    # server → client heartbeat
HEARTBEAT_TIMEOUT_S = 90     # close if no message received
```
