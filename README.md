# Entangled

**Real-time entity sync engine for native apps — like Git for your app state.**

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Entangled is a full-stack real-time data synchronization middleware. It uses a Git-like versioned protocol to keep server and client in "entangled" state: when one side changes, the other syncs automatically.

```
┌─────────────┐     subscribe      ┌─────────────┐      CRUD        ┌──────────┐
│  React UI   │ ←── delta push ──→ │  Rust Cache  │ ←── WS sync ──→ │  Server  │
│  (hooks)    │     0 extra RTT    │  (SQLite)    │     op-log       │  (DB)    │
└─────────────┘                    └─────────────┘                   └──────────┘
```

## Why Entangled?

| Traditional Approach | Entangled |
|---|---|
| Data changes → push → invalidate → **re-fetch everything** | Data changes → **delta push (only changes)** → local apply |
| 2 round-trips per change (push + request/response) | 1 message per change (delta push) |
| Client manages entity relationships for cascade | Server handles cascade, client receives independent pushes |
| Reconnect → re-fetch all data | Reconnect → incremental sync from last known version |
| WASM SQLite in browser (2-5MB overhead) | **Native SQLite** via Rust (zero overhead for desktop apps) |

## Key Features

- 🔄 **Git-like Sync Protocol** — snapshot, delta, head_n, up_to_date (like clone, pull, shallow clone)
- 🦀 **Rust-native Client** — persistent SQLite cache, zero WASM overhead for Tauri/desktop apps
- 🐍 **Python Server** — works with any ASGI framework (Starlette, FastAPI, etc.)
- 📡 **Transport Agnostic** — bring your own WebSocket, or use the built-in transport
- 🎯 **Entity-scoped Subscriptions** — subscribe to `(entity, params)` pairs with ref-counting
- ⚡ **Optimistic Updates** — pending/confirmed/failed status tracking with automatic rollback
- 🌊 **Stream Support** — append-only entities (messages, logs) with cursor-based pagination
- 🔗 **Cascade Invalidation** — server-side relation graph, zero client configuration
- 🔐 **Per-entity Access Control** — `check_access` callback on each EntityDef
- 📦 **~2,200 lines total** — entire middleware in a small, auditable codebase

## Quick Start

### 1. Define Entities (Python Server)

```python
from entangled.server import EntityDef, EntityRelation, EntityStore, create_ws_handler

todos = EntityDef(
    name="todos",
    key_params=["project_id"],
    sync_type="list",           # "list" = mutable CRUD, "stream" = append-only
    op_log_size=1000,           # retain last 1000 ops for delta sync

    list_fn=lambda store, uid, params: db.query(
        "SELECT * FROM todos WHERE project_id=?", params["project_id"]
    ),
    create_fn=lambda store, uid, params, data: db.insert(
        "todos", {**data, "project_id": params["project_id"]}
    ),
    update_fn=lambda store, uid, eid, data, params: db.update("todos", eid, data),
    delete_fn=lambda store, uid, eid, params: db.delete("todos", eid),

    # Cascade: when a todo changes, subscribers of todo-items also get notified
    relations=[
        EntityRelation(target="todo-items", param_map={"id": "todo_id"}),
    ],
)

store = EntityStore([todos])

# Starlette / FastAPI
app.add_websocket_route("/ws", create_ws_handler(store, auth_fn=my_auth))
```

### 2. Connect (Rust Client)

```rust
use entangled_client::{EntangledClient, EntangledConfig};

// Standalone: Entangled owns the WS connection
let client = EntangledClient::connect(EntangledConfig {
    ws_url: "wss://api.example.com/ws".into(),
    auth: Box::new(jwt_auth),
    db_dir: "/data/entangled".into(),
}).await;

// Subscribe to an entity
client.subscribe("todos", Some(json!({"project_id": "p1"}))).await;

// Read from local SQLite cache (instant, no network)
let todos = client.get_list("todos", Some(json!({"project_id": "p1"})));

// Or: Embedded mode — host owns the WS, feeds frames to Entangled
let client = EntangledClient::embedded("/data/entangled", "user-123");
client.handle_sync_frame(incoming_frame);
```

### 3. Tauri Integration

```rust
// In your Tauri app's main.rs
use entangled_client::commands::*;

fn main() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![
            entangled_subscribe,
            entangled_unsubscribe,
            entangled_method,
            entangled_method_optimistic,
            entity_list,
            entity_get,
        ])
        .run(tauri::generate_context!())
        .unwrap();
}
```

```typescript
// In your React frontend
import { invoke } from '@tauri-apps/api/core';

// Subscribe (Rust manages lifecycle)
await invoke('entangled_subscribe', { entity: 'todos', params: { projectId: 'p1' } });

// Read from Rust SQLite cache (instant)
const todos = await invoke('entity_list', { entity: 'todos', params: { projectId: 'p1' } });

// Write via Entangled Method (optimistic)
await invoke('entangled_method_optimistic', {
  entity: 'todos',
  method: 'create',
  args: { data: { title: 'Buy milk' } },
  params: { projectId: 'p1' },
});
```

## Architecture

```
packages/
├── protocol/              # Wire format types (TypeScript)
│   └── src/index.ts       # SubscribeFrame, SyncFrame, SyncOp, EntangledMethodArgs
│
├── server-python/         # Server engine (Python 3.10+)
│   └── entangled/server/
│       ├── defs.py        # EntityDef — entity declaration (the ONLY business code)
│       ├── store.py       # EntityStore — generic CRUD dispatch + ABC protocol
│       ├── sync.py        # SyncRegistry — version + op-log + sync decisions
│       ├── notifier.py    # Push to subscribers + cascade invalidation
│       └── ws_handler.py  # WS protocol handler (Starlette-compatible)
│
└── client-rust/           # Client engine (Rust)
    └── src/
        ├── lib.rs         # Crate root + re-exports
        ├── schema.rs      # SchemaRegistry — dynamic entity registration
        ├── cache.rs       # Cache — versioned SQLite cache + delta apply + TTL GC
        ├── push.rs        # process_sync — handles 4 sync modes
        ├── client.rs      # EntangledClient — unified API
        ├── auth.rs        # AuthProvider trait (host-injected)
        ├── transport.rs   # WS connection management (optional, feature-gated)
        └── commands.rs    # Tauri IPC commands (optional, feature-gated)
```

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture Guide](docs/architecture.md) | Deep dive into the sync protocol, data flow, and design decisions |
| [Server API Reference](docs/server-api.md) | Complete Python server API — EntityDef, EntityStore, WS handler |
| [Client API Reference](docs/client-api.md) | Rust client API — EntangledClient, Cache, Tauri commands |
| [Protocol Specification](docs/protocol.md) | Wire format, frame types, sync modes, and message flow |
| [Constitution](CONSTITUTION.md) | Inviolable architectural contracts |

## How It Works

### Git-like Sync Protocol

Every `(entity, params)` combination has its own version counter and op-log, like a Git branch:

```
subscribe(version=null)  → snapshot    (git clone)
subscribe(version=38)    → delta      (git pull fast-forward: ops v39-v42)
subscribe(version=5)     → snapshot   (op-log gc'd, like fresh clone)
subscribe(version=42)    → up_to_date (nothing to sync)
subscribe(depth=50)      → head_n     (git clone --depth 50)
```

### Sync Operations (like Git commits)

| Op | Meaning | Data |
|---|---|---|
| `insert` | New item created | Full item data |
| `update` | Item modified | Changed fields only |
| `delete` | Item removed | None |
| `invalidate` | Cascade: dependent entity changed | None (triggers re-subscribe) |

### Three Data Shapes

| Shape | Use Case | Sync Strategy |
|---|---|---|
| **List** | agents, devices, todos | Full snapshot + delta |
| **Form** | settings, config | Single-item snapshot + delta |
| **Stream** | messages, logs | Bounded head_n + delta + infinite scroll |

## Design Principles

1. **One declaration, three layers react** — Write `EntityDef` once; sync, cache, push, and cascade work automatically
2. **Server owns business logic** — Relations, cascade, permissions are server-side. Client is a pure cache.
3. **Delta over re-fetch** — Changes are applied locally, never re-fetched
4. **Graceful degradation** — Op-log too old → snapshot. Disconnect → reconnect from last version. Always works.
5. **Transport agnostic** — Server uses `WsSender` protocol; client supports standalone or embedded mode
6. **Native performance** — Rust SQLite cache, not WASM. Designed for desktop apps.

## Codebase Stats

| Package | Lines | Purpose |
|---|---|---|
| `@entangled/protocol` | ~186 | Wire format types (TypeScript) |
| `entangled-server` (Python) | ~1,400 | Server engine (defs + store + sync + notifier + ws_handler) |
| `entangled-client` (Rust) | ~800 | Client engine (cache + push + client + auth + transport + commands) |
| **Total** | **~2,400** | Complete full-stack sync middleware |

## License

MIT — see [LICENSE](LICENSE).
