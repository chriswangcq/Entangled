# Entangled

**Real-time Entity Sync Engine for Tauri + React apps.**

Declare your entities once on the server — caching, push updates, and cascade invalidation happen automatically across all layers.

```
Server (Python)              Client Engine (Rust)         UI (React)
━━━━━━━━━━━━━━━              ━━━━━━━━━━━━━━━━━━           ━━━━━━━━━━
EntityDef registry    ──WS──→  Dynamic schema cache        useList / useForm
DB read/write                  Push → update → cascade     Optimistic updates
Push broadcast                 Zero business logic         Zero protocol logic
```

## Quick Start

### Server (Python)

```python
from entangled.server import EntityDef, create_ws_handler

todos = EntityDef(
    name="todos",
    key_params=["project_id"],
    list_fn=lambda store, user_id, params: db.query("SELECT * FROM todos WHERE project_id = ?", params["project_id"]),
    create_fn=lambda store, user_id, params, data: db.insert("todos", data),
)

# Mount the WS handler on your FastAPI/Starlette app
app.add_websocket_route("/ws", create_ws_handler([todos]))
```

### Client (React + Tauri)

```typescript
import { createListStore } from '@entangled/react';

const todosStore = createListStore<Todo>({
  name: 'todos',
  getId: (t) => t.id,
});

function TodoList({ projectId }: { projectId: string }) {
  const { items, create, remove, isLoading } = todosStore.useList({ projectId });
  // ✅ Auto-cached, auto-synced, auto-cascade-invalidated
}
```

## Packages

| Package | Path | Description |
|---------|------|-------------|
| `entangled-server` | `packages/server-python/` | Python server: EntityDef + EntityStore + WS handler |
| `entangled-client` | `packages/client-rust/` | Rust client engine: cache + push + cascade |
| `@entangled/react` | `packages/react/` | React hooks: useList, useForm, useStream, writePipeline |
| `@entangled/protocol` | `packages/protocol/` | Shared WS protocol types (TS + Python) |

## Architecture

```
┌─────────────────────────────────────────────────┐
│  Server defines schema (entities + relations)    │
│  WS push notifies clients of changes             │
└──────────────────────┬──────────────────────────┘
                       │ WebSocket
┌──────────────────────▼──────────────────────────┐
│  Rust Engine (generic, no business logic)         │
│  • Receives schema dynamically from server        │
│  • Caches entities locally (HashMap)              │
│  • Processes pushes → updates cache → cascades    │
│  • Emits batched "entities_changed" to React       │
└──────────────────────┬──────────────────────────┘
                       │ Tauri IPC
┌──────────────────────▼──────────────────────────┐
│  React Hooks (thin layer)                         │
│  • useList/useForm → invoke('entity_list')        │
│  • listen('entities_changed') → invalidate        │
│  • writePipeline: optimistic updates (pure JS)    │
└─────────────────────────────────────────────────┘
```

## Key Differentiators

| | Entangled | Firebase | Convex | tRPC |
|---|---|---|---|---|
| Self-hosted | ✅ | ❌ | ❌ | ✅ |
| Any database | ✅ | ❌ | ❌ | ✅ |
| Real-time push | ✅ | ✅ | ✅ | Manual |
| Cascade invalidation | Declarative | Manual | Manual | Manual |
| Optimistic updates | Built-in | Manual | Built-in | Manual |
| Rust client engine | ✅ | ❌ | ❌ | ❌ |
| Desktop-grade perf | ✅ | ❌ | ❌ | ❌ |

## License

MIT
