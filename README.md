# Entangled

**Real-time entity sync engine — like Git for your app state.**

Entangled 是一个全栈实时数据同步中间件。它用类似 Git 的版本化协议，让服务端和客户端之间保持「纠缠态」：一端变化，另一端自动同步。

```
┌─────────────┐     subscribe      ┌─────────────┐      CRUD        ┌──────────┐
│  React UI   │ ←── delta push ──→ │  Rust Cache  │ ←── WS sync ──→ │  Server  │
│  (hooks)    │     0 extra RTT    │  (version)   │     op-log       │  (DB)    │
└─────────────┘                    └─────────────┘                   └──────────┘
```

## Why Entangled?

| 传统方案 | Entangled |
|---|---|
| 数据变化 → push → invalidate → **re-fetch 全量** | 数据变化 → **delta push 只传变化** → 本地 apply |
| 每次变化 2 次通信 (push + request/response) | 每次变化 1 次通信 (delta push) |
| Client 需要知道实体关系做级联 | Server 处理级联，client 只收独立 push |
| 断线重连 → re-fetch 所有数据 | 断线重连 → 从上次版本增量同步 |

## Architecture

```
packages/
├── protocol/          # 协议类型定义 (TypeScript)
│   └── src/index.ts   # SubscribeFrame, SyncFrame, SyncOp ...
│
├── server-python/     # 服务端引擎 (Python/Starlette)
│   └── entangled/server/
│       ├── defs.py        # EntityDef — 业务声明（唯一的业务代码）
│       ├── store.py       # EntityStore — 通用 CRUD 路由
│       ├── sync.py        # SyncRegistry — 版本 + op-log + 同步决策
│       ├── notifier.py    # 订阅推送 + 级联失效
│       └── ws_handler.py  # WS 协议处理
│
├── client-rust/       # 客户端引擎 (Rust/Tauri)
│   └── src/
│       ├── schema.rs      # SchemaRegistry — 动态 entity 注册
│       ├── cache.rs       # Cache — 版本化本地缓存 + delta apply
│       ├── push.rs        # process_sync — 处理 4 种 sync mode
│       └── commands.rs    # Tauri IPC commands
│
└── react/             # React hooks (TypeScript)
    └── src/
        ├── useList.ts     # List hook (bounded CRUD collection)
        ├── useForm.ts     # Form hook (single object)
        ├── useStream.ts   # Stream hook (append-only, infinite scroll)
        ├── client.ts      # subscribe/unsubscribe + entity CRUD client
        └── syncListener.ts # Rust → React Query 事件桥接
```

## How It Works

### 1. Define Entities (Server)

```python
from entangled.server import EntityDef, EntityRelation, EntityStore, create_ws_handler

todos = EntityDef(
    name="todos",
    key_params=["project_id"],
    sync_type="list",           # "list" = mutable CRUD, "stream" = append-only
    op_log_size=1000,           # 保留最近 1000 条操作记录

    list_fn=lambda store, uid, params: db.query("SELECT * FROM todos WHERE project_id=?", params["project_id"]),
    create_fn=lambda store, uid, params, data: db.insert("todos", {**data, "project_id": params["project_id"]}),
    update_fn=lambda store, uid, eid, data, params: db.update("todos", eid, data),
    delete_fn=lambda store, uid, eid, params: db.delete("todos", eid),

    # 级联：todos 变化时，todo-items 的订阅者也收到通知
    relations=[
        EntityRelation(target="todo-items", param_map={"id": "todo_id"}),
    ],
)

store = EntityStore([todos])
app.add_websocket_route("/ws", create_ws_handler(store, auth_fn=my_auth))
```

That's it. Subscribe, push, cache, cascade, sync are all automatic.

### 2. Create React Stores (Client)

```typescript
import { createListStore, startSyncListener } from '@entangled/react';

export const todosStore = createListStore<Todo>({
  name: 'todos',
  keyParams: ['projectId'],
  getId: (t) => t.id,
});

// Start sync listener once at app startup
startSyncListener(queryClient);
```

### 3. Use in Components

```tsx
function TodoList({ projectId }: { projectId: string }) {
  const { items, create, update, remove, isLoading } = todosStore.useList({ projectId });
  //       ↑ auto-synced                    ↑ optimistic by default

  return (
    <ul>
      {items.map(todo => (
        <li key={todo.id}>
          {todo.title}
          <button onClick={() => update(todo.id, { done: true })}>✓</button>
          <button onClick={() => remove(todo.id)}>×</button>
        </li>
      ))}
      <button onClick={() => create({ title: 'New todo' })}>+</button>
    </ul>
  );
}
```

**What happens under the hood:**
1. `useList` mounts → sends `subscribe("todos", {project_id: "p1"})` to server
2. Server responds with `sync(mode: "snapshot", data: [...all todos...], version: 42)`
3. Rust cache stores data at version 42
4. User creates a todo → optimistic UI update → WS request → Server writes DB
5. Server records op in op-log (v43) → pushes `sync(mode: "delta", ops: [{insert, t99, {...}}])` to ALL subscribers
6. Rust applies delta directly to cache (no re-fetch) → emits change → React re-reads
7. Component unmounts → sends `unsubscribe` → server stops pushing

## Git-like Sync Protocol

Every `(entity, params)` combination has its own version counter and op-log, like a Git branch:

```
subscribe(version=null)  → snapshot (git clone)
subscribe(version=38)    → delta: ops v39-v42 (git pull fast-forward)
subscribe(version=5)     → snapshot (op-log gc'd, like git clone after gc)
subscribe(version=42)    → up_to_date (nothing to fetch)
subscribe(depth=50)      → head_n: latest 50 items (git clone --depth)
```

### Op types (like Git commits)

| Op | Meaning | Data |
|---|---|---|
| `insert` | New item created | Full item data |
| `update` | Item modified | Patch (changed fields only) |
| `delete` | Item removed | None |
| `invalidate` | Cascade: dependent entity changed | None (triggers re-subscribe) |

### Automatic degradation

```
Delta available?  → Send only changed ops (minimal bandwidth)
Op-log gap?       → Fall back to full snapshot (safe, automatic)
Client unchanged? → Send "up_to_date" (zero data)
```

## Three Data Shapes

| Shape | Hook | Example | Sync Mode |
|---|---|---|---|
| **List** | `useList()` | agents, devices, todos | snapshot + delta |
| **Form** | `useForm()` | settings, config | snapshot + delta |
| **Stream** | `useStream()` | messages, logs | head_n + delta + infinite scroll |

## Key Design Principles

1. **One declaration, three layers react** — Write `EntityDef` once, sync/cache/push/cascade work automatically
2. **Server owns business logic** — Relations, cascade, permissions are server-side. Client is a pure cache.
3. **Delta over re-fetch** — Changes are applied locally, not re-fetched
4. **Graceful degradation** — Op-log too old → snapshot. Disconnect → reconnect with version. Always works.
5. **Multi-user real-time** — All subscribers receive changes, regardless of who triggered them

## Stats

| Package | Lines | Purpose |
|---|---|---|
| `@entangled/protocol` | ~155 | Wire format types |
| `entangled-server` | ~750 | Python server engine |
| `entangled-client` | ~450 | Rust cache engine |
| `@entangled/react` | ~860 | React hooks |
| **Total** | **~2,200** | Complete middleware |
