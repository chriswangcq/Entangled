# Protocol Specification

Complete wire format specification for the Entangled sync protocol.

## Overview

Entangled communicates over a single WebSocket connection using JSON messages. All messages have a `type` field that determines their structure.

## Message Types

| Direction | Type | Purpose |
|-----------|------|---------|
| C → S | `subscribe` | Establish entity entanglement |
| C → S | `unsubscribe` | Break entity entanglement |
| C → S | `request` | Entity CRUD / custom action |
| C → S | `load_more` | Backward pagination (streams) |
| C → S | `ping` | Client heartbeat |
| S → C | `sync` | Data synchronization frame |
| S → C | `response` | Request response |
| S → C | `push` | Server-initiated event (schema, etc.) |
| S → C | `heartbeat` | Server heartbeat |
| S → C | `pong` | Ping response |
| S → C | `error` | Protocol error |

---

## Connection Lifecycle

```
Client                                          Server
  │                                               │
  │ ═══════════ WS Connect ════════════════════▶  │
  │                                               │
  │  ◀──── push(schema, {entities, hash}) ─────   │  1. Schema push
  │                                               │
  │  ──── subscribe(entity, params, version) ──▶  │  2. Subscribe
  │  ◀──── sync(snapshot/delta/head_n) ────────   │  3. Initial data
  │                                               │
  │  ──── request(op:create, entity, data) ────▶  │  4. CRUD
  │  ◀──── response(request_id, data) ─────────   │  5. Response
  │  ◀──── sync(delta, ops:[insert]) ──────────   │  6. Delta to ALL subscribers
  │                                               │
  │  ◀──── heartbeat(ts) ─────────────────────    │  7. Server heartbeat (30s)
  │  ──── ping ────────────────────────────────▶  │  8. Client heartbeat
  │  ◀──── pong ──────────────────────────────    │
  │                                               │
  │  ──── unsubscribe(entity, params) ─────────▶  │  9. Cleanup
  │                                               │
  │ ═══════════ WS Disconnect ══════════════════  │
```

---

## Client → Server Messages

### `subscribe`

Establish a subscription to an entity. Server responds with a `sync` frame.

```json
{
  "type": "subscribe",
  "entity": "todos",
  "params": { "project_id": "p1" },
  "version": null,
  "head": null,
  "depth": 50
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `entity` | string | ✅ | Entity name |
| `params` | object | ❌ | Key params for scoping |
| `version` | int \| null | ❌ | Client's last known server version. `null` = first subscribe |
| `head` | string \| null | ❌ | Reserved for stream cursor |
| `depth` | int | ❌ | Max items for initial sync (stream entities) |

**Server-side cascade**: If the entity has `subscription_cascade`, the server automatically subscribes the client to all cascade targets and sends one `sync` frame per target.

### `unsubscribe`

Break a subscription.

```json
{
  "type": "unsubscribe",
  "entity": "todos",
  "params": { "project_id": "p1" }
}
```

### `request`

Dispatch a CRUD operation or custom action.

```json
{
  "type": "request",
  "request_id": "r-abc-123",
  "action": "entity",
  "data": {
    "op": "create",
    "entity": "todos",
    "params": { "project_id": "p1" },
    "data": { "title": "Buy milk" }
  }
}
```

#### Supported Operations

| `op` | Required Fields | Description |
|------|----------------|-------------|
| `list` | `entity` | List all items |
| `list_all` | `entity` | List all (no scope limit) |
| `list_stream` | `entity`, optional `before_id`/`after_id`/`limit` | Cursor-based pagination |
| `get` | `entity`, `id` | Get single item |
| `create` | `entity`, `data` | Create new item |
| `update` | `entity`, `id`, `data` | Update existing item |
| `upsert` | `entity`, `id`, `data` | Insert or update |
| `delete` | `entity`, `id` | Delete item |
| `action` | `entity`, `action_name`, optional `data` | Custom action |

### `load_more`

First-class backward pagination for stream entities.

```json
{
  "type": "load_more",
  "request_id": "lm-abc-123",
  "entity": "messages",
  "params": { "agent_id": "a1" },
  "before_id": "msg-oldest-in-view",
  "limit": 50
}
```

### `ping`

Client heartbeat.

```json
{ "type": "ping" }
```

---

## Server → Client Messages

### `sync`

Data synchronization frame. Four modes:

#### Snapshot (full data)

```json
{
  "type": "sync",
  "entity": "todos",
  "params": { "project_id": "p1" },
  "mode": "snapshot",
  "version": 42,
  "data": [
    { "id": "t1", "title": "Buy milk", "done": false },
    { "id": "t2", "title": "Walk dog", "done": true }
  ]
}
```

#### Delta (incremental ops)

```json
{
  "type": "sync",
  "entity": "todos",
  "params": { "project_id": "p1" },
  "mode": "delta",
  "version": 44,
  "baseVersion": 42,
  "ops": [
    { "version": 43, "op": "insert", "id": "t3", "data": { "id": "t3", "title": "New todo" }, "ts": 1711234567.89, "requestId": "r1" },
    { "version": 44, "op": "update", "id": "t1", "data": { "done": true }, "ts": 1711234568.01 }
  ]
}
```

#### Head N (bounded stream)

```json
{
  "type": "sync",
  "entity": "messages",
  "params": { "agent_id": "a1" },
  "mode": "head_n",
  "version": 100,
  "data": [ /* latest N items, ASC order (oldest first) */ ],
  "hasMore": true
}
```

#### Up To Date

```json
{
  "type": "sync",
  "entity": "todos",
  "params": { "project_id": "p1" },
  "mode": "up_to_date",
  "version": 42
}
```

### `response`

Response to a `request` or `load_more`.

```json
{
  "type": "response",
  "request_id": "r-abc-123",
  "data": {
    "success": true,
    "data": { "id": "t3", "title": "Buy milk" }
  }
}
```

#### Error Response

```json
{
  "type": "response",
  "request_id": "r-abc-123",
  "data": {
    "success": false,
    "error": "Permission denied"
  }
}
```

#### Load More Response

```json
{
  "type": "response",
  "request_id": "lm-abc-123",
  "data": {
    "success": true,
    "entries": [ /* older items */ ],
    "has_more": true
  }
}
```

### `push`

Server-initiated event.

#### Schema Push (on connect)

```json
{
  "type": "push",
  "event": "schema",
  "data": {
    "entities": [
      {
        "name": "todos",
        "keyParams": ["project_id"],
        "pushEvents": ["entity_change:todos"],
        "syncType": "list",
        "syncLimit": null,
        "subscriptionMode": "lazy",
        "dataOrder": "desc",
        "capabilities": {
          "listStream": false,
          "existsBefore": false,
          "upsert": false,
          "actions": ["archive"]
        }
      }
    ],
    "hash": "a1b2c3d4e5f6"
  }
}
```

### `heartbeat`

Server heartbeat (every 30 seconds).

```json
{ "type": "heartbeat", "ts": 1711234567.89 }
```

### `pong`

Response to client `ping`.

```json
{ "type": "pong" }
```

### `error`

Protocol-level error.

```json
{ "type": "error", "error": "entity is required" }
```

---

## Sync Op Types

| Op | Meaning | `data` field | Client action |
|---|---|---|---|
| `insert` | New item created | Full item data | Add to cache |
| `update` | Item modified | Changed fields (patch) | Merge into cached item |
| `delete` | Item removed | `null` | Remove from cache |
| `invalidate` | Cascade: related entity changed | `null` | Re-subscribe to get fresh data |

---

## Data Ordering

- **Server `data_order`**: Declares the order returned by `list_fn` / `list_stream_fn`
  - `"desc"` = newest first (default for messages)
  - `"asc"` = oldest first (default for logs)
- **Sync engine normalization**: All `sync` frames send data in **ASC order** (oldest first)
- **Client**: Receives ASC-ordered data, can render in any order

---

## Connection Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| Push queue max size | 1000 | Backpressure: drops oldest when full |
| Heartbeat interval | 30s | Server → client |
| Heartbeat timeout | 90s | Close connection if idle |
| Load more limit cap | 500 | Max items per load_more request |
| List entry cap | 5000 | Max items returned by list operation |

---

## TypeScript Types

All wire types are defined in `@entangled/protocol`:

```typescript
import type {
  // Schema
  EntitySchema,
  EntityRelation,

  // Sync
  SyncOp,
  SyncMode,        // 'snapshot' | 'delta' | 'head_n' | 'up_to_date'
  SyncFrame,

  // Subscribe
  SubscribeFrame,
  UnsubscribeFrame,

  // Request/Response
  EntityOp,        // 'list' | 'get' | 'create' | 'update' | 'delete' | 'action' | ...
  EntityRequest,
  EntityResponse,
  RequestFrame,
  ResponseFrame,

  // Method
  EntangledMethodArgs,

  // Events
  EntitiesChangedEvent,
  PushFrame,
  SchemaPush,

  // Optimistic
  EntangledMeta,   // { _status, _op, _tempId, _error, _retry }
  Entangled,       // T & EntangledMeta
} from '@entangled/protocol';
```
