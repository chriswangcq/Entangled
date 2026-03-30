# Client API Reference

Complete reference for the `entangled-client` Rust crate.

## Installation

```toml
# Cargo.toml
[dependencies]
entangled-client = { version = "0.2", features = ["transport"] }

# Optional: Tauri integration
entangled-client = { version = "0.2", features = ["transport", "tauri"] }
```

### Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `transport` | ✅ | Built-in WS connection management (`tokio-tungstenite`) |
| `tauri` | ❌ | Tauri IPC command wrappers |

---

## `EntangledClient`

The main client API.

### Construction

```rust
use entangled_client::{EntangledClient, EntangledConfig};

// Mode 1: Standalone (Entangled owns the WS)
let client = EntangledClient::connect(EntangledConfig {
    ws_url: "wss://api.example.com/ws".into(),
    auth: Box::new(jwt_auth),
    db_dir: PathBuf::from("/data/entangled"),
}).await;

// Mode 2: Embedded (host owns the WS)
let client = EntangledClient::embedded(
    PathBuf::from("/data/entangled"),
    "user-123",
);
```

### Subscribe / Unsubscribe

```rust
// Subscribe to an entity (starts receiving delta pushes)
client.subscribe("todos", Some(json!({"project_id": "p1"}))).await;

// Subscribe with depth (stream entities)
client.subscribe_with_depth("messages", Some(json!({"agent_id": "a1"})), 50).await;

// Unsubscribe
client.unsubscribe("todos", Some(json!({"project_id": "p1"}))).await;
```

### Read (Local Cache)

```rust
// Get list from local SQLite cache — instant, zero network
let items: Vec<Value> = client.get_list("todos", Some(json!({"project_id": "p1"})));

// Get single item
let item: Option<Value> = client.get_item("todos", "todo-123", None);

// Raw cache access
let cache: &Cache = client.cache();
```

### Write (via Server)

```rust
// Standard CRUD
let created = client.method("todos", "create", json!({
    "data": { "title": "Buy milk" },
}), Some(json!({"project_id": "p1"}))).await;

let updated = client.method("todos", "update", json!({
    "id": "todo-123",
    "data": { "title": "Buy oat milk" },
}), None).await;

client.method("todos", "delete", json!({
    "id": "todo-123",
}), None).await;

// Custom action
let result = client.method("todos", "archive", json!({
    "payload": { "older_than_days": 30 },
}), Some(json!({"project_id": "p1"}))).await;
```

### Change Notifications

```rust
// Get a broadcast receiver for entity change events
let mut rx = client.on_change();

tokio::spawn(async move {
    while let Ok(event) = rx.recv().await {
        // event: EntityChanged { entity, action, params, request_ids }
        println!("Changed: {} ({})", event.entity, event.action);
    }
});
```

### Sync Frame Handling (Embedded Mode)

```rust
// When host receives a sync frame from WS, feed it to the client
client.handle_sync_frame(json_value);
// This updates the local cache and emits entities_changed events
```

---

## `Cache`

Persistent SQLite cache with version tracking.

```rust
use entangled_client::{Cache, CacheKey};

// Open/create cache database
let cache = Cache::open(PathBuf::from("/data/entangled/cache.db"))?;

// Cache key = (entity, params)
let key = CacheKey::new("todos", Some(json!({"project_id": "p1"})));

// Read
let items: Vec<Value> = cache.get_list(&key);
let item: Option<Value> = cache.get_item(&key, "todo-123");
let version: Option<u64> = cache.get_version(&key);

// Write (called internally by process_sync)
cache.apply_snapshot(&key, &items, version);
cache.apply_delta(&key, &ops, new_version);
cache.apply_head_n(&key, &items, version, has_more);

// Optimistic operations
cache.write_pending(&key, "temp-123", &pending_item, "create");
cache.confirm_pending(&key, "temp-123", &confirmed_item);
cache.rollback_pending(&key, "temp-123");

// Maintenance
cache.cleanup_stale(Duration::from_secs(3600));  // TTL garbage collection
```

---

## `SyncFrame` Processing

```rust
use entangled_client::push::{process_sync, SyncFrame, EntityChanged};

// process_sync handles all 4 sync modes:
let changes: Vec<EntityChanged> = process_sync(&cache, &frame);

// EntityChanged {
//     entity: "todos",
//     action: "sync",  // or "insert", "update", "delete", "invalidate"
//     params: Some({"project_id": "p1"}),
//     request_ids: vec!["r1"],  // for optimistic correlation
// }
```

### Sync Modes

| Mode | Cache Action |
|------|-------------|
| `snapshot` | Replace all items for this key |
| `delta` | Apply ops (insert/update/delete) individually |
| `head_n` | Replace items + store `hasMore` flag |
| `up_to_date` | No change (version confirmed current) |

---

## `AuthProvider` Trait

```rust
use entangled_client::AuthProvider;

/// Implement to provide authentication headers for WS connections.
pub trait AuthProvider: Send + Sync {
    /// Return headers to include in the WS upgrade request.
    fn auth_headers(&self) -> Vec<(String, String)>;

    /// Called when auth fails (e.g., 401 response). Return true to retry.
    fn on_auth_failure(&self) -> bool { false }
}

// Example: JWT authentication
struct JwtAuth { token: String }

impl AuthProvider for JwtAuth {
    fn auth_headers(&self) -> Vec<(String, String)> {
        vec![("Authorization".into(), format!("Bearer {}", self.token))]
    }
}
```

---

## `SchemaRegistry`

Dynamic entity schema management:

```rust
use entangled_client::schema::SchemaRegistry;

let mut registry = SchemaRegistry::new();

// Receive schema from server (via "push" event with "schema" data)
registry.update_from_schema(schema_data);

// Look up entity info
if let Some(entity) = registry.get("todos") {
    println!("Sync type: {}", entity.sync_type);  // "list" or "stream"
    println!("Key params: {:?}", entity.key_params);
}
```

---

## Tauri Commands

When the `tauri` feature is enabled, these commands are available:

### `entangled_subscribe`

```typescript
await invoke('entangled_subscribe', {
  entity: 'todos',
  params: { projectId: 'p1' },
  depth: 50,  // optional, for streams
});
```

### `entangled_unsubscribe`

```typescript
await invoke('entangled_unsubscribe', {
  entity: 'todos',
  params: { projectId: 'p1' },
});
```

### `entangled_method`

Standard write operation (waits for server response):

```typescript
const result = await invoke('entangled_method', {
  entity: 'todos',
  method: 'create',  // or 'update', 'delete', 'upsert', or custom action name
  args: {
    data: { title: 'Buy milk' },
  },
  params: { projectId: 'p1' },
});
```

### `entangled_method_optimistic`

Optimistic write (instant UI update, server confirmation in background):

```typescript
const result = await invoke('entangled_method_optimistic', {
  entity: 'todos',
  method: 'create',
  args: {
    data: { title: 'Buy milk' },
  },
  params: { projectId: 'p1' },
});
// UI updates immediately; if server fails, item shows _status: 'failed'
```

### `entity_list`

Read from local SQLite cache:

```typescript
const todos: Todo[] = await invoke('entity_list', {
  entity: 'todos',
  params: { projectId: 'p1' },
});
// Returns instantly from local cache — zero network
```

### `entity_get`

Read single item from cache:

```typescript
const todo: Todo | null = await invoke('entity_get', {
  entity: 'todos',
  entityId: 'todo-123',
  params: { projectId: 'p1' },
});
```

### `SubscriptionRegistry`

Ref-counted subscription management (used internally by commands):

```rust
use entangled_client::commands::{SubscriptionRegistry, SubscriptionEntry};

let mut registry = SubscriptionRegistry::new();

// acquire returns true if this is the FIRST subscriber (should send WS subscribe)
let is_new = registry.acquire("todos", Some(params), Some(50));

// release returns true if this is the LAST subscriber (should send WS unsubscribe)
let was_last = registry.release("todos", Some(params));

// Get all active subscriptions (for reconnect)
let all: Vec<SubscriptionEntry> = registry.all_active();
```

---

## Events

### `entities_changed`

Emitted via Tauri event system when cache is updated:

```typescript
import { listen } from '@tauri-apps/api/event';

const unlisten = await listen('entities_changed', (event) => {
  const changes = event.payload.changes;
  // changes: Array<{
  //   entity: string,
  //   action: string,
  //   params?: Record<string, string>,
  //   requestIds?: string[],
  // }>

  for (const change of changes) {
    if (change.entity === 'todos') {
      // Invalidate React Query cache, re-read from Rust SQLite
      queryClient.invalidateQueries({ queryKey: ['todos', change.params] });
    }
  }
});
```

---

## Error Handling

All Tauri commands return `Result<T, String>`. Errors include:

| Error | Cause |
|-------|-------|
| `"WS not connected"` | Method called before WS connection established |
| `"Unknown entity: X"` | Entity not in schema |
| `"Timeout"` | Server didn't respond within deadline |
| `"Auth failed"` | AuthProvider returned invalid credentials |
| `"Cache error: ..."` | SQLite I/O error |
