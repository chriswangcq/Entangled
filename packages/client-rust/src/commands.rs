//! Tauri commands — thin adapter exposing EntangledClient to JS webview.
//!
//! This module is feature-gated behind `tauri`.
//! Generic (non-Tauri) apps should use `EntangledClient` directly.

use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use crate::cache::{Cache, CacheKey};
use crate::push::{process_sync_with_contract, SyncFrame};
use crate::schema::{EntitySchema, SchemaRegistry};

// ── Subscription Registry (monotonic, Rust-owned) ───────────────────────

/// One active subscription tracked by the registry.
#[derive(Debug, Clone)]
pub struct SubscriptionEntry {
    pub entity: String,
    pub params: Option<Value>,
    pub depth: Option<u64>,
}

/// Monotonic subscription registry.
///
/// The app subscribes every server-advertised entity for the current user once
/// schema arrives.  Subscriptions stay active until the whole registry is
/// cleared on identity/schema reset.  On reconnect, `active_entries()` provides
/// the full set to re-send — no React involvement required.
pub struct SubscriptionRegistry {
    entries: HashMap<String, SubscriptionEntry>,
}

impl SubscriptionRegistry {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Ensure a subscription exists.  Returns `true` only when a new entry is
    /// inserted and the caller should send a WS subscribe.
    pub fn acquire(&mut self, entity: &str, params: Option<Value>, depth: Option<u64>) -> bool {
        let key = subscription_key(entity, &params);
        if let Some(entry) = self.entries.get_mut(&key) {
            if depth.is_some() {
                entry.depth = depth;
            }
            false
        } else {
            self.entries.insert(
                key,
                SubscriptionEntry {
                    entity: entity.to_string(),
                    params,
                    depth,
                },
            );
            true
        }
    }

    /// All active entries — used for reconnect resubscribe.
    pub fn active_entries(&self) -> Vec<&SubscriptionEntry> {
        self.entries.values().collect()
    }

    /// Clear all subscriptions (e.g. on logout / user switch).
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

/// Build a deterministic subscription key from (entity, params).
/// Mirrors the TypeScript `subscriptionKey()` for consistency.
fn subscription_key(entity: &str, params: &Option<Value>) -> String {
    match params {
        Some(Value::Object(map)) if !map.is_empty() => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let parts: Vec<String> = keys
                .iter()
                .map(|k| {
                    let v = map.get(*k).and_then(|v| v.as_str()).unwrap_or("");
                    format!("{}={}", k, v)
                })
                .collect();
            format!("{}\0{}", entity, parts.join("&"))
        }
        _ => entity.to_string(),
    }
}

/// Shared state for Tauri — wraps cache in std::sync::Mutex (rusqlite is !Sync).
pub struct EntangledState {
    pub registry: Arc<std::sync::RwLock<SchemaRegistry>>,
    pub cache: Arc<Cache>,
    pub subscriptions: Arc<std::sync::RwLock<SubscriptionRegistry>>,
    /// Highest server-advertised Sync Contract version from direct Entangled WS `schema` push.
    pub sync_contract_version: Arc<AtomicU32>,
    pub schema_notify: Arc<tokio::sync::Notify>,
}

// Safety: Cache is strictly Send+Sync thanks to r2d2::Pool.
// RwLock<SchemaRegistry> handles concurrent reads.
unsafe impl Send for EntangledState {}
unsafe impl Sync for EntangledState {}

impl EntangledState {
    /// Create with in-memory cache for tests and embedded callers.
    pub fn new() -> Self {
        Self {
            registry: Arc::new(std::sync::RwLock::new(SchemaRegistry::new())),
            cache: Arc::new(Cache::new_in_memory()),
            subscriptions: Arc::new(std::sync::RwLock::new(SubscriptionRegistry::new())),
            sync_contract_version: Arc::new(AtomicU32::new(0)),
            schema_notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Create with persistent SQLite cache at the given directory.
    pub fn with_db_dir(dir: &PathBuf) -> Self {
        let db_path = dir.join("entangled_cache.db");
        Self {
            registry: Arc::new(std::sync::RwLock::new(SchemaRegistry::new())),
            cache: Arc::new(Cache::new(&db_path)),
            subscriptions: Arc::new(std::sync::RwLock::new(SubscriptionRegistry::new())),
            sync_contract_version: Arc::new(AtomicU32::new(0)),
            schema_notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Create with per-user SQLite cache (recommended for multi-user apps).
    pub fn with_user_db(dir: &PathBuf, user_id: &str) -> Self {
        let user_dir = dir.join(user_id);
        std::fs::create_dir_all(&user_dir).ok();
        let db_path = user_dir.join("entangled.db");
        Self {
            registry: Arc::new(std::sync::RwLock::new(SchemaRegistry::new())),
            cache: Arc::new(Cache::new(&db_path)),
            subscriptions: Arc::new(std::sync::RwLock::new(SubscriptionRegistry::new())),
            sync_contract_version: Arc::new(AtomicU32::new(0)),
            schema_notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Record server-advertised contract version (monotonic max).
    pub fn set_sync_contract_version(&self, version: u32) {
        let mut cur = self.sync_contract_version.load(Ordering::Acquire);
        while version > cur {
            match self.sync_contract_version.compare_exchange_weak(
                cur,
                version,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    tracing::info!(
                        target: "entangled_sync_contract",
                        version,
                        "sync contract version updated"
                    );
                    return;
                }
                Err(c) => cur = c,
            }
        }
    }

    pub fn get_sync_contract_version(&self) -> u32 {
        self.sync_contract_version.load(Ordering::Acquire)
    }

    pub fn register_schema(&self, entities: Vec<EntitySchema>, sync_contract_version: u32) {
        let count = entities.len();
        {
            let mut registry = self.registry.write().unwrap();
            registry.register_all(entities);
        }
        self.set_sync_contract_version(sync_contract_version);
        self.schema_notify.notify_waiters();
        tracing::info!(
            target: "entangled_schema",
            count,
            sync_contract_version,
            "registered schema from direct Entangled WS"
        );
    }

    pub fn schema_snapshot(&self) -> (Vec<EntitySchema>, u32) {
        let rows = self.registry.read().unwrap().all();
        (rows, self.get_sync_contract_version())
    }

    pub async fn wait_schema_snapshot(
        &self,
        timeout_ms: u64,
    ) -> Result<(Vec<EntitySchema>, u32), String> {
        let deadline =
            tokio::time::Instant::now() + std::time::Duration::from_millis(timeout_ms.max(1));
        loop {
            let (rows, version) = self.schema_snapshot();
            if !rows.is_empty() {
                return Ok((rows, version));
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Err(format!(
                    "Entangled schema not registered after {}ms",
                    timeout_ms
                ));
            }
            let remaining = deadline.saturating_duration_since(now);
            if tokio::time::timeout(remaining, self.schema_notify.notified())
                .await
                .is_err()
            {
                return Err(format!(
                    "Entangled schema not registered after {}ms",
                    timeout_ms
                ));
            }
        }
    }
}

// ── Helper: build CacheKey from (entity, params) ─────────────────────────

fn make_key(entity: &str, params: Option<Value>) -> CacheKey {
    let params_map = params
        .and_then(|p| p.as_object().cloned())
        .unwrap_or_default();
    if params_map.is_empty() {
        CacheKey::new_empty(entity)
    } else {
        CacheKey::new(entity, &params_map)
    }
}

fn params_object(params: &Option<Value>) -> serde_json::Map<String, Value> {
    params
        .as_ref()
        .and_then(|p| p.as_object().cloned())
        .unwrap_or_default()
}

fn values_match(actual: &Value, expected: &Value) -> bool {
    if actual == expected {
        return true;
    }
    scalar_to_string(actual)
        .zip(scalar_to_string(expected))
        .map(|(a, e)| a == e)
        .unwrap_or(false)
}

fn scalar_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

fn row_matches_params(row: &Value, params: &serde_json::Map<String, Value>) -> bool {
    params.iter().all(|(key, expected)| {
        row.get(key)
            .map(|actual| values_match(actual, expected))
            .unwrap_or(false)
    })
}

fn item_id(item: &Value, id_field: &str) -> Option<String> {
    item.get(id_field).and_then(|v| match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    })
}

fn merge_global_then_exact(
    global_filtered: Vec<Value>,
    exact: Vec<Value>,
    id_field: &str,
) -> Vec<Value> {
    let mut seen = HashSet::new();
    let mut out = Vec::with_capacity(global_filtered.len() + exact.len());

    for item in global_filtered.into_iter().chain(exact.into_iter()) {
        if let Some(id) = item_id(&item, id_field) {
            if seen.insert(id) {
                out.push(item);
            }
        } else {
            out.push(item);
        }
    }

    out
}

fn id_field_for_entity(state: &EntangledState, entity: &str) -> String {
    state
        .registry
        .read()
        .unwrap()
        .get(entity)
        .and_then(|s| s.id_field.clone())
        .unwrap_or_else(|| "id".to_string())
}

fn is_stream_entity(state: &EntangledState, entity: &str) -> bool {
    state
        .registry
        .read()
        .unwrap()
        .get(entity)
        .and_then(|s| s.sync_type.as_deref())
        == Some("stream")
}

fn filtered_user_scope_list(
    entity: &str,
    params: Option<Value>,
    state: &EntangledState,
) -> Vec<Value> {
    let params_map = params_object(&params);
    let cache = &state.cache;
    let exact = cache.get_list(&make_key(entity, params.clone()));

    if params_map.is_empty() {
        return exact;
    }

    if is_stream_entity(state, entity) {
        return exact;
    }

    let global = cache.get_list(&CacheKey::new_empty(entity));
    let global_filtered = global
        .into_iter()
        .filter(|row| row_matches_params(row, &params_map))
        .collect::<Vec<_>>();
    if global_filtered.is_empty() {
        return exact;
    }

    let id_field = id_field_for_entity(state, entity);
    merge_global_then_exact(global_filtered, exact, &id_field)
}

fn filtered_user_scope_item(
    entity: &str,
    id: &str,
    params: Option<Value>,
    state: &EntangledState,
) -> Option<Value> {
    let params_map = params_object(&params);
    let cache = &state.cache;

    if params_map.is_empty() {
        return cache.get_item(&CacheKey::new_empty(entity), id);
    }

    if is_stream_entity(state, entity) {
        return cache.get_item(&make_key(entity, params), id);
    }

    let global = cache.get_item(&CacheKey::new_empty(entity), id);
    if let Some(row) = global {
        if row_matches_params(&row, &params_map) {
            return Some(row);
        }
    }

    cache.get_item(&make_key(entity, params), id)
}

#[cfg(test)]
mod user_scope_read_tests {
    use super::{filtered_user_scope_item, filtered_user_scope_list, make_key, EntangledState};
    use crate::cache::CacheKey;
    use crate::schema::EntitySchema;
    use serde_json::json;

    fn state_with_messages(sync_type: &str) -> EntangledState {
        let state = EntangledState::new();
        state
            .registry
            .write()
            .unwrap()
            .register_all(vec![EntitySchema {
                name: "messages".to_string(),
                key_params: vec!["agent_id".to_string()],
                push_events: vec![],
                id_field: Some("id".to_string()),
                sync_type: Some(sync_type.to_string()),
                sync_limit: Some(50),
                subscription_mode: Some("lazy".to_string()),
                capabilities: None,
            }]);
        state.cache.apply_snapshot(
            &CacheKey::new_empty("messages"),
            &[
                json!({"id": "m1", "agent_id": "a1", "text": "hello"}),
                json!({"id": "m2", "agent_id": "a2", "text": "other"}),
            ],
            1,
            "id",
            false,
        );
        state
    }

    #[test]
    fn non_stream_scoped_list_reads_from_unscoped_user_cache() {
        let state = state_with_messages("list");
        let rows = filtered_user_scope_list("messages", Some(json!({"agent_id": "a1"})), &state);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"], "m1");
    }

    #[test]
    fn non_stream_scoped_item_reads_from_unscoped_user_cache() {
        let state = state_with_messages("list");
        let row =
            filtered_user_scope_item("messages", "m1", Some(json!({"agent_id": "a1"})), &state)
                .expect("row should come from unscoped cache");
        assert_eq!(row["text"], "hello");

        let miss =
            filtered_user_scope_item("messages", "m2", Some(json!({"agent_id": "a1"})), &state);
        assert!(miss.is_none());
    }

    #[test]
    fn stream_scoped_list_uses_exact_cache_key_only() {
        let state = state_with_messages("stream");
        let params = json!({"agent_id": "a1"});
        state.cache.apply_snapshot(
            &make_key("messages", Some(params.clone())),
            &[json!({"id": "m3", "agent_id": "a1", "text": "scoped fresh"})],
            2,
            "id",
            false,
        );

        let rows = filtered_user_scope_list("messages", Some(params), &state);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"], "m3");
    }

    #[test]
    fn stream_scoped_item_uses_exact_cache_key_only() {
        let state = state_with_messages("stream");
        let params = json!({"agent_id": "a1"});
        state.cache.apply_snapshot(
            &make_key("messages", Some(params.clone())),
            &[json!({"id": "m3", "agent_id": "a1", "text": "scoped fresh"})],
            2,
            "id",
            false,
        );

        assert!(filtered_user_scope_item("messages", "m1", Some(params.clone()), &state).is_none());
        let row = filtered_user_scope_item("messages", "m3", Some(params), &state)
            .expect("row should come from exact scoped stream cache");
        assert_eq!(row["text"], "scoped fresh");
    }
}

/// Get list from SQLite cache (read path — always local, never hits the server).
/// Empty vec means no rows for this key (cold or legitimately empty after sync).
#[cfg(feature = "tauri")]
#[tauri::command]
pub async fn entity_list(
    entity: String,
    params: Option<Value>,
    state: tauri::State<'_, EntangledState>,
) -> Result<Vec<Value>, String> {
    Ok(filtered_user_scope_list(&entity, params, &state))
}

/// Get single item from cache.
#[cfg(feature = "tauri")]
#[tauri::command]
pub async fn entity_get(
    entity: String,
    id: String,
    params: Option<Value>,
    state: tauri::State<'_, EntangledState>,
) -> Result<Option<Value>, String> {
    Ok(filtered_user_scope_item(&entity, &id, params, &state))
}

/// Get current version for an entity (for subscribe with since_version).
#[cfg(feature = "tauri")]
#[tauri::command]
pub async fn entity_version(
    entity: String,
    params: Option<Value>,
    state: tauri::State<'_, EntangledState>,
) -> Result<Option<u64>, String> {
    let key = make_key(&entity, params);
    let cache = &state.cache;
    Ok(cache.get_version(&key))
}

/// Process a sync frame from the server.
#[cfg(feature = "tauri")]
#[tauri::command]
pub async fn entity_apply_sync(
    frame: Value,
    state: tauri::State<'_, EntangledState>,
) -> Result<Option<String>, String> {
    let sync_frame: SyncFrame =
        serde_json::from_value(frame).map_err(|e| format!("Invalid sync frame: {}", e))?;

    let cache = &state.cache;
    let ver = state.get_sync_contract_version();
    let changed = process_sync_with_contract(&cache, &sync_frame, ver);
    Ok(changed.map(|c| c.entity))
}

/// Check if a stream entity has more older items in cache.
#[cfg(feature = "tauri")]
#[tauri::command]
pub async fn entity_has_more(
    entity: String,
    params: Option<Value>,
    state: tauri::State<'_, EntangledState>,
) -> Result<bool, String> {
    let key = make_key(&entity, params);
    let cache = &state.cache;
    Ok(cache.has_more_before(&key))
}

/// Prepend older items into cache (called after JS fetches a page via WS).
/// Returns the number of items prepended.
#[cfg(feature = "tauri")]
#[tauri::command]
pub async fn entity_prepend_page(
    entity: String,
    params: Option<Value>,
    items: Vec<Value>,
    has_more: bool,
    id_field: Option<String>,
    state: tauri::State<'_, EntangledState>,
) -> Result<usize, String> {
    let key = make_key(&entity, params);
    let cache = &state.cache;
    let id_field = id_field
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "entity_prepend_page requires id_field".to_string())?;
    let count = cache.prepend_older(&key, &items, has_more, id_field);

    tracing::info!(
        "[Cache] {} prepend_page: {} items, has_more={}",
        entity,
        count,
        has_more
    );

    Ok(count)
}
