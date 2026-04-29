//! Tauri commands — thin adapter exposing EntangledClient to JS webview.
//!
//! This module is feature-gated behind `tauri`.
//! Generic (non-Tauri) apps should use `EntangledClient` directly.

use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use crate::cache::{Cache, CacheKey};
use crate::push::{process_sync_with_contract, SyncFrame};
use crate::schema::{EntitySchema, SchemaRegistry};

// ── Subscription Registry (ref-counted, Rust-owned) ─────────────────────

/// One active subscription tracked by the registry.
#[derive(Debug, Clone)]
pub struct SubscriptionEntry {
    pub entity: String,
    pub params: Option<Value>,
    pub depth: Option<u64>,
    pub ref_count: u32,
}

/// Ref-counted subscription registry.
///
/// Multiple React hooks (or eager bootstrap) may subscribe to the same
/// (entity, params) key.  The registry ensures only **one** WS subscribe
/// is sent when the first consumer acquires, and one unsubscribe when the
/// last consumer releases.
///
/// On reconnect, `active_entries()` provides the full set of subscriptions
/// to re-send — no React involvement required.
pub struct SubscriptionRegistry {
    entries: HashMap<String, SubscriptionEntry>,
}

impl SubscriptionRegistry {
    pub fn new() -> Self {
        Self { entries: HashMap::new() }
    }

    /// Acquire a subscription.  Returns `true` if this is the **first**
    /// subscriber (ref went 0 → 1) — the caller should send WS subscribe.
    pub fn acquire(
        &mut self,
        entity: &str,
        params: Option<Value>,
        depth: Option<u64>,
    ) -> bool {
        let key = subscription_key(entity, &params);
        if let Some(entry) = self.entries.get_mut(&key) {
            entry.ref_count += 1;
            // update depth if provided (stream may override)
            if depth.is_some() {
                entry.depth = depth;
            }
            false // already subscribed
        } else {
            self.entries.insert(key, SubscriptionEntry {
                entity: entity.to_string(),
                params,
                depth,
                ref_count: 1,
            });
            true // first subscriber
        }
    }

    /// Release a subscription.  Returns `true` if ref hit zero — the
    /// caller should send WS unsubscribe.
    pub fn release(&mut self, entity: &str, params: &Option<Value>) -> bool {
        let key = subscription_key(entity, params);
        if let Some(entry) = self.entries.get_mut(&key) {
            if entry.ref_count <= 1 {
                self.entries.remove(&key);
                return true; // last subscriber gone
            }
            entry.ref_count -= 1;
        }
        false
    }

    /// All active entries (ref > 0) — used for reconnect resubscribe.
    pub fn active_entries(&self) -> Vec<&SubscriptionEntry> {
        self.entries.values().filter(|e| e.ref_count > 0).collect()
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
            let parts: Vec<String> = keys.iter().map(|k| {
                let v = map.get(*k)
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                format!("{}={}", k, v)
            }).collect();
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
        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_millis(timeout_ms.max(1));
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

/// Get list from SQLite cache (read path — always local, never hits the server).
/// Empty vec means no rows for this key (cold or legitimately empty after sync).
#[cfg(feature = "tauri")]
#[tauri::command]
pub async fn entity_list(
    entity: String,
    params: Option<Value>,
    state: tauri::State<'_, EntangledState>,
) -> Result<Vec<Value>, String> {
    let key = make_key(&entity, params);
    let cache = &state.cache;
    Ok(cache.get_list(&key))
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
    let key = make_key(&entity, params);
    let cache = &state.cache;
    Ok(cache.get_item(&key, &id))
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
    let sync_frame: SyncFrame = serde_json::from_value(frame)
        .map_err(|e| format!("Invalid sync frame: {}", e))?;

    let cache = &state.cache;
    let ver = state.get_sync_contract_version();
    let changed = process_sync_with_contract(&cache, &sync_frame, ver);
    Ok(changed.map(|c| c.entity))
}

/// Clear all cache (on logout / reconnect).
#[cfg(feature = "tauri")]
#[tauri::command]
pub async fn entity_cache_clear(
    state: tauri::State<'_, EntangledState>,
) -> Result<(), String> {
    let cache = &state.cache;
    cache.clear_all();
    Ok(())
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
        entity, count, has_more
    );

    Ok(count)
}
