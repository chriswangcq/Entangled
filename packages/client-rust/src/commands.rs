//! Tauri commands — thin adapter exposing EntangledClient to JS webview.
//!
//! This module is feature-gated behind `tauri`.
//! Generic (non-Tauri) apps should use `EntangledClient` directly.

use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::path::PathBuf;

use crate::cache::{Cache, CacheKey};
use crate::push::{process_sync, SyncFrame};
use crate::schema::SchemaRegistry;

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
}

// Safety: Cache is strictly Send+Sync thanks to r2d2::Pool.
// RwLock<SchemaRegistry> handles concurrent reads.
unsafe impl Send for EntangledState {}
unsafe impl Sync for EntangledState {}

impl EntangledState {
    /// Create with in-memory cache (fallback).
    pub fn new() -> Self {
        Self {
            registry: Arc::new(std::sync::RwLock::new(SchemaRegistry::new())),
            cache: Arc::new(Cache::new_in_memory()),
            subscriptions: Arc::new(std::sync::RwLock::new(SubscriptionRegistry::new())),
        }
    }

    /// Create with persistent SQLite cache at the given directory.
    pub fn with_db_dir(dir: &PathBuf) -> Self {
        let db_path = dir.join("entangled_cache.db");
        Self {
            registry: Arc::new(std::sync::RwLock::new(SchemaRegistry::new())),
            cache: Arc::new(Cache::new(&db_path)),
            subscriptions: Arc::new(std::sync::RwLock::new(SubscriptionRegistry::new())),
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
    let changed = process_sync(&cache, &sync_frame, "id");
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
    state: tauri::State<'_, EntangledState>,
) -> Result<usize, String> {
    let key = make_key(&entity, params);
    let cache = &state.cache;
    let count = cache.prepend_older(&key, &items, has_more, "id");

    tracing::info!(
        "[Cache] {} prepend_page: {} items, has_more={}",
        entity, count, has_more
    );

    Ok(count)
}

// ── Optimistic Operations (Rust-native) ─────────────────────────────

/// Write a pending op into SQLite for immediate optimistic UI.
/// The UI calls `entity_list_merged` to read confirmed + pending data.
#[cfg(feature = "tauri")]
#[tauri::command]
pub async fn entity_optimistic_add(
    entity: String,
    params: Option<Value>,
    request_id: String,
    op: String,
    item_id: String,
    data: Value,
    state: tauri::State<'_, EntangledState>,
) -> Result<(), String> {
    let key = make_key(&entity, params);
    state.cache.add_pending_op(&key, &request_id, &op, &item_id, &data);
    tracing::debug!(
        "[Optimistic] {} add pending: op={}, id={}, rid={}",
        entity, op, item_id, request_id
    );
    Ok(())
}

/// Mark a pending op as failed.
#[cfg(feature = "tauri")]
#[tauri::command]
pub async fn entity_optimistic_fail(
    request_id: String,
    error: String,
    state: tauri::State<'_, EntangledState>,
) -> Result<(), String> {
    state.cache.fail_pending_op(&request_id, &error);
    tracing::debug!("[Optimistic] fail pending: rid={}, err={}", request_id, error);
    Ok(())
}

/// Remove a pending op (on mutation success or manual cleanup).
#[cfg(feature = "tauri")]
#[tauri::command]
pub async fn entity_optimistic_remove(
    request_id: String,
    state: tauri::State<'_, EntangledState>,
) -> Result<(), String> {
    state.cache.remove_pending_op(&request_id);
    Ok(())
}

/// Read confirmed items + pending ops merged (the sole read path for optimistic UI).
/// Returns items with `_status` metadata injected.
#[cfg(feature = "tauri")]
#[tauri::command]
pub async fn entity_list_merged(
    entity: String,
    params: Option<Value>,
    state: tauri::State<'_, EntangledState>,
) -> Result<Vec<Value>, String> {
    let key = make_key(&entity, params);
    Ok(state.cache.get_list_with_pending(&key))
}
