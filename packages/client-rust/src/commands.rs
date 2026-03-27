//! Tauri commands — thin adapter exposing EntangledClient to JS webview.
//!
//! This module is feature-gated behind `tauri` and is NovAIC-specific.
//! Generic apps should use `EntangledClient` directly.

use serde_json::Value;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::path::PathBuf;

use crate::cache::{Cache, CacheKey};
use crate::push::{process_sync, SyncFrame};
use crate::schema::SchemaRegistry;

/// Shared state for Tauri — wraps cache in std::sync::Mutex (rusqlite is !Sync).
pub struct EntangledState {
    pub registry: Arc<StdMutex<SchemaRegistry>>,
    pub cache: Arc<StdMutex<Cache>>,
}

// Safety: StdMutex<Cache> is Send+Sync even though Cache contains rusqlite::Connection (!Sync).
// StdMutex provides exclusive access, so no concurrent borrows are possible.
unsafe impl Send for EntangledState {}
unsafe impl Sync for EntangledState {}

impl EntangledState {
    /// Create with in-memory cache (fallback).
    pub fn new() -> Self {
        Self {
            registry: Arc::new(StdMutex::new(SchemaRegistry::new())),
            cache: Arc::new(StdMutex::new(Cache::new_in_memory())),
        }
    }

    /// Create with persistent SQLite cache at the given directory.
    pub fn with_db_dir(dir: &PathBuf) -> Self {
        let db_path = dir.join("entangled_cache.db");
        Self {
            registry: Arc::new(StdMutex::new(SchemaRegistry::new())),
            cache: Arc::new(StdMutex::new(Cache::new(&db_path))),
        }
    }

    /// Create with per-user SQLite cache (recommended for multi-user apps).
    pub fn with_user_db(dir: &PathBuf, user_id: &str) -> Self {
        let user_dir = dir.join(user_id);
        std::fs::create_dir_all(&user_dir).ok();
        let db_path = user_dir.join("entangled.db");
        Self {
            registry: Arc::new(StdMutex::new(SchemaRegistry::new())),
            cache: Arc::new(StdMutex::new(Cache::new(&db_path))),
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

/// Get list from cache. Returns null if not cached or stale.
#[cfg(feature = "tauri")]
#[tauri::command]
pub async fn entity_list(
    entity: String,
    params: Option<Value>,
    state: tauri::State<'_, EntangledState>,
) -> Result<Option<Vec<Value>>, String> {
    let key = make_key(&entity, params);
    let cache = state.cache.lock().unwrap();
    if cache.is_fresh(&key) {
        Ok(Some(cache.get_list(&key)))
    } else {
        Ok(None)
    }
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
    let cache = state.cache.lock().unwrap();
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
    let cache = state.cache.lock().unwrap();
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

    let cache = state.cache.lock().unwrap();
    let changed = process_sync(&cache, &sync_frame, "id");
    Ok(changed.map(|c| c.entity))
}

/// Clear all cache (on logout / reconnect).
#[cfg(feature = "tauri")]
#[tauri::command]
pub async fn entity_cache_clear(
    state: tauri::State<'_, EntangledState>,
) -> Result<(), String> {
    let cache = state.cache.lock().unwrap();
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
    let cache = state.cache.lock().unwrap();
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
    let cache = state.cache.lock().unwrap();
    let count = cache.prepend_older(&key, &items, has_more, "id");

    tracing::info!(
        "[Cache] {} prepend_page: {} items, has_more={}",
        entity, count, has_more
    );

    Ok(count)
}
