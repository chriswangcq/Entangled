//! Tauri commands — expose entity engine to JS webview.

use serde_json::Value;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::cache::{Cache, CacheKey, hash_params};
use crate::push::{process_sync, SyncFrame};
use crate::schema::SchemaRegistry;

/// Shared state for the entity engine.
pub struct EntangledState {
    pub registry: Arc<RwLock<SchemaRegistry>>,
    pub cache: Arc<RwLock<Cache>>,
}

impl EntangledState {
    pub fn new() -> Self {
        Self {
            registry: Arc::new(RwLock::new(SchemaRegistry::new())),
            cache: Arc::new(RwLock::new(Cache::new())),
        }
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
    let params_map = params
        .and_then(|p| p.as_object().cloned())
        .unwrap_or_default();
    let key = if params_map.is_empty() {
        CacheKey::new_empty(&entity)
    } else {
        CacheKey::new(&entity, &params_map)
    };

    let cache = state.cache.read().await;
    match cache.get(&key) {
        Some(data) if data.is_fresh() => {
            Ok(Some(data.get_list().into_iter().cloned().collect()))
        }
        _ => Ok(None), // Cache miss or stale → JS should subscribe
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
    let params_map = params
        .and_then(|p| p.as_object().cloned())
        .unwrap_or_default();
    let key = if params_map.is_empty() {
        CacheKey::new_empty(&entity)
    } else {
        CacheKey::new(&entity, &params_map)
    };

    let cache = state.cache.read().await;
    Ok(cache.get(&key).and_then(|d| d.get(&id).cloned()))
}

/// Get current version for an entity (for subscribe with since_version).
#[cfg(feature = "tauri")]
#[tauri::command]
pub async fn entity_version(
    entity: String,
    params: Option<Value>,
    state: tauri::State<'_, EntangledState>,
) -> Result<Option<u64>, String> {
    let params_map = params
        .and_then(|p| p.as_object().cloned())
        .unwrap_or_default();
    let key = if params_map.is_empty() {
        CacheKey::new_empty(&entity)
    } else {
        CacheKey::new(&entity, &params_map)
    };

    let cache = state.cache.read().await;
    Ok(cache.get(&key).map(|d| d.version))
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

    let mut cache = state.cache.write().await;
    let changed = process_sync(&mut cache, &sync_frame, "id");
    Ok(changed.map(|c| c.entity))
}

/// Clear all cache (on logout / reconnect).
#[cfg(feature = "tauri")]
#[tauri::command]
pub async fn entity_cache_clear(
    state: tauri::State<'_, EntangledState>,
) -> Result<(), String> {
    let mut cache = state.cache.write().await;
    cache.clear_all();
    Ok(())
}
