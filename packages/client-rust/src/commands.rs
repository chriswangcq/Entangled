//! Tauri commands — expose entity engine to the JS webview.
//!
//! These commands are the IPC bridge between Rust cache and React hooks.
//! Enable with the `tauri` feature flag.

use serde_json::Value;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::cache::{Cache, hash_params};
use crate::schema::SchemaRegistry;

/// Shared state for the entity engine, stored as Tauri managed state.
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

/// Get a list of entities from cache.
/// If cache miss, returns null (JS side should then fetch from WS).
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
    let params_hash = hash_params(&params_map);

    let cache = state.cache.read().await;
    match cache.get_list(&entity, params_hash) {
        Some(items) => Ok(Some(items.into_iter().cloned().collect())),
        None => Ok(None), // Cache miss — JS should fetch from WS
    }
}

/// Get a single entity item from cache.
#[cfg(feature = "tauri")]
#[tauri::command]
pub async fn entity_get(
    entity: String,
    id: String,
    state: tauri::State<'_, EntangledState>,
) -> Result<Option<Value>, String> {
    let cache = state.cache.read().await;
    Ok(cache.get(&entity, &id).cloned())
}

/// Store items in cache (called after WS fetch completes).
#[cfg(feature = "tauri")]
#[tauri::command]
pub async fn entity_cache_put_list(
    entity: String,
    params: Option<Value>,
    items: Vec<Value>,
    id_field: Option<String>,
    state: tauri::State<'_, EntangledState>,
) -> Result<(), String> {
    let params_map = params
        .and_then(|p| p.as_object().cloned())
        .unwrap_or_default();
    let params_hash = hash_params(&params_map);
    let id_key = id_field.unwrap_or_else(|| "id".to_string());

    let entries: Vec<(String, Value)> = items.into_iter()
        .filter_map(|item| {
            let id = item.get(&id_key)?.as_str()?.to_string();
            Some((id, item))
        })
        .collect();

    let mut cache = state.cache.write().await;
    cache.put_list(&entity, params_hash, entries);
    Ok(())
}

/// Store a single item in cache.
#[cfg(feature = "tauri")]
#[tauri::command]
pub async fn entity_cache_put(
    entity: String,
    id: String,
    data: Value,
    state: tauri::State<'_, EntangledState>,
) -> Result<(), String> {
    let mut cache = state.cache.write().await;
    cache.put(&entity, &id, data);
    Ok(())
}

/// Clear all cached data (e.g. on logout or reconnect).
#[cfg(feature = "tauri")]
#[tauri::command]
pub async fn entity_cache_clear(
    state: tauri::State<'_, EntangledState>,
) -> Result<(), String> {
    let mut cache = state.cache.write().await;
    cache.clear_all();
    Ok(())
}
