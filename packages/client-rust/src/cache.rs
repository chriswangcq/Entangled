//! Entity cache — local in-memory store with staleness tracking.

use serde_json::Value;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Cache entry for a single entity item.
#[derive(Debug, Clone)]
struct CacheEntry {
    data: Value,
    fetched_at: Instant,
    stale: bool,
}

/// Cache key for a list query: (entity_name, sorted params hash).
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct ListKey {
    entity: String,
    params_hash: u64,
}

/// Per-entity cache with items + list ordering.
#[derive(Debug, Default)]
struct EntityCache {
    /// id → entry
    items: HashMap<String, CacheEntry>,
    /// list_key → ordered item IDs
    lists: HashMap<u64, ListCacheEntry>,
}

#[derive(Debug, Clone)]
struct ListCacheEntry {
    ids: Vec<String>,
    fetched_at: Instant,
    stale: bool,
}

/// The main entity cache — holds all entities.
#[derive(Debug, Default)]
pub struct Cache {
    entities: HashMap<String, EntityCache>,
    /// Default stale duration
    default_stale: Duration,
}

impl Cache {
    pub fn new() -> Self {
        Self {
            entities: HashMap::new(),
            default_stale: Duration::from_secs(30),
        }
    }

    // ── Read ────────────────────────────────────────────────────

    /// Get a single item from cache. Returns None if not cached or stale.
    pub fn get(&self, entity: &str, id: &str) -> Option<&Value> {
        let ec = self.entities.get(entity)?;
        let entry = ec.items.get(id)?;
        if entry.stale {
            return None;
        }
        if entry.fetched_at.elapsed() > self.default_stale {
            return None;
        }
        Some(&entry.data)
    }

    /// Get a list from cache. Returns None if not cached or stale.
    pub fn get_list(&self, entity: &str, params_hash: u64) -> Option<Vec<&Value>> {
        let ec = self.entities.get(entity)?;
        let list_entry = ec.lists.get(&params_hash)?;
        if list_entry.stale {
            return None;
        }
        if list_entry.fetched_at.elapsed() > self.default_stale {
            return None;
        }
        let items: Vec<&Value> = list_entry.ids.iter()
            .filter_map(|id| ec.items.get(id).map(|e| &e.data))
            .collect();
        Some(items)
    }

    // ── Write ───────────────────────────────────────────────────

    /// Store a single item in the cache.
    pub fn put(&mut self, entity: &str, id: &str, data: Value) {
        let ec = self.entities.entry(entity.to_string()).or_default();
        ec.items.insert(id.to_string(), CacheEntry {
            data,
            fetched_at: Instant::now(),
            stale: false,
        });
    }

    /// Store a list result in the cache.
    pub fn put_list(&mut self, entity: &str, params_hash: u64, items: Vec<(String, Value)>) {
        let ec = self.entities.entry(entity.to_string()).or_default();
        let ids: Vec<String> = items.iter().map(|(id, _)| id.clone()).collect();

        for (id, data) in items {
            ec.items.insert(id, CacheEntry {
                data,
                fetched_at: Instant::now(),
                stale: false,
            });
        }

        ec.lists.insert(params_hash, ListCacheEntry {
            ids,
            fetched_at: Instant::now(),
            stale: false,
        });
    }

    // ── Invalidation ────────────────────────────────────────────

    /// Mark a specific item as stale.
    pub fn mark_stale(&mut self, entity: &str, id: &str) {
        if let Some(ec) = self.entities.get_mut(entity) {
            if let Some(entry) = ec.items.get_mut(id) {
                entry.stale = true;
            }
            // Also mark all lists containing this entity as stale
            for list in ec.lists.values_mut() {
                list.stale = true;
            }
        }
    }

    /// Mark all items of an entity (optionally filtered by params) as stale.
    pub fn mark_entity_stale(&mut self, entity: &str) {
        if let Some(ec) = self.entities.get_mut(entity) {
            for entry in ec.items.values_mut() {
                entry.stale = true;
            }
            for list in ec.lists.values_mut() {
                list.stale = true;
            }
        }
    }

    /// Remove a specific item from the cache.
    pub fn remove(&mut self, entity: &str, id: &str) {
        if let Some(ec) = self.entities.get_mut(entity) {
            ec.items.remove(id);
            // Mark all lists as stale since ordering may have changed
            for list in ec.lists.values_mut() {
                list.stale = true;
            }
        }
    }

    /// Clear all cached data (e.g. on reconnect).
    pub fn clear_all(&mut self) {
        self.entities.clear();
    }
}

/// Hash params deterministically for use as list cache key.
pub fn hash_params(params: &serde_json::Map<String, Value>) -> u64 {
    use std::hash::{Hash, Hasher};
    use std::collections::hash_map::DefaultHasher;

    let mut hasher = DefaultHasher::new();
    // Sort keys for deterministic hashing
    let mut keys: Vec<&String> = params.keys().collect();
    keys.sort();
    for key in keys {
        key.hash(&mut hasher);
        if let Some(v) = params.get(key) {
            v.to_string().hash(&mut hasher);
        }
    }
    hasher.finish()
}
