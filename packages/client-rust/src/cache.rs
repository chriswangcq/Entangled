//! Entity cache — local store with Git-like version tracking + delta application.
//!
//! Each (entity, params) has a version number. Deltas (insert/update/delete ops)
//! are applied directly to the cache without re-fetching from the server.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::Instant;

// ── Sync Op (matches protocol) ──────────────────────────────────

/// A single mutation operation, received from server delta sync.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncOp {
    pub version: u64,
    pub op: String,   // "insert" | "update" | "delete" | "invalidate"
    pub id: String,
    pub data: Option<Value>,
    #[serde(default)]
    pub ts: f64,
    /// Correlation ID — traces back to the WS request that caused this op
    #[serde(default)]
    pub request_id: Option<String>,
}

// ── Per-(entity, params) cache ──────────────────────────────────

/// Cache for a single (entity, params) combination.
#[derive(Debug)]
pub struct EntityData {
    /// Items keyed by ID
    pub items: HashMap<String, Value>,
    /// Ordered list of IDs (insertion order)
    pub order: Vec<String>,
    /// Current synced version (like git HEAD commit count)
    pub version: u64,
    /// Whether this entity has an active subscription
    pub subscribed: bool,
    /// Last sync time
    pub synced_at: Option<Instant>,
}

impl Default for EntityData {
    fn default() -> Self {
        Self {
            items: HashMap::new(),
            order: Vec::new(),
            version: 0,
            subscribed: false,
            synced_at: None,
        }
    }
}

impl EntityData {
    /// Apply a full snapshot (git clone / re-clone).
    pub fn apply_snapshot(&mut self, items: Vec<Value>, version: u64, id_field: &str) {
        self.items.clear();
        self.order.clear();
        for item in items {
            if let Some(id) = item.get(id_field).and_then(|v| v.as_str()) {
                let id = id.to_string();
                self.order.push(id.clone());
                self.items.insert(id, item);
            }
        }
        self.version = version;
        self.synced_at = Some(Instant::now());
    }

    /// Apply delta ops (git pull fast-forward).
    /// Returns false if base_version doesn't match (need re-sync).
    pub fn apply_delta(&mut self, base_version: u64, ops: &[SyncOp], new_version: u64) -> bool {
        if base_version != self.version {
            tracing::warn!(
                "[Cache] Version mismatch: local={}, base={}, need resync",
                self.version, base_version
            );
            return false; // Need full re-sync
        }

        let mut has_invalidate = false;

        for op in ops {
            match op.op.as_str() {
                "insert" => {
                    if let Some(ref data) = op.data {
                        // O(1) check via HashMap, not O(n) Vec scan
                        if !self.items.contains_key(&op.id) {
                            self.order.push(op.id.clone());
                        }
                        self.items.insert(op.id.clone(), data.clone());
                    }
                }
                "update" => {
                    if let Some(ref patch) = op.data {
                        if let Some(existing) = self.items.get_mut(&op.id) {
                            // Merge patch into existing
                            if let (Some(obj), Some(p)) = (existing.as_object_mut(), patch.as_object()) {
                                for (k, v) in p {
                                    obj.insert(k.clone(), v.clone());
                                }
                            }
                        } else {
                            // Item doesn't exist locally, insert it
                            self.items.insert(op.id.clone(), patch.clone());
                            self.order.push(op.id.clone());
                        }
                    }
                }
                "delete" => {
                    self.items.remove(&op.id);
                    self.order.retain(|id| id != &op.id);
                }
                "invalidate" => {
                    has_invalidate = true;
                }
                _ => {
                    tracing::debug!("[Cache] Unknown op: {}", op.op);
                }
            }
        }

        self.version = new_version;
        // Don't mark as fresh if invalidated — will trigger re-subscribe
        if !has_invalidate {
            self.synced_at = Some(Instant::now());
        } else {
            self.synced_at = None;
        }
        true
    }

    /// Get all items in order.
    pub fn get_list(&self) -> Vec<&Value> {
        self.order.iter()
            .filter_map(|id| self.items.get(id))
            .collect()
    }

    /// Get a single item by ID.
    pub fn get(&self, id: &str) -> Option<&Value> {
        self.items.get(id)
    }

    /// Whether data is fresh (has been synced and not invalidated).
    pub fn is_fresh(&self) -> bool {
        self.synced_at.is_some()
    }
}

// ── Main cache ──────────────────────────────────────────────────

/// Cache key: (entity_name, params_hash)
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct CacheKey {
    pub entity: String,
    pub params_hash: u64,
}

impl CacheKey {
    pub fn new(entity: &str, params: &serde_json::Map<String, Value>) -> Self {
        Self {
            entity: entity.to_string(),
            params_hash: hash_params(params),
        }
    }

    pub fn new_empty(entity: &str) -> Self {
        Self {
            entity: entity.to_string(),
            params_hash: 0,
        }
    }
}

/// The main entity cache — holds all (entity, params) combinations.
#[derive(Debug, Default)]
pub struct Cache {
    data: HashMap<CacheKey, EntityData>,
}

impl Cache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get or create entity data for a cache key.
    pub fn entry(&mut self, key: CacheKey) -> &mut EntityData {
        self.data.entry(key).or_default()
    }

    /// Get entity data (read-only).
    pub fn get(&self, key: &CacheKey) -> Option<&EntityData> {
        self.data.get(key)
    }

    /// Remove entity data.
    pub fn remove(&mut self, key: &CacheKey) {
        self.data.remove(key);
    }

    /// Clear all.
    pub fn clear_all(&mut self) {
        self.data.clear();
    }

    /// Get all cache keys for a given entity name.
    pub fn keys_for_entity(&self, entity: &str) -> Vec<&CacheKey> {
        self.data.keys()
            .filter(|k| k.entity == entity)
            .collect()
    }
}

/// Hash params deterministically.
pub fn hash_params(params: &serde_json::Map<String, Value>) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
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
