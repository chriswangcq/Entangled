//! Entity cache — persistent SQLite store with Git-like version tracking.
//!
//! Each (entity, params) has a version number and items stored in SQLite.
//! Deltas (insert/update/delete ops) are applied directly to the local DB.
//!
//! Two tables:
//!   - `entity_meta`: version, has_more_before, subscribed per (entity, params_hash)
//!   - `entity_items`: actual entity data as JSON blobs, ordered by `seq`

use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};

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

// ── Cache Key ───────────────────────────────────────────────────

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

// ── EntityMeta ──────────────────────────────────────────────────

/// Metadata for a single (entity, params) combination.
#[derive(Debug, Clone)]
pub struct EntityMeta {
    pub version: u64,
    pub subscribed: bool,
    pub has_more_before: bool,
}

impl Default for EntityMeta {
    fn default() -> Self {
        Self {
            version: 0,
            subscribed: false,
            has_more_before: false,
        }
    }
}

// ── Global seq counter for ordering ─────────────────────────────

static SEQ_COUNTER: AtomicI64 = AtomicI64::new(0);

fn next_seq() -> i64 {
    SEQ_COUNTER.fetch_add(1, Ordering::Relaxed)
}

// ── Main Cache (SQLite-backed) ──────────────────────────────────

/// The main entity cache — backed by a local SQLite database.
pub struct Cache {
    conn: Connection,
}

impl std::fmt::Debug for Cache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cache").field("backend", &"SQLite").finish()
    }
}

impl Default for Cache {
    fn default() -> Self {
        Self::new_in_memory()
    }
}

impl Cache {
    /// Create cache backed by a file.
    pub fn new(path: &Path) -> Self {
        let conn = Connection::open(path)
            .expect("Failed to open entity cache database");
        let cache = Self { conn };
        cache.init_schema();
        cache
    }

    /// Create cache in memory (for testing or ephemeral use).
    pub fn new_in_memory() -> Self {
        let conn = Connection::open_in_memory()
            .expect("Failed to create in-memory database");
        let cache = Self { conn };
        cache.init_schema();
        cache
    }

    fn init_schema(&self) {
        self.conn.execute_batch("
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            PRAGMA cache_size=-8000;
            PRAGMA temp_store=MEMORY;

            CREATE TABLE IF NOT EXISTS entity_meta (
                entity          TEXT NOT NULL,
                params_hash     INTEGER NOT NULL,
                version         INTEGER NOT NULL DEFAULT 0,
                subscribed      INTEGER NOT NULL DEFAULT 0,
                has_more        INTEGER NOT NULL DEFAULT 0,
                last_accessed   INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (entity, params_hash)
            );

            CREATE TABLE IF NOT EXISTS entity_items (
                entity      TEXT NOT NULL,
                params_hash INTEGER NOT NULL,
                item_id     TEXT NOT NULL,
                data        TEXT NOT NULL,
                seq         INTEGER NOT NULL,
                PRIMARY KEY (entity, params_hash, item_id)
            );

            CREATE INDEX IF NOT EXISTS idx_entity_items_seq
                ON entity_items (entity, params_hash, seq);
        ").expect("Failed to initialize entity cache schema");

        // Migrate: add last_accessed column if missing (existing DBs)
        let _ = self.conn.execute(
            "ALTER TABLE entity_meta ADD COLUMN last_accessed INTEGER NOT NULL DEFAULT 0",
            [],
        );

        // Init seq counter from max existing seq
        let max_seq: i64 = self.conn.query_row(
            "SELECT COALESCE(MAX(seq), 0) FROM entity_items",
            [],
            |row| row.get(0),
        ).unwrap_or(0);
        SEQ_COUNTER.store(max_seq + 1, Ordering::Relaxed);
    }

    /// Touch last_accessed timestamp for a cache key.
    fn touch(&self, key: &CacheKey) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        self.conn.execute(
            "UPDATE entity_meta SET last_accessed = ?3 WHERE entity = ?1 AND params_hash = ?2",
            params![key.entity, key.params_hash as i64, now],
        ).ok();
    }

    /// Evict cache entries not accessed in the last `max_age_secs` seconds.
    /// Returns number of entries evicted.
    pub fn gc_stale(&self, max_age_secs: u64) -> usize {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let cutoff = now - max_age_secs as i64;

        // Find stale keys
        let mut stmt = self.conn.prepare(
            "SELECT entity, params_hash FROM entity_meta WHERE last_accessed < ?1 AND last_accessed > 0"
        ).unwrap();
        let keys: Vec<(String, i64)> = stmt.query_map(
            params![cutoff],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        ).unwrap()
        .filter_map(|r| r.ok())
        .collect();

        let count = keys.len();
        for (entity, ph) in &keys {
            self.conn.execute(
                "DELETE FROM entity_items WHERE entity = ?1 AND params_hash = ?2",
                params![entity, ph],
            ).ok();
            self.conn.execute(
                "DELETE FROM entity_meta WHERE entity = ?1 AND params_hash = ?2",
                params![entity, ph],
            ).ok();
        }

        if count > 0 {
            tracing::info!("[Cache] GC: evicted {} stale entries (older than {}s)", count, max_age_secs);
        }
        count
    }

    // ── Meta operations ─────────────────────────────────────────

    /// Get metadata for a cache key.
    pub fn get_meta(&self, key: &CacheKey) -> EntityMeta {
        self.conn.query_row(
            "SELECT version, subscribed, has_more FROM entity_meta WHERE entity = ?1 AND params_hash = ?2",
            params![key.entity, key.params_hash as i64],
            |row| Ok(EntityMeta {
                version: row.get::<_, i64>(0)? as u64,
                subscribed: row.get::<_, bool>(1)?,
                has_more_before: row.get::<_, bool>(2)?,
            }),
        ).unwrap_or_default()
    }

    /// Upsert meta (single SQL, no SELECT+UPDATE dance).
    #[allow(dead_code)]
    fn upsert_meta(&self, key: &CacheKey, version: u64, subscribed: bool, has_more: bool) {
        self.conn.execute(
            "INSERT INTO entity_meta (entity, params_hash, version, subscribed, has_more)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(entity, params_hash) DO UPDATE SET
                version = excluded.version,
                subscribed = excluded.subscribed,
                has_more = excluded.has_more",
            params![key.entity, key.params_hash as i64, version as i64, subscribed, has_more],
        ).ok();
    }

    /// Set subscribed flag only.
    pub fn set_subscribed(&self, key: &CacheKey, subscribed: bool) {
        self.conn.execute(
            "INSERT INTO entity_meta (entity, params_hash, subscribed)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(entity, params_hash) DO UPDATE SET subscribed = excluded.subscribed",
            params![key.entity, key.params_hash as i64, subscribed],
        ).ok();
    }

    /// Get version (for subscribe with since_version).
    /// Returns `Some` after at least one successful sync (`subscribed`) or when version > 0.
    /// Server may report `version == 0` before any op-log bump; that is still a valid synced state.
    pub fn get_version(&self, key: &CacheKey) -> Option<u64> {
        let meta = self.get_meta(key);
        if meta.subscribed || meta.version > 0 {
            Some(meta.version)
        } else {
            None
        }
    }

    /// Whether data is fresh (version > 0, not invalidated).
    pub fn is_fresh(&self, key: &CacheKey) -> bool {
        self.get_meta(key).version > 0
    }

    /// Whether there are more older items.
    pub fn has_more_before(&self, key: &CacheKey) -> bool {
        self.get_meta(key).has_more_before
    }

    // ── Item operations ─────────────────────────────────────────

    /// Get all items in order. Also touches last_accessed for TTL.
    pub fn get_list(&self, key: &CacheKey) -> Vec<Value> {
        self.touch(key);
        let mut stmt = self.conn.prepare_cached(
            "SELECT data FROM entity_items WHERE entity = ?1 AND params_hash = ?2 ORDER BY seq ASC"
        ).unwrap();
        stmt.query_map(
            params![key.entity, key.params_hash as i64],
            |row| {
                let json_str: String = row.get(0)?;
                Ok(serde_json::from_str(&json_str).unwrap_or(Value::Null))
            },
        ).unwrap()
        .filter_map(|r| r.ok())
        .filter(|v| !v.is_null())
        .collect()
    }

    /// Get a single item by ID.
    pub fn get_item(&self, key: &CacheKey, id: &str) -> Option<Value> {
        self.conn.query_row(
            "SELECT data FROM entity_items WHERE entity = ?1 AND params_hash = ?2 AND item_id = ?3",
            params![key.entity, key.params_hash as i64, id],
            |row| {
                let json_str: String = row.get(0)?;
                Ok(serde_json::from_str(&json_str).unwrap_or(Value::Null))
            },
        ).ok()
    }

    /// Upsert a single item (used by public apply_delta internally — keep for future direct use).
    #[allow(dead_code)]
    fn upsert_item(&self, key: &CacheKey, id: &str, data: &Value) {
        let json_str = serde_json::to_string(data).unwrap_or_default();
        let seq = next_seq();
        self.conn.execute(
            "INSERT INTO entity_items (entity, params_hash, item_id, data, seq)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(entity, params_hash, item_id) DO UPDATE SET data = excluded.data",
            params![key.entity, key.params_hash as i64, id, json_str, seq],
        ).ok();
    }

    /// Delete a single item.
    #[allow(dead_code)]
    fn delete_item(&self, key: &CacheKey, id: &str) {
        self.conn.execute(
            "DELETE FROM entity_items WHERE entity = ?1 AND params_hash = ?2 AND item_id = ?3",
            params![key.entity, key.params_hash as i64, id],
        ).ok();
    }

    /// Delete all items for a key.
    fn clear_items(&self, key: &CacheKey) {
        self.conn.execute(
            "DELETE FROM entity_items WHERE entity = ?1 AND params_hash = ?2",
            params![key.entity, key.params_hash as i64],
        ).ok();
    }

    // ── Snapshot / Delta / Prepend ───────────────────────────────

    /// Apply a full snapshot (git clone / re-clone).
    /// Use `has_more = false` for full snapshots, `true` for head_n partial syncs.
    pub fn apply_snapshot(&self, key: &CacheKey, items: &[Value], version: u64, id_field: &str, has_more: bool) {
        // Wrap in transaction: DELETE + N INSERTs + meta update → 1 fsync
        let tx = self.conn.unchecked_transaction().unwrap();
        tx.execute(
            "DELETE FROM entity_items WHERE entity = ?1 AND params_hash = ?2",
            params![key.entity, key.params_hash as i64],
        ).ok();

        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO entity_items (entity, params_hash, item_id, data, seq) VALUES (?1, ?2, ?3, ?4, ?5)"
            ).unwrap();
            for item in items {
                if let Some(id) = item.get(id_field).and_then(|v| v.as_str()) {
                    let json_str = serde_json::to_string(item).unwrap_or_default();
                    let seq = next_seq();
                    stmt.execute(params![key.entity, key.params_hash as i64, id, json_str, seq]).ok();
                }
            }
        }

        tx.execute(
            "INSERT INTO entity_meta (entity, params_hash, version, subscribed, has_more)
             VALUES (?1, ?2, ?3, 1, ?4)
             ON CONFLICT(entity, params_hash) DO UPDATE SET
                version = excluded.version, subscribed = 1, has_more = excluded.has_more",
            params![key.entity, key.params_hash as i64, version as i64, has_more],
        ).ok();

        tx.commit().ok();

        tracing::info!(
            "[Cache] {} snapshot v{} ({} items, has_more={})",
            key.entity, version, items.len(), has_more
        );
    }

    /// Apply delta ops (git pull fast-forward).
    /// Returns false if base_version doesn't match (need re-sync).
    pub fn apply_delta(&self, key: &CacheKey, base_version: u64, ops: &[SyncOp], new_version: u64) -> bool {
        let meta = self.get_meta(key);
        if base_version != meta.version {
            tracing::warn!(
                "[Cache] Version mismatch: local={}, base={}, need resync",
                meta.version, base_version
            );
            return false;
        }

        // Wrap all ops in a single transaction
        let tx = self.conn.unchecked_transaction().unwrap();
        let mut has_invalidate = false;

        for op in ops {
            match op.op.as_str() {
                "insert" => {
                    if let Some(ref data) = op.data {
                        let json_str = serde_json::to_string(data).unwrap_or_default();
                        let seq = next_seq();
                        tx.execute(
                            "INSERT INTO entity_items (entity, params_hash, item_id, data, seq)
                             VALUES (?1, ?2, ?3, ?4, ?5)
                             ON CONFLICT(entity, params_hash, item_id) DO UPDATE SET data = excluded.data",
                            params![key.entity, key.params_hash as i64, op.id, json_str, seq],
                        ).ok();
                    }
                }
                "update" => {
                    if let Some(ref patch) = op.data {
                        // Read existing → merge → write back
                        let existing: Option<String> = tx.query_row(
                            "SELECT data FROM entity_items WHERE entity = ?1 AND params_hash = ?2 AND item_id = ?3",
                            params![key.entity, key.params_hash as i64, op.id],
                            |row| row.get(0),
                        ).ok();

                        let merged = if let Some(ref json_str) = existing {
                            let mut val: Value = serde_json::from_str(json_str).unwrap_or(Value::Null);
                            if let (Some(obj), Some(p)) = (val.as_object_mut(), patch.as_object()) {
                                for (k, v) in p {
                                    obj.insert(k.clone(), v.clone());
                                }
                            }
                            serde_json::to_string(&val).unwrap_or_default()
                        } else {
                            serde_json::to_string(patch).unwrap_or_default()
                        };

                        if existing.is_some() {
                            // Update data, keep seq
                            tx.execute(
                                "UPDATE entity_items SET data = ?4 WHERE entity = ?1 AND params_hash = ?2 AND item_id = ?3",
                                params![key.entity, key.params_hash as i64, op.id, merged],
                            ).ok();
                        } else {
                            // Insert new
                            let seq = next_seq();
                            tx.execute(
                                "INSERT INTO entity_items (entity, params_hash, item_id, data, seq) VALUES (?1, ?2, ?3, ?4, ?5)",
                                params![key.entity, key.params_hash as i64, op.id, merged, seq],
                            ).ok();
                        }
                    }
                }
                "delete" => {
                    tx.execute(
                        "DELETE FROM entity_items WHERE entity = ?1 AND params_hash = ?2 AND item_id = ?3",
                        params![key.entity, key.params_hash as i64, op.id],
                    ).ok();
                }
                "invalidate" => {
                    has_invalidate = true;
                }
                _ => {
                    tracing::debug!("[Cache] Unknown op: {}", op.op);
                }
            }
        }

        // Update version
        let new_ver = if has_invalidate { 0i64 } else { new_version as i64 };
        tx.execute(
            "INSERT INTO entity_meta (entity, params_hash, version)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(entity, params_hash) DO UPDATE SET version = excluded.version",
            params![key.entity, key.params_hash as i64, new_ver],
        ).ok();

        tx.commit().ok();
        true
    }

    /// Prepend older items to the front (stream backward pagination).
    /// Returns number of items actually prepended.
    pub fn prepend_older(&self, key: &CacheKey, items: &[Value], has_more: bool, id_field: &str) -> usize {
        // Get the current minimum seq for this entity
        let min_seq: i64 = self.conn.query_row(
            "SELECT COALESCE(MIN(seq), 0) FROM entity_items WHERE entity = ?1 AND params_hash = ?2",
            params![key.entity, key.params_hash as i64],
            |row| row.get(0),
        ).unwrap_or(0);

        // Items come in newest-first order from server (DESC), reverse to get chronological
        let items_reversed: Vec<&Value> = items.iter().rev().collect();
        let total = items_reversed.len() as i64;

        let tx = self.conn.unchecked_transaction().unwrap();
        let mut count = 0usize;

        {
            let mut insert_stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO entity_items (entity, params_hash, item_id, data, seq) VALUES (?1, ?2, ?3, ?4, ?5)"
            ).unwrap();

            for (i, item) in items_reversed.iter().enumerate() {
                if let Some(id) = item.get(id_field).and_then(|v| v.as_str()) {
                    let json_str = serde_json::to_string(item).unwrap_or_default();
                    let seq = min_seq - total + i as i64;
                    let inserted = insert_stmt.execute(
                        params![key.entity, key.params_hash as i64, id, json_str, seq],
                    ).unwrap_or(0);
                    if inserted > 0 {
                        count += 1;
                    }
                }
            }
        }

        // Update has_more
        tx.execute(
            "INSERT INTO entity_meta (entity, params_hash, has_more)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(entity, params_hash) DO UPDATE SET has_more = excluded.has_more",
            params![key.entity, key.params_hash as i64, has_more],
        ).ok();

        tx.commit().ok();

        tracing::info!(
            "[Cache] {} prepend: {} items (of {}), has_more={}",
            key.entity, count, items.len(), has_more
        );
        count
    }

    // ── Cache management ────────────────────────────────────────

    /// Remove all data for a key.
    pub fn remove(&self, key: &CacheKey) {
        self.clear_items(key);
        self.conn.execute(
            "DELETE FROM entity_meta WHERE entity = ?1 AND params_hash = ?2",
            params![key.entity, key.params_hash as i64],
        ).ok();
    }

    /// Clear everything.
    pub fn clear_all(&self) {
        self.conn.execute_batch("
            DELETE FROM entity_items;
            DELETE FROM entity_meta;
        ").ok();
    }

    /// Get all cache keys for a given entity name.
    pub fn keys_for_entity(&self, entity: &str) -> Vec<CacheKey> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT params_hash FROM entity_meta WHERE entity = ?1"
        ).unwrap();
        stmt.query_map(
            params![entity],
            |row| {
                let ph: i64 = row.get(0)?;
                Ok(CacheKey {
                    entity: entity.to_string(),
                    params_hash: ph as u64,
                })
            },
        ).unwrap()
        .filter_map(|r| r.ok())
        .collect()
    }

    /// Get item count for a key.
    pub fn item_count(&self, key: &CacheKey) -> usize {
        self.conn.query_row(
            "SELECT COUNT(*) FROM entity_items WHERE entity = ?1 AND params_hash = ?2",
            params![key.entity, key.params_hash as i64],
            |row| row.get::<_, i64>(0),
        ).unwrap_or(0) as usize
    }

    /// Database size in bytes (for diagnostics).
    pub fn db_size(&self) -> u64 {
        let page_count: i64 = self.conn.query_row("PRAGMA page_count", [], |r| r.get(0)).unwrap_or(0);
        let page_size: i64 = self.conn.query_row("PRAGMA page_size", [], |r| r.get(0)).unwrap_or(4096);
        (page_count * page_size) as u64
    }
}
