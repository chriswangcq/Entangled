//! Sync frame processing — handle server sync responses.
//!
//! Processes the 4 sync modes:
//! - snapshot: apply full data (git clone)
//! - delta: apply ops in-place (git pull fast-forward)
//! - head_n: apply partial data (git clone --depth)
//! - up_to_date: no-op (already current)

use serde::Deserialize;
use serde_json::Value;

use crate::cache::{Cache, CacheKey, EntityData, SyncOp};

/// A change notification for the UI layer.
#[derive(Debug, Clone, serde::Serialize)]
pub struct EntityChanged {
    pub entity: String,
    pub action: String,  // "synced" | "delta" | "invalidated"
}

/// Incoming sync frame from server.
#[derive(Debug, Deserialize)]
pub struct SyncFrame {
    pub entity: String,
    pub params: Option<serde_json::Map<String, Value>>,
    pub mode: String,  // "snapshot" | "delta" | "head_n" | "up_to_date"
    pub version: u64,

    // snapshot / head_n
    pub data: Option<Vec<Value>>,
    #[serde(default)]
    pub has_more: bool,

    // delta
    pub base_version: Option<u64>,
    pub ops: Option<Vec<SyncOp>>,
}

/// Process a sync frame from the server.
/// Returns what changed (for emitting to React).
pub fn process_sync(
    cache: &mut Cache,
    frame: &SyncFrame,
    id_field: &str,
) -> Option<EntityChanged> {
    let key = match &frame.params {
        Some(p) => CacheKey::new(&frame.entity, p),
        None => CacheKey::new_empty(&frame.entity),
    };

    match frame.mode.as_str() {
        "snapshot" | "head_n" => {
            let data = frame.data.as_ref()?;
            let entry = cache.entry(key);
            entry.apply_snapshot(data.clone(), frame.version, id_field);
            entry.subscribed = true;

            tracing::info!(
                "[Sync] {} snapshot v{} ({} items)",
                frame.entity, frame.version, data.len()
            );

            Some(EntityChanged {
                entity: frame.entity.clone(),
                action: "synced".into(),
            })
        }

        "delta" => {
            let ops = frame.ops.as_ref()?;
            let base = frame.base_version.unwrap_or(0);
            let entry = cache.entry(key.clone());

            if entry.apply_delta(base, ops, frame.version) {
                tracing::debug!(
                    "[Sync] {} delta v{}→v{} ({} ops)",
                    frame.entity, base, frame.version, ops.len()
                );
                Some(EntityChanged {
                    entity: frame.entity.clone(),
                    action: "delta".into(),
                })
            } else {
                // Version mismatch — need resync
                // Mark as not fresh so next read triggers re-subscribe
                let entry = cache.entry(key);
                entry.synced_at = None;

                tracing::warn!(
                    "[Sync] {} delta version mismatch, marking stale",
                    frame.entity
                );
                Some(EntityChanged {
                    entity: frame.entity.clone(),
                    action: "invalidated".into(),
                })
            }
        }

        "up_to_date" => {
            tracing::debug!("[Sync] {} up_to_date v{}", frame.entity, frame.version);
            None // No change needed
        }

        _ => {
            tracing::warn!("[Sync] Unknown sync mode: {}", frame.mode);
            None
        }
    }
}
