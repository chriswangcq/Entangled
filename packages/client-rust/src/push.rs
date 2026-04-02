//! Sync frame processing — handle server sync responses.
//!
//! Processes the 4 sync modes:
//! - snapshot: apply full data (git clone)
//! - delta: apply ops in-place (git pull fast-forward)
//! - head_n: apply partial data (git clone --depth)
//! - up_to_date: no-op (already current)

use std::collections::HashMap;

use serde::Deserialize;
use serde_json::Value;

use crate::cache::{Cache, CacheKey, SyncOp};

fn params_json_to_strings(p: &serde_json::Map<String, Value>) -> HashMap<String, String> {
    p.iter()
        .map(|(k, v)| {
            let s = match v {
                Value::String(s) => s.clone(),
                Value::Null => String::new(),
                _ => v.to_string(),
            };
            (k.clone(), s)
        })
        .collect()
}

fn entity_changed(
    entity: &str,
    action: &str,
    request_ids: Vec<String>,
    frame_params: &Option<serde_json::Map<String, Value>>,
) -> EntityChanged {
    EntityChanged {
        entity: entity.to_string(),
        action: action.to_string(),
        params: frame_params.as_ref().map(params_json_to_strings),
        request_ids,
    }
}

/// A change notification for the UI layer.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityChanged {
    pub entity: String,
    pub action: String,  // "synced" | "delta" | "invalidated"
    /// Subscription key params (React Query invalidation).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<HashMap<String, String>>,
    /// requestIds from ops — for optimistic state confirmation
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub request_ids: Vec<String>,
}

/// Incoming sync frame from server (server uses camelCase for several fields).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncFrame {
    pub entity: String,
    pub params: Option<serde_json::Map<String, Value>>,
    pub mode: String,  // "snapshot" | "delta" | "head_n" | "up_to_date"
    pub version: u64,

    /// Primary key field name (e.g. "id", "model_id"). Sent by server.
    /// If absent, client uses `default_id_field_for_entity` (build-time generated from
    /// `generated_entity_id_fields.json`; sync via `scripts/sync_entity_id_fields.sh`).
    pub id_field: Option<String>,

    // snapshot / head_n
    pub data: Option<Vec<Value>>,
    #[serde(default)]
    pub has_more: bool,

    // delta
    pub base_version: Option<u64>,
    pub ops: Option<Vec<SyncOp>>,
}

/// Re-export for callers that use `push::default_id_field_for_entity`.
pub use crate::id_field::default_id_field_for_entity;

/// Process a sync frame from the server.
/// Returns what changed (for emitting to React).
pub fn process_sync(cache: &Cache, frame: &SyncFrame) -> Option<EntityChanged> {
    let id_field = frame
        .id_field
        .as_deref()
        .unwrap_or_else(|| default_id_field_for_entity(&frame.entity));
    let key = match &frame.params {
        Some(p) => CacheKey::new(&frame.entity, p),
        None => CacheKey::new_empty(&frame.entity),
    };

    match frame.mode.as_str() {
        "snapshot" => {
            let data = frame.data.as_ref()?;
            cache.apply_snapshot(&key, data, frame.version, id_field, false);
            cache.set_subscribed(&key, true);

            tracing::info!(
                "[Sync] {} snapshot v{} ({} items)",
                frame.entity, frame.version, data.len()
            );

            Some(entity_changed(
                &frame.entity,
                "synced",
                Vec::new(),
                &frame.params,
            ))
        }

        "head_n" => {
            let data = frame.data.as_ref()?;
            cache.apply_snapshot(&key, data, frame.version, id_field, frame.has_more);

            tracing::info!(
                "[Sync] {} head_n v{} ({} items, has_more={})",
                frame.entity, frame.version, data.len(), frame.has_more
            );

            Some(entity_changed(
                &frame.entity,
                "synced",
                Vec::new(),
                &frame.params,
            ))
        }

        "delta" => {
            let ops = frame.ops.as_ref()?;
            let base = frame.base_version.unwrap_or(0);

            // Collect requestIds from ops for optimistic confirmation
            let request_ids: Vec<String> = ops.iter()
                .filter_map(|op| op.request_id.clone())
                .collect();

            if cache.apply_delta(&key, base, ops, frame.version) {
                tracing::debug!(
                    "[Sync] {} delta v{}→v{} ({} ops, {} requestIds)",
                    frame.entity, base, frame.version, ops.len(), request_ids.len()
                );
                Some(entity_changed(
                    &frame.entity,
                    "delta",
                    request_ids,
                    &frame.params,
                ))
            } else {
                // Version mismatch — need resync
                tracing::warn!(
                    "[Sync] {} delta version mismatch, marking stale",
                    frame.entity
                );
                Some(entity_changed(
                    &frame.entity,
                    "invalidated",
                    Vec::new(),
                    &frame.params,
                ))
            }
        }

        "up_to_date" => {
            cache.align_version_from_server(&key, frame.version);
            tracing::debug!("[Sync] {} up_to_date v{}", frame.entity, frame.version);
            None
        }

        _ => {
            tracing::warn!("[Sync] Unknown sync mode: {}", frame.mode);
            None
        }
    }
}
