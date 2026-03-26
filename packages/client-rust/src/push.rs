//! Push event processing — update cache and notify UI.
//!
//! Cascade invalidation is handled server-side. The server pushes
//! separate entity_change events for each affected entity.
//! The client just receives them and marks cache entries stale.

use serde::Serialize;
use serde_json::Value;

use crate::cache::Cache;
use crate::schema::SchemaRegistry;

/// A change notification for the UI layer (emitted in a batch).
#[derive(Debug, Clone, Serialize)]
pub struct EntityChanged {
    pub entity: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub action: String,
}

/// Process a single push event: update cache + return what changed.
///
/// The server sends one push per affected entity (including cascaded ones).
/// This function just handles the cache side — no graph walking needed.
pub fn process_push(
    registry: &SchemaRegistry,
    cache: &mut Cache,
    event: &str,
    payload: &Value,
) -> Option<EntityChanged> {
    // Find which entity this push belongs to
    let entity_name = registry.entity_for_event(event)?.to_string();

    let action = payload.get("action")
        .and_then(|v| v.as_str())
        .unwrap_or("updated")
        .to_string();

    let entity_id = payload.get("entity_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // Update cache based on action
    match action.as_str() {
        "deleted" => {
            if let Some(ref id) = entity_id {
                cache.remove(&entity_name, id);
            } else {
                cache.mark_entity_stale(&entity_name);
            }
        }
        _ => {
            // If inline data provided, update cache directly (avoids re-fetch)
            if let (Some(ref id), Some(data)) = (&entity_id, payload.get("data")) {
                cache.put(&entity_name, id, data.clone());
            } else if let Some(ref id) = entity_id {
                cache.mark_stale(&entity_name, id);
            } else {
                cache.mark_entity_stale(&entity_name);
            }
        }
    }

    Some(EntityChanged {
        entity: entity_name,
        id: entity_id,
        action,
    })
}

/// Process multiple push events and return all changes (for batched emit).
pub fn process_pushes(
    registry: &SchemaRegistry,
    cache: &mut Cache,
    events: &[(String, Value)],
) -> Vec<EntityChanged> {
    events.iter()
        .filter_map(|(event, payload)| process_push(registry, cache, event, payload))
        .collect()
}
