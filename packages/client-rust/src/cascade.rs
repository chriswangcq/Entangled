//! Cascade invalidation — walk the entity relation graph.
//!
//! When entity A changes, all entities that have a relation pointing
//! FROM A are invalidated. This is resolved transitively.

use std::collections::HashSet;

use serde::Serialize;
use serde_json::Value;

use crate::cache::Cache;
use crate::schema::SchemaRegistry;

/// A single change notification (emitted to JS in a batch).
#[derive(Debug, Clone, Serialize)]
pub struct EntityChanged {
    pub entity: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Map<String, Value>>,
}

/// Process a push event: update cache + cascade invalidation.
///
/// Returns the list of all affected entities (including cascaded ones)
/// for the UI layer to invalidate.
pub fn process_push(
    registry: &SchemaRegistry,
    cache: &mut Cache,
    event: &str,
    payload: &Value,
) -> Vec<EntityChanged> {
    // 1. Find which entity this push belongs to
    let entity_name = match registry.entity_for_event(event) {
        Some(name) => name.to_string(),
        None => return vec![],  // Unknown event, ignore
    };

    let action = payload.get("action")
        .and_then(|v| v.as_str())
        .unwrap_or("updated");

    let entity_id = payload.get("entity_id")
        .and_then(|v| v.as_str());

    let params = payload.get("params")
        .and_then(|v| v.as_object())
        .cloned();

    // 2. Update cache
    match action {
        "deleted" => {
            if let Some(id) = entity_id {
                cache.remove(&entity_name, id);
            }
        }
        _ => {
            // If inline data is provided, update cache directly
            if let (Some(id), Some(data)) = (entity_id, payload.get("data")) {
                cache.put(&entity_name, id, data.clone());
            } else if let Some(id) = entity_id {
                cache.mark_stale(&entity_name, id);
            } else {
                cache.mark_entity_stale(&entity_name);
            }
        }
    }

    // 3. Collect affected entities (source + cascaded)
    let mut changed = vec![EntityChanged {
        entity: entity_name.clone(),
        params: params.clone(),
    }];

    // 4. Walk cascade graph
    let mut visited = HashSet::new();
    visited.insert(entity_name.clone());

    cascade(
        registry,
        cache,
        &entity_name,
        action,
        &params.unwrap_or_default(),
        &mut changed,
        &mut visited,
    );

    changed
}

fn cascade(
    registry: &SchemaRegistry,
    cache: &mut Cache,
    entity: &str,
    action: &str,
    source_params: &serde_json::Map<String, Value>,
    out: &mut Vec<EntityChanged>,
    visited: &mut HashSet<String>,
) {
    let Some(schema) = registry.get(entity) else { return };

    for rel in &schema.relations {
        // Filter by on_actions
        if let Some(ref actions) = rel.on_actions {
            if !actions.iter().any(|a| a == action) {
                continue;
            }
        }

        // Avoid cycles
        let rel_key = format!("{}:{:?}", rel.target, rel.param_map);
        if visited.contains(&rel_key) {
            continue;
        }
        visited.insert(rel_key);

        // Map params: source.id → target.todo_id
        let mut target_params = serde_json::Map::new();
        for (src_key, tgt_key) in &rel.param_map {
            if let Some(val) = source_params.get(src_key) {
                target_params.insert(tgt_key.clone(), val.clone());
            }
        }

        // Mark target entity stale
        cache.mark_entity_stale(&rel.target);

        out.push(EntityChanged {
            entity: rel.target.clone(),
            params: if target_params.is_empty() { None } else { Some(target_params.clone()) },
        });

        // Recurse
        cascade(registry, cache, &rel.target, action, &target_params, out, visited);
    }
}
