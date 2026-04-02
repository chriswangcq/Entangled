//! Dynamic entity schema — received from server, never hardcoded.
//!
//! The client only needs to know: entity name → push events.
//! Relations and cascade logic are server-side business logic.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Entity schema definition — pushed from server after WS connect.
///
/// The client only uses `name` and `push_events` to know which
/// push events correspond to which entity. Relations, key_params,
/// and other schema details are server-side concerns.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntitySchema {
    /// Entity name, e.g. "todos"
    pub name: String,
    /// Key params for scoping, e.g. ["project_id"]
    #[serde(default)]
    pub key_params: Vec<String>,
    /// Push events this entity subscribes to
    #[serde(default)]
    pub push_events: Vec<String>,
    /// Row PK JSON field when server includes it (Sync Contract schema).
    #[serde(default)]
    pub id_field: Option<String>,
}

/// Schema registry — maps push events to entity names.
///
/// Populated from the server's "schema" push on connect.
/// The client doesn't need to know about relations — the server
/// handles cascade and pushes separate events for each affected entity.
#[derive(Debug, Default)]
pub struct SchemaRegistry {
    schemas: HashMap<String, EntitySchema>,
    /// Reverse index: push_event → entity_name
    event_to_entity: HashMap<String, String>,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register schemas from a server "schema" push event.
    pub fn register_all(&mut self, schemas: Vec<EntitySchema>) {
        for schema in schemas {
            for event in &schema.push_events {
                self.event_to_entity.insert(event.clone(), schema.name.clone());
            }
            tracing::info!("[Entangled] Registered entity: {}", schema.name);
            self.schemas.insert(schema.name.clone(), schema);
        }
    }

    /// Get entity schema by name.
    pub fn get(&self, name: &str) -> Option<&EntitySchema> {
        self.schemas.get(name)
    }

    /// Look up which entity a push event belongs to.
    pub fn entity_for_event(&self, event: &str) -> Option<&str> {
        self.event_to_entity.get(event).map(|s| s.as_str())
    }

    /// Get all registered entity names.
    pub fn entity_names(&self) -> Vec<&str> {
        self.schemas.keys().map(|s| s.as_str()).collect()
    }
}
