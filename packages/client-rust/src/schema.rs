//! Dynamic entity schema — received from server, never hardcoded.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A relation (pointer) from one entity to another.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityRelation {
    /// Target entity name, e.g. "todo-items"
    pub target: String,
    /// Map source params to target params, e.g. {"id": "todo_id"}
    #[serde(default)]
    pub param_map: HashMap<String, String>,
    /// Only cascade on specific actions. None = all actions.
    #[serde(default)]
    pub on_actions: Option<Vec<String>>,
}

/// Entity schema definition — pushed from server after WS connect.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntitySchema {
    /// Entity name, e.g. "todos"
    pub name: String,
    /// Key params for scoping, e.g. ["project_id"]
    #[serde(default)]
    pub key_params: Vec<String>,
    /// Push events this entity subscribes to
    #[serde(default)]
    pub push_events: Vec<String>,
    /// Relations to other entities
    #[serde(default)]
    pub relations: Vec<EntityRelation>,
}

/// Schema registry — holds all entity schemas received from the server.
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
            tracing::info!("[Entangled] Registered entity: {} (relations: {})",
                schema.name, schema.relations.len());
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
