//! Dynamic entity schema — received from server, never hardcoded.
//!
//! The client only needs to know the server-advertised entity contract.
//! Relations and cascade logic are server-side business logic.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Entity schema definition — pushed from server after WS connect.
///
/// The App mirrors this schema to TypeScript; keep fields lossless enough for
/// id-field, eager/lazy, and capability guardrails.
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
    /// Sync strategy advertised by the server, e.g. "full" or "stream".
    #[serde(default)]
    pub sync_type: Option<String>,
    /// Server-side default sync limit when present.
    #[serde(default)]
    pub sync_limit: Option<u64>,
    /// Subscription mode advertised to the App, e.g. "eager" or "lazy".
    #[serde(default)]
    pub subscription_mode: Option<String>,
    /// Server capabilities are opaque to Rust but consumed by App tests/TS.
    #[serde(default)]
    pub capabilities: Option<Value>,
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
        self.schemas.clear();
        self.event_to_entity.clear();
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

    /// Return all schemas in stable order for host/UI bootstrap.
    pub fn all(&self) -> Vec<EntitySchema> {
        let mut rows: Vec<EntitySchema> = self.schemas.values().cloned().collect();
        rows.sort_by(|a, b| a.name.cmp(&b.name));
        rows
    }

    pub fn is_empty(&self) -> bool {
        self.schemas.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::{EntitySchema, SchemaRegistry};

    #[test]
    fn register_all_replaces_previous_schema_and_preserves_app_fields() {
        let mut registry = SchemaRegistry::new();
        registry.register_all(vec![EntitySchema {
            name: "old".to_string(),
            key_params: vec![],
            push_events: vec!["old_changed".to_string()],
            id_field: Some("id".to_string()),
            sync_type: Some("full".to_string()),
            sync_limit: None,
            subscription_mode: Some("lazy".to_string()),
            capabilities: None,
        }]);
        registry.register_all(vec![EntitySchema {
            name: "messages".to_string(),
            key_params: vec!["agent_id".to_string()],
            push_events: vec!["messages_changed".to_string()],
            id_field: Some("id".to_string()),
            sync_type: Some("stream".to_string()),
            sync_limit: Some(50),
            subscription_mode: Some("eager".to_string()),
            capabilities: None,
        }]);

        assert!(registry.get("old").is_none());
        assert_eq!(registry.entity_for_event("old_changed"), None);
        let rows = registry.all();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].name, "messages");
        assert_eq!(rows[0].key_params, vec!["agent_id"]);
        assert_eq!(rows[0].id_field.as_deref(), Some("id"));
        assert_eq!(rows[0].sync_type.as_deref(), Some("stream"));
        assert_eq!(rows[0].sync_limit, Some(50));
        assert_eq!(rows[0].subscription_mode.as_deref(), Some("eager"));
    }
}
