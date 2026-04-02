//! Default primary-key JSON field per entity when sync frames omit `idField`.
//!
//! Match arms are **generated at build time** from the same JSON as Gateway:
//! `novaic-gateway/gateway/entity/generated_entity_id_fields.json`, or the sync copy
//! `entity_id_fields.json` in this crate (see repo `scripts/sync_entity_id_fields.sh`).

include!(concat!(env!("OUT_DIR"), "/generated_id_field.rs"));

/// Default JSON primary-key field when the server omits `idField` on a sync frame
/// (e.g. older Gateway).
pub fn default_id_field_for_entity(entity: &str) -> &'static str {
    default_id_field_for_entity_inner(entity)
}

#[cfg(test)]
mod tests {
    use super::default_id_field_for_entity;
    use serde_json::Value;

    #[test]
    fn matches_embedded_entity_id_fields_json() {
        let json = include_str!("../entity_id_fields.json");
        let v: Value = serde_json::from_str(json).expect("parse entity_id_fields.json");
        let entities = v["entities"].as_object().expect("entities");
        for (ent, fid) in entities {
            assert_eq!(
                default_id_field_for_entity(ent),
                fid.as_str().expect("id field string"),
                "entity {}",
                ent
            );
        }
    }

    #[test]
    fn unknown_entity_falls_back_to_id() {
        assert_eq!(default_id_field_for_entity("totally-unknown-entity-xyz"), "id");
    }
}
