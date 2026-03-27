//! Gateway-driven subscription policy: `subscriptionCascade`, ref-counted wire subscribe.
//!
//! Mirrors `Entangled/packages/react/src/subscriptionSchema.ts` so non-React hosts share behavior.

use serde_json::{Map, Value};
use std::collections::HashMap;

use crate::cache::CacheKey;

/// Same key shape as `make_key` in `commands.rs` / TS `subscriptionKey`.
pub fn subscription_cache_key(entity: &str, params: Option<&Map<String, Value>>) -> CacheKey {
    let params_map = params.cloned().unwrap_or_default();
    if params_map.is_empty() {
        CacheKey::new_empty(entity)
    } else {
        CacheKey::new(entity, &params_map)
    }
}

// ── Schema (from GET /api/entangled/schema) ───────────────────────────────

#[derive(Debug, Clone)]
pub struct EntitySubscriptionSchemaRow {
    pub name: String,
    pub subscription_cascade: Vec<String>,
}

/// Holds `subscriptionCascade` per entity name.
#[derive(Debug, Default, Clone)]
pub struct SubscriptionSchemaStore {
    by_name: HashMap<String, EntitySubscriptionSchemaRow>,
}

impl SubscriptionSchemaStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace registry from Gateway JSON array (same shape as TS `EntitySubscriptionSchema[]`).
    pub fn set_from_json_array(&mut self, rows: &[Value]) -> Result<(), String> {
        self.by_name.clear();
        for row in rows {
            let name = row
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "schema row missing name".to_string())?
                .to_string();
            let subscription_cascade: Vec<String> = row
                .get("subscriptionCascade")
                .or_else(|| row.get("subscription_cascade"))
                .and_then(|v| v.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(String::from))
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_default();
            self.by_name.insert(
                name.clone(),
                EntitySubscriptionSchemaRow {
                    name,
                    subscription_cascade,
                },
            );
        }
        Ok(())
    }

    pub fn cascade_for(&self, entity: &str) -> &[String] {
        self.by_name
            .get(entity)
            .map(|r| r.subscription_cascade.as_slice())
            .unwrap_or(&[])
    }
}

/// Primary entity first, then cascade targets (deduped). Matches `cascadeTargets()` in TS.
pub fn cascade_targets_for(entity: &str, schema: &SubscriptionSchemaStore) -> Vec<String> {
    let mut out = vec![entity.to_string()];
    for t in schema.cascade_for(entity) {
        if t != entity && !out.contains(t) {
            out.push(t.clone());
        }
    }
    out
}

// ── Ledger (ref-count + depth + params for resubscribe) ───────────────────

/// Tracks active subscribe wires — paired cascade subscribe/unsubscribe.
#[derive(Debug, Default, Clone)]
pub struct SubscriptionLedger {
    ref_counts: HashMap<CacheKey, u32>,
    depths: HashMap<CacheKey, Option<u64>>,
    param_maps: HashMap<CacheKey, Map<String, Value>>,
}

impl SubscriptionLedger {
    pub fn new() -> Self {
        Self::default()
    }

    fn make_key(entity: &str, params: &Map<String, Value>) -> CacheKey {
        subscription_cache_key(entity, Some(params))
    }

    /// Subscribe `root_entity` and cascade targets. Returns entity names that need a new WS subscribe (ref 0→1).
    pub fn subscribe_cascade(
        &mut self,
        root_entity: &str,
        params: Option<&Map<String, Value>>,
        depth: Option<u64>,
        schema: &SubscriptionSchemaStore,
    ) -> Vec<String> {
        let params_map = params.cloned().unwrap_or_default();
        let targets = cascade_targets_for(root_entity, schema);
        let mut need_wire = Vec::new();
        for target in targets {
            let key = Self::make_key(&target, &params_map);
            let prev = self.ref_counts.get(&key).copied().unwrap_or(0);
            if prev > 0 {
                self.ref_counts.insert(key, prev + 1);
                continue;
            }
            self.ref_counts.insert(key.clone(), 1);
            self.depths.insert(key.clone(), depth);
            self.param_maps.insert(key, params_map.clone());
            need_wire.push(target);
        }
        need_wire
    }

    /// Unsubscribe cascade in reverse order. Returns entity names that need WS unsubscribe (ref→0).
    pub fn unsubscribe_cascade(
        &mut self,
        root_entity: &str,
        params: Option<&Map<String, Value>>,
        schema: &SubscriptionSchemaStore,
    ) -> Vec<String> {
        let params_map = params.cloned().unwrap_or_default();
        let mut targets = cascade_targets_for(root_entity, schema);
        let mut need_wire = Vec::new();
        for target in targets.drain(..).rev() {
            let key = Self::make_key(&target, &params_map);
            let prev = self.ref_counts.get(&key).copied().unwrap_or(0);
            if prev == 0 {
                continue;
            }
            let n = prev - 1;
            if n == 0 {
                self.ref_counts.remove(&key);
                self.depths.remove(&key);
                self.param_maps.remove(&key);
                need_wire.push(target);
            } else {
                self.ref_counts.insert(key, n);
            }
        }
        need_wire
    }

    pub fn is_active(&self, key: &CacheKey) -> bool {
        self.ref_counts.get(key).copied().unwrap_or(0) > 0
    }

    pub fn depth_for_key(&self, key: &CacheKey) -> Option<u64> {
        self.depths.get(key).cloned().flatten()
    }

    /// Params payload for WS `subscribe` (omit empty object).
    pub fn params_value_for_key(&self, key: &CacheKey) -> Option<Value> {
        self.param_maps.get(key).and_then(|m| {
            if m.is_empty() {
                None
            } else {
                Some(Value::Object(m.clone()))
            }
        })
    }

    /// Entries with ref>0 for reconnect resubscribe (same order as TS `resubscribeAll` iteration).
    pub fn active_resubscribe_entries(&self) -> Vec<(CacheKey, Option<u64>)> {
        let mut out: Vec<(CacheKey, Option<u64>)> = self
            .ref_counts
            .iter()
            .filter(|(_, &c)| c > 0)
            .map(|(k, _)| (k.clone(), self.depths.get(k).cloned().flatten()))
            .collect();
        out.sort_by(|a, b| a.0.entity.cmp(&b.0.entity).then(a.0.params_hash.cmp(&b.0.params_hash)));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn cascade_dedupes() {
        let mut s = SubscriptionSchemaStore::new();
        let rows = vec![json!({
            "name": "a",
            "subscriptionCascade": ["b", "a", "b"]
        })];
        s.set_from_json_array(&rows).unwrap();
        let t = cascade_targets_for("a", &s);
        assert_eq!(t, vec!["a", "b"]);
    }

    #[test]
    fn ledger_ref_and_wire() {
        let mut schema = SubscriptionSchemaStore::new();
        schema
            .set_from_json_array(&[json!({"name": "root", "subscriptionCascade": ["child"]})])
            .unwrap();
        let mut ledger = SubscriptionLedger::new();
        let p = Map::new();
        let w1 = ledger.subscribe_cascade("root", Some(&p), Some(50), &schema);
        assert_eq!(w1, vec!["root", "child"]);
        let w2 = ledger.subscribe_cascade("root", Some(&p), Some(50), &schema);
        assert!(w2.is_empty());
        let u1 = ledger.unsubscribe_cascade("root", Some(&p), &schema);
        assert!(u1.is_empty());
        let u2 = ledger.unsubscribe_cascade("root", Some(&p), &schema);
        assert_eq!(u2, vec!["child", "root"]);
    }
}
