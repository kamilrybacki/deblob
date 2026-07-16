//! Deterministic schema-family selection. A family's *required* field set
//! is derived purely from its index, so the mapping never depends on RNG
//! state — the same `family` index always yields the same field set,
//! independent of how many records precede it in the stream.

use serde_json::{Map, Value};

use crate::fields::{FieldSpec, CORE_FIELDS, SIGNATURE_POOL};

/// Ceiling on `distinct_schemas`: [`SIGNATURE_POOL`] has this many fields,
/// and a family's signature is the bitmask of `family` over that pool, so
/// families are only guaranteed structurally distinct for indices below
/// `2^SIGNATURE_POOL.len()`.
pub const MAX_DISTINCT_SCHEMAS: usize = 1 << 20;

/// The required field set for one schema family: [`CORE_FIELDS`] (always
/// present) plus the [`SIGNATURE_POOL`] fields selected by `family`'s bit
/// pattern.
#[derive(Debug, Clone)]
pub struct SchemaTemplate {
    pub family: usize,
    pub fields: Vec<FieldSpec>,
}

/// Build the [`SchemaTemplate`] for `family`. Bit `j` of `family` (0-based,
/// LSB first) selects `SIGNATURE_POOL[j]`, so distinct `family` values in
/// `0..2^SIGNATURE_POOL.len()` always select distinct field sets: the
/// mapping `family -> field set` is injective by construction, not by
/// chance.
pub fn schema_for(family: usize) -> SchemaTemplate {
    let mut fields = CORE_FIELDS.to_vec();
    for (j, f) in SIGNATURE_POOL.iter().enumerate() {
        if (family >> j) & 1 == 1 {
            fields.push(*f);
        }
    }
    SchemaTemplate { family, fields }
}

impl SchemaTemplate {
    /// Build the base JSON object for this family: every required field
    /// set to a representative sample value.
    pub fn base_object(&self, seed_hint: usize) -> Map<String, Value> {
        let mut m = Map::new();
        for f in &self.fields {
            m.insert(f.name.to_string(), f.sample_value(seed_hint));
        }
        m
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinct_family_indices_yield_distinct_field_name_sets() {
        use std::collections::BTreeSet;
        let mut seen: BTreeSet<Vec<&'static str>> = BTreeSet::new();
        for family in 0..500 {
            let tpl = schema_for(family);
            let mut names: Vec<&'static str> = tpl.fields.iter().map(|f| f.name).collect();
            names.sort_unstable();
            assert!(
                seen.insert(names),
                "family {family} collided with an earlier family's field set"
            );
        }
    }

    #[test]
    fn core_fields_always_present() {
        let tpl = schema_for(12345);
        for core in CORE_FIELDS {
            assert!(tpl.fields.iter().any(|f| f.name == core.name));
        }
    }
}
