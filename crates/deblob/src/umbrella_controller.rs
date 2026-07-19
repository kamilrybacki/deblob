//! Umbrella proposal controller — turns silver-annotated schemas into PROVISIONAL
//! gold [`deblob_umbrella::types::UmbrellaSchema`]s + their per-child
//! [`deblob_umbrella::types::ChildTransform`]s, ready for human review via
//! `POST /api/v1/umbrellas/{id}/approve`.
//!
//! HITL invariant: this module ONLY ever creates `Provisional` umbrellas —
//! promotion to `Active` is exclusively the human-triggered `approve` gate's job.
//!
//! Grouping is deliberately CONSERVATIVE (precision over coverage, per the joint
//! design): schemas are grouped only when they share an IDENTICAL set of
//! `canonical_field_id`s. This avoids the transitive/false-merge risk of lenient
//! (shared-≥K) grouping — cross-source convergence of near-but-not-equal shapes is
//! left to a human widening the umbrella later, never inferred here.

use crate::api::umbrellas::child_fields_from_schema;
use crate::api::{ApiError, ApiState};
use deblob_core::ports::SchemaRecord;
use deblob_umbrella::adjudicate::{assemble_transform, DeterministicAnchor};
use deblob_umbrella::store::{StoreError, UmbrellaState};
use deblob_umbrella::types::{
    Cardinality, FieldType, JsonPath, ScalarType, UmbrellaField, UmbrellaSchema,
};
use deblob_umbrella::verify::{verify_static, ChildField};
use std::collections::BTreeMap;

fn from_store(e: StoreError) -> ApiError {
    ApiError::from_umbrella_store(e)
}

/// Stable, deterministic umbrella id from the (sorted) canonical-field-id set — so
/// re-running propose over the same cohort targets the SAME umbrella rather than
/// minting duplicates. FNV-1a, no external dep, no clock/random.
fn umbrella_id_for(cfids: &[String]) -> String {
    let joined = cfids.join("\u{1f}");
    let mut h: u64 = 0xcbf29ce4_84222325;
    for b in joined.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x00000100_000001b3);
    }
    format!("umb_{h:016x}")
}

/// Build the gold umbrella field set from the shared canonical-field-ids. Each
/// field's type/unit is taken from a contributing child field carrying that id;
/// cardinality is Required (every member of an identical-cfid-set group has it).
fn build_umbrella(umbrella_id: &str, cfids: &[String], members: &[&[ChildField]]) -> UmbrellaSchema {
    let mut fields = Vec::new();
    for (i, cfid) in cfids.iter().enumerate() {
        let src = members
            .iter()
            .flat_map(|m| m.iter())
            .find(|f| f.canonical_field_id.as_ref().map(|c| c.as_str()) == Some(cfid.as_str()));
        let (ty, unit) = match src {
            Some(f) => (FieldType::Scalar(f.ty), f.unit.clone()),
            None => (FieldType::Scalar(ScalarType::String), None),
        };
        let key = cfid
            .strip_prefix("cfid_")
            .or_else(|| cfid.strip_prefix("cfid."))
            .unwrap_or(cfid);
        let path = JsonPath::parse(&format!("$.{key}"))
            .unwrap_or_else(|_| JsonPath::parse(&format!("$.f{i}")).unwrap());
        fields.push(UmbrellaField {
            canonical_field_id: src
                .and_then(|f| f.canonical_field_id.clone())
                .unwrap_or_else(|| deblob_core::semantic::CanonicalFieldId::new(cfid.clone())),
            path,
            name: key.to_string(),
            ty,
            unit,
            cardinality: Cardinality::Required,
        });
    }
    UmbrellaSchema {
        umbrella_id: umbrella_id.to_string(),
        label: format!("consolidated-{}", &umbrella_id[4..12.min(umbrella_id.len())]),
        version: 1,
        fields,
    }
}

/// Proposes new PROVISIONAL umbrellas from the current registry state. Returns the
/// `umbrella_id`s created/updated this run (empty when nothing consolidatable was
/// found). Idempotent: an existing `Active`/`Rejected` umbrella is never clobbered.
pub async fn propose_umbrellas(state: &ApiState) -> Result<Vec<String>, ApiError> {
    // 1. gather every schema's silver-annotated (canonical_field_id-bearing) fields
    let mut schemas: Vec<(SchemaRecord, Vec<ChildField>)> = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let (page, next) = state
            .registry
            .list_schemas(cursor.clone(), 200)
            .await
            .map_err(ApiError::from_core)?;
        for rec in page {
            // `SchemaRecord::semantic` is always `None` in practice —
            // annotations live in the append-only `SemanticStore` (spec
            // P2-D Task 5/6), so fetch the schema's CURRENT active
            // annotation from there instead (mirrors `approve`'s own fix
            // for the exact same gap). Best-effort: a store error or "no
            // active revision yet" both fall back to unannotated, which
            // simply excludes the schema from this run's grouping.
            let semantic_metadata = match state.semantic.active_semantic(&rec.schema_id).await {
                Ok(Some((metadata, _, _))) => Some(metadata),
                Ok(None) | Err(_) => None,
            };

            let annotated: Vec<ChildField> =
                child_fields_from_schema(&rec, semantic_metadata.as_ref())
                    .into_iter()
                    .filter(|f| f.canonical_field_id.is_some())
                    .collect();
            if !annotated.is_empty() {
                schemas.push((rec, annotated));
            }
        }
        match next {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }

    // 2. group by IDENTICAL canonical-field-id set (conservative)
    let mut groups: BTreeMap<Vec<String>, Vec<usize>> = BTreeMap::new();
    for (i, (_, fields)) in schemas.iter().enumerate() {
        let mut cfids: Vec<String> = fields
            .iter()
            .filter_map(|f| f.canonical_field_id.as_ref().map(|c| c.as_str().to_string()))
            .collect();
        cfids.sort();
        cfids.dedup();
        groups.entry(cfids).or_default().push(i);
    }

    let anchor = DeterministicAnchor;
    let mut created = Vec::new();
    for (cfids, member_idxs) in groups {
        if member_idxs.len() < 2 {
            continue; // need ≥2 sources to consolidate
        }
        let umbrella_id = umbrella_id_for(&cfids);
        if let Some(existing) = state.umbrellas.get_umbrella(&umbrella_id).await.map_err(from_store)? {
            if existing.state != UmbrellaState::Provisional {
                continue; // never clobber a human-decided umbrella
            }
        }
        let member_fields: Vec<&[ChildField]> =
            member_idxs.iter().map(|&i| schemas[i].1.as_slice()).collect();
        let umbrella = build_umbrella(&umbrella_id, &cfids, &member_fields);
        let umbrella_rev = format!("{umbrella_id}@{}", umbrella.version);

        // 3. assemble + statically verify a transform per member
        let mut transforms = Vec::new();
        for &i in &member_idxs {
            let (rec, fields) = &schemas[i];
            let child_id = rec.schema_id.as_str();
            let t = assemble_transform(child_id, child_id, &umbrella, &umbrella_rev, fields, &anchor);
            if verify_static(&t, &umbrella, fields).is_empty() {
                transforms.push(t);
            }
        }
        if transforms.len() < 2 {
            continue; // need ≥2 members with a verified transform
        }

        // 4. persist PROVISIONAL only (HITL gate promotes to active)
        state
            .umbrellas
            .put_umbrella(&umbrella, UmbrellaState::Provisional)
            .await
            .map_err(from_store)?;
        for t in &transforms {
            state.umbrellas.put_transform(t).await.map_err(from_store)?;
        }
        created.push(umbrella_id);
    }
    Ok(created)
}
