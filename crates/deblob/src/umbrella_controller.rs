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

/// The per-field grouping signature: one `(canonical_field_id, type_tag)` pair.
/// Grouping on the FULL typed signature — not the bare cfid set — is what keeps
/// two schemas that agree on field *identity* but disagree on field *type*
/// (e.g. `scopetest: bool` vs `scopetest: number`) in SEPARATE candidate
/// umbrellas. Merging them would force `build_umbrella` to pick one type,
/// silently dropping the other member's transform at `verify_static` (a lossy
/// cast is not a valid binding) — precision-over-coverage, per the joint design.
type FieldSig = (String, String);

/// A stable, order-independent type tag for one child field (scalar type +
/// array-ness). `Debug` of `ScalarType` is a stable enum name; the array
/// wrapper distinguishes `array<T>` from a bare scalar `T`.
fn field_type_tag(f: &ChildField) -> String {
    if f.is_array {
        format!("array<{:?}>", f.ty)
    } else {
        format!("{:?}", f.ty)
    }
}

/// Stable, deterministic umbrella id from the (sorted) typed field signature — so
/// re-running propose over the same cohort targets the SAME umbrella rather than
/// minting duplicates, while two type-incompatible cohorts (same cfids, different
/// types) map to DISTINCT umbrellas. FNV-1a, no external dep, no clock/random.
fn umbrella_id_for(sig: &[FieldSig]) -> String {
    let joined = sig
        .iter()
        .map(|(c, t)| format!("{c}={t}"))
        .collect::<Vec<_>>()
        .join("\u{1f}");
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

    // 2. group by IDENTICAL typed field signature (cfid + type). Conservative:
    // schemas consolidate only when they agree on BOTH which canonical fields
    // they carry AND each field's scalar type — a type disagreement means a
    // genuinely different field, left for a human to reconcile, never merged
    // here (see `FieldSig`).
    let mut groups: BTreeMap<Vec<FieldSig>, Vec<usize>> = BTreeMap::new();
    for (i, (_, fields)) in schemas.iter().enumerate() {
        let mut sig: Vec<FieldSig> = fields
            .iter()
            .filter_map(|f| {
                f.canonical_field_id
                    .as_ref()
                    .map(|c| (c.as_str().to_string(), field_type_tag(f)))
            })
            .collect();
        sig.sort();
        sig.dedup();
        groups.entry(sig).or_default().push(i);
    }

    let anchor = DeterministicAnchor;
    let mut created = Vec::new();
    for (sig, member_idxs) in groups {
        if member_idxs.len() < 2 {
            continue; // need ≥2 sources to consolidate
        }
        let cfids: Vec<String> = sig.iter().map(|(c, _)| c.clone()).collect();
        let umbrella_id = umbrella_id_for(&sig);
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

        // 3b. Name/value corroboration guard (joint design Stage 2/4). Always
        // computes + LOGS the per-field verdict (shadow). Returns whether any
        // field is CONTRADICTORY. Stage 4: only when `enforce_value_guard` is
        // on does a contradiction actually SUPPRESS the auto-proposal (routed
        // to human review via the log) — off by default, so the guard's
        // behavior is observable on real cohorts before it can block anything.
        let any_contradictory =
            shadow_evaluate_guard(state, &umbrella_id, &umbrella, &member_idxs, &schemas).await;
        if state.enforce_value_guard && any_contradictory {
            tracing::warn!(
                target: "umbrella_guard_shadow",
                umbrella = umbrella_id.as_str(),
                "auto-proposal SUPPRESSED: value guard found a CONTRADICTORY field (enforcement on) — left for human review"
            );
            continue;
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

/// Shadow-mode guard evaluation for one proposed umbrella: fetches each
/// member's durable value profile, joins per umbrella field by canonical path,
/// and logs the [`crate::umbrella_guard`] verdict + cause codes. Best-effort,
/// side-effect-free beyond logging — a `CONTRADICTORY` verdict is recorded,
/// never acted on, at this stage.
async fn shadow_evaluate_guard(
    state: &ApiState,
    umbrella_id: &str,
    umbrella: &UmbrellaSchema,
    member_idxs: &[usize],
    schemas: &[(SchemaRecord, Vec<ChildField>)],
) -> bool {
    use crate::umbrella_guard::{evaluate_field, MemberEvidence, ValueVerdict};
    use std::collections::HashMap;

    let mut any_contradictory = false;

    // Fetch each member's value-profile leaves once (path -> (mask, count)).
    let mut member_leaves: HashMap<usize, HashMap<String, (u8, u64)>> = HashMap::new();
    let mut member_has_profile: HashMap<usize, bool> = HashMap::new();
    for &i in member_idxs {
        let rec = &schemas[i].0;
        let mut leaves = HashMap::new();
        let mut has = false;
        if let Some(vp_ref) = &rec.value_profile_ref {
            if let Ok(Some(snap)) = state.value_profiles.get_value_profile(vp_ref).await {
                has = true;
                for l in snap.leaves {
                    leaves.insert(l.path, (l.numeric_bucket_mask, l.type_counts.number));
                }
            }
        }
        member_leaves.insert(i, leaves);
        member_has_profile.insert(i, has);
    }

    for uf in &umbrella.fields {
        let cfid = uf.canonical_field_id.as_str();
        let mut evidence = Vec::new();
        for &i in member_idxs {
            // Find this member's child field carrying the umbrella field's cfid.
            let Some(cf) = schemas[i]
                .1
                .iter()
                .find(|f| f.canonical_field_id.as_ref().map(|c| c.as_str()) == Some(cfid))
            else {
                continue;
            };
            let leaf_path = {
                let p = String::from(cf.path.clone());
                p.strip_prefix("$.").unwrap_or(&p).to_string()
            };
            let name = leaf_path.rsplit('.').next().unwrap_or(&leaf_path).to_string();
            let (mask, count) = member_leaves
                .get(&i)
                .and_then(|m| m.get(&leaf_path).copied())
                .unwrap_or((0, 0));
            evidence.push(MemberEvidence {
                name,
                unit: cf.unit.as_ref().map(|u| u.code.clone()),
                mask,
                numeric_count: count,
                has_profile: *member_has_profile.get(&i).unwrap_or(&false),
            });
        }
        let guard = evaluate_field(&evidence, state.umbrella_min_support);
        if guard.value_verdict == ValueVerdict::Contradictory {
            any_contradictory = true;
        }
        tracing::info!(
            target: "umbrella_guard_shadow",
            umbrella = umbrella_id,
            field = String::from(uf.path.clone()),
            cfid,
            verdict = ?guard.value_verdict,
            name_corroborated = guard.name_corroborated,
            causes = ?guard.causes,
            "shadow guard verdict"
        );
    }
    any_contradictory
}

#[cfg(test)]
mod repro {
    use super::*;
    use deblob_core::id::{FamilyId, FamilyVersion, SchemaId};
    use deblob_core::ports::SchemaRecord;
    use deblob_core::semantic::{
        CanonicalFieldId, FieldEntry, FieldSemantics, PathSegment, SemanticMetadata,
    };

    fn rec(seed: u8, canonical: &str) -> SchemaRecord {
        SchemaRecord {
            schema_id: SchemaId::from_digest(&[seed; 32]),
            family_id: FamilyId::new_v7(),
            version: FamilyVersion(1),
            canonical: canonical.to_string(),
            canonicalizer: deblob_monoid::GENERALIZER.to_string(),
            provenance: serde_json::json!({}),
            semantic: None,
            semantic_fingerprint: None,
            privacy_class: None,
            value_profile_ref: None,
            value_profile_summary: None,
        }
    }

    fn meta(keys: [&str; 3], cfids: [&str; 3]) -> SemanticMetadata {
        SemanticMetadata {
            event_type: None,
            fields: keys
                .iter()
                .zip(cfids.iter())
                .map(|(k, c)| FieldEntry {
                    path: vec![PathSegment::Key((*k).to_string())],
                    semantics: FieldSemantics {
                        canonical_field_id: Some(CanonicalFieldId::new((*c).to_string())),
                        identifier_namespace: None,
                        unit: None,
                        numeric_scale: None,
                        temporal: None,
                        enum_semantics: None,
                    },
                })
                .collect(),
        }
    }

    const CFIDS: [&str; 3] = ["cfid_scopetest", "cfid_v", "cfid_w"];

    fn canon3(keys: [&str; 3], types: [&str; 3]) -> String {
        format!(
            r#"{{"optional":false,"types":["object"],"children":{{"{}":{{"optional":false,"types":["{}"]}},"{}":{{"optional":false,"types":["{}"]}},"{}":{{"optional":false,"types":["{}"]}}}}}}"#,
            keys[0], types[0], keys[1], types[1], keys[2], types[2]
        )
    }

    fn annotated_fields(rec: &SchemaRecord, m: &SemanticMetadata) -> Vec<ChildField> {
        child_fields_from_schema(rec, Some(m))
            .into_iter()
            .filter(|f| f.canonical_field_id.is_some())
            .collect()
    }

    fn typed_sig(fields: &[ChildField]) -> Vec<FieldSig> {
        let mut sig: Vec<FieldSig> = fields
            .iter()
            .filter_map(|f| {
                f.canonical_field_id
                    .as_ref()
                    .map(|c| (c.as_str().to_string(), field_type_tag(f)))
            })
            .collect();
        sig.sort();
        sig.dedup();
        sig
    }

    /// Runs the propose inner loop (group by typed sig -> build -> verify) over
    /// a set of already-annotated members, returning `(umbrella_id, verified
    /// transform count)` for every group that reached >=2 verified transforms.
    fn consolidate(members: &[&[ChildField]]) -> Vec<(String, usize)> {
        let mut groups: BTreeMap<Vec<FieldSig>, Vec<usize>> = BTreeMap::new();
        for (i, fields) in members.iter().enumerate() {
            groups.entry(typed_sig(fields)).or_default().push(i);
        }
        let anchor = DeterministicAnchor;
        let mut out = Vec::new();
        for (sig, idxs) in groups {
            if idxs.len() < 2 {
                continue;
            }
            let cfids: Vec<String> = sig.iter().map(|(c, _)| c.clone()).collect();
            let umb_id = umbrella_id_for(&sig);
            let group_members: Vec<&[ChildField]> = idxs.iter().map(|&i| members[i]).collect();
            let umbrella = build_umbrella(&umb_id, &cfids, &group_members);
            let umb_rev = format!("{umb_id}@{}", umbrella.version);
            let verified = idxs
                .iter()
                .filter(|&&i| {
                    let t = assemble_transform("c", "c", &umbrella, &umb_rev, members[i], &anchor);
                    verify_static(&t, &umbrella, members[i]).is_empty()
                })
                .count();
            if verified >= 2 {
                out.push((umb_id, verified));
            }
        }
        out
    }

    // Mirrors the two live demo schemas: identical typed signature {scopetest,v,w}
    // all `number` over DIFFERENT shapes (aaa/bbb/ccc vs xxx/yyy/zzz) -> ONE
    // umbrella with 2 verified transforms.
    #[test]
    fn two_flat_schemas_same_cfids_consolidate() {
        let r1 = rec(1, &canon3(["aaa", "bbb", "ccc"], ["number"; 3]));
        let f1 = annotated_fields(&r1, &meta(["aaa", "bbb", "ccc"], CFIDS));
        let r2 = rec(2, &canon3(["xxx", "yyy", "zzz"], ["number"; 3]));
        let f2 = annotated_fields(&r2, &meta(["xxx", "yyy", "zzz"], CFIDS));
        assert_eq!(f1.len(), 3);
        assert_eq!(f2.len(), 3);

        let out = consolidate(&[f1.as_slice(), f2.as_slice()]);
        assert_eq!(out.len(), 1, "expected exactly one umbrella: {out:?}");
        assert_eq!(out[0].1, 2, "both members must verify");
    }

    // Regression for the live `npush` poisoning bug: a THIRD schema carrying the
    // SAME cfid set but with `scopetest: bool` (vs the pair's `number`) must NOT
    // pull the two number-typed schemas out of consolidation. Type-partitioned
    // grouping puts the bool schema in its own (singleton, dropped) group; the
    // number pair still yields its umbrella.
    #[test]
    fn type_incompatible_third_schema_does_not_poison_pair() {
        let r1 = rec(1, &canon3(["aaa", "bbb", "ccc"], ["number"; 3]));
        let f1 = annotated_fields(&r1, &meta(["aaa", "bbb", "ccc"], CFIDS));
        let r2 = rec(2, &canon3(["xxx", "yyy", "zzz"], ["number"; 3]));
        let f2 = annotated_fields(&r2, &meta(["xxx", "yyy", "zzz"], CFIDS));
        // The poisoner: scopetest is `bool`, not `number`.
        let r3 = rec(3, &canon3(["p", "q", "r"], ["bool", "number", "number"]));
        let f3 = annotated_fields(&r3, &meta(["p", "q", "r"], CFIDS));
        assert_eq!(f3.len(), 3);

        let out = consolidate(&[f1.as_slice(), f2.as_slice(), f3.as_slice()]);
        assert_eq!(
            out.len(),
            1,
            "bool-typed third schema must form its own group, not block the number pair: {out:?}"
        );
        assert_eq!(out[0].1, 2, "the number pair must still both verify");
    }
}
