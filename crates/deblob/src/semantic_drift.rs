//! Two proposal-only semantic diagnostics (P2-D Task 7,
//! `deblob-p2d-hermes-review.md` §5): **semantic drift** and **same-`sem_`
//! / different-`sch_`**. Both SURFACE a signal; neither ever ACTS on it.
//!
//! ## Hard invariant: proposal-only, one-directional
//!
//! Nothing in this module ever aliases, merges, promotes, or mutates a
//! family, schema, `sem_`, or candidate. Every function here either:
//!   - is a pure function over already-fetched data ([`structural_relation`],
//!     [`detect_semantic_drift`], [`classify_semantic_collision`]), so it
//!     cannot touch storage at all; or
//!   - is an orchestrator ([`check_family_version_drift`],
//!     [`scan_semantic_collisions`]) that only ever calls READ methods on
//!     [`deblob_core::ports::Registry`] / [`crate::semantic_store::SemanticStore`]
//!     (`get_schema`, `active_semantic`, `schemas_by_semantic`) plus
//!     [`crate::metrics::Metrics`]' counters. Neither trait's write methods
//!     (`publish`, `append_revision`, ...) are ever called from here.
//!
//! The integration test in `crates/deblob/tests/semantic_drift_it.rs`
//! exercises both orchestrators against a real Redis and asserts the
//! `deblob:schema:*`/`deblob:family:*`/`deblob:sem-active:*`/
//! `deblob:sem-index:*` keys are byte-identical before and after.
//!
//! ## (a) Semantic drift
//!
//! A family "drifts" when it gains a structurally-COMPATIBLE new version
//! whose ACTIVE `sem_` differs from the prior version's active `sem_`. It
//! does NOT split the family — [`SemanticDrift`] only ever carries the two
//! `FamilyVersion`s and two `sem_`s, never a mutation. `None` -> `Some`
//! (a version's first semantic annotation) is explicitly NOT a drift — see
//! [`detect_semantic_drift`]'s doc.
//!
//! ## (b) Same-`sem_`, different-`sch_`
//!
//! [`crate::semantic_store::SemanticStore::schemas_by_semantic`] (the
//! reverse index Task 5 built) is scanned for `sem_`s shared by ≥2
//! schemas. Every pair is classified by [`structural_relation`]
//! (compatible / incompatible / identical-paths-changed-types) and by
//! [`CollisionStrength`] (annotation coverage). Only `strong`/`medium`
//! findings are review candidates; `weak` is logged via
//! `deblob_semantic_collision_total{strength="weak"}` and nothing else —
//! per the brief, sparse identical annotations do not prove equivalence.

use std::collections::BTreeMap;

use deblob_core::error::CoreError;
use deblob_core::id::{FamilyId, FamilyVersion, SchemaId, SemanticId};
use deblob_core::ports::Registry;
use deblob_core::revision::SemError;
use deblob_core::semantic::{PathSegment, SemanticMetadata};

use crate::metrics::Metrics;
use crate::semantic_store::SemanticStore;

// ---------------------------------------------------------------------
// Structural relation between two `deblob-canon-v1` shape JSON documents
// ---------------------------------------------------------------------

/// Errors from walking a `deblob-canon-v1` canonical shape JSON string
/// (`SchemaRecord::canonical`) to recover its typed field paths and the
/// leaf-vs-container type at each path. Mirrors
/// `deblob_semantic::path::PathError`'s two failure modes exactly, but this
/// module needs the per-path TYPE too (`deblob_semantic::path` only
/// enumerates paths, never types), so it walks the same grammar itself
/// rather than depending on that crate's private walker.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ShapeWalkError {
    #[error("canonical shape is not valid JSON: {0}")]
    InvalidJson(String),
    #[error("canonical shape does not match the deblob-canon-v1 shape grammar")]
    MalformedShape,
}

/// One of `"null"`/`"bool"`/`"num"`/`"str"` (a leaf) or `"obj"`/`"arr"` (a
/// container) — verbatim the same `"t"` discriminator values
/// `deblob-canon-v1` shape JSON itself uses (`deblob_fingerprint::shape`),
/// so no separate type vocabulary is invented here.
type ShapeType = &'static str;

fn leaf_type(t: &str) -> Option<ShapeType> {
    match t {
        "null" => Some("null"),
        "bool" => Some("bool"),
        "num" => Some("num"),
        "str" => Some("str"),
        _ => None,
    }
}

/// Walks `canonical` (a `deblob-canon-v1` shape JSON document) and returns
/// every field path reachable through at least one key/wildcard segment,
/// mapped to the `"t"` discriminator of the value found AT that path.
/// Mirrors `deblob_semantic::path::canonical_field_paths`'s walk exactly
/// (object fields contribute one `Key` segment each; arrays contribute one
/// shared `Wildcard` segment; the document root is never a path itself),
/// with the one addition this module needs: the type recorded per path.
///
/// For a `Wildcard` path backed by a heterogeneous array (`of` holding more
/// than one distinct element shape), the FIRST element's type wins — a
/// deliberate simplification: P2-D's structural-relation classification
/// only needs "did the type at this path change", and heterogeneous arrays
/// are already a rare, already-ambiguous case that P4's cross-field
/// semantic groups are better positioned to reason about.
pub fn typed_paths(
    canonical: &str,
) -> Result<BTreeMap<Vec<PathSegment>, ShapeType>, ShapeWalkError> {
    let value: serde_json::Value =
        serde_json::from_str(canonical).map_err(|e| ShapeWalkError::InvalidJson(e.to_string()))?;
    let mut out = BTreeMap::new();
    let mut current = Vec::new();
    walk_typed(&value, &mut current, &mut out)?;
    Ok(out)
}

fn node_type(value: &serde_json::Value) -> Result<&str, ShapeWalkError> {
    value
        .get("t")
        .and_then(serde_json::Value::as_str)
        .ok_or(ShapeWalkError::MalformedShape)
}

/// Maps a raw `"t"` discriminator string to the [`ShapeType`] this module
/// tracks per path — `null`/`bool`/`num`/`str` via [`leaf_type`], `obj`/
/// `arr` directly, anything else a [`ShapeWalkError::MalformedShape`].
/// Deliberately NOT `leaf_type(t).unwrap_or(match t {...})`: `unwrap_or`'s
/// argument is evaluated EAGERLY (unlike `unwrap_or_else`), so a `match`
/// containing a bare `return Err(..)` inside that argument position fires
/// on every call regardless of whether `leaf_type` returned `Some` — a real
/// bug this module hit once already (every leaf field call short-circuited
/// the whole walk). This helper's `if`/`else` keeps the short-circuiting
/// return conditional on actually needing it.
fn child_ty_of(t: &str) -> Result<ShapeType, ShapeWalkError> {
    if let Some(leaf) = leaf_type(t) {
        Ok(leaf)
    } else {
        match t {
            "obj" => Ok("obj"),
            "arr" => Ok("arr"),
            _ => Err(ShapeWalkError::MalformedShape),
        }
    }
}

fn walk_typed(
    value: &serde_json::Value,
    current: &mut Vec<PathSegment>,
    out: &mut BTreeMap<Vec<PathSegment>, ShapeType>,
) -> Result<(), ShapeWalkError> {
    let t = node_type(value)?;
    match t {
        "null" | "bool" | "num" | "str" => Ok(()), // leaf: own path already recorded by the parent below
        "obj" => {
            let fields = value
                .get("f")
                .and_then(serde_json::Value::as_object)
                .ok_or(ShapeWalkError::MalformedShape)?;
            for (k, v) in fields {
                let child_t = node_type(v)?;
                let child_ty = child_ty_of(child_t)?;
                current.push(PathSegment::Key(k.clone()));
                out.entry(current.clone()).or_insert(child_ty);
                walk_typed(v, current, out)?;
                current.pop();
            }
            Ok(())
        }
        "arr" => {
            let elements = value
                .get("of")
                .and_then(serde_json::Value::as_array)
                .ok_or(ShapeWalkError::MalformedShape)?;
            current.push(PathSegment::Wildcard);
            for element in elements {
                let child_t = node_type(element)?;
                let child_ty = child_ty_of(child_t)?;
                // First element's type wins for a heterogeneous array — see
                // `typed_paths`'s doc.
                out.entry(current.clone()).or_insert(child_ty);
                walk_typed(element, current, out)?;
            }
            current.pop();
            Ok(())
        }
        _ => Err(ShapeWalkError::MalformedShape),
    }
}

/// Count of LEAF field paths (type ∈ `"null"`/`"bool"`/`"num"`/`"str"`,
/// i.e. NOT `"obj"`/`"arr"`) in `canonical` — the denominator for
/// [`CollisionStrength`]'s annotation-coverage fraction.
pub fn leaf_field_count(canonical: &str) -> Result<usize, ShapeWalkError> {
    let types = typed_paths(canonical)?;
    Ok(types.values().filter(|t| leaf_type(t).is_some()).count())
}

/// How two schemas' structural shapes relate, for the same-`sem_`
/// classification (brief §5) and for gating semantic-drift eligibility.
///
/// Classification is by TYPE at each path COMMON to both shapes — adding or
/// removing fields never by itself makes two shapes `Incompatible` (that's
/// ordinary additive/subtractive schema evolution); only a type change at a
/// path both shapes share does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StructuralRelation {
    /// No common path changed type (fields may have been added/removed).
    Compatible,
    /// The two shapes have the EXACT SAME set of field paths, but at least
    /// one path's type differs — brief §5's "high-value review case".
    IdenticalPathsChangedTypes,
    /// Field-path sets differ AND at least one common path changed type.
    Incompatible,
}

/// Classifies the structural relation between two `deblob-canon-v1`
/// canonical shape JSON documents. Pure; touches no storage.
pub fn structural_relation(
    canonical_a: &str,
    canonical_b: &str,
) -> Result<StructuralRelation, ShapeWalkError> {
    let types_a = typed_paths(canonical_a)?;
    let types_b = typed_paths(canonical_b)?;

    let mut any_type_mismatch = false;
    for (path, ty_a) in &types_a {
        if let Some(ty_b) = types_b.get(path) {
            if ty_a != ty_b {
                any_type_mismatch = true;
            }
        }
    }

    if !any_type_mismatch {
        return Ok(StructuralRelation::Compatible);
    }

    let paths_a: Vec<&Vec<PathSegment>> = types_a.keys().collect();
    let paths_b: Vec<&Vec<PathSegment>> = types_b.keys().collect();
    if paths_a == paths_b {
        Ok(StructuralRelation::IdenticalPathsChangedTypes)
    } else {
        Ok(StructuralRelation::Incompatible)
    }
}

// ---------------------------------------------------------------------
// (a) Semantic drift
// ---------------------------------------------------------------------

/// A computed (never acted-on) semantic-drift signal: a family gained a
/// structurally-compatible new version whose active `sem_` differs from
/// the prior version's active `sem_`. Carries no method that could mutate
/// anything — it is a plain, queryable record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticDrift {
    pub family_id: FamilyId,
    pub prior_version: FamilyVersion,
    pub new_version: FamilyVersion,
    pub prior_sem: SemanticId,
    pub new_sem: SemanticId,
}

/// Detects semantic drift between one family's two adjacent versions.
/// Pure — touches no storage; the caller supplies both versions' canonical
/// shape JSON and active `sem_` (or `None` if unannotated).
///
/// Returns `Ok(None)` (never `Ok(Some(..))`) for every one of:
///   - either version has no active `sem_` at all (covers the documented
///     `None` -> `Some` first-annotation case, `Some` -> `None`, and
///     `None` -> `None` — none of these is "the active sem_ differs
///     between two annotated versions", which is what drift means);
///   - both versions carry the SAME `sem_` (no drift by definition);
///   - the two versions are NOT [`StructuralRelation::Compatible`] (per
///     brief §5: drift only fires for a structurally-compatible new
///     version — an incompatible or identical-paths-changed-types
///     transition is a DIFFERENT diagnostic, not drift).
pub fn detect_semantic_drift(
    family_id: FamilyId,
    prior_version: FamilyVersion,
    prior_canonical: &str,
    prior_sem: Option<&SemanticId>,
    new_version: FamilyVersion,
    new_canonical: &str,
    new_sem: Option<&SemanticId>,
) -> Result<Option<SemanticDrift>, ShapeWalkError> {
    let (prior_sem, new_sem) = match (prior_sem, new_sem) {
        (Some(p), Some(n)) => (p, n),
        _ => return Ok(None),
    };
    if prior_sem == new_sem {
        return Ok(None);
    }
    if structural_relation(prior_canonical, new_canonical)? != StructuralRelation::Compatible {
        return Ok(None);
    }
    Ok(Some(SemanticDrift {
        family_id,
        prior_version,
        new_version,
        prior_sem: prior_sem.clone(),
        new_sem: new_sem.clone(),
    }))
}

// ---------------------------------------------------------------------
// (b) Same-sem_, different-sch_
// ---------------------------------------------------------------------

/// Annotation-coverage strength of a same-`sem_` collision (brief §5).
/// Only `Strong`/`Medium` are review candidates; `Weak` is logged and
/// discarded — see [`SemanticCollisionFinding::is_review_candidate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollisionStrength {
    /// Same `canonical_event_type_id` AND ≥80% of the WEAKER schema's leaf
    /// fields carry a `canonical_field_id`.
    Strong,
    /// Same `canonical_event_type_id` and SOME (but <80%) leaf-field
    /// coverage.
    Medium,
    /// No `canonical_event_type_id`, or zero leaf-field coverage — only
    /// sparse unit/namespace/enum overlap, per brief §5.
    Weak,
}

impl CollisionStrength {
    /// The bounded Prometheus label value — `deblob_semantic_collision_total{strength}`.
    pub fn as_str(self) -> &'static str {
        match self {
            CollisionStrength::Strong => "strong",
            CollisionStrength::Medium => "medium",
            CollisionStrength::Weak => "weak",
        }
    }
}

/// Fraction of `canonical`'s LEAF fields whose path also appears in
/// `metadata.fields` with a non-`None` `canonical_field_id`. `0.0` when
/// `canonical` has zero leaf fields (never a divide-by-zero panic, and
/// never treated as 100% coverage of nothing).
fn canonical_field_id_coverage(
    metadata: &SemanticMetadata,
    canonical: &str,
) -> Result<f64, ShapeWalkError> {
    let types = typed_paths(canonical)?;
    let total_leaf = types.values().filter(|t| leaf_type(t).is_some()).count();
    if total_leaf == 0 {
        return Ok(0.0);
    }
    let annotated_leaf = metadata
        .fields
        .iter()
        .filter(|f| {
            f.semantics.canonical_field_id.is_some()
                && types
                    .get(&f.path)
                    .map(|t| leaf_type(t).is_some())
                    .unwrap_or(false)
        })
        .count();
    Ok(annotated_leaf as f64 / total_leaf as f64)
}

/// A finding for one pair of schemas sharing one `sem_` (brief §5's
/// `same_semantic_fingerprint_different_structure`). A plain, queryable
/// record — nothing here can mutate a family/schema/`sem_`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticCollisionFinding {
    pub sem_id: SemanticId,
    pub sch_a: SchemaId,
    pub sch_b: SchemaId,
    pub relation: StructuralRelation,
    pub strength: CollisionStrength,
    /// `true` only for `Strong`/`Medium` — `Weak` is diagnostic-only and
    /// must never be treated as a candidate for anything downstream.
    pub is_review_candidate: bool,
}

/// Classifies one same-`sem_` pair. `metadata` is the shared active
/// `SemanticMetadata` both schemas carry (identical by construction: `sem_`
/// is a pure hash of `SemanticMetadata`, so two schemas sharing a `sem_`
/// share byte-identical metadata — see `deblob_semantic::digest`). Pure;
/// touches no storage.
pub fn classify_semantic_collision(
    sem_id: SemanticId,
    sch_a: SchemaId,
    canonical_a: &str,
    sch_b: SchemaId,
    canonical_b: &str,
    metadata: &SemanticMetadata,
) -> Result<SemanticCollisionFinding, ShapeWalkError> {
    let relation = structural_relation(canonical_a, canonical_b)?;

    let coverage_a = canonical_field_id_coverage(metadata, canonical_a)?;
    let coverage_b = canonical_field_id_coverage(metadata, canonical_b)?;
    // Conservative: both schemas in the pair must show coverage, so the
    // pair's strength is bounded by whichever one has LESS evidence.
    let min_coverage = coverage_a.min(coverage_b);
    let has_event_type = metadata.event_type.is_some();

    let strength = if has_event_type && min_coverage >= 0.8 {
        CollisionStrength::Strong
    } else if has_event_type && min_coverage > 0.0 {
        CollisionStrength::Medium
    } else {
        CollisionStrength::Weak
    };
    let is_review_candidate = !matches!(strength, CollisionStrength::Weak);

    Ok(SemanticCollisionFinding {
        sem_id,
        sch_a,
        sch_b,
        relation,
        strength,
        is_review_candidate,
    })
}

// ---------------------------------------------------------------------
// Orchestrators: real reads via Registry/SemanticStore, real metrics
// ---------------------------------------------------------------------

/// Errors surfaced by this module's orchestrators — a thin union over the
/// two read-only stores' own error types plus "the schema this diagnostic
/// needed doesn't exist", never a new failure mode of its own.
#[derive(Debug, thiserror::Error)]
pub enum SemDriftError {
    #[error("registry: {0}")]
    Registry(#[from] CoreError),
    #[error("semantic store: {0}")]
    Semantic(#[from] SemError),
    #[error("canonical shape: {0}")]
    Shape(#[from] ShapeWalkError),
    #[error("schema {0:?} not found")]
    SchemaNotFound(SchemaId),
}

/// Orchestrates (a): fetches `prior_sch`/`new_sch`'s canonical shape
/// ([`Registry::get_schema`]) and active `sem_` ([`SemanticStore::
/// active_semantic`]), runs [`detect_semantic_drift`], and — ONLY if it
/// fires — increments `deblob_semantic_drift_total`. Every call this makes
/// is a READ; nothing here ever calls `publish`/`append_revision`.
#[allow(clippy::too_many_arguments)]
pub async fn check_family_version_drift(
    registry: &dyn Registry,
    sem_store: &dyn SemanticStore,
    metrics: &Metrics,
    family_id: FamilyId,
    prior_sch: &SchemaId,
    prior_version: FamilyVersion,
    new_sch: &SchemaId,
    new_version: FamilyVersion,
) -> Result<Option<SemanticDrift>, SemDriftError> {
    let prior_record = registry
        .get_schema(prior_sch)
        .await?
        .ok_or_else(|| SemDriftError::SchemaNotFound(prior_sch.clone()))?;
    let new_record = registry
        .get_schema(new_sch)
        .await?
        .ok_or_else(|| SemDriftError::SchemaNotFound(new_sch.clone()))?;

    let prior_sem = sem_store
        .active_semantic(prior_sch)
        .await?
        .map(|(_, sem, _)| sem);
    let new_sem = sem_store
        .active_semantic(new_sch)
        .await?
        .map(|(_, sem, _)| sem);

    let drift = detect_semantic_drift(
        family_id,
        prior_version,
        &prior_record.canonical,
        prior_sem.as_ref(),
        new_version,
        &new_record.canonical,
        new_sem.as_ref(),
    )?;

    if drift.is_some() {
        metrics.record_semantic_drift();
    }
    Ok(drift)
}

/// Orchestrates (b): reads the reverse index ([`SemanticStore::
/// schemas_by_semantic`]) for `sem_id`; if it maps to fewer than 2 schemas
/// there is nothing to collide, so this returns `Ok(vec![])` without
/// touching metrics at all. Otherwise classifies every unordered pair via
/// [`classify_semantic_collision`] and increments
/// `deblob_semantic_collision_total{strength}` once per pair, for EVERY
/// strength including `weak` (brief §5: weak is "logged for evaluation").
/// Every call this makes is a READ.
pub async fn scan_semantic_collisions(
    registry: &dyn Registry,
    sem_store: &dyn SemanticStore,
    metrics: &Metrics,
    sem_id: &SemanticId,
) -> Result<Vec<SemanticCollisionFinding>, SemDriftError> {
    let schema_ids = sem_store.schemas_by_semantic(sem_id).await?;
    if schema_ids.len() < 2 {
        return Ok(vec![]);
    }

    // Every member's active metadata hashes to this exact sem_id (that's
    // literally what the reverse index indexes), and sem_ is a pure hash of
    // SemanticMetadata (deblob_semantic::digest) — so every member's active
    // metadata is byte-identical. Read it once, from the first member,
    // rather than once per schema.
    let (metadata, _, _) = sem_store
        .active_semantic(&schema_ids[0])
        .await?
        .ok_or_else(|| SemDriftError::SchemaNotFound(schema_ids[0].clone()))?;

    let mut findings = Vec::with_capacity(schema_ids.len() * (schema_ids.len() - 1) / 2);
    for i in 0..schema_ids.len() {
        for j in (i + 1)..schema_ids.len() {
            let sch_a = &schema_ids[i];
            let sch_b = &schema_ids[j];
            let record_a = registry
                .get_schema(sch_a)
                .await?
                .ok_or_else(|| SemDriftError::SchemaNotFound(sch_a.clone()))?;
            let record_b = registry
                .get_schema(sch_b)
                .await?
                .ok_or_else(|| SemDriftError::SchemaNotFound(sch_b.clone()))?;

            let finding = classify_semantic_collision(
                sem_id.clone(),
                sch_a.clone(),
                &record_a.canonical,
                sch_b.clone(),
                &record_b.canonical,
                &metadata,
            )?;
            metrics.record_semantic_collision(finding.strength.as_str());
            findings.push(finding);
        }
    }
    Ok(findings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use deblob_core::semantic::{
        CanonicalEventTypeId, CanonicalFieldId, FieldEntry, FieldSemantics, Unit, UnitSystem,
    };
    use deblob_fingerprint::{canonical_bytes, parse_bounded, shape_of, Limits};

    fn canon(json: &[u8]) -> String {
        let node = parse_bounded(json, &Limits::default()).unwrap();
        let shape = shape_of(&node);
        String::from_utf8(canonical_bytes(&shape)).unwrap()
    }

    fn sem_id(seed: u8) -> SemanticId {
        SemanticId::from_digest(&[seed; 32])
    }

    fn sch_id(seed: u8) -> SchemaId {
        SchemaId::from_digest(&[seed; 32])
    }

    fn empty_semantics() -> FieldSemantics {
        FieldSemantics {
            canonical_field_id: None,
            identifier_namespace: None,
            unit: None,
            numeric_scale: None,
            temporal: None,
            enum_semantics: None,
        }
    }

    // -- typed_paths / structural_relation --------------------------------

    #[test]
    fn typed_paths_reports_leaf_and_container_types() {
        let canonical = canon(br#"{"a":{"b":1},"c":[1,2]}"#);
        let types = typed_paths(&canonical).unwrap();
        assert_eq!(
            types.get(&vec![PathSegment::Key("a".to_string())]),
            Some(&"obj")
        );
        assert_eq!(
            types.get(&vec![
                PathSegment::Key("a".to_string()),
                PathSegment::Key("b".to_string())
            ]),
            Some(&"num")
        );
        assert_eq!(
            types.get(&vec![PathSegment::Key("c".to_string())]),
            Some(&"arr")
        );
        assert_eq!(
            types.get(&vec![
                PathSegment::Key("c".to_string()),
                PathSegment::Wildcard
            ]),
            Some(&"num")
        );
    }

    #[test]
    fn leaf_field_count_excludes_containers() {
        let canonical = canon(br#"{"a":{"b":1},"c":"x"}"#);
        // leaves: a.b (num), c (str) = 2; "a" itself is a container.
        assert_eq!(leaf_field_count(&canonical).unwrap(), 2);
    }

    #[test]
    fn structural_relation_pure_addition_is_compatible() {
        let a = canon(br#"{"x":1}"#);
        let b = canon(br#"{"x":1,"y":"new"}"#);
        assert_eq!(
            structural_relation(&a, &b).unwrap(),
            StructuralRelation::Compatible
        );
    }

    #[test]
    fn structural_relation_identical_paths_changed_type_is_flagged() {
        let a = canon(br#"{"x":1}"#);
        let b = canon(br#"{"x":"one"}"#);
        assert_eq!(
            structural_relation(&a, &b).unwrap(),
            StructuralRelation::IdenticalPathsChangedTypes
        );
    }

    #[test]
    fn structural_relation_disjoint_type_change_is_incompatible() {
        let a = canon(br#"{"x":1,"y":true}"#);
        let b = canon(br#"{"x":"one","z":false}"#);
        assert_eq!(
            structural_relation(&a, &b).unwrap(),
            StructuralRelation::Incompatible
        );
    }

    #[test]
    fn structural_relation_identical_shapes_are_compatible() {
        let a = canon(br#"{"x":1}"#);
        let b = canon(br#"{"x":2}"#); // values never affect shape
        assert_eq!(
            structural_relation(&a, &b).unwrap(),
            StructuralRelation::Compatible
        );
    }

    // -- (a) semantic drift ------------------------------------------------

    #[test]
    fn same_sem_across_compatible_versions_is_not_drift() {
        let a = canon(br#"{"x":1}"#);
        let b = canon(br#"{"x":1,"y":2}"#);
        let s = sem_id(1);
        let result = detect_semantic_drift(
            FamilyId::new_v7(),
            FamilyVersion(1),
            &a,
            Some(&s),
            FamilyVersion(2),
            &b,
            Some(&s),
        )
        .unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn different_sem_on_compatible_versions_fires_drift_without_splitting_family() {
        let a = canon(br#"{"x":1}"#);
        let b = canon(br#"{"x":1,"y":2}"#);
        let family_id = FamilyId::new_v7();
        let prior_sem = sem_id(1);
        let new_sem = sem_id(2);

        let result = detect_semantic_drift(
            family_id.clone(),
            FamilyVersion(1),
            &a,
            Some(&prior_sem),
            FamilyVersion(2),
            &b,
            Some(&new_sem),
        )
        .unwrap();

        let drift = result.expect("must fire drift");
        assert_eq!(drift.family_id, family_id);
        assert_eq!(drift.prior_version, FamilyVersion(1));
        assert_eq!(drift.new_version, FamilyVersion(2));
        assert_eq!(drift.prior_sem, prior_sem);
        assert_eq!(drift.new_sem, new_sem);
        // The record carries only descriptive fields — there is no method
        // on SemanticDrift, FamilyId, or FamilyVersion that this test (or
        // any caller) could use to mutate a family: proof by construction
        // that "does not split the family" holds, not just by convention.
    }

    #[test]
    fn none_to_some_first_annotation_is_not_drift() {
        let a = canon(br#"{"x":1}"#);
        let b = canon(br#"{"x":1,"y":2}"#);
        let new_sem = sem_id(2);
        let result = detect_semantic_drift(
            FamilyId::new_v7(),
            FamilyVersion(1),
            &a,
            None,
            FamilyVersion(2),
            &b,
            Some(&new_sem),
        )
        .unwrap();
        assert_eq!(result, None, "first annotation must never read as drift");
    }

    #[test]
    fn some_to_none_annotation_removal_is_not_drift() {
        let a = canon(br#"{"x":1}"#);
        let b = canon(br#"{"x":1,"y":2}"#);
        let prior_sem = sem_id(1);
        let result = detect_semantic_drift(
            FamilyId::new_v7(),
            FamilyVersion(1),
            &a,
            Some(&prior_sem),
            FamilyVersion(2),
            &b,
            None,
        )
        .unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn none_to_none_is_not_drift() {
        let a = canon(br#"{"x":1}"#);
        let b = canon(br#"{"x":1,"y":2}"#);
        let result = detect_semantic_drift(
            FamilyId::new_v7(),
            FamilyVersion(1),
            &a,
            None,
            FamilyVersion(2),
            &b,
            None,
        )
        .unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn different_sem_on_incompatible_versions_does_not_fire_drift() {
        // Brief §5: drift only fires for a structurally-COMPATIBLE new
        // version. A type change at a shared path is a different signal
        // entirely (same-sem_/diff-structure's IdenticalPathsChangedTypes
        // case, or plain Incompatible) — never drift.
        let a = canon(br#"{"x":1}"#);
        let b = canon(br#"{"x":"one"}"#); // x: num -> str
        let prior_sem = sem_id(1);
        let new_sem = sem_id(2);
        let result = detect_semantic_drift(
            FamilyId::new_v7(),
            FamilyVersion(1),
            &a,
            Some(&prior_sem),
            FamilyVersion(2),
            &b,
            Some(&new_sem),
        )
        .unwrap();
        assert_eq!(result, None);
    }

    // -- (b) same-sem_/different-sch_ --------------------------------------

    fn metadata_with_event_type_full_coverage() -> SemanticMetadata {
        SemanticMetadata {
            event_type: Some(CanonicalEventTypeId::new("user.created")),
            fields: vec![
                FieldEntry {
                    path: vec![PathSegment::Key("a".to_string())],
                    semantics: FieldSemantics {
                        canonical_field_id: Some(CanonicalFieldId::new("user.a")),
                        ..empty_semantics()
                    },
                },
                FieldEntry {
                    path: vec![PathSegment::Key("b".to_string())],
                    semantics: FieldSemantics {
                        canonical_field_id: Some(CanonicalFieldId::new("user.b")),
                        ..empty_semantics()
                    },
                },
            ],
        }
    }

    #[test]
    fn two_schemas_high_coverage_same_event_type_is_strong_review_candidate() {
        let canonical_a = canon(br#"{"a":1,"b":2}"#);
        let canonical_b = canon(br#"{"a":1,"b":2}"#);
        let metadata = metadata_with_event_type_full_coverage();

        let finding = classify_semantic_collision(
            sem_id(1),
            sch_id(1),
            &canonical_a,
            sch_id(2),
            &canonical_b,
            &metadata,
        )
        .unwrap();

        assert_eq!(finding.strength, CollisionStrength::Strong);
        assert!(finding.is_review_candidate);
        assert_eq!(finding.relation, StructuralRelation::Compatible);
    }

    #[test]
    fn two_schemas_only_shared_unit_no_event_type_is_weak_not_a_candidate() {
        let canonical_a = canon(br#"{"temperature":1}"#);
        let canonical_b = canon(br#"{"temperature":1}"#);
        let metadata = SemanticMetadata {
            event_type: None,
            fields: vec![FieldEntry {
                path: vec![PathSegment::Key("temperature".to_string())],
                semantics: FieldSemantics {
                    unit: Some(Unit {
                        system: UnitSystem::Ucum,
                        code: "Cel".to_string(),
                    }),
                    ..empty_semantics()
                },
            }],
        };

        let finding = classify_semantic_collision(
            sem_id(2),
            sch_id(3),
            &canonical_a,
            sch_id(4),
            &canonical_b,
            &metadata,
        )
        .unwrap();

        assert_eq!(finding.strength, CollisionStrength::Weak);
        assert!(
            !finding.is_review_candidate,
            "weak must never be a review candidate"
        );
    }

    #[test]
    fn partial_coverage_with_event_type_is_medium() {
        let canonical_a = canon(br#"{"a":1,"b":2,"c":3,"d":4,"e":5}"#);
        let canonical_b = canon(br#"{"a":1,"b":2,"c":3,"d":4,"e":5}"#);
        // Only 1 of 5 leaf fields annotated with canonical_field_id: 20% <
        // 80%, so this must land Medium, not Strong.
        let metadata = SemanticMetadata {
            event_type: Some(CanonicalEventTypeId::new("user.created")),
            fields: vec![FieldEntry {
                path: vec![PathSegment::Key("a".to_string())],
                semantics: FieldSemantics {
                    canonical_field_id: Some(CanonicalFieldId::new("user.a")),
                    ..empty_semantics()
                },
            }],
        };

        let finding = classify_semantic_collision(
            sem_id(5),
            sch_id(5),
            &canonical_a,
            sch_id(6),
            &canonical_b,
            &metadata,
        )
        .unwrap();

        assert_eq!(finding.strength, CollisionStrength::Medium);
        assert!(finding.is_review_candidate);
    }

    #[test]
    fn coverage_is_the_minimum_across_the_pair_not_the_max() {
        // schema A has 2 leaf fields, both annotated (100% coverage);
        // schema B has 10 leaf fields, only the same 2 annotated (20%).
        // The pair's strength must be bounded by B's weaker coverage.
        let canonical_a = canon(br#"{"a":1,"b":2}"#);
        let canonical_b =
            canon(br#"{"a":1,"b":2,"c":3,"d":4,"e":5,"f":6,"g":7,"h":8,"i":9,"j":10}"#);
        let metadata = metadata_with_event_type_full_coverage();

        let finding = classify_semantic_collision(
            sem_id(7),
            sch_id(7),
            &canonical_a,
            sch_id(8),
            &canonical_b,
            &metadata,
        )
        .unwrap();

        assert_eq!(
            finding.strength,
            CollisionStrength::Medium,
            "must be bounded by the weaker schema's coverage, not the stronger one"
        );
    }

    #[test]
    fn identical_paths_changed_types_relation_is_reported_on_the_finding() {
        let canonical_a = canon(br#"{"a":1}"#);
        let canonical_b = canon(br#"{"a":"one"}"#);
        let metadata = SemanticMetadata {
            event_type: Some(CanonicalEventTypeId::new("user.created")),
            fields: vec![],
        };

        let finding = classify_semantic_collision(
            sem_id(9),
            sch_id(9),
            &canonical_a,
            sch_id(10),
            &canonical_b,
            &metadata,
        )
        .unwrap();

        assert_eq!(
            finding.relation,
            StructuralRelation::IdenticalPathsChangedTypes
        );
    }
}
