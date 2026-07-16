//! Deterministic structural-distance top-k retrieval over the P1 bucketed
//! index (deblob-p2ab Task 3; weighted-distance formula authoritative per
//! `docs/superpowers/plans/deblob-p2ab-hermes-review.md` § "Task 3 —
//! retrieval").
//!
//! This is the top-k candidate set the SLM (Task 5's shadow classifier)
//! sees — a schema omitted here can never be recovered by the model
//! downstream, so this module is load-bearing for the whole SLM lane's
//! recall. NO embeddings: per the Hermes review, structural distance is
//! used unless/until a later eval shows known-family `recall@3 < 95%` or
//! semantic-renaming false-splits exceed 25% of all false splits — fix
//! normalization / family dedup / weighting / bucket boundaries first.
//!
//! Two-sided distance input: both a candidate cluster's live
//! `deblob_monoid::Profile` and a retrieved schema's stored
//! `SchemaRecord::canonical` JSON are the SAME generalized wire format
//! (`{"optional":bool,"types":[...],"children":{...},"elem":{...}}`,
//! `deblob_monoid::profile::write_generalized_field`) — [`FieldSig`]
//! mirrors that shape closely enough to score distance from either side
//! without a value round trip through JSON on the candidate side.

use std::collections::{BTreeMap, BTreeSet};

use deblob_core::error::CoreError;
use deblob_core::ports::{FamilyRef, Registry};
use deblob_fingerprint::fieldband;
use deblob_monoid::{FieldNode, Profile};
use deblob_slm::contract::FamilyCandidate;
use unicode_normalization::UnicodeNormalization;

use crate::policy::generalized_shape_summary;

/// Contract version this retrieval module speaks — bumped whenever the
/// weighting formula, neighbor-bucket algorithm, or family-representative
/// dedup rule changes, so a shadow-log record (Task 5) can distinguish
/// "same candidate, different retrieval behavior" from noise.
pub const RETRIEVAL_VERSION: u32 = 2;

/// Default `k` (Hermes review, Task 3): the offline eval separately
/// exercises k = 1, 3, 5.
pub const DEFAULT_K: usize = 3;

/// Weighted structural-distance component weights (Hermes review, Task 3
/// — authoritative). Sums to 1.0, checked by `weights_sum_and_bounds`.
mod weight {
    pub const FIELD_PATH_TYPE: f32 = 0.35;
    pub const NAME_OVERLAP: f32 = 0.25;
    pub const PRESENCE_OVERLAP: f32 = 0.15;
    pub const DEPTH_SIMILARITY: f32 = 0.10;
    pub const NULLABILITY: f32 = 0.10;
    pub const ARRAY_MAP_SHAPE: f32 = 0.05;
}

/// The composite structural distance decomposed into its six weighted
/// components, each already normalized to `[0.0, 1.0]`. `total()` applies
/// the Hermes-review weights (which sum to 1.0), so `total()` is itself
/// always in `[0.0, 1.0]`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DistanceComponents {
    /// 35%: field-path / type-signature distance.
    pub field_path_type: f32,
    /// 25%: normalized field-name token overlap.
    pub name_overlap: f32,
    /// 15%: required / presence overlap.
    pub presence_overlap: f32,
    /// 10%: nesting / depth similarity.
    pub depth_similarity: f32,
    /// 10%: nullability & type-union similarity.
    pub nullability: f32,
    /// 5%: array / map shape similarity.
    pub array_map_shape: f32,
}

impl DistanceComponents {
    /// The weighted composite distance. Always in `[0.0, 1.0]` because
    /// every component is in `[0.0, 1.0]` and the weights sum to `1.0`.
    pub fn total(&self) -> f32 {
        weight::FIELD_PATH_TYPE * self.field_path_type
            + weight::NAME_OVERLAP * self.name_overlap
            + weight::PRESENCE_OVERLAP * self.presence_overlap
            + weight::DEPTH_SIMILARITY * self.depth_similarity
            + weight::NULLABILITY * self.nullability
            + weight::ARRAY_MAP_SHAPE * self.array_map_shape
    }
}

// --- Field-tree signature: the common shape both sides of a distance
// comparison are reduced to -------------------------------------------

/// A parsed field-tree signature mirroring the generalized-canonical wire
/// format (`deblob_monoid::Profile::generalized_canonical_json`) closely
/// enough for distance scoring. Built either from a live candidate
/// `Profile` (`from_node`, no JSON round trip) or from a retrieved
/// schema's stored canonical JSON (`from_canonical_json`).
#[derive(Debug, Clone, Default)]
struct FieldSig {
    optional: bool,
    types: BTreeSet<String>,
    children: BTreeMap<String, FieldSig>,
    elem: Option<Box<FieldSig>>,
}

/// Mirrors `deblob_monoid::profile::write_generalized_field`'s recursion
/// exactly: `denom` is the presence count `node.present` is compared
/// against for optionality, and each child/array-element recursion is
/// denominated against the CURRENT node's `types.object`/`types.array`
/// (not the child's own count) — the same asymmetry the wire format
/// itself encodes.
fn field_sig_from_node(node: &FieldNode, denom: u64) -> FieldSig {
    let mut sig = FieldSig {
        optional: node.present < denom,
        ..Default::default()
    };
    for (name, count) in [
        ("array", node.types.array),
        ("bool", node.types.bool),
        ("null", node.types.null),
        ("number", node.types.number),
        ("object", node.types.object),
        ("string", node.types.string),
    ] {
        if count > 0 {
            sig.types.insert(name.to_string());
        }
    }
    for (k, v) in &node.children {
        sig.children
            .insert(k.clone(), field_sig_from_node(v, node.types.object));
    }
    if let Some(elem) = &node.array_elem {
        sig.elem = Some(Box::new(field_sig_from_node(elem, node.types.array)));
    }
    sig
}

/// Parses one generalized-canonical JSON `serde_json::Value` node into a
/// [`FieldSig`]. Malformed / unexpected shapes degrade to
/// [`FieldSig::default`] (never a panic, never an error) — a corrupt or
/// foreign-format `canonical` string simply scores as maximally distant
/// rather than aborting retrieval for the whole bucket.
fn parse_field_sig(value: &serde_json::Value) -> FieldSig {
    let mut sig = FieldSig::default();
    let Some(obj) = value.as_object() else {
        return sig;
    };
    sig.optional = obj
        .get("optional")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if let Some(types) = obj.get("types").and_then(|v| v.as_array()) {
        sig.types = types
            .iter()
            .filter_map(|t| t.as_str().map(str::to_string))
            .collect();
    }
    if let Some(children) = obj.get("children").and_then(|v| v.as_object()) {
        for (k, v) in children {
            sig.children.insert(k.clone(), parse_field_sig(v));
        }
    }
    if let Some(elem) = obj.get("elem") {
        sig.elem = Some(Box::new(parse_field_sig(elem)));
    }
    sig
}

fn field_sig_from_canonical(canonical: &str) -> FieldSig {
    serde_json::from_str::<serde_json::Value>(canonical)
        .map(|v| parse_field_sig(&v))
        .unwrap_or_default()
}

// --- Structural features: a FieldSig flattened into the sets/maps the
// distance components compare -------------------------------------------

/// A [`FieldSig`] flattened into the per-path sets/maps the six distance
/// components compare. `path` is a dot-joined field path from the
/// document root (array elements append a literal `"[]"` segment).
#[derive(Debug, Clone, Default)]
struct StructuralFeatures {
    /// path -> type-union set, for every field at any depth.
    paths: BTreeMap<String, BTreeSet<String>>,
    /// Paths where `optional == false`.
    required: BTreeSet<String>,
    /// Paths whose type set contains `"null"`.
    nullable: BTreeSet<String>,
    /// Paths whose type set contains `"array"`.
    array_paths: BTreeSet<String>,
    /// Paths whose type set contains `"object"`.
    object_paths: BTreeSet<String>,
    /// Deterministically normalized name tokens (see
    /// [`normalize_name_tokens`]) across every field NAME in the tree —
    /// drives the name-overlap distance component. Names here are used
    /// ONLY as opaque tokens for a numeric distance score; they are never
    /// echoed into a prompt (deblob-p2ab Task 4 owns prompt safety).
    name_tokens: BTreeSet<String>,
    /// Max nesting depth of the field tree (root = 0).
    depth: u32,
}

fn extract_features(root: &FieldSig) -> StructuralFeatures {
    let mut f = StructuralFeatures {
        depth: max_depth(root),
        ..Default::default()
    };
    walk(root, "", &mut f);
    f
}

fn walk(sig: &FieldSig, prefix: &str, f: &mut StructuralFeatures) {
    for (name, child) in &sig.children {
        let path = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}.{name}")
        };
        record_path(f, &path, child);
        for token in normalize_name_tokens(name) {
            f.name_tokens.insert(token);
        }
        walk(child, &path, f);
        if let Some(elem) = &child.elem {
            let elem_path = format!("{path}[]");
            record_path(f, &elem_path, elem);
            walk(elem, &elem_path, f);
        }
    }
    // A root that is itself an array (top-level payloads that are JSON
    // arrays, not objects) has no name to tokenize, but its element shape
    // still contributes path/type information.
    if prefix.is_empty() {
        if let Some(elem) = &sig.elem {
            record_path(f, "[]", elem);
            walk(elem, "[]", f);
        }
    }
}

fn record_path(f: &mut StructuralFeatures, path: &str, sig: &FieldSig) {
    f.paths.insert(path.to_string(), sig.types.clone());
    if !sig.optional {
        f.required.insert(path.to_string());
    }
    if sig.types.contains("null") {
        f.nullable.insert(path.to_string());
    }
    if sig.types.contains("array") {
        f.array_paths.insert(path.to_string());
    }
    if sig.types.contains("object") {
        f.object_paths.insert(path.to_string());
    }
}

fn max_depth(sig: &FieldSig) -> u32 {
    let mut m = 0u32;
    for child in sig.children.values() {
        m = m.max(1 + max_depth(child));
    }
    if let Some(elem) = &sig.elem {
        m = m.max(1 + max_depth(elem));
    }
    m
}

/// Length cap applied to each normalized name token (deblob-p2ab Task 3:
/// "length-cap"). Generous enough to never truncate a realistic field
/// name, tight enough to bound a pathological one.
const MAX_TOKEN_LEN: usize = 40;
/// Cap on the number of tokens extracted from a single field name, so a
/// pathological name (e.g. thousands of separator characters) can't blow
/// up the name-token set.
const MAX_TOKENS_PER_NAME: usize = 16;

/// Deterministically normalizes a field NAME into lowercase tokens for
/// name-overlap distance scoring: Unicode-NFC-normalizes, then splits on
/// non-alphanumeric separators AND camelCase boundaries, case-folds each
/// token, and length-/count-caps the result.
///
/// `userId`, `user_id`, and `USER-ID` all normalize to the token set
/// `{"user", "id"}` — this is used ONLY as input to a numeric distance
/// score. Field names must never become prompt instructions; that
/// invariant belongs to deblob-p2ab Task 4's prompt builder, not this
/// distance function, but this function never emits anything beyond a
/// bounded, case-folded token string either way.
fn normalize_name_tokens(name: &str) -> Vec<String> {
    let nfc: String = name.nfc().collect();
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut prev_was_lower_or_digit = false;

    for c in nfc.chars() {
        if c.is_alphanumeric() {
            if c.is_uppercase() && prev_was_lower_or_digit && !current.is_empty() {
                tokens.push(std::mem::take(&mut current));
            }
            for lc in c.to_lowercase() {
                current.push(lc);
            }
            prev_was_lower_or_digit = c.is_lowercase() || c.is_numeric();
        } else {
            if !current.is_empty() {
                tokens.push(std::mem::take(&mut current));
            }
            prev_was_lower_or_digit = false;
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }

    tokens
        .into_iter()
        .filter(|t| !t.is_empty())
        .take(MAX_TOKENS_PER_NAME)
        .map(|t| t.chars().take(MAX_TOKEN_LEN).collect())
        .collect()
}

// --- Distance components -------------------------------------------------

fn jaccard_distance<T: Ord>(a: &BTreeSet<T>, b: &BTreeSet<T>) -> f32 {
    if a.is_empty() && b.is_empty() {
        return 0.0;
    }
    let intersection = a.intersection(b).count();
    let union = a.union(b).count();
    1.0 - (intersection as f32 / union as f32)
}

fn depth_distance(a: u32, b: u32) -> f32 {
    let denom = a.max(b).max(1) as f32;
    (a as f32 - b as f32).abs() / denom
}

/// For every field path in the union of both sides: `1 - jaccard(types)`
/// if the path exists on both sides, `1.0` (max distance) if it exists on
/// only one. Averaged over the union (`0.0` if both sides have no paths
/// at all — e.g. two empty/scalar-root candidates).
fn field_path_type_distance(a: &StructuralFeatures, b: &StructuralFeatures) -> f32 {
    let all_paths: BTreeSet<&String> = a.paths.keys().chain(b.paths.keys()).collect();
    if all_paths.is_empty() {
        return 0.0;
    }
    let total: f32 = all_paths
        .iter()
        .map(|path| match (a.paths.get(*path), b.paths.get(*path)) {
            (Some(ta), Some(tb)) => jaccard_distance(ta, tb),
            _ => 1.0,
        })
        .sum();
    total / all_paths.len() as f32
}

/// Blends nullable-path overlap with whole-tree type-union overlap (the
/// full multiset of every type ever seen at any path), so two schemas
/// that agree on which fields are nullable AND on their overall type
/// vocabulary score as similar.
fn nullability_distance(a: &StructuralFeatures, b: &StructuralFeatures) -> f32 {
    let nullable_d = jaccard_distance(&a.nullable, &b.nullable);
    let union_a: BTreeSet<String> = a.paths.values().flatten().cloned().collect();
    let union_b: BTreeSet<String> = b.paths.values().flatten().cloned().collect();
    let type_union_d = jaccard_distance(&union_a, &union_b);
    (nullable_d + type_union_d) / 2.0
}

fn array_map_shape_distance(a: &StructuralFeatures, b: &StructuralFeatures) -> f32 {
    let array_d = jaccard_distance(&a.array_paths, &b.array_paths);
    let object_d = jaccard_distance(&a.object_paths, &b.object_paths);
    (array_d + object_d) / 2.0
}

/// The pure, Redis-free composite distance (Hermes review, Task 3
/// weights). Deterministic: identical inputs always produce an identical
/// `DistanceComponents`.
fn compute_distance(a: &StructuralFeatures, b: &StructuralFeatures) -> DistanceComponents {
    DistanceComponents {
        field_path_type: field_path_type_distance(a, b),
        name_overlap: jaccard_distance(&a.name_tokens, &b.name_tokens),
        presence_overlap: jaccard_distance(&a.required, &b.required),
        depth_similarity: depth_distance(a.depth, b.depth),
        nullability: nullability_distance(a, b),
        array_map_shape: array_map_shape_distance(a, b),
    }
}

// --- Bucket neighborhood ---------------------------------------------------

/// The `(band, depth)` neighborhood [`retrieve_topk`] discovers buckets
/// across: the field-count bands `{n-1, n, n+1}` map to (via
/// [`deblob_fingerprint::fieldband`]), and nesting depth in `{d-1, d,
/// d+1}` (deblob-p2ab Task 3: "same field-count band ±1, same/near
/// depth").
///
/// Deliberately independent of `top_keys_sorted` / `reqhash8` — unlike an
/// exact [`deblob_fingerprint::bucket_key`], which hashes the candidate's
/// OWN top-level key names, this neighborhood is blind to field NAMES
/// entirely. That is the fix: a family whose top-level fields were merely
/// renamed (e.g. `widgetCount` -> `widget_count`, any case/separator
/// variant, same structure) hashes to a DIFFERENT `reqhash8` at the SAME
/// band/depth, so an exact bucket_key lookup can never find it, but
/// `Registry::list_families_by_band_depth`'s prefix scan over this
/// neighborhood can — the six-component distance scorer (in particular its
/// normalized name-overlap component) gets a chance to actually rank it
/// instead of the family being invisible to retrieval altogether.
///
/// A deterministic function of the candidate ALONE — no registry round
/// trip is needed to compute it. Deduplicated.
fn neighbor_bands_and_depths(profile: &Profile) -> (Vec<u32>, Vec<u32>) {
    let summary = generalized_shape_summary(profile);
    let bands: BTreeSet<u32> = [
        summary.top_level_fields.saturating_sub(1),
        summary.top_level_fields,
        summary.top_level_fields + 1,
    ]
    .into_iter()
    .map(fieldband)
    .collect();
    let depths: BTreeSet<u32> = [
        summary.depth.saturating_sub(1),
        summary.depth,
        summary.depth + 1,
    ]
    .into_iter()
    .collect();

    (bands.into_iter().collect(), depths.into_iter().collect())
}

// --- Ranking + family-representative dedup ---------------------------------

/// One scored `FamilyRef`, kept alongside its parsed features until
/// family-level dedup is done.
struct Scored {
    candidate: FamilyCandidate,
}

fn score_refs(candidate_features: &StructuralFeatures, refs: &[FamilyRef]) -> Vec<Scored> {
    refs.iter()
        .map(|r| {
            let features = extract_features(&field_sig_from_canonical(&r.canonical));
            let distance = compute_distance(candidate_features, &features).total();
            Scored {
                candidate: FamilyCandidate {
                    family_id: r.family_id.clone(),
                    schema_id: r.schema_id.clone(),
                    version: r.version.0,
                    distance,
                    rank: 0, // assigned after final sort
                },
            }
        })
        .collect()
}

fn approx_eq(a: f32, b: f32) -> bool {
    (a - b).abs() < 1e-6
}

/// Per family, keeps only the NEAREST version (ties broken by version
/// ascending, then `schema_id` for full determinism) — this is what
/// guarantees the returned top-k spans distinct families rather than
/// surfacing several adjacent versions of the same family (deblob-p2ab
/// Task 3: "family REPRESENTATIVES, not 5 adjacent versions of one
/// family"). Sorted by `(distance, family_id)` for stable, deterministic
/// ranking independent of `HashMap`/registry iteration order.
fn family_representatives(scored: Vec<Scored>) -> Vec<FamilyCandidate> {
    let mut by_family: BTreeMap<String, Vec<FamilyCandidate>> = BTreeMap::new();
    for s in scored {
        by_family
            .entry(s.candidate.family_id.as_str().to_string())
            .or_default()
            .push(s.candidate);
    }

    let mut representatives: Vec<FamilyCandidate> = by_family
        .into_values()
        .filter_map(|mut versions| {
            versions.sort_by(|a, b| {
                a.distance
                    .partial_cmp(&b.distance)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then(a.version.cmp(&b.version))
                    .then(a.schema_id.as_str().cmp(b.schema_id.as_str()))
            });
            versions.into_iter().next()
        })
        .collect();

    representatives.sort_by(|a, b| {
        a.distance
            .partial_cmp(&b.distance)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.family_id.as_str().cmp(b.family_id.as_str()))
    });

    representatives
}

/// The result of one `retrieve_topk` call.
#[derive(Debug, Clone, PartialEq)]
pub struct RetrievalResult {
    /// Rank-ascending, distinct-family top-k (`rank` 1-based, `distance`
    /// in `[0.0, 1.0]`). Empty for the gold-ABSENT arm — a candidate with
    /// no nearby families.
    pub candidates: Vec<FamilyCandidate>,
    /// Every family-representative that tied (within floating-point
    /// tolerance) with the last (lowest-ranked) entry in `candidates` but
    /// was excluded by the `k` cutoff — surfaces retrieval ambiguity at
    /// the boundary rather than silently dropping it. Empty when
    /// `candidates` is empty or there was no tie at the cutoff.
    pub ties: Vec<FamilyCandidate>,
    /// `distance(rank 2) - distance(rank 1)`; `0.0` if fewer than two
    /// candidates were retrieved.
    pub top1_top2_margin: f32,
    /// [`RETRIEVAL_VERSION`] at the time this result was produced.
    pub retrieval_version: u32,
}

/// Ranks `refs` (already fetched from the registry) against
/// `candidate_features` and returns the top-`k` distinct-family result.
/// Pure and Redis-free — the only I/O in this module is the
/// `Registry::list_families_in_buckets` call inside [`retrieve_topk`].
fn rank_candidates(
    candidate_features: &StructuralFeatures,
    refs: &[FamilyRef],
    k: usize,
) -> RetrievalResult {
    let scored = score_refs(candidate_features, refs);
    let representatives = family_representatives(scored);

    let top: Vec<FamilyCandidate> = representatives
        .iter()
        .take(k)
        .enumerate()
        .map(|(i, c)| FamilyCandidate {
            rank: (i + 1) as u32,
            ..c.clone()
        })
        .collect();

    let top1_top2_margin = if top.len() >= 2 {
        top[1].distance - top[0].distance
    } else {
        0.0
    };

    let ties = match top.last() {
        Some(boundary) => representatives
            .iter()
            .skip(top.len())
            .filter(|r| approx_eq(r.distance, boundary.distance))
            .cloned()
            .map(|c| FamilyCandidate {
                rank: top.len() as u32,
                ..c
            })
            .collect(),
        None => Vec::new(),
    };

    RetrievalResult {
        candidates: top,
        ties,
        top1_top2_margin,
        retrieval_version: RETRIEVAL_VERSION,
    }
}

/// Retrieves the deterministic top-`k` family candidates for `candidate`'s
/// structural cluster: gathers every family across `candidate`'s
/// `(band, depth)` bucket neighborhood ([`neighbor_bands_and_depths`]) via
/// [`Registry::list_families_by_band_depth`] — a widened, name-blind
/// discovery that finds renamed-top-level-field families an exact-key
/// lookup would miss (deblob-p2ab Task 3 recall fix) — scores each by
/// weighted structural distance, collapses to one representative per
/// family, and returns the nearest `k`, ranked ascending by distance.
///
/// A candidate with no nearby families returns an empty `candidates` (the
/// gold-ABSENT arm) — never an error; the caller (the Task 5 shadow
/// classifier) is expected to have the model abstain with
/// `candidate_missing` in that case.
pub async fn retrieve_topk(
    candidate: &Profile,
    registry: &dyn Registry,
    k: usize,
) -> Result<RetrievalResult, CoreError> {
    let candidate_sig = field_sig_from_node(&candidate.root, candidate.count);
    let candidate_features = extract_features(&candidate_sig);

    let (bands, depths) = neighbor_bands_and_depths(candidate);
    let refs = registry
        .list_families_by_band_depth(&bands, &depths)
        .await?;

    Ok(rank_candidates(&candidate_features, &refs, k))
}

/// [`retrieve_topk`] with [`DEFAULT_K`].
pub async fn retrieve_topk_default(
    candidate: &Profile,
    registry: &dyn Registry,
) -> Result<RetrievalResult, CoreError> {
    retrieve_topk(candidate, registry, DEFAULT_K).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use deblob_core::id::{FamilyId, FamilyVersion, SchemaId};
    use deblob_fingerprint::{parse_bounded, Limits};
    use std::sync::atomic::{AtomicUsize, Ordering};

    // -- test fixtures ------------------------------------------------

    fn profile_from_json(json: &str) -> Profile {
        let node = parse_bounded(json.as_bytes(), &Limits::default()).unwrap();
        Profile::from_node(&node)
    }

    /// Builds a minimal generalized-canonical JSON string for a
    /// top-level object with the given `(name, types, optional)` fields
    /// — enough to drive the distance function without a full `Profile`
    /// round trip.
    fn gen_canonical(fields: &[(&str, &[&str], bool)]) -> String {
        let children: serde_json::Map<String, serde_json::Value> = fields
            .iter()
            .map(|(name, types, optional)| {
                (
                    name.to_string(),
                    serde_json::json!({"optional": optional, "types": types}),
                )
            })
            .collect();
        serde_json::json!({
            "optional": false,
            "types": ["object"],
            "children": children,
        })
        .to_string()
    }

    fn schema_id(byte: u8) -> SchemaId {
        SchemaId::from_digest(&[byte; 32])
    }

    struct FakeRegistry {
        families: Vec<FamilyRef>,
        calls: AtomicUsize,
    }

    impl FakeRegistry {
        fn new(families: Vec<FamilyRef>) -> Self {
            Self {
                families,
                calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl Registry for FakeRegistry {
        async fn get_schema(
            &self,
            _id: &SchemaId,
        ) -> Result<Option<deblob_core::ports::SchemaRecord>, CoreError> {
            unimplemented!("retrieval never reads a schema by id directly")
        }
        async fn resolve_structural(
            &self,
            _bucket_key: &str,
            _fingerprint: &SchemaId,
        ) -> Result<Option<SchemaId>, CoreError> {
            unimplemented!("retrieval never resolves the hot-path exact index")
        }
        async fn publish(
            &self,
            _record: deblob_core::ports::SchemaRecord,
            _alias_from: &deblob_core::id::CandidateId,
            _bucket_key: &str,
            _variant_members: &[(String, String)],
            _actor: &str,
            _reason: &str,
        ) -> Result<FamilyVersion, CoreError> {
            unimplemented!("retrieval never publishes")
        }
        async fn get_alias(
            &self,
            _id: &deblob_core::id::CandidateId,
        ) -> Result<Option<SchemaId>, CoreError> {
            unimplemented!("retrieval never resolves aliases")
        }
        async fn list_schemas(
            &self,
            _cursor: Option<String>,
            _limit: usize,
        ) -> Result<(Vec<deblob_core::ports::SchemaRecord>, Option<String>), CoreError> {
            unimplemented!("retrieval never lists all schemas")
        }
        async fn list_families_in_buckets(
            &self,
            _bucket_keys: &[String],
        ) -> Result<Vec<FamilyRef>, CoreError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.families.clone())
        }
        async fn list_families_by_band_depth(
            &self,
            _bands: &[u32],
            _depths: &[u32],
        ) -> Result<Vec<FamilyRef>, CoreError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.families.clone())
        }
        async fn family_version_schema(
            &self,
            _family_id: &deblob_core::id::FamilyId,
            _version: FamilyVersion,
        ) -> Result<Option<SchemaId>, CoreError> {
            unimplemented!("not exercised by retrieval tests")
        }
    }

    /// A `Registry` fake that, unlike [`FakeRegistry`] above, actually
    /// simulates the real bucket-key semantics: every family is stored
    /// alongside the REAL `bucket_key` it would have been published under
    /// (derived from its own canonical shape via `bucket_key`), and
    /// `list_families_in_buckets`/`list_families_by_band_depth` genuinely
    /// filter by it — exact-key match for the former, `(band, depth)`
    /// prefix match (ignoring `reqhash8`) for the latter. This is what lets
    /// [`renamed_top_level_field_family_is_retrieved`] actually distinguish
    /// "found via the old exact-key path" from "found via the new widened
    /// path" instead of a mock that returns every family regardless of the
    /// query, which would pass even with the pre-fix bug present.
    struct BucketAwareFakeRegistry {
        families: Vec<(String, FamilyRef)>,
    }

    impl BucketAwareFakeRegistry {
        fn new(families: Vec<(String, FamilyRef)>) -> Self {
            Self { families }
        }
    }

    /// Splits a stored `"deblob:index:{band}:{depth}:{reqhash8}"` bucket
    /// key into its `(band, depth)` components, for
    /// `BucketAwareFakeRegistry::list_families_by_band_depth`'s prefix
    /// simulation. `None` for a malformed key (never produced by the real
    /// `bucket_key`, but defensive all the same).
    fn band_and_depth_of(bucket_key: &str) -> Option<(u32, u32)> {
        let mut parts = bucket_key.split(':');
        if parts.next()? != "deblob" || parts.next()? != "index" {
            return None;
        }
        let band: u32 = parts.next()?.parse().ok()?;
        let depth: u32 = parts.next()?.parse().ok()?;
        Some((band, depth))
    }

    #[async_trait::async_trait]
    impl Registry for BucketAwareFakeRegistry {
        async fn get_schema(
            &self,
            _id: &SchemaId,
        ) -> Result<Option<deblob_core::ports::SchemaRecord>, CoreError> {
            unimplemented!("retrieval never reads a schema by id directly")
        }
        async fn resolve_structural(
            &self,
            _bucket_key: &str,
            _fingerprint: &SchemaId,
        ) -> Result<Option<SchemaId>, CoreError> {
            unimplemented!("retrieval never resolves the hot-path exact index")
        }
        async fn publish(
            &self,
            _record: deblob_core::ports::SchemaRecord,
            _alias_from: &deblob_core::id::CandidateId,
            _bucket_key: &str,
            _variant_members: &[(String, String)],
            _actor: &str,
            _reason: &str,
        ) -> Result<FamilyVersion, CoreError> {
            unimplemented!("retrieval never publishes")
        }
        async fn get_alias(
            &self,
            _id: &deblob_core::id::CandidateId,
        ) -> Result<Option<SchemaId>, CoreError> {
            unimplemented!("retrieval never resolves aliases")
        }
        async fn list_schemas(
            &self,
            _cursor: Option<String>,
            _limit: usize,
        ) -> Result<(Vec<deblob_core::ports::SchemaRecord>, Option<String>), CoreError> {
            unimplemented!("retrieval never lists all schemas")
        }
        async fn list_families_in_buckets(
            &self,
            bucket_keys: &[String],
        ) -> Result<Vec<FamilyRef>, CoreError> {
            let wanted: BTreeSet<&str> = bucket_keys.iter().map(String::as_str).collect();
            Ok(self
                .families
                .iter()
                .filter(|(k, _)| wanted.contains(k.as_str()))
                .map(|(_, r)| r.clone())
                .collect())
        }
        async fn list_families_by_band_depth(
            &self,
            bands: &[u32],
            depths: &[u32],
        ) -> Result<Vec<FamilyRef>, CoreError> {
            let bands: BTreeSet<u32> = bands.iter().copied().collect();
            let depths: BTreeSet<u32> = depths.iter().copied().collect();
            Ok(self
                .families
                .iter()
                .filter(|(k, _)| match band_and_depth_of(k) {
                    Some((b, d)) => bands.contains(&b) && depths.contains(&d),
                    None => false,
                })
                .map(|(_, r)| r.clone())
                .collect())
        }
        async fn family_version_schema(
            &self,
            _family_id: &deblob_core::id::FamilyId,
            _version: FamilyVersion,
        ) -> Result<Option<SchemaId>, CoreError> {
            unimplemented!("not exercised by retrieval tests")
        }
    }

    fn family_ref(
        family: &FamilyId,
        schema_byte: u8,
        version: u32,
        canonical: String,
    ) -> FamilyRef {
        FamilyRef {
            family_id: family.clone(),
            schema_id: schema_id(schema_byte),
            version: FamilyVersion(version),
            canonical,
        }
    }

    // -- 1. closest_family_ranks_first ---------------------------------

    #[tokio::test]
    async fn closest_family_ranks_first() {
        let candidate = profile_from_json(r#"{"user_id":"a","email":"b","age":1}"#);

        let fam_near = FamilyId::new_v7();
        let fam_mid = FamilyId::new_v7();
        let fam_far = FamilyId::new_v7();

        let families = vec![
            // Identical field set/types -> nearest.
            family_ref(
                &fam_near,
                1,
                1,
                gen_canonical(&[
                    ("user_id", &["string"], false),
                    ("email", &["string"], false),
                    ("age", &["number"], false),
                ]),
            ),
            // Shares one field, differs on the rest -> mid.
            family_ref(
                &fam_mid,
                2,
                1,
                gen_canonical(&[
                    ("user_id", &["string"], false),
                    ("shipping_address", &["string"], true),
                    ("total", &["number"], true),
                ]),
            ),
            // Shares nothing -> farthest.
            family_ref(
                &fam_far,
                3,
                1,
                gen_canonical(&[
                    ("widget_count", &["number"], false),
                    ("color", &["string"], false),
                ]),
            ),
        ];

        let registry = FakeRegistry::new(families);
        let result = retrieve_topk(&candidate, &registry, 3).await.unwrap();

        assert_eq!(result.candidates.len(), 3);
        assert_eq!(result.candidates[0].rank, 1);
        assert_eq!(result.candidates[0].family_id, fam_near);
        assert_eq!(result.candidates[0].distance, 0.0);
        // Strictly increasing distance by rank.
        assert!(result.candidates[0].distance < result.candidates[1].distance);
        assert!(result.candidates[1].distance < result.candidates[2].distance);
        assert_eq!(result.candidates[2].family_id, fam_far);
    }

    // -- 2. weights_sum_and_bounds --------------------------------------

    #[test]
    fn weights_sum_and_bounds() {
        let sum = weight::FIELD_PATH_TYPE
            + weight::NAME_OVERLAP
            + weight::PRESENCE_OVERLAP
            + weight::DEPTH_SIMILARITY
            + weight::NULLABILITY
            + weight::ARRAY_MAP_SHAPE;
        assert!(
            (sum - 1.0).abs() < 1e-6,
            "weights must sum to 1.0, got {sum}"
        );

        let all_zero = DistanceComponents {
            field_path_type: 0.0,
            name_overlap: 0.0,
            presence_overlap: 0.0,
            depth_similarity: 0.0,
            nullability: 0.0,
            array_map_shape: 0.0,
        };
        assert_eq!(all_zero.total(), 0.0);

        let all_one = DistanceComponents {
            field_path_type: 1.0,
            name_overlap: 1.0,
            presence_overlap: 1.0,
            depth_similarity: 1.0,
            nullability: 1.0,
            array_map_shape: 1.0,
        };
        assert!((all_one.total() - 1.0).abs() < 1e-6);

        // A known weighted mix: only field_path_type and name_overlap set.
        let mixed = DistanceComponents {
            field_path_type: 1.0,
            name_overlap: 1.0,
            presence_overlap: 0.0,
            depth_similarity: 0.0,
            nullability: 0.0,
            array_map_shape: 0.0,
        };
        let expected = weight::FIELD_PATH_TYPE + weight::NAME_OVERLAP;
        assert!((mixed.total() - expected).abs() < 1e-6);
        assert!(mixed.total() >= 0.0 && mixed.total() <= 1.0);
    }

    // -- 3. family_representatives_not_versions --------------------------

    #[tokio::test]
    async fn family_representatives_not_versions() {
        let candidate = profile_from_json(r#"{"id":"a","status":"b"}"#);

        let fam_multi = FamilyId::new_v7();
        let fam_b = FamilyId::new_v7();
        let fam_c = FamilyId::new_v7();

        let families = vec![
            // Three versions of the SAME family, at varying distance.
            family_ref(
                &fam_multi,
                1,
                1,
                gen_canonical(&[("id", &["string"], false), ("status", &["string"], false)]),
            ),
            family_ref(
                &fam_multi,
                2,
                2,
                gen_canonical(&[
                    ("id", &["string"], false),
                    ("status", &["string"], false),
                    ("extra", &["number"], true),
                ]),
            ),
            family_ref(
                &fam_multi,
                3,
                3,
                gen_canonical(&[("id", &["string"], false)]),
            ),
            family_ref(
                &fam_b,
                4,
                1,
                gen_canonical(&[("id", &["string"], false), ("status", &["string"], true)]),
            ),
            family_ref(&fam_c, 5, 1, gen_canonical(&[("id", &["number"], false)])),
        ];

        let registry = FakeRegistry::new(families);
        let result = retrieve_topk(&candidate, &registry, 3).await.unwrap();

        assert_eq!(result.candidates.len(), 3);
        let family_ids: BTreeSet<String> = result
            .candidates
            .iter()
            .map(|c| c.family_id.as_str().to_string())
            .collect();
        assert_eq!(
            family_ids.len(),
            3,
            "top-3 must span 3 distinct families, got {family_ids:?}"
        );
        assert!(family_ids.contains(fam_multi.as_str()));
        assert!(family_ids.contains(fam_b.as_str()));
        assert!(family_ids.contains(fam_c.as_str()));
    }

    // -- 4. gold_absent_returns_empty ------------------------------------

    #[tokio::test]
    async fn gold_absent_returns_empty() {
        let candidate = profile_from_json(r#"{"totally_novel_field":true}"#);
        let registry = FakeRegistry::new(vec![]);

        let result = retrieve_topk(&candidate, &registry, 3).await.unwrap();

        assert!(result.candidates.is_empty());
        assert!(result.ties.is_empty());
        assert_eq!(result.top1_top2_margin, 0.0);
    }

    // -- 5. deterministic_ranking -----------------------------------------

    #[tokio::test]
    async fn deterministic_ranking() {
        let candidate = profile_from_json(r#"{"a":1,"b":2}"#);

        // Two families that are EXACT ties on distance, to exercise
        // tie-break stability by family_id as well.
        let fam_x = FamilyId::new_v7();
        let fam_y = FamilyId::new_v7();
        let families = vec![
            family_ref(&fam_x, 1, 1, gen_canonical(&[("q", &["string"], false)])),
            family_ref(&fam_y, 2, 1, gen_canonical(&[("r", &["string"], false)])),
        ];

        let registry = FakeRegistry::new(families);
        let first = retrieve_topk(&candidate, &registry, 3).await.unwrap();
        let second = retrieve_topk(&candidate, &registry, 3).await.unwrap();

        assert_eq!(first, second);
    }

    // -- 6. margin_computed -------------------------------------------------

    #[tokio::test]
    async fn margin_computed() {
        let candidate = profile_from_json(r#"{"user_id":"a","email":"b","age":1}"#);

        let fam_a = FamilyId::new_v7();
        let fam_b = FamilyId::new_v7();
        let fam_c = FamilyId::new_v7();
        let families = vec![
            family_ref(
                &fam_a,
                1,
                1,
                gen_canonical(&[
                    ("user_id", &["string"], false),
                    ("email", &["string"], false),
                    ("age", &["number"], false),
                ]),
            ),
            family_ref(
                &fam_b,
                2,
                1,
                gen_canonical(&[("user_id", &["string"], false)]),
            ),
            family_ref(
                &fam_c,
                3,
                1,
                gen_canonical(&[("nothing_in_common", &["bool"], false)]),
            ),
        ];

        let registry = FakeRegistry::new(families);
        let result = retrieve_topk(&candidate, &registry, 3).await.unwrap();

        assert!(result.candidates.len() >= 2);
        let expected_margin = result.candidates[1].distance - result.candidates[0].distance;
        assert!((result.top1_top2_margin - expected_margin).abs() < 1e-6);
    }

    // -- 7. name_normalization ------------------------------------------

    #[test]
    fn name_normalization() {
        let a: BTreeSet<String> = normalize_name_tokens("userId").into_iter().collect();
        let b: BTreeSet<String> = normalize_name_tokens("user_id").into_iter().collect();
        let c: BTreeSet<String> = normalize_name_tokens("USER-ID").into_iter().collect();

        let expected: BTreeSet<String> = ["user", "id"].into_iter().map(String::from).collect();
        assert_eq!(a, expected);
        assert_eq!(b, expected);
        assert_eq!(c, expected);
    }

    // -- 8. renamed_top_level_field_family_is_retrieved -------------------
    //
    // Core proof of the deblob-p2ab Task 3 recall fix: a family whose
    // top-level fields are a pure case/separator rename of the candidate's
    // (same structure, different NAMES) lands in a DIFFERENT reqhash8
    // bucket at the SAME field-count band + depth. Before the fix,
    // retrieval only ever fetched the candidate's own EXACT bucket_key, so
    // this family was never even handed to the distance scorer — a hard 0%
    // recall case. After the fix, `retrieve_topk` discovers it via
    // `Registry::list_families_by_band_depth`'s widened (band, depth)
    // prefix scan, and the scorer's 0.25-weighted name-overlap component
    // (the only component keyed on NORMALIZED names rather than literal
    // field-path strings — `field_path_type`/`presence_overlap` still key
    // off the raw, un-renamed path string, so a rename doesn't collapse
    // distance to ~0) gives it real credit: it must rank strictly closer
    // than a totally unrelated family.

    #[tokio::test]
    async fn renamed_top_level_field_family_is_retrieved() {
        use deblob_fingerprint::{bucket_key, ShapeSummary};

        // Candidate: camelCase top-level fields.
        let candidate = profile_from_json(r#"{"widgetCount":1,"itemName":"a"}"#);
        let candidate_bucket = bucket_key(&generalized_shape_summary(&candidate));

        // Family: snake_case rename of the SAME two fields/types, same
        // structure (flat 2-field object) -> same field-count band + depth
        // as the candidate, but a DIFFERENT reqhash8 (different top-level
        // key names hash differently).
        let fam_renamed = FamilyId::new_v7();
        let renamed_canonical = gen_canonical(&[
            ("widget_count", &["number"], false),
            ("item_name", &["string"], false),
        ]);
        let renamed_bucket = bucket_key(&ShapeSummary {
            top_level_fields: 2,
            depth: 2,
            top_keys_sorted: vec!["item_name".to_string(), "widget_count".to_string()],
        });

        // Control: a family sharing nothing with the candidate, in the
        // SAME (band, depth) neighborhood, so it's discoverable too — this
        // is what "strictly closer than an unrelated family" is measured
        // against.
        let fam_unrelated = FamilyId::new_v7();
        let unrelated_canonical = gen_canonical(&[
            ("totally_different", &["bool"], false),
            ("also", &["null"], false),
        ]);
        let unrelated_bucket = bucket_key(&ShapeSummary {
            top_level_fields: 2,
            depth: 2,
            top_keys_sorted: vec!["also".to_string(), "totally_different".to_string()],
        });

        // Sanity on the fixture itself: the renamed family must actually
        // land in a DIFFERENT bucket than the candidate's own, or the test
        // would prove nothing about the fix.
        assert_ne!(
            candidate_bucket, renamed_bucket,
            "fixture must land in a different reqhash8 bucket to exercise the defect"
        );

        let registry = BucketAwareFakeRegistry::new(vec![
            (
                renamed_bucket,
                family_ref(&fam_renamed, 1, 1, renamed_canonical),
            ),
            (
                unrelated_bucket,
                family_ref(&fam_unrelated, 2, 1, unrelated_canonical),
            ),
        ]);

        // Pin the OLD (pre-fix) behavior directly: an exact-bucket_key
        // lookup restricted to the candidate's own bucket finds nothing.
        let old_style = registry
            .list_families_in_buckets(&[candidate_bucket])
            .await
            .unwrap();
        assert!(
            old_style.is_empty(),
            "exact reqhash8 bucket lookup must miss the renamed-field family — this is the defect being fixed"
        );

        // NEW behavior: retrieve_topk now uses widened (band, depth)
        // discovery, so it MUST find and score BOTH families.
        let result = retrieve_topk(&candidate, &registry, 3).await.unwrap();

        assert_eq!(
            result.candidates.len(),
            2,
            "both same-band/depth families must now be retrieved, got {:?}",
            result.candidates
        );
        assert_eq!(
            result.candidates[0].family_id, fam_renamed,
            "the renamed family must rank ahead of the unrelated one"
        );
        assert_eq!(result.candidates[0].rank, 1);
        assert_eq!(result.candidates[1].family_id, fam_unrelated);

        // Exact composition check: field_path_type (0.35) and
        // presence_overlap (0.15) are keyed on the literal, un-normalized
        // field-path string, so a rename still maxes them out at 1.0 each;
        // name_overlap (0.25), depth_similarity (0.10), nullability
        // (0.10), and array_map_shape (0.05) all score 0.0 (normalized
        // names match, and every other structural signal is identical) ->
        // 0.35*1.0 + 0.15*1.0 = 0.5 exactly.
        assert!(
            (result.candidates[0].distance - 0.5).abs() < 1e-6,
            "renamed-field family's distance should be exactly the un-normalized-path-component weight sum, got {}",
            result.candidates[0].distance
        );
        assert!(
            result.candidates[0].distance < result.candidates[1].distance,
            "renamed family (distance {}) must score strictly closer than the unrelated family (distance {})",
            result.candidates[0].distance,
            result.candidates[1].distance
        );
    }

    // -- 9. ties_field_populated -------------------------------------------
    //
    // Closes review concern (b): pins `ties` semantics with a dedicated
    // test before Task 5 depends on them. Two families tie EXACTLY on
    // distance (both reproduce the candidate's field set/types); with
    // k=1 only one can be returned in `candidates`, but the other must
    // surface in `ties` rather than being silently dropped. A third,
    // clearly-farther family must never appear in `ties`.

    #[tokio::test]
    async fn ties_field_populated() {
        let candidate = profile_from_json(r#"{"a":1,"b":2}"#);

        let fam_x = FamilyId::new_v7();
        let fam_y = FamilyId::new_v7();
        let fam_z = FamilyId::new_v7();

        let families = vec![
            // fam_x and fam_y are EXACT structural ties (distance 0.0):
            // both reproduce the candidate's own field set/types.
            family_ref(
                &fam_x,
                1,
                1,
                gen_canonical(&[("a", &["number"], false), ("b", &["number"], false)]),
            ),
            family_ref(
                &fam_y,
                2,
                1,
                gen_canonical(&[("a", &["number"], false), ("b", &["number"], false)]),
            ),
            // fam_z shares nothing with the candidate -> clearly farther,
            // must never appear in `ties`.
            family_ref(
                &fam_z,
                3,
                1,
                gen_canonical(&[("nothing_shared", &["bool"], false)]),
            ),
        ];

        let registry = FakeRegistry::new(families);
        let result = retrieve_topk(&candidate, &registry, 1).await.unwrap();

        assert_eq!(result.candidates.len(), 1, "k=1 must cap candidates at one");
        let winner = result.candidates[0].family_id.clone();
        assert!(winner == fam_x || winner == fam_y);

        assert_eq!(
            result.ties.len(),
            1,
            "the OTHER exact-tie family must surface in `ties`, not be silently dropped, got {:?}",
            result.ties
        );
        let expected_loser = if winner == fam_x { &fam_y } else { &fam_x };
        assert_eq!(&result.ties[0].family_id, expected_loser);
        assert_eq!(
            result.ties[0].rank, 1,
            "ties are recorded at the boundary rank they were excluded from"
        );
        assert!(
            approx_eq(result.ties[0].distance, result.candidates[0].distance),
            "a tie must have the same distance as the boundary candidate"
        );
        assert!(
            !result.ties.iter().any(|t| t.family_id == fam_z),
            "fam_z is not part of the tie and must never appear in `ties`"
        );
    }
}
