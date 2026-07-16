//! P2-D Task 9: the PURE, path-independent semantic-signature feature
//! multiset + exact weighted-multiset-Jaccard similarity core, per
//! `docs/superpowers/plans/deblob-p2d-02-hermes-similarity.md` §1/§2/§3/§5
//! (authoritative). Strictly diagnostic. This module has NO side effects,
//! NO I/O, NO storage, and NO index/API concerns — those are Task 10
//! (`deblob-redis` + the `deblob` bin).
//!
//! Reuses Task 3's already-canonicalized (NFC + vocabulary-resolved)
//! [`SemanticMetadata`] directly; nothing here re-normalizes independently
//! (determinism guard #3).

use deblob_core::semantic::{SemanticMetadata, TemporalKind, Unit, UnitSystem};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

/// Version tag for the signature-extractor + byte encoding scheme. Distinct
/// from `sem_`'s `"deblob-semantic-v1"` (digest.rs) — a different identity
/// dimension with its own domain (determinism guard #1).
pub const SIGNATURE_VERSION: &str = "deblob-semantic-signature-v1";

/// Version tag for the feature weight table (§2). Bumping the weights
/// requires a new version even if the feature *shapes* are unchanged, so a
/// score is always reported alongside the weights version it was computed
/// under (Task 10's response shape).
pub const WEIGHTS_VERSION: &str = "deblob-semantic-signature-weights-v1";

/// Count-capped multiset cap: `effective_count = min(actual_count, 4)`.
const MAX_FEATURE_COUNT: u32 = 4;

// ---- feature classes (§2) -------------------------------------------------

/// One leading tag byte per feature class. The tag is the first byte of
/// every encoded feature, so decoding a feature's class (for weight lookup
/// or strength classification) never needs to re-parse the whole value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
enum FeatureTag {
    Event = 0,
    Field = 1,
    FieldIdns = 2,
    FieldUnit = 3,
    Idns = 4,
    Unit = 5,
    FieldEnum = 6,
    EnumMeaning = 7,
    FieldTemporal = 8,
    Temporal = 9,
}

impl FeatureTag {
    fn weight(self) -> u32 {
        match self {
            FeatureTag::Event => 24,
            FeatureTag::Field => 12,
            FeatureTag::FieldIdns => 10,
            FeatureTag::FieldUnit => 8,
            FeatureTag::Idns => 6,
            FeatureTag::Unit => 4,
            FeatureTag::FieldEnum => 4,
            FeatureTag::EnumMeaning => 3,
            FeatureTag::FieldTemporal => 3,
            FeatureTag::Temporal => 1,
        }
    }

    /// Every feature byte string produced by this module starts with one of
    /// the tag bytes above — this is a total, panicking-only-on-an-internal
    /// invariant-violation decode, never fed untrusted bytes.
    fn from_byte(b: u8) -> FeatureTag {
        match b {
            0 => FeatureTag::Event,
            1 => FeatureTag::Field,
            2 => FeatureTag::FieldIdns,
            3 => FeatureTag::FieldUnit,
            4 => FeatureTag::Idns,
            5 => FeatureTag::Unit,
            6 => FeatureTag::FieldEnum,
            7 => FeatureTag::EnumMeaning,
            8 => FeatureTag::FieldTemporal,
            9 => FeatureTag::Temporal,
            other => unreachable!("signature.rs never emits feature tag byte {other}"),
        }
    }

    /// Display name for Task 10's `matched_feature_classes` response field.
    fn class_name(self) -> &'static str {
        match self {
            FeatureTag::Event => "canonical_event_type_id",
            FeatureTag::Field => "canonical_field_id",
            FeatureTag::FieldIdns => "field_identifier_namespace",
            FeatureTag::FieldUnit => "field_unit",
            FeatureTag::Idns => "identifier_namespace",
            FeatureTag::Unit => "unit",
            FeatureTag::FieldEnum => "field_enum_meaning",
            FeatureTag::EnumMeaning => "enum_meaning",
            FeatureTag::FieldTemporal => "field_temporal",
            FeatureTag::Temporal => "temporal",
        }
    }
}

// ---- typed length-prefixed encoding (determinism guard #2) ---------------
//
// `tag || len || value || ...`: every variable-length component is
// prefixed with its own 4-byte big-endian length, so concatenating several
// components is never ambiguous — an embedded `:` (or any other byte) in a
// code cannot shift a boundary and collide two different (component)
// tuples into the same bytes. This is a self-contained encoding, separate
// from (and not required to match) `canon.rs`'s own byte layout — the two
// modules are different identity dimensions with different version tags.

fn push_lp_str(out: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}

fn push_lp_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}

fn unit_system_byte(system: UnitSystem) -> u8 {
    match system {
        UnitSystem::Ucum => 0,
        UnitSystem::Iso4217 => 1,
        UnitSystem::Registered => 2,
    }
}

fn temporal_kind_byte(kind: TemporalKind) -> u8 {
    match kind {
        TemporalKind::Instant => 0,
        TemporalKind::LocalDatetime => 1,
        TemporalKind::Date => 2,
        TemporalKind::Duration => 3,
    }
}

fn encode_event(event_type: &str) -> Vec<u8> {
    let mut out = vec![FeatureTag::Event as u8];
    push_lp_str(&mut out, event_type);
    out
}

fn encode_field(cfid: &str) -> Vec<u8> {
    let mut out = vec![FeatureTag::Field as u8];
    push_lp_str(&mut out, cfid);
    out
}

fn encode_field_idns(cfid: &str, namespace: &str) -> Vec<u8> {
    let mut out = vec![FeatureTag::FieldIdns as u8];
    push_lp_str(&mut out, cfid);
    push_lp_str(&mut out, namespace);
    out
}

fn encode_field_unit(cfid: &str, unit: &Unit) -> Vec<u8> {
    let mut out = vec![FeatureTag::FieldUnit as u8];
    push_lp_str(&mut out, cfid);
    out.push(unit_system_byte(unit.system));
    push_lp_str(&mut out, &unit.code);
    out
}

fn encode_idns(namespace: &str) -> Vec<u8> {
    let mut out = vec![FeatureTag::Idns as u8];
    push_lp_str(&mut out, namespace);
    out
}

fn encode_unit(unit: &Unit) -> Vec<u8> {
    let mut out = vec![FeatureTag::Unit as u8];
    out.push(unit_system_byte(unit.system));
    push_lp_str(&mut out, &unit.code);
    out
}

fn encode_field_enum(cfid: &str, vocabulary: &str, code: &str) -> Vec<u8> {
    let mut out = vec![FeatureTag::FieldEnum as u8];
    push_lp_str(&mut out, cfid);
    push_lp_str(&mut out, vocabulary);
    push_lp_str(&mut out, code);
    out
}

fn encode_enum_meaning(vocabulary: &str, code: &str) -> Vec<u8> {
    let mut out = vec![FeatureTag::EnumMeaning as u8];
    push_lp_str(&mut out, vocabulary);
    push_lp_str(&mut out, code);
    out
}

fn encode_field_temporal(cfid: &str, kind: TemporalKind) -> Vec<u8> {
    let mut out = vec![FeatureTag::FieldTemporal as u8];
    push_lp_str(&mut out, cfid);
    out.push(temporal_kind_byte(kind));
    out
}

fn encode_temporal(kind: TemporalKind) -> Vec<u8> {
    vec![FeatureTag::Temporal as u8, temporal_kind_byte(kind)]
}

fn bump(features: &mut BTreeMap<Vec<u8>, u32>, key: Vec<u8>) {
    let entry = features.entry(key).or_insert(0);
    *entry = (*entry + 1).min(MAX_FEATURE_COUNT);
}

// ---- SemanticSignature -----------------------------------------------------

/// A deterministic, path-independent feature multiset extracted from a
/// [`SemanticMetadata`]. Two structurally different schemas that assert the
/// same controlled-vocabulary meanings produce the same signature; feature
/// counts are capped (`min(actual, 4)`); features are sorted lexicographically
/// by encoded bytes (a `BTreeMap`, never a `HashMap`/`DefaultHasher` —
/// determinism guard #8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticSignature {
    features: BTreeMap<Vec<u8>, u32>,
    event_type: Option<String>,
    canonical_field_ids: BTreeSet<String>,
    identifier_namespaces: BTreeSet<String>,
}

impl SemanticSignature {
    /// Deterministic byte encoding of the whole signature: the
    /// [`SIGNATURE_VERSION`] domain tag, a NUL separator, then every
    /// `(feature_bytes, effective_count)` pair in the map's already-sorted
    /// key order. Two signatures are byte-identical iff they are `==`.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(SIGNATURE_VERSION.as_bytes());
        out.push(0);
        for (feature, count) in &self.features {
            push_lp_bytes(&mut out, feature);
            out.extend_from_slice(&count.to_be_bytes());
        }
        out
    }

    /// Number of distinct (post-cap) features in the multiset.
    pub fn feature_count(&self) -> usize {
        self.features.len()
    }

    pub fn canonical_field_ids(&self) -> &BTreeSet<String> {
        &self.canonical_field_ids
    }

    pub fn identifier_namespaces(&self) -> &BTreeSet<String> {
        &self.identifier_namespaces
    }

    pub fn event_type(&self) -> Option<&str> {
        self.event_type.as_deref()
    }

    /// Sorted lowercase-hex-encoded feature keys — Task 10's bounded
    /// inverted-index posting keys (`deblob:sem-sig:<hex>`). Sorted because
    /// `features` is a `BTreeMap` (determinism guard #8): two calls on
    /// `==` signatures always produce the SAME `Vec` in the SAME order, so
    /// `deblob-redis` can serialize it directly (e.g. as a JSON array) and
    /// compare it byte-for-byte across an incremental write and a full
    /// rebuild (§5.12). Hex, not base64/base32: a plain byte-for-byte
    /// encoding with no padding-alphabet ambiguity, matching this crate's
    /// existing `HEXLOWER`-style usage elsewhere in the workspace, but
    /// implemented locally (no extra dependency) since it's a two-line
    /// transform.
    pub fn feature_keys_hex(&self) -> Vec<String> {
        self.features.keys().map(|k| hex_encode(k)).collect()
    }
}

/// Lowercase-hex encoding, deliberately hand-rolled (no `data-encoding`
/// dependency) since `deblob-semantic` otherwise has no encoding-library
/// dependency at all — see [`SemanticSignature::feature_keys_hex`].
fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        write!(&mut out, "{b:02x}").expect("writing to a String never fails");
    }
    out
}

/// Extracts `metadata`'s path-independent semantic signature (§2). Emits
/// atomic AND compound features; compounds bind to `canonical_field_id`
/// (never the field's path — that's what makes this path-independent). When
/// a field has no `canonical_field_id`, only its low-weight standalone
/// features (`unit:`, `idns:`, `temporal:`, `enum-meaning:`) are emitted —
/// no `field-*` compound. `NO` path/name/position feature is ever emitted.
pub fn semantic_signature(metadata: &SemanticMetadata) -> SemanticSignature {
    let mut features: BTreeMap<Vec<u8>, u32> = BTreeMap::new();
    let mut canonical_field_ids: BTreeSet<String> = BTreeSet::new();
    let mut identifier_namespaces: BTreeSet<String> = BTreeSet::new();

    let event_type = metadata.event_type.as_ref().map(|e| e.as_str().to_string());
    if let Some(event) = &event_type {
        bump(&mut features, encode_event(event));
    }

    for entry in &metadata.fields {
        let sem = &entry.semantics;
        let cfid: Option<&str> = sem.canonical_field_id.as_ref().map(|c| c.as_str());

        if let Some(cfid) = cfid {
            canonical_field_ids.insert(cfid.to_string());
            bump(&mut features, encode_field(cfid));
        }

        if let Some(namespace) = &sem.identifier_namespace {
            let namespace = namespace.as_str();
            identifier_namespaces.insert(namespace.to_string());
            if let Some(cfid) = cfid {
                bump(&mut features, encode_field_idns(cfid, namespace));
            }
            bump(&mut features, encode_idns(namespace));
        }

        if let Some(unit) = &sem.unit {
            if let Some(cfid) = cfid {
                bump(&mut features, encode_field_unit(cfid, unit));
            }
            bump(&mut features, encode_unit(unit));
        }

        if let Some(enum_semantics) = &sem.enum_semantics {
            for mapping in enum_semantics {
                let meaning = &mapping.meaning;
                if let Some(cfid) = cfid {
                    bump(
                        &mut features,
                        encode_field_enum(cfid, &meaning.vocabulary, &meaning.code),
                    );
                }
                bump(
                    &mut features,
                    encode_enum_meaning(&meaning.vocabulary, &meaning.code),
                );
            }
        }

        if let Some(temporal) = &sem.temporal {
            if let Some(kind) = temporal.kind {
                if let Some(cfid) = cfid {
                    bump(&mut features, encode_field_temporal(cfid, kind));
                }
                bump(&mut features, encode_temporal(kind));
            }
        }
    }

    SemanticSignature {
        features,
        event_type,
        canonical_field_ids,
        identifier_namespaces,
    }
}

// ---- similarity: exact weighted multiset Jaccard (§1) ---------------------

/// A similarity score kept as the exact rational `(numerator, denominator)`
/// — never collapsed to a float. `numerator = Σ w_f·min(cA,f, cB,f)`,
/// `denominator = Σ w_f·max(cA,f, cB,f)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Score {
    pub numerator: u64,
    pub denominator: u64,
}

impl Score {
    /// Ranking comparison via checked `u128` cross-multiplication —
    /// `self.numerator * other.denominator` vs `other.numerator *
    /// self.denominator` — never float division. `u64::MAX² < u128::MAX`,
    /// so the widening multiply is always exact and never overflows for any
    /// pair of valid `Score`s.
    pub fn cmp_rank(&self, other: &Score) -> Ordering {
        let lhs = (self.numerator as u128)
            .checked_mul(other.denominator as u128)
            .expect("u128 cross-multiplication overflow");
        let rhs = (other.numerator as u128)
            .checked_mul(self.denominator as u128)
            .expect("u128 cross-multiplication overflow");
        lhs.cmp(&rhs)
    }

    /// Presentation-only fixed-point decimal string (`precision` digits
    /// after the point), computed via integer scaling — NEVER a float, and
    /// NEVER used for ranking (use [`Score::cmp_rank`] for that).
    /// `denominator == 0` (both signatures had zero total weight, i.e.
    /// neither carried any of the extracted feature classes at all) renders
    /// as an all-zero decimal rather than dividing.
    pub fn decimal_string(&self, precision: usize) -> String {
        let precision = precision.max(1);
        if self.denominator == 0 {
            return format!("0.{}", "0".repeat(precision));
        }
        let scale = 10u128.pow(precision as u32);
        let scaled = (self.numerator as u128 * scale) / self.denominator as u128;
        let whole = scaled / scale;
        let frac = scaled % scale;
        format!("{whole}.{frac:0width$}", width = precision)
    }
}

/// Exact weighted multiset Jaccard similarity between two signatures (§1).
/// No cosine, no MinHash, no float — the rational `(numerator,
/// denominator)` is the score of record; rank with [`Score::cmp_rank`].
pub fn similarity(a: &SemanticSignature, b: &SemanticSignature) -> Score {
    let mut numerator: u64 = 0;
    let mut denominator: u64 = 0;

    let mut keys: BTreeSet<&[u8]> = BTreeSet::new();
    keys.extend(a.features.keys().map(Vec::as_slice));
    keys.extend(b.features.keys().map(Vec::as_slice));

    for key in keys {
        let count_a = a.features.get(key).copied().unwrap_or(0) as u64;
        let count_b = b.features.get(key).copied().unwrap_or(0) as u64;
        let weight = u64::from(FeatureTag::from_byte(key[0]).weight());

        numerator = numerator
            .checked_add(
                weight
                    .checked_mul(count_a.min(count_b))
                    .expect("weighted min overflow"),
            )
            .expect("numerator accumulation overflow");
        denominator = denominator
            .checked_add(
                weight
                    .checked_mul(count_a.max(count_b))
                    .expect("weighted max overflow"),
            )
            .expect("denominator accumulation overflow");
    }

    Score {
        numerator,
        denominator,
    }
}

// ---- strength (§3) ---------------------------------------------------------

/// Match strength, returned separately from the numeric [`Score`]. Variant
/// declaration order is significant: `Insufficient < Weak < Medium <
/// Strong`, which lets the differing-event-types rule cap a strength with a
/// plain `.min(Strength::Medium)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Strength {
    Insufficient,
    Weak,
    Medium,
    Strong,
}

/// True when `signature` carries at least one anchor feature
/// (`canonical_event_type_id` / `canonical_field_id` / `identifier_namespace`).
/// A signature with none must never be used to expand a search toward the
/// whole vault (Task 10 §4) — that gating check is exactly this predicate.
pub fn has_anchor(signature: &SemanticSignature) -> bool {
    signature.event_type.is_some()
        || !signature.canonical_field_ids.is_empty()
        || !signature.identifier_namespaces.is_empty()
}

/// Feature classes (by tag) present with nonzero count in BOTH `a` and `b`.
fn overlapping_tags(a: &SemanticSignature, b: &SemanticSignature) -> BTreeSet<FeatureTag> {
    let mut tags = BTreeSet::new();
    for (key, &count_a) in &a.features {
        if count_a == 0 {
            continue;
        }
        if let Some(&count_b) = b.features.get(key) {
            if count_b > 0 {
                tags.insert(FeatureTag::from_byte(key[0]));
            }
        }
    }
    tags
}

/// Classifies the match strength between two signatures (§3):
///
/// ```text
/// strong:  same canonical_event_type_id  OR  >=2 shared canonical_field_id
///          with >=50% canonical-field coverage
/// medium:  >=1 shared canonical_field_id  OR  shared identifier_namespace
///          + another semantic feature
/// weak:    overlap only units / temporal kinds / enum vocab+codes
/// insufficient: no (shared) anchor features
/// ```
///
/// "Canonical-field coverage" is interpreted here as the shared-field count
/// relative to the SMALLER side's distinct `canonical_field_id` count (how
/// much of the more narrowly-annotated schema is covered by the overlap) —
/// the spec text underdetermines the denominator, and this reading is the
/// one that keeps the predicate independent of which schema happens to be
/// larger (avoiding asymmetric strength depending on argument order for
/// identical *shared* content, i.e. still symmetric because it's a `min`).
///
/// If both schemas declare event types and they DIFFER, the computed
/// strength is capped at `Medium` regardless of raw score.
pub fn strength(a: &SemanticSignature, b: &SemanticSignature) -> Strength {
    if !has_anchor(a) || !has_anchor(b) {
        return Strength::Insufficient;
    }

    let shared_field_count = a
        .canonical_field_ids
        .intersection(&b.canonical_field_ids)
        .count();
    let min_fields = a.canonical_field_ids.len().min(b.canonical_field_ids.len());
    let coverage_at_least_half = min_fields > 0 && shared_field_count * 2 >= min_fields;

    let same_event = matches!(
        (&a.event_type, &b.event_type),
        (Some(x), Some(y)) if x == y
    );
    let differing_event_types = matches!(
        (&a.event_type, &b.event_type),
        (Some(x), Some(y)) if x != y
    );

    let shared_namespace = !a
        .identifier_namespaces
        .is_disjoint(&b.identifier_namespaces);
    let overlap_tags = overlapping_tags(a, b);
    let has_other_feature = overlap_tags.iter().any(|tag| *tag != FeatureTag::Idns);

    let mut computed = if same_event || (shared_field_count >= 2 && coverage_at_least_half) {
        Strength::Strong
    } else if shared_field_count >= 1 || (shared_namespace && has_other_feature) {
        Strength::Medium
    } else if overlap_tags.contains(&FeatureTag::Unit)
        || overlap_tags.contains(&FeatureTag::Temporal)
        || overlap_tags.contains(&FeatureTag::EnumMeaning)
    {
        Strength::Weak
    } else {
        Strength::Insufficient
    };

    if differing_event_types {
        computed = computed.min(Strength::Medium);
    }

    computed
}

/// The feature classes (by display name) that overlap between `a` and `b`
/// — Task 10's `matched_feature_classes` response field.
pub fn matched_feature_classes(a: &SemanticSignature, b: &SemanticSignature) -> Vec<&'static str> {
    overlapping_tags(a, b)
        .into_iter()
        .map(FeatureTag::class_name)
        .collect()
}

/// Count of shared anchor features (`canonical_event_type_id` match counts
/// as 1, plus every shared `canonical_field_id`, plus every shared
/// `identifier_namespace`) — Task 10's `shared_anchor_count` response
/// field.
pub fn shared_anchor_count(a: &SemanticSignature, b: &SemanticSignature) -> usize {
    let same_event = usize::from(matches!(
        (&a.event_type, &b.event_type),
        (Some(x), Some(y)) if x == y
    ));
    let shared_fields = a
        .canonical_field_ids
        .intersection(&b.canonical_field_ids)
        .count();
    let shared_namespaces = a
        .identifier_namespaces
        .intersection(&b.identifier_namespaces)
        .count();
    same_event + shared_fields + shared_namespaces
}

// ---- unit tests -------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use deblob_core::semantic::{CanonicalFieldId, FieldEntry, FieldSemantics, PathSegment};

    fn key(s: &str) -> PathSegment {
        PathSegment::Key(s.to_string())
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

    #[test]
    fn empty_metadata_has_no_features_and_no_anchor() {
        let metadata = SemanticMetadata {
            event_type: None,
            fields: vec![],
        };
        let sig = semantic_signature(&metadata);
        assert_eq!(sig.feature_count(), 0);
        assert!(!has_anchor(&sig));
    }

    #[test]
    fn field_without_canonical_field_id_emits_no_compound_features() {
        let metadata = SemanticMetadata {
            event_type: None,
            fields: vec![FieldEntry {
                path: vec![key("x")],
                semantics: FieldSemantics {
                    unit: Some(Unit {
                        system: UnitSystem::Ucum,
                        code: "Cel".to_string(),
                    }),
                    ..empty_semantics()
                },
            }],
        };
        let sig = semantic_signature(&metadata);
        // Only the standalone `unit:` feature, no `field-unit:` compound.
        assert_eq!(sig.feature_count(), 1);
        let (only_key, _) = sig.features.iter().next().unwrap();
        assert_eq!(FeatureTag::from_byte(only_key[0]), FeatureTag::Unit);
    }

    #[test]
    fn feature_count_caps_at_four_within_extraction() {
        let mut features = BTreeMap::new();
        let feature_key = encode_field("dup");
        for _ in 0..10 {
            bump(&mut features, feature_key.clone());
        }
        assert_eq!(features.get(&feature_key), Some(&MAX_FEATURE_COUNT));
    }

    #[test]
    fn similarity_of_a_signature_with_itself_is_one() {
        let metadata = SemanticMetadata {
            event_type: None,
            fields: vec![FieldEntry {
                path: vec![key("x")],
                semantics: FieldSemantics {
                    canonical_field_id: Some(CanonicalFieldId::new("f")),
                    ..empty_semantics()
                },
            }],
        };
        let sig = semantic_signature(&metadata);
        let score = similarity(&sig, &sig);
        assert!(score.denominator > 0);
        assert_eq!(score.numerator, score.denominator);
    }

    #[test]
    fn feature_keys_hex_is_sorted_and_deterministic_across_calls() {
        let metadata = SemanticMetadata {
            event_type: None,
            fields: vec![FieldEntry {
                path: vec![key("x")],
                semantics: FieldSemantics {
                    canonical_field_id: Some(CanonicalFieldId::new("f")),
                    identifier_namespace: Some(deblob_core::semantic::NamespaceCode::new("ns")),
                    ..empty_semantics()
                },
            }],
        };
        let sig = semantic_signature(&metadata);
        let first = sig.feature_keys_hex();
        let second = sig.feature_keys_hex();
        assert_eq!(first, second, "must be deterministic across calls");
        let mut sorted = first.clone();
        sorted.sort();
        assert_eq!(first, sorted, "must already be lexicographically sorted");
        assert_eq!(first.len(), sig.feature_count());
        // Every entry is valid lowercase hex of even length.
        for entry in &first {
            assert!(entry.len() % 2 == 0);
            assert!(entry
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
        }
    }

    #[test]
    fn decimal_string_is_presentation_only_and_never_divides_by_zero() {
        let zero = Score {
            numerator: 0,
            denominator: 0,
        };
        assert_eq!(zero.decimal_string(6), "0.000000");

        let half = Score {
            numerator: 1,
            denominator: 2,
        };
        assert_eq!(half.decimal_string(2), "0.50");
    }
}
