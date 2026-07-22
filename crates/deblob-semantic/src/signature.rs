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
pub const WEIGHTS_VERSION: &str = "deblob-semantic-signature-weights-v2-idf-log2";

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

    /// The raw (un-hex-encoded) feature-key bytes, in the SAME lexicographic
    /// order as [`Self::feature_keys_hex`] (both iterate the `BTreeMap`'s
    /// sorted keys). Task 10's IDF handler pairs these with the per-feature
    /// document frequencies it fetches (keyed by the hex form) so the injected
    /// `idf_mult` closure — which receives raw feature bytes from
    /// [`similarity_weighted`]'s union loop — can look a feature's `df` up by
    /// its raw bytes without re-hashing.
    pub fn feature_keys(&self) -> Vec<Vec<u8>> {
        self.features.keys().cloned().collect()
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

// ---- IDF quantization (jr-deblob-similarity-idf-221040) --------------------
//
// Inverse document frequency generalizes the GENERIC_CFIDS stop-list from a
// hand-picked handful to the whole long tail: a feature's discriminative mass
// is scaled by how RARE it is across the active-annotated corpus. The score
// stays an exact rational because the IDF multiplier is an INTEGER
// (`floor(log2(N/df))`) applied on top of the integer class weight — never a
// float `log`. The corpus statistics (`N`, per-feature `df`) live in
// `deblob-redis`; this crate stays pure by taking the multiplier as an
// injected closure (`similarity_weighted`), keeping the Task-9/Task-10
// boundary intact.

/// Cap on the integer IDF multiplier. `floor(log2(N/df))` for an ultra-rare
/// feature (seen in a single schema of a large corpus) can be large; capping
/// bounds the weighted numerator/denominator and stops one rare feature from
/// dominating a score. Hermes-recommended initial value (`jr-deblob-similarity-idf-221040`),
/// subject to empirical calibration.
pub const IDF_MAX: u64 = 16;

/// Minimum IDF multiplier a shared discriminative `canonical_field_id` must
/// clear to act as an ANCHOR / earn `Medium`+ strength. This is the long-tail
/// generalization of [`GENERIC_CFIDS`]: a cfid measured to be present in more
/// than ~`1/2^ANCHOR_IDF_MIN` of the corpus is treated as too common to anchor
/// on its own, EVEN if it is not on the hard stop-list. [`GENERIC_CFIDS`]
/// remains a hard floor (always non-anchor, regardless of measured `df`) — IDF
/// only ever REMOVES anchoring from a common field, never grants it to a
/// stop-listed one.
pub const ANCHOR_IDF_MIN: u64 = 2;

/// Minimum active-annotated population `N` before IDF weighting engages. Below
/// this, `df/N` is not a statistically meaningful frequency estimate — on a
/// handful of schemas a genuinely discriminative field trivially looks
/// "present in half the corpus" and would be wrongly demoted (Hermes'
/// "dynamic-score stability as the corpus grows" gap, `jr-deblob-similarity-idf-221040`).
/// While `N < IDF_MIN_POPULATION`, [`idf_multiplier`] saturates to [`IDF_MAX`]
/// for every feature, which uniformly scales all weights (leaving the exact
/// b24 ranking) and keeps every discriminative cfid an anchor — so IDF is a
/// no-op until the corpus is large enough to trust, then activates
/// automatically. Observable via the response's `idf_population_n`. Conservative
/// initial value; subject to empirical calibration.
pub const IDF_MIN_POPULATION: u64 = 32;

/// Integer IDF multiplier for a feature with document-frequency `df` in an
/// active-annotated population of `n` schemas: `floor(log2(n / df))`, clamped
/// to `[0, IDF_MAX]`.
///
/// * A feature present in HALF the corpus or more (`n/df < 2`) yields `0`: it
///   carries no discriminative mass, contributes nothing to the weighted
///   score, and (per the handler) is dropped from the candidate union.
/// * `df == 0` — a feature the corpus has never posted, only reachable via a
///   stale/racing read — is treated as maximally rare (`IDF_MAX`) rather than
///   dividing by zero.
///
/// Exact integer arithmetic only (`leading_zeros`-based `floor(log2)`), never a
/// float — so `similarity_weighted`'s score stays an exact rational.
pub fn idf_multiplier(n: u64, df: u64) -> u64 {
    // Corpus too small for a meaningful frequency estimate: saturate so IDF is
    // a uniform no-op (== b24 structural behavior) until enough annotated data
    // exists (see [`IDF_MIN_POPULATION`]).
    if n < IDF_MIN_POPULATION {
        return IDF_MAX;
    }
    if df == 0 {
        return IDF_MAX;
    }
    let ratio = n / df; // integer floor division
    if ratio < 2 {
        // present in >= half the corpus: no discriminative mass
        return 0;
    }
    // floor(log2(ratio)) for ratio >= 2 == index of the highest set bit.
    let log2 = 63 - ratio.leading_zeros() as u64;
    log2.min(IDF_MAX)
}

/// Exact weighted multiset Jaccard similarity between two signatures (§1),
/// with a per-feature IDF multiplier applied ON TOP of the feature-class
/// weight (`jr-deblob-similarity-idf-221040`). `idf_mult(feature_bytes)`
/// returns an INTEGER multiplier the caller (the handler, which owns the
/// Redis `df`/`N` reads) injects, so this crate performs no I/O. The effective
/// per-feature weight is `class_weight * idf_mult`; both are integers, so the
/// score remains the exact rational `(numerator, denominator)`. A feature
/// whose `idf_mult` is `0` contributes to neither the numerator nor the
/// denominator (it drops out of the union entirely).
pub fn similarity_weighted(
    a: &SemanticSignature,
    b: &SemanticSignature,
    idf_mult: &impl Fn(&[u8]) -> u64,
) -> Score {
    let mut numerator: u64 = 0;
    let mut denominator: u64 = 0;

    let mut keys: BTreeSet<&[u8]> = BTreeSet::new();
    keys.extend(a.features.keys().map(Vec::as_slice));
    keys.extend(b.features.keys().map(Vec::as_slice));

    for key in keys {
        let count_a = a.features.get(key).copied().unwrap_or(0) as u64;
        let count_b = b.features.get(key).copied().unwrap_or(0) as u64;
        let weight = u64::from(FeatureTag::from_byte(key[0]).weight())
            .checked_mul(idf_mult(key))
            .expect("class-weight * idf overflow");

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

/// Exact weighted multiset Jaccard similarity between two signatures (§1) —
/// the pre-IDF behavior, i.e. [`similarity_weighted`] with a constant unit
/// multiplier. Kept for callers (and tests) that score without corpus
/// statistics; the IDF-aware neighbor handler uses [`similarity_weighted`]
/// with a real `df`/`N`-derived multiplier.
pub fn similarity(a: &SemanticSignature, b: &SemanticSignature) -> Score {
    similarity_weighted(a, b, &|_| 1)
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

/// Structurally-generic canonical_field_ids that appear across essentially every
/// domain (a temporal stamp, a display name, a tally, a bare value, a state flag)
/// and therefore carry almost no discriminative signal. They must NOT act as
/// anchors on their own: two unrelated schemas that share only `cfid_timestamp`
/// are not "similar". Excluding them from the anchor set is the stop-word
/// complement to the (future) IDF/document-frequency weighting — see
/// `jr-deblob-similarity-220904`. Kept deliberately SHORT + cross-domain-generic;
/// domain-bearing cfids (latitude, carbon, price, currency, region, power, unit,
/// …) remain anchors. df at investigation time: timestamp 62%, name 45%, count/
/// status ~25-28% of annotated schemas, vs the discriminative tail <15%.
pub const GENERIC_CFIDS: &[&str] = &[
    "cfid_timestamp",
    "cfid_name",
    "cfid_count",
    "cfid_value",
    "cfid_status",
];

/// True when `cfid` carries domain-discriminative signal (i.e. is not one of the
/// ubiquitous structural [`GENERIC_CFIDS`]).
pub fn is_discriminative_cfid(cfid: &str) -> bool {
    !GENERIC_CFIDS.contains(&cfid)
}

/// True when `cfid` is an EFFECTIVE anchor under the injected IDF lookup: it is
/// not a hard-stop-list generic ([`is_discriminative_cfid`]) AND its measured
/// IDF multiplier clears [`ANCHOR_IDF_MIN`] (i.e. it is rare enough across the
/// corpus to carry real signal). This is the long-tail generalization of the
/// [`GENERIC_CFIDS`] stop-list (`jr-deblob-similarity-idf-221040`): IDF can only
/// ever REMOVE anchoring from a measured-common field, never grant it to a
/// stop-listed one. With a saturating `idf_mult` (`|_| u64::MAX`) this reduces
/// exactly to [`is_discriminative_cfid`] — the pre-IDF (b24) behavior.
fn is_anchor_cfid(cfid: &str, idf_mult: &impl Fn(&[u8]) -> u64) -> bool {
    is_discriminative_cfid(cfid) && idf_mult(&encode_field(cfid)) >= ANCHOR_IDF_MIN
}

/// The count of shared canonical_field_ids that are effective ANCHORS under
/// `idf_mult` (generic stop-word cfids AND measured-common cfids excluded) —
/// the quantity that drives anchoring + strength, so neither a shared
/// `cfid_timestamp` nor a shared but ubiquitous discriminative field alone
/// makes two schemas neighbors.
fn shared_anchor_field_count(
    a: &SemanticSignature,
    b: &SemanticSignature,
    idf_mult: &impl Fn(&[u8]) -> u64,
) -> usize {
    a.canonical_field_ids
        .intersection(&b.canonical_field_ids)
        .filter(|c| is_anchor_cfid(c, idf_mult))
        .count()
}

/// True when `signature` carries at least one anchor feature — an event type, a
/// DISCRIMINATIVE `canonical_field_id`, or an `identifier_namespace`. A signature
/// whose only cfids are generic ([`GENERIC_CFIDS`]) has NO anchor: it must never
/// expand a search toward the whole vault (Task 10 §4) nor be returned as a
/// neighbor, because it shares no domain-bearing signal (`jr-deblob-similarity-220904`).
pub fn has_anchor(signature: &SemanticSignature) -> bool {
    has_anchor_weighted(signature, &|_| u64::MAX)
}

/// IDF-aware [`has_anchor`]: a `canonical_field_id` only counts as an anchor
/// when it is an effective anchor under `idf_mult` ([`is_anchor_cfid`]) — so a
/// schema whose only discriminative cfids are measured-ubiquitous has no anchor
/// and never expands the search (`jr-deblob-similarity-idf-221040`). `event_type`
/// and `identifier_namespace` are already high-signal and are not IDF-gated in
/// this first cut. With `idf_mult = |_| u64::MAX` this is exactly [`has_anchor`].
pub fn has_anchor_weighted(
    signature: &SemanticSignature,
    idf_mult: &impl Fn(&[u8]) -> u64,
) -> bool {
    signature.event_type.is_some()
        || signature
            .canonical_field_ids
            .iter()
            .any(|c| is_anchor_cfid(c, idf_mult))
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
    strength_weighted(a, b, &|_| u64::MAX)
}

/// IDF-aware [`strength`]: identical logic, but "discriminative field" is
/// replaced by "effective anchor under `idf_mult`" ([`is_anchor_cfid`]).
/// A measured-ubiquitous discriminative cfid therefore no longer earns `Medium`
/// on its own, which is what stops the strength-FIRST ranking from preserving a
/// false close (Hermes, `jr-deblob-similarity-idf-221040`: IDF must touch
/// strength, not only the score). With `idf_mult = |_| u64::MAX` this is exactly
/// [`strength`] (the b24 behavior).
pub fn strength_weighted(
    a: &SemanticSignature,
    b: &SemanticSignature,
    idf_mult: &impl Fn(&[u8]) -> u64,
) -> Strength {
    if !has_anchor_weighted(a, idf_mult) || !has_anchor_weighted(b, idf_mult) {
        return Strength::Insufficient;
    }

    // Shared/total ANCHOR cfid counts (generic stop-word cfids AND
    // measured-common cfids excluded) — so sharing only a ubiquitous field, or
    // only a discriminative-but-corpus-common one, never earns Medium+ strength
    // (jr-deblob-similarity-220904, -idf-221040).
    let shared_field_count = shared_anchor_field_count(a, b, idf_mult);
    let disc_a = a
        .canonical_field_ids
        .iter()
        .filter(|c| is_anchor_cfid(c, idf_mult))
        .count();
    let disc_b = b
        .canonical_field_ids
        .iter()
        .filter(|c| is_anchor_cfid(c, idf_mult))
        .count();
    let min_fields = disc_a.min(disc_b);
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
    shared_anchor_count_weighted(a, b, &|_| u64::MAX)
}

/// IDF-aware [`shared_anchor_count`]: only effective-anchor shared cfids under
/// `idf_mult` count (`jr-deblob-similarity-idf-221040`). With
/// `idf_mult = |_| u64::MAX` this is exactly [`shared_anchor_count`].
pub fn shared_anchor_count_weighted(
    a: &SemanticSignature,
    b: &SemanticSignature,
    idf_mult: &impl Fn(&[u8]) -> u64,
) -> usize {
    let same_event = usize::from(matches!(
        (&a.event_type, &b.event_type),
        (Some(x), Some(y)) if x == y
    ));
    let shared_fields = shared_anchor_field_count(a, b, idf_mult);
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

    fn sig_with_cfids(cfids: &[&str]) -> SemanticSignature {
        let fields = cfids
            .iter()
            .enumerate()
            .map(|(i, c)| FieldEntry {
                path: vec![key(&format!("f{i}"))],
                semantics: FieldSemantics {
                    canonical_field_id: Some(CanonicalFieldId::new(*c)),
                    ..empty_semantics()
                },
            })
            .collect();
        semantic_signature(&SemanticMetadata {
            event_type: None,
            fields,
        })
    }

    #[test]
    fn generic_only_signature_has_no_anchor_and_never_neighbors() {
        // jr-deblob-similarity-220904: a schema annotated with ONLY a ubiquitous
        // stop-word cfid (cfid_timestamp) carries no domain signal — it must not
        // anchor, and must not be "similar" to unrelated schemas that merely also
        // carry a timestamp.
        assert!(!has_anchor(&sig_with_cfids(&["cfid_timestamp"])));
        assert!(!has_anchor(&sig_with_cfids(&["cfid_name", "cfid_count"])));
        assert!(has_anchor(&sig_with_cfids(&["cfid_carbon"])));
        assert!(has_anchor(&sig_with_cfids(&[
            "cfid_timestamp",
            "cfid_carbon"
        ])));

        // timestamp-only query vs a carbon schema that also has a timestamp:
        // the only overlap is the generic timestamp -> Insufficient (no neighbor).
        let ts_only = sig_with_cfids(&["cfid_timestamp"]);
        let carbon = sig_with_cfids(&["cfid_carbon", "cfid_timestamp"]);
        assert_eq!(strength(&ts_only, &carbon), Strength::Insufficient);

        // two unrelated schemas each with a DISTINCT discriminative cfid but a
        // shared timestamp -> still Insufficient (the bug's exact shape).
        let power = sig_with_cfids(&["cfid_power", "cfid_timestamp"]);
        assert_eq!(strength(&carbon, &power), Strength::Insufficient);

        // genuinely related — both carbon (a discriminative shared anchor) -> Medium+.
        let carbon2 = sig_with_cfids(&["cfid_carbon", "cfid_region"]);
        assert!(strength(&carbon, &carbon2) >= Strength::Medium);
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

    // ---- IDF (jr-deblob-similarity-idf-221040) ----------------------------

    #[test]
    fn idf_multiplier_is_exact_integer_floor_log2() {
        // Below IDF_MIN_POPULATION the multiplier saturates (IDF is a no-op) so
        // a tiny corpus keeps pure b24 structural behavior.
        assert_eq!(idf_multiplier(IDF_MIN_POPULATION - 1, 10), IDF_MAX);
        assert_eq!(idf_multiplier(3, 2), IDF_MAX);
        // df == 0: a feature the corpus has never posted -> maximally rare.
        assert_eq!(idf_multiplier(100, 0), IDF_MAX);
        // Present in >= half the corpus -> zero discriminative mass.
        assert_eq!(idf_multiplier(100, 100), 0);
        assert_eq!(idf_multiplier(100, 60), 0); // ratio 1
        assert_eq!(idf_multiplier(100, 50), 1); // ratio 2  -> floor(log2 2)=1
        assert_eq!(idf_multiplier(100, 33), 1); // ratio 3  -> floor(log2 3)=1
        assert_eq!(idf_multiplier(100, 25), 2); // ratio 4  -> 2
        assert_eq!(idf_multiplier(1024, 1), 10); // ratio 1024 -> 10 (< cap)
        assert_eq!(idf_multiplier(1u64 << 40, 1), IDF_MAX); // clamped to cap
    }

    #[test]
    fn similarity_weighted_unit_closure_equals_similarity() {
        let a = sig_with_cfids(&["cfid_carbon", "cfid_region"]);
        let b = sig_with_cfids(&["cfid_carbon", "cfid_power"]);
        assert_eq!(similarity_weighted(&a, &b, &|_| 1), similarity(&a, &b));
    }

    #[test]
    fn idf_zero_multiplier_drops_a_common_shared_feature_from_the_score() {
        // Two schemas share a corpus-COMMON cfid and each has a distinct rare
        // one (not shared). Under IDF the shared common field contributes zero
        // mass -> numerator 0; unit weighting counts it fully -> nonzero.
        let a = sig_with_cfids(&["cfid_common", "cfid_rare_a"]);
        let b = sig_with_cfids(&["cfid_common", "cfid_rare_b"]);
        let common_key = encode_field("cfid_common");
        let idf = |k: &[u8]| if k == common_key.as_slice() { 0 } else { 8 };
        assert_eq!(similarity_weighted(&a, &b, &idf).numerator, 0);
        assert!(similarity(&a, &b).numerator > 0);
    }

    #[test]
    fn idf_anchoring_demotes_a_measured_common_discriminative_cfid() {
        // gpu-price vs electricity-price analog: both carry a discriminative but
        // corpus-COMMON cfid_price. b24 structural strength earns Medium off that
        // one shared field; under IDF cfid_price is measured ubiquitous (idf 0 <
        // ANCHOR_IDF_MIN) so it is NOT an anchor -> Insufficient. (The residual
        // rare-shared-cfid cross-domain case is what the domain gate handles.)
        let gpu = sig_with_cfids(&["cfid_price"]);
        let elec = sig_with_cfids(&["cfid_price"]);
        assert_eq!(strength(&gpu, &elec), Strength::Medium);

        let price_key = encode_field("cfid_price");
        let idf = |k: &[u8]| if k == price_key.as_slice() { 0 } else { 8 };
        assert!(!has_anchor_weighted(&gpu, &idf));
        assert_eq!(strength_weighted(&gpu, &elec, &idf), Strength::Insufficient);
        assert_eq!(shared_anchor_count_weighted(&gpu, &elec, &idf), 0);
    }

    #[test]
    fn idf_keeps_a_rare_shared_cfid_as_an_anchor() {
        let a = sig_with_cfids(&["cfid_carbon"]);
        let b = sig_with_cfids(&["cfid_carbon"]);
        let idf = |_: &[u8]| 8u64; // rare everywhere
        assert!(has_anchor_weighted(&a, &idf));
        assert!(strength_weighted(&a, &b, &idf) >= Strength::Medium);
    }

    #[test]
    fn saturating_idf_matches_b24_behavior_exactly() {
        // The delegating wrappers pass |_| u64::MAX, which must reproduce b24.
        let a = sig_with_cfids(&["cfid_carbon", "cfid_timestamp"]);
        let b = sig_with_cfids(&["cfid_carbon", "cfid_power"]);
        assert_eq!(strength_weighted(&a, &b, &|_| u64::MAX), strength(&a, &b));
        assert_eq!(has_anchor_weighted(&a, &|_| u64::MAX), has_anchor(&a));
        assert_eq!(
            shared_anchor_count_weighted(&a, &b, &|_| u64::MAX),
            shared_anchor_count(&a, &b)
        );
    }
}
