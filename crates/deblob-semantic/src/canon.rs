//! Byte-level canonical serialization of [`SemanticMetadata`] (P2-D Task 3,
//! `deblob-p2d-hermes-review.md` §2/§3). This is a hand-rolled, fully
//! self-delimiting binary protocol — NOT generic JSON hashing — so that the
//! mapping from distinct `SemanticMetadata` values to distinct byte strings
//! is injective by construction (every variable-length component is
//! length-prefixed or tag-discriminated; nothing relies on delimiters or on
//! one field's content never containing another field's separator).
//!
//! `sch_` (the structural identity) is NEVER referenced here: this module
//! only sees [`SemanticMetadata`], a type that has no schema-id field at
//! all, so "same semantics, different structure → same `sem_`" holds by
//! construction, not by convention.
//!
//! Scope: canonicalization only. Hashing/domain-separation is `digest.rs`;
//! storage/API/similarity-signature are later tasks.

use std::collections::BTreeMap;

use deblob_core::semantic::{
    CanonicalEventTypeId, EpochBase, FieldSemantics, MeaningCode, PathSegment, SemanticMetadata,
    Temporal, TemporalKind, TemporalResolution, Unit, UnitSystem,
};
use unicode_normalization::UnicodeNormalization;

/// Errors from canonicalizing a [`SemanticMetadata`]. Each variant reports
/// only the *structural* location of the problem — never raw offending
/// content beyond what's needed to locate it — mirroring the style of
/// `deblob_semantic::vocab::VocabError`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CanonError {
    /// A `PathSegment::Key` contains a NUL byte or a Unicode control
    /// character. `field_index` is the position in `SemanticMetadata::fields`;
    /// `segment_index` is the position within that field's path. (Rust
    /// `String`/`char` cannot hold unpaired surrogates or invalid code
    /// points at all, so that half of the spec's guard is enforced by the
    /// type system already — this variant only needs to cover NUL/control.)
    #[error(
        "field {field_index}, path segment {segment_index}: contains NUL or a control character"
    )]
    InvalidPathKey {
        field_index: usize,
        segment_index: usize,
    },
    /// Two (or more) field entries normalized to the exact same canonical
    /// path bytes.
    #[error("duplicate canonical path (fields {first_field_index} and {second_field_index})")]
    DuplicatePath {
        first_field_index: usize,
        second_field_index: usize,
    },
    /// Two `enum_semantics` keys within the same field canonicalized to the
    /// same typed key bytes (e.g. `"1"` and `"1.0"` both present as
    /// separate map entries) — not reachable via a plain `BTreeMap<String,
    /// _>` with genuinely distinct value semantics, so this is a defensive
    /// guard against a silently-lossy digest rather than a documented spec
    /// requirement.
    #[error("field {field_index}: duplicate canonical enum-semantics key")]
    DuplicateEnumKey { field_index: usize },
}

// ---- wire primitives -------------------------------------------------
//
// Every variable-length value is length-prefixed with a 4-byte big-endian
// count; every sum-typed value starts with a 1-byte tag. Concatenating any
// sequence of these primitives is therefore unambiguous to parse (though we
// never need to parse it back — only to guarantee the encode direction is
// injective).

fn push_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_be_bytes());
}

fn push_i64(out: &mut Vec<u8>, v: i64) {
    out.extend_from_slice(&v.to_be_bytes());
}

fn push_lp_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    push_u32(out, bytes.len() as u32);
    out.extend_from_slice(bytes);
}

fn push_lp_str(out: &mut Vec<u8>, s: &str) {
    push_lp_bytes(out, s.as_bytes());
}

// ---- typed paths -------------------------------------------------------

const TAG_PATH_KEY: u8 = 0;
const TAG_PATH_WILDCARD: u8 = 1;

/// Encodes one typed path (`Vec<PathSegment>`) as a self-delimiting byte
/// string: a 4-byte segment count, then per segment a 1-byte tag
/// (`Key`/`Wildcard` encode distinctly) and, for `Key`, a length-prefixed
/// NFC-normalized UTF-8 payload.
///
/// This is what makes the anti-ambiguity invariant hold: a field literally
/// named `"a.b"` (one `Key("a.b")`, count=1) encodes with a different
/// segment count *and* a different key length than `[Key("a"), Key("b")]`
/// (count=2), so the two can never collide regardless of what separator
/// byte a naive dotted-string join might have picked.
fn encode_path(field_index: usize, path: &[PathSegment]) -> Result<Vec<u8>, CanonError> {
    let mut out = Vec::new();
    push_u32(&mut out, path.len() as u32);
    for (segment_index, segment) in path.iter().enumerate() {
        match segment {
            PathSegment::Wildcard => out.push(TAG_PATH_WILDCARD),
            PathSegment::Key(key) => {
                if key.chars().any(|c| c.is_control()) {
                    return Err(CanonError::InvalidPathKey {
                        field_index,
                        segment_index,
                    });
                }
                let normalized: String = key.nfc().collect();
                out.push(TAG_PATH_KEY);
                push_lp_str(&mut out, &normalized);
            }
        }
    }
    Ok(out)
}

// ---- numeric canonicalization (P1 numeric rule, no float) --------------

/// A JSON-number-grammar string reduced to a canonical (sign, significant
/// digits, decimal exponent) triple with no trailing zeros in `digits`
/// (unless the value is exactly zero), so `1`, `1.0`, and `1e0` all reduce
/// to the identical triple. Never touches `f64` — parsing and normalizing
/// stay in decimal-text/integer arithmetic throughout.
struct CanonicalDecimal {
    negative: bool,
    /// ASCII digits, no leading zeros (except the single digit `"0"`), no
    /// trailing zeros (except the single digit `"0"`).
    digits: String,
    /// `value = (-1)^negative * digits * 10^exponent`.
    exponent: i64,
}

/// Parses `s` against the strict JSON number grammar
/// (`-?(0|[1-9]\d*)(\.\d+)?([eE][+-]?\d+)?`), requiring a full match, and
/// reduces it to a [`CanonicalDecimal`]. Returns `None` for anything that
/// isn't a well-formed JSON number literal (including an out-of-range
/// exponent) — callers fall back to treating the input as an opaque string
/// in that case, which stays deterministic and injective, just without the
/// `1`/`1.0`/`1e0` unification.
fn canonical_decimal(s: &str) -> Option<CanonicalDecimal> {
    let bytes = s.as_bytes();
    let mut i = 0usize;
    let negative = if bytes.first() == Some(&b'-') {
        i += 1;
        true
    } else {
        false
    };

    let int_start = i;
    match bytes.get(i) {
        Some(b'0') => i += 1,
        Some(c) if c.is_ascii_digit() => {
            while matches!(bytes.get(i), Some(c) if c.is_ascii_digit()) {
                i += 1;
            }
        }
        _ => return None,
    }
    let int_part = &s[int_start..i];

    let mut frac_part = "";
    if bytes.get(i) == Some(&b'.') {
        let start = i + 1;
        let mut j = start;
        while matches!(bytes.get(j), Some(c) if c.is_ascii_digit()) {
            j += 1;
        }
        if j == start {
            return None; // "." with no following digit
        }
        frac_part = &s[start..j];
        i = j;
    }

    let mut exp_field: i64 = 0;
    if matches!(bytes.get(i), Some(b'e') | Some(b'E')) {
        i += 1;
        let exp_negative = match bytes.get(i) {
            Some(b'+') => {
                i += 1;
                false
            }
            Some(b'-') => {
                i += 1;
                true
            }
            _ => false,
        };
        let start = i;
        while matches!(bytes.get(i), Some(c) if c.is_ascii_digit()) {
            i += 1;
        }
        if i == start {
            return None; // "e"/"E" with no following digit
        }
        let magnitude: i64 = s[start..i].parse().ok()?;
        exp_field = if exp_negative { -magnitude } else { magnitude };
    }

    if i != bytes.len() {
        return None; // trailing garbage: not a number literal
    }

    let mut digits = String::with_capacity(int_part.len() + frac_part.len());
    digits.push_str(int_part);
    digits.push_str(frac_part);
    let mut exponent = exp_field.checked_sub(frac_part.len() as i64)?;

    let trimmed_leading = digits.trim_start_matches('0');
    let mut digits = if trimmed_leading.is_empty() {
        "0".to_string()
    } else {
        trimmed_leading.to_string()
    };

    if digits == "0" {
        // Unify -0 / 0 / 0.0 / 0e5 etc. into one canonical zero.
        return Some(CanonicalDecimal {
            negative: false,
            digits,
            exponent: 0,
        });
    }

    while digits.ends_with('0') {
        digits.pop();
        exponent = exponent.checked_add(1)?;
    }

    Some(CanonicalDecimal {
        negative,
        digits,
        exponent,
    })
}

// ---- enum_semantics: typed-value-sorted map -----------------------------

const TAG_KV_STRING: u8 = 0;
const TAG_KV_NUMBER: u8 = 1;
const TAG_KV_BOOL: u8 = 2;

/// Encodes one `enum_semantics` map key with a leading type discriminator
/// so `integer 1`, `string "1"`, and `boolean true` never collide, and
/// numeric-looking keys canonicalize via [`canonical_decimal`] so `1` /
/// `1.0` / `1e0` sort and compare identically.
fn typed_key_bytes(key: &str) -> Vec<u8> {
    let mut out = Vec::new();
    if key == "true" || key == "false" {
        out.push(TAG_KV_BOOL);
        out.push(u8::from(key == "true"));
        return out;
    }
    if let Some(dec) = canonical_decimal(key) {
        out.push(TAG_KV_NUMBER);
        out.push(u8::from(dec.negative));
        push_i64(&mut out, dec.exponent);
        push_lp_str(&mut out, &dec.digits);
        return out;
    }
    out.push(TAG_KV_STRING);
    push_lp_str(&mut out, key);
    out
}

fn encode_meaning_code(mc: &MeaningCode, out: &mut Vec<u8>) {
    push_lp_str(out, &mc.vocabulary);
    push_lp_str(out, &mc.code);
}

/// Encodes a non-empty `enum_semantics` map: entries sorted by their
/// canonical typed-key bytes (not by the raw `BTreeMap` string order), as a
/// 4-byte entry count followed by `(typed_key_bytes, vocabulary, code)`
/// triples. Caller guarantees `map` is non-empty (an empty map normalizes
/// to the attribute being absent, handled by [`encode_field_semantics`]).
fn encode_enum_semantics(
    field_index: usize,
    map: &BTreeMap<String, MeaningCode>,
    out: &mut Vec<u8>,
) -> Result<(), CanonError> {
    let mut entries: Vec<(Vec<u8>, &MeaningCode)> =
        map.iter().map(|(k, v)| (typed_key_bytes(k), v)).collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    for pair in entries.windows(2) {
        if pair[0].0 == pair[1].0 {
            return Err(CanonError::DuplicateEnumKey { field_index });
        }
    }

    push_u32(out, entries.len() as u32);
    for (key_bytes, mc) in &entries {
        out.extend_from_slice(key_bytes);
        encode_meaning_code(mc, out);
    }
    Ok(())
}

// ---- Unit / Temporal -----------------------------------------------------

const TAG_UNIT_UCUM: u8 = 0;
const TAG_UNIT_ISO4217: u8 = 1;
const TAG_UNIT_REGISTERED: u8 = 2;

fn encode_unit(unit: &Unit, out: &mut Vec<u8>) {
    let tag = match unit.system {
        UnitSystem::Ucum => TAG_UNIT_UCUM,
        UnitSystem::Iso4217 => TAG_UNIT_ISO4217,
        UnitSystem::Registered => TAG_UNIT_REGISTERED,
    };
    out.push(tag);
    // UCUM codes are case-sensitive by spec (Task 1/2): no case folding.
    push_lp_str(out, &unit.code);
}

fn encode_temporal_kind(kind: TemporalKind) -> u8 {
    match kind {
        TemporalKind::Instant => 0,
        TemporalKind::LocalDatetime => 1,
        TemporalKind::Date => 2,
        TemporalKind::Duration => 3,
    }
}

fn encode_temporal_resolution(resolution: TemporalResolution) -> u8 {
    match resolution {
        TemporalResolution::S => 0,
        TemporalResolution::Ms => 1,
        TemporalResolution::Us => 2,
        TemporalResolution::Ns => 3,
    }
}

const TAG_EPOCH_UNIX: u8 = 0;
const TAG_EPOCH_REGISTERED: u8 = 1;

fn encode_epoch(epoch: &EpochBase, out: &mut Vec<u8>) {
    match epoch {
        EpochBase::Unix => out.push(TAG_EPOCH_UNIX),
        EpochBase::Registered(code) => {
            out.push(TAG_EPOCH_REGISTERED);
            push_lp_str(out, code);
        }
    }
}

const BIT_T_KIND: u8 = 1 << 0;
const BIT_T_EPOCH: u8 = 1 << 1;
const BIT_T_RESOLUTION: u8 = 1 << 2;

/// Encodes a `Temporal` struct as a 1-byte presence bitmap (fixed
/// `kind`/`epoch`/`resolution` order, matching the struct's own field
/// order) followed by only the present sub-attributes' bytes. Returns
/// `None` when every sub-attribute is `None` — a `Temporal` with all fields
/// absent normalizes to the whole attribute being absent, per spec.
fn encode_temporal(temporal: &Temporal) -> Option<Vec<u8>> {
    let mut bitmap = 0u8;
    let mut body = Vec::new();

    if let Some(kind) = temporal.kind {
        bitmap |= BIT_T_KIND;
        body.push(encode_temporal_kind(kind));
    }
    if let Some(epoch) = &temporal.epoch {
        bitmap |= BIT_T_EPOCH;
        encode_epoch(epoch, &mut body);
    }
    if let Some(resolution) = temporal.resolution {
        bitmap |= BIT_T_RESOLUTION;
        body.push(encode_temporal_resolution(resolution));
    }

    if bitmap == 0 {
        return None;
    }
    let mut out = Vec::with_capacity(1 + body.len());
    out.push(bitmap);
    out.extend_from_slice(&body);
    Some(out)
}

// ---- FieldSemantics -------------------------------------------------------

const BIT_CANONICAL_FIELD_ID: u8 = 1 << 0;
const BIT_IDENTIFIER_NAMESPACE: u8 = 1 << 1;
const BIT_UNIT: u8 = 1 << 2;
const BIT_NUMERIC_SCALE: u8 = 1 << 3;
const BIT_TEMPORAL: u8 = 1 << 4;
const BIT_ENUM_SEMANTICS: u8 = 1 << 5;

/// Encodes one field's `FieldSemantics` as a 1-byte presence bitmap (fixed
/// protocol attribute order: `canonical_field_id`, `identifier_namespace`,
/// `unit`, `numeric_scale`, `temporal`, `enum_semantics` — matching the
/// axis order in `deblob-p2d-hermes-review.md` §1) followed by only the
/// present attributes' bytes, in that same fixed order. A missing attribute
/// contributes nothing beyond its absent bit — no null placeholder value is
/// ever emitted. Returns `None` (the whole `FieldEntry` is to be removed)
/// when the bitmap ends up `0` — including when the only "populated"
/// attribute was a `Temporal` that itself normalized to absent, or an
/// `enum_semantics` map that was present but empty.
fn encode_field_semantics(
    field_index: usize,
    semantics: &FieldSemantics,
) -> Result<Option<Vec<u8>>, CanonError> {
    let mut bitmap = 0u8;
    let mut body = Vec::new();

    if let Some(cfid) = &semantics.canonical_field_id {
        bitmap |= BIT_CANONICAL_FIELD_ID;
        push_lp_str(&mut body, cfid.as_str());
    }
    if let Some(ns) = &semantics.identifier_namespace {
        bitmap |= BIT_IDENTIFIER_NAMESPACE;
        push_lp_str(&mut body, ns.as_str());
    }
    if let Some(unit) = &semantics.unit {
        bitmap |= BIT_UNIT;
        encode_unit(unit, &mut body);
    }
    if let Some(scale) = semantics.numeric_scale {
        bitmap |= BIT_NUMERIC_SCALE;
        push_i64(&mut body, scale);
    }
    if let Some(temporal) = &semantics.temporal {
        if let Some(temporal_bytes) = encode_temporal(temporal) {
            bitmap |= BIT_TEMPORAL;
            body.extend_from_slice(&temporal_bytes);
        }
    }
    if let Some(enum_map) = &semantics.enum_semantics {
        if !enum_map.is_empty() {
            bitmap |= BIT_ENUM_SEMANTICS;
            encode_enum_semantics(field_index, enum_map, &mut body)?;
        }
    }

    if bitmap == 0 {
        return Ok(None);
    }
    let mut out = Vec::with_capacity(1 + body.len());
    out.push(bitmap);
    out.extend_from_slice(&body);
    Ok(Some(out))
}

// ---- top level: SemanticMetadata -----------------------------------------

/// The result of normalizing a [`SemanticMetadata`]: the schema-level
/// `event_type` bytes (if present) and the surviving field entries, sorted
/// by canonical path bytes and deduplicated/validated. `pub(crate)` so
/// `digest.rs` can decide emptiness without recomputing this work or
/// duplicating the normalization rules.
#[derive(Debug)]
pub(crate) struct Normalized {
    pub(crate) event_type_bytes: Option<Vec<u8>>,
    pub(crate) fields: Vec<(Vec<u8>, Vec<u8>)>,
}

impl Normalized {
    /// True when no canonical semantic assertion survived normalization —
    /// no `event_type` and every field entry was removed (either it never
    /// had an attribute, or its only attributes normalized away to
    /// nothing). This is the exact condition under which
    /// [`crate::digest::semantic_fingerprint`] must return `Ok(None)`.
    pub(crate) fn is_empty(&self) -> bool {
        self.event_type_bytes.is_none() && self.fields.is_empty()
    }
}

fn encode_event_type(event_type: &CanonicalEventTypeId) -> Vec<u8> {
    let mut out = Vec::new();
    push_lp_str(&mut out, event_type.as_str());
    out
}

/// Normalizes `metadata`: validates and canonically encodes every field's
/// path (rejecting NUL/control-char keys), detects duplicate normalized
/// paths across ALL field entries (independent of whether their semantics
/// survive), encodes each field's semantics (dropping entries whose
/// semantics normalize to nothing), and sorts the survivors by canonical
/// path bytes.
pub(crate) fn normalize(metadata: &SemanticMetadata) -> Result<Normalized, CanonError> {
    let event_type_bytes = metadata.event_type.as_ref().map(encode_event_type);

    // Encode every path first (this both validates keys and gives us the
    // duplicate-detection key), regardless of whether the field's semantics
    // will later be dropped as empty — duplicate paths are a structural
    // problem with the input independent of what's annotated on them.
    let mut encoded_paths: Vec<Vec<u8>> = Vec::with_capacity(metadata.fields.len());
    for (field_index, field) in metadata.fields.iter().enumerate() {
        encoded_paths.push(encode_path(field_index, &field.path)?);
    }

    // Duplicate detection: compare against every earlier field's path bytes
    // (metadata.fields.len() is small — controlled schema field counts —
    // so O(n^2) here keeps the error trivially reportable with both
    // conflicting indices, and there's no ordering requirement yet since
    // sorting happens after empties are dropped).
    for later in 1..encoded_paths.len() {
        for earlier in 0..later {
            if encoded_paths[earlier] == encoded_paths[later] {
                return Err(CanonError::DuplicatePath {
                    first_field_index: earlier,
                    second_field_index: later,
                });
            }
        }
    }

    let mut fields = Vec::with_capacity(metadata.fields.len());
    for (field_index, (field, path_bytes)) in metadata.fields.iter().zip(encoded_paths).enumerate()
    {
        if let Some(attr_bytes) = encode_field_semantics(field_index, &field.semantics)? {
            fields.push((path_bytes, attr_bytes));
        }
    }
    fields.sort_by(|a, b| a.0.cmp(&b.0));

    Ok(Normalized {
        event_type_bytes,
        fields,
    })
}

pub(crate) fn encode_normalized(normalized: &Normalized) -> Vec<u8> {
    let mut out = Vec::new();
    match &normalized.event_type_bytes {
        Some(bytes) => {
            out.push(1);
            out.extend_from_slice(bytes);
        }
        None => out.push(0),
    }
    push_u32(&mut out, normalized.fields.len() as u32);
    for (path_bytes, attr_bytes) in &normalized.fields {
        out.extend_from_slice(path_bytes);
        out.extend_from_slice(attr_bytes);
    }
    out
}

/// Deterministic byte-level canonical encoding of `metadata` — a hand-rolled
/// binary protocol, never `serde_json`, so key ordering and the exact
/// preimage bytes are fully under this module's control. Field entries are
/// emitted sorted by canonical path bytes (not input order); `event_type`
/// (if present) is emitted at a fixed schema-level position; a field entry
/// whose semantics carry no surviving attribute is removed entirely.
///
/// This function is always well-defined, including for a `metadata` with no
/// surviving assertions (it returns the fixed, deterministic encoding of
/// "nothing present"). It does NOT decide whether a `sem_` should exist —
/// that policy (`Ok(None)` when nothing survives, "no bytes, no hash, no
/// sentinel") lives in [`crate::digest::semantic_fingerprint`], which
/// checks emptiness *before* ever calling this encoder.
pub fn canonical_semantic_bytes(metadata: &SemanticMetadata) -> Result<Vec<u8>, CanonError> {
    let normalized = normalize(metadata)?;
    Ok(encode_normalized(&normalized))
}

#[cfg(test)]
mod tests {
    use super::*;
    use deblob_core::semantic::{FieldEntry, Unit, UnitSystem};
    use std::collections::BTreeMap;

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

    fn field(path: Vec<PathSegment>, semantics: FieldSemantics) -> FieldEntry {
        FieldEntry { path, semantics }
    }

    fn metadata(fields: Vec<FieldEntry>) -> SemanticMetadata {
        SemanticMetadata {
            event_type: None,
            fields,
        }
    }

    // -- typed path anti-ambiguity -----------------------------------

    #[test]
    fn single_key_with_dot_differs_from_two_key_path() {
        let one_segment = encode_path(0, &[key("a.b")]).unwrap();
        let two_segments = encode_path(0, &[key("a"), key("b")]).unwrap();
        assert_ne!(one_segment, two_segments);
    }

    #[test]
    fn wildcard_differs_from_literal_star_key() {
        let wildcard = encode_path(0, &[PathSegment::Wildcard]).unwrap();
        let star_key = encode_path(0, &[key("*")]).unwrap();
        assert_ne!(wildcard, star_key);
    }

    #[test]
    fn path_key_rejects_control_char() {
        let err = encode_path(0, &[key("a\u{0000}b")]).unwrap_err();
        assert_eq!(
            err,
            CanonError::InvalidPathKey {
                field_index: 0,
                segment_index: 0
            }
        );
    }

    #[test]
    fn path_key_nfc_normalizes() {
        // "e" + combining acute (NFD) vs precomposed "é" (NFC) must encode
        // identically after NFC normalization.
        let nfd = encode_path(0, &[key("e\u{0301}")]).unwrap();
        let nfc = encode_path(0, &[key("\u{00e9}")]).unwrap();
        assert_eq!(nfd, nfc);
    }

    // -- numeric canonicalization --------------------------------------

    #[test]
    fn canonical_decimal_unifies_int_float_and_exponent_forms() {
        let a = canonical_decimal("1").unwrap();
        let b = canonical_decimal("1.0").unwrap();
        let c = canonical_decimal("1e0").unwrap();
        assert_eq!(
            (a.negative, a.digits.clone(), a.exponent),
            (false, "1".to_string(), 0)
        );
        assert_eq!(
            (a.negative, a.digits.clone()),
            (b.negative, b.digits.clone())
        );
        assert_eq!(a.exponent, b.exponent);
        assert_eq!(
            (b.negative, b.digits.clone(), b.exponent),
            (c.negative, c.digits.clone(), c.exponent)
        );
    }

    #[test]
    fn canonical_decimal_distinguishes_different_magnitudes() {
        let one = canonical_decimal("1").unwrap();
        let ten = canonical_decimal("10").unwrap();
        assert_ne!((one.digits, one.exponent), (ten.digits, ten.exponent));
    }

    #[test]
    fn canonical_decimal_unifies_negative_zero() {
        let a = canonical_decimal("-0").unwrap();
        let b = canonical_decimal("0.0").unwrap();
        let c = canonical_decimal("0e5").unwrap();
        for d in [&a, &b, &c] {
            assert!(!d.negative);
            assert_eq!(d.digits, "0");
            assert_eq!(d.exponent, 0);
        }
    }

    #[test]
    fn canonical_decimal_rejects_leading_zero_and_non_numbers() {
        assert!(canonical_decimal("01").is_none());
        assert!(canonical_decimal("abc").is_none());
        assert!(canonical_decimal("1.").is_none());
        assert!(canonical_decimal("").is_none());
        assert!(canonical_decimal("--1").is_none());
    }

    #[test]
    fn typed_key_bytes_number_and_lookalike_string_are_distinguished_by_tag() {
        // A number-shaped key always takes the NUMBER tag; only a
        // non-numeric key takes the STRING tag, so the two tag spaces never
        // overlap for the same textual content.
        assert_eq!(typed_key_bytes("1")[0], TAG_KV_NUMBER);
        assert_eq!(typed_key_bytes("abc")[0], TAG_KV_STRING);
        assert_eq!(typed_key_bytes("true")[0], TAG_KV_BOOL);
    }

    // -- field-semantics normalization -----------------------------------

    #[test]
    fn empty_field_semantics_encodes_to_none() {
        assert_eq!(encode_field_semantics(0, &empty_semantics()).unwrap(), None);
    }

    #[test]
    fn temporal_with_all_none_normalizes_field_semantics_to_none() {
        let semantics = FieldSemantics {
            temporal: Some(Temporal {
                kind: None,
                epoch: None,
                resolution: None,
            }),
            ..empty_semantics()
        };
        assert_eq!(encode_field_semantics(0, &semantics).unwrap(), None);
    }

    #[test]
    fn empty_enum_semantics_map_normalizes_field_semantics_to_none() {
        let semantics = FieldSemantics {
            enum_semantics: Some(BTreeMap::new()),
            ..empty_semantics()
        };
        assert_eq!(encode_field_semantics(0, &semantics).unwrap(), None);
    }

    #[test]
    fn single_populated_attribute_is_not_none() {
        let semantics = FieldSemantics {
            numeric_scale: Some(2),
            ..empty_semantics()
        };
        assert!(encode_field_semantics(0, &semantics).unwrap().is_some());
    }

    // -- duplicate path / duplicate enum key -----------------------------

    #[test]
    fn normalize_rejects_duplicate_normalized_paths() {
        let meta = metadata(vec![
            field(vec![key("a")], empty_semantics()),
            field(
                vec![key("a")],
                FieldSemantics {
                    numeric_scale: Some(1),
                    ..empty_semantics()
                },
            ),
        ]);
        let err = normalize(&meta).unwrap_err();
        assert_eq!(
            err,
            CanonError::DuplicatePath {
                first_field_index: 0,
                second_field_index: 1
            }
        );
    }

    #[test]
    fn normalize_rejects_duplicate_paths_even_when_both_semantics_are_empty() {
        let meta = metadata(vec![
            field(vec![key("a")], empty_semantics()),
            field(vec![key("a")], empty_semantics()),
        ]);
        assert!(matches!(
            normalize(&meta).unwrap_err(),
            CanonError::DuplicatePath { .. }
        ));
    }

    #[test]
    fn enum_semantics_rejects_duplicate_canonical_key() {
        let mut map = BTreeMap::new();
        map.insert(
            "1".to_string(),
            MeaningCode {
                vocabulary: "deblob/x/v1".to_string(),
                code: "a".to_string(),
            },
        );
        map.insert(
            "1.0".to_string(),
            MeaningCode {
                vocabulary: "deblob/x/v1".to_string(),
                code: "b".to_string(),
            },
        );
        let mut out = Vec::new();
        let err = encode_enum_semantics(0, &map, &mut out).unwrap_err();
        assert_eq!(err, CanonError::DuplicateEnumKey { field_index: 0 });
    }

    // -- ordering ------------------------------------------------------

    #[test]
    fn field_entries_are_sorted_by_canonical_path_regardless_of_input_order() {
        let f_z = field(
            vec![key("z")],
            FieldSemantics {
                numeric_scale: Some(1),
                ..empty_semantics()
            },
        );
        let f_a = field(
            vec![key("a")],
            FieldSemantics {
                numeric_scale: Some(2),
                ..empty_semantics()
            },
        );
        let forward = metadata(vec![f_z.clone(), f_a.clone()]);
        let reverse = metadata(vec![f_a, f_z]);
        assert_eq!(
            canonical_semantic_bytes(&forward).unwrap(),
            canonical_semantic_bytes(&reverse).unwrap()
        );
    }

    // -- top-level bytes: determinism / sensitivity -----------------------

    fn full_metadata() -> SemanticMetadata {
        let mut enum_semantics = BTreeMap::new();
        enum_semantics.insert(
            "ACTIVE".to_string(),
            MeaningCode {
                vocabulary: "deblob/order-status/v1".to_string(),
                code: "pending".to_string(),
            },
        );
        SemanticMetadata {
            event_type: Some(CanonicalEventTypeId::new("user.created")),
            fields: vec![field(
                vec![key("temperature")],
                FieldSemantics {
                    canonical_field_id: Some(deblob_core::semantic::CanonicalFieldId::new(
                        "temperature.ambient",
                    )),
                    identifier_namespace: Some(deblob_core::semantic::NamespaceCode::new(
                        "acme.customer_id",
                    )),
                    unit: Some(Unit {
                        system: UnitSystem::Ucum,
                        code: "Cel".to_string(),
                    }),
                    numeric_scale: Some(2),
                    temporal: Some(Temporal {
                        kind: Some(TemporalKind::Instant),
                        epoch: Some(EpochBase::Unix),
                        resolution: Some(TemporalResolution::S),
                    }),
                    enum_semantics: Some(enum_semantics),
                },
            )],
        }
    }

    #[test]
    fn canonical_bytes_are_deterministic() {
        let meta = full_metadata();
        assert_eq!(
            canonical_semantic_bytes(&meta).unwrap(),
            canonical_semantic_bytes(&meta).unwrap()
        );
    }

    #[test]
    fn unit_code_change_changes_bytes() {
        let mut meta = full_metadata();
        meta.fields[0].semantics.unit = Some(Unit {
            system: UnitSystem::Ucum,
            code: "[degF]".to_string(),
        });
        assert_ne!(
            canonical_semantic_bytes(&full_metadata()).unwrap(),
            canonical_semantic_bytes(&meta).unwrap()
        );
    }

    #[test]
    fn temporal_resolution_change_changes_bytes() {
        let mut meta = full_metadata();
        meta.fields[0].semantics.temporal = Some(Temporal {
            kind: Some(TemporalKind::Instant),
            epoch: Some(EpochBase::Unix),
            resolution: Some(TemporalResolution::Ms),
        });
        assert_ne!(
            canonical_semantic_bytes(&full_metadata()).unwrap(),
            canonical_semantic_bytes(&meta).unwrap()
        );
    }

    #[test]
    fn empty_metadata_still_yields_deterministic_bytes() {
        let meta = metadata(vec![]);
        let bytes = canonical_semantic_bytes(&meta).unwrap();
        assert_eq!(bytes, canonical_semantic_bytes(&meta).unwrap());
        // event_type absent tag (0) + field count 0 (4 bytes) = 5 bytes.
        assert_eq!(bytes, vec![0u8, 0, 0, 0, 0]);
    }
}
