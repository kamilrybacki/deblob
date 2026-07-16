//! Controlled semantic-metadata types (P2-D Task 1, spec §5 `sem_`
//! dimension; AMENDED per `deblob-p2d-hermes-review.md` §1/§3).
//!
//! These are just typed wrappers over the controlled vocabulary — validating
//! a code against the actual vocabulary tables is Task 2 (a different
//! crate). This module only defines the shapes. `privacy_class` is
//! deliberately NOT reachable from any of these types: it is governance
//! metadata, lives on `SchemaRecord` (ports.rs), and must never enter the
//! `sem_` digest preimage.

use crate::id::SemanticId;
use std::collections::BTreeMap;

/// A validated-elsewhere (Task 2) identifier-namespace code, e.g.
/// `"acme.customer_id"`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct NamespaceCode(String);
impl NamespaceCode {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A validated-elsewhere (Task 2) canonical-field-id code, e.g.
/// `"temperature.ambient"`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct CanonicalFieldId(String);
impl CanonicalFieldId {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A validated-elsewhere (Task 2) schema-level canonical event-type code,
/// e.g. `"user.created"` vs `"user.deleted"` (Hermes review §1: the largest
/// omission in the original axis set — same fields, different meaning).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct CanonicalEventTypeId(String);
impl CanonicalEventTypeId {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Data-sensitivity classification for a schema. Governance metadata: varies
/// by jurisdiction/tenant/policy-version without the field's *meaning*
/// changing, so it lives on `SchemaRecord` (ports.rs) — NEVER inside
/// `FieldSemantics` or the `sem_` digest preimage (Hermes review §1/§3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrivacyClass {
    Public,
    Internal,
    Pii,
    Secret,
}

/// The controlled namespace a [`Unit::code`] is drawn from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnitSystem {
    /// Unified Code for Units of Measure. Codes are case-sensitive — never
    /// lowercase/uppercase-normalize a UCUM `code`.
    Ucum,
    /// ISO 4217 currency codes. Currency is expressed here, never as a
    /// proprietary string like `"currency.usd"`.
    Iso4217,
    /// An operator-registered unit code outside UCUM/ISO4217 (validation of
    /// the registry itself is Task 2).
    Registered,
}

/// A namespaced unit of measure, e.g. `{system: Ucum, code: "Cel"}` or
/// `{system: Iso4217, code: "USD"}`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Unit {
    pub system: UnitSystem,
    pub code: String,
}

/// A namespaced, versioned controlled meaning that an enum's own observed
/// value maps to, e.g. `{vocabulary: "deblob/order-status/v1", code:
/// "pending"}`. The vocabulary artifact a `vocabulary` string names is
/// immutable and version-addressed (Task 2/vocabulary rules) — a registered
/// code must never be silently redefined.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MeaningCode {
    pub vocabulary: String,
    pub code: String,
}

/// One typed path segment. `Wildcard` (an array element position) is a
/// distinct typed value, NOT the literal string `"*"` — a raw `"*"` string
/// must not deserialize into `Wildcard`. Canonical byte-level encoding
/// (Task 3) does NOT use this derived `Ord` for the `sem_` digest preimage
/// (it sorts by encoded bytes instead); this derive exists so `Vec<PathSegment>`
/// can key a `BTreeSet`/`BTreeMap` (Task 4: enumerating/validating field
/// paths against a schema's structural canonical form) — declaration order
/// (`Key` before `Wildcard`) is an arbitrary but deterministic tie-break,
/// not a spec-mandated ordering.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PathSegment {
    Key(String),
    Wildcard,
}

/// Controlled semantic metadata for a single canonical field path. Every
/// attribute is a typed code newtype or enum — never free prose — so the
/// semantic identity of a field is always drawn from a controlled
/// vocabulary (validation of the codes themselves is Task 2). Deliberately
/// has NO `privacy_class` field (Hermes review §1/§3: that is governance,
/// not intrinsic meaning, and lives on `SchemaRecord` instead).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldSemantics {
    pub canonical_field_id: Option<CanonicalFieldId>,
    pub identifier_namespace: Option<NamespaceCode>,
    pub unit: Option<Unit>,
    /// Signed decimal scale: a stored `1234` at `scale: 2` means `12.34`.
    /// Scale changes meaning; this is NOT numeric precision (precision is
    /// physical, i.e. `sch_`'s concern, not `sem_`'s).
    pub numeric_scale: Option<i64>,
    pub temporal: Option<Temporal>,
    /// Keys are the schema's own enum VALUES (as observed in the data);
    /// values are the controlled [`MeaningCode`] each value maps to. A
    /// `BTreeMap` keeps this deterministic for hashing/fingerprinting.
    pub enum_semantics: Option<BTreeMap<String, MeaningCode>>,
}

/// One field's typed path plus its controlled semantics. `SemanticMetadata`
/// carries a `Vec<FieldEntry>` rather than a path-keyed map because paths
/// are themselves typed segments (Task 3 defines their canonical sort
/// order/dedup, not this type).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldEntry {
    pub path: Vec<PathSegment>,
    pub semantics: FieldSemantics,
}

/// What kind of point/duration in time a field represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TemporalKind {
    Instant,
    LocalDatetime,
    Date,
    Duration,
}

/// The sub-second (or coarser) unit a numeric temporal value is expressed
/// in, e.g. epoch-seconds vs epoch-milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TemporalResolution {
    S,
    Ms,
    Us,
    Ns,
}

/// The epoch a numeric temporal value counts from.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EpochBase {
    Unix,
    /// An operator-registered non-Unix epoch, named by this code (validation
    /// is Task 2).
    Registered(String),
}

/// Minimal temporal semantics: covers the common epoch-seconds-vs-
/// milliseconds false-merge. Full timezone machinery (`encoding`,
/// `timezone_policy`, `timezone`, `timezone_field`) is deferred to P4.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Temporal {
    pub kind: Option<TemporalKind>,
    pub epoch: Option<EpochBase>,
    pub resolution: Option<TemporalResolution>,
}

/// Controlled semantic metadata for a whole schema: an optional
/// schema-level canonical event type plus the per-field entries.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticMetadata {
    pub event_type: Option<CanonicalEventTypeId>,
    pub fields: Vec<FieldEntry>,
}

/// A schema's `sem_` identity. Always wraps a real [`SemanticId`] — there is
/// deliberately NO "un-annotated" variant here (Hermes review §3): a schema
/// that carries no controlled semantic metadata is expressed as
/// `Option<SemanticFingerprint> = None` by the caller, never as a sentinel
/// value inside this type.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct SemanticFingerprint(pub SemanticId);

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn privacy_class_round_trips_snake_case() {
        assert_eq!(
            serde_json::to_string(&PrivacyClass::Public).unwrap(),
            "\"public\""
        );
        assert_eq!(
            serde_json::to_string(&PrivacyClass::Internal).unwrap(),
            "\"internal\""
        );
        assert_eq!(
            serde_json::to_string(&PrivacyClass::Pii).unwrap(),
            "\"pii\""
        );
        assert_eq!(
            serde_json::to_string(&PrivacyClass::Secret).unwrap(),
            "\"secret\""
        );
        let round: PrivacyClass = serde_json::from_str("\"pii\"").unwrap();
        assert_eq!(round, PrivacyClass::Pii);
    }

    #[test]
    fn field_semantics_has_no_privacy_class_field_and_rejects_unknown_fields() {
        // privacy_class is governance metadata (Hermes review §1/§3); it
        // must not be a recognized key on FieldSemantics at all, and
        // deny_unknown_fields must reject it (and any other bogus field).
        let json = r#"{"privacy_class": "pii"}"#;
        let result: Result<FieldSemantics, _> = serde_json::from_str(json);
        assert!(result.is_err());

        let json = r#"{"bogus_field": true}"#;
        let result: Result<FieldSemantics, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn field_semantics_round_trips_all_attributes() {
        let mut enum_semantics = BTreeMap::new();
        enum_semantics.insert(
            "ACTIVE".to_string(),
            MeaningCode {
                vocabulary: "deblob/order-status/v1".to_string(),
                code: "pending".to_string(),
            },
        );

        let fs = FieldSemantics {
            canonical_field_id: Some(CanonicalFieldId::new("temperature.ambient")),
            identifier_namespace: Some(NamespaceCode::new("acme.customer_id")),
            unit: Some(Unit {
                system: UnitSystem::Ucum,
                code: "Cel".to_string(),
            }),
            numeric_scale: Some(2),
            temporal: Some(Temporal {
                kind: Some(TemporalKind::Instant),
                epoch: Some(EpochBase::Unix),
                resolution: Some(TemporalResolution::Ms),
            }),
            enum_semantics: Some(enum_semantics),
        };

        let json = serde_json::to_string(&fs).unwrap();
        let round: FieldSemantics = serde_json::from_str(&json).unwrap();
        assert_eq!(fs, round);
    }

    #[test]
    fn field_semantics_all_none_round_trips() {
        let fs = FieldSemantics {
            canonical_field_id: None,
            identifier_namespace: None,
            unit: None,
            numeric_scale: None,
            temporal: None,
            enum_semantics: None,
        };
        let json = serde_json::to_string(&fs).unwrap();
        let round: FieldSemantics = serde_json::from_str(&json).unwrap();
        assert_eq!(fs, round);
    }

    #[test]
    fn unit_round_trips_system_and_code() {
        let u = Unit {
            system: UnitSystem::Iso4217,
            code: "USD".to_string(),
        };
        let json = serde_json::to_value(&u).unwrap();
        assert_eq!(json["system"], "iso4217");
        assert_eq!(json["code"], "USD");
        let round: Unit = serde_json::from_value(json).unwrap();
        assert_eq!(u, round);
    }

    #[test]
    fn unit_ucum_code_is_case_sensitive_no_normalization() {
        // UCUM distinguishes case (e.g. "mL" vs "ML"); the type must not
        // normalize case in either direction.
        let u = Unit {
            system: UnitSystem::Ucum,
            code: "mL".to_string(),
        };
        let json = serde_json::to_string(&u).unwrap();
        let round: Unit = serde_json::from_str(&json).unwrap();
        assert_eq!(round.code, "mL");
    }

    #[test]
    fn path_segment_wildcard_serializes_distinctly_from_key_star() {
        let wildcard = PathSegment::Wildcard;
        let key_star = PathSegment::Key("*".to_string());

        let wildcard_json = serde_json::to_value(&wildcard).unwrap();
        let key_star_json = serde_json::to_value(&key_star).unwrap();
        assert_ne!(wildcard_json, key_star_json);

        // A raw bare string "*" (what an untyped/naive path representation
        // would produce) must NOT deserialize into Wildcard.
        let raw_star: Result<PathSegment, _> = serde_json::from_str("\"*\"");
        assert!(raw_star.is_err());

        // Round-trip both variants through their own serialized form.
        let round_wildcard: PathSegment = serde_json::from_value(wildcard_json).unwrap();
        assert_eq!(round_wildcard, PathSegment::Wildcard);
        let round_key_star: PathSegment = serde_json::from_value(key_star_json).unwrap();
        assert_eq!(round_key_star, PathSegment::Key("*".to_string()));
    }

    #[test]
    fn field_entry_rejects_unknown_field() {
        let json = r#"{"path": [], "semantics": {}, "bogus": 1}"#;
        let result: Result<FieldEntry, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn semantic_metadata_carries_event_type_and_field_entries() {
        let meta = SemanticMetadata {
            event_type: Some(CanonicalEventTypeId::new("user.created")),
            fields: vec![FieldEntry {
                path: vec![PathSegment::Key("temperature".to_string())],
                semantics: FieldSemantics {
                    canonical_field_id: None,
                    identifier_namespace: None,
                    unit: Some(Unit {
                        system: UnitSystem::Ucum,
                        code: "Cel".to_string(),
                    }),
                    numeric_scale: None,
                    temporal: None,
                    enum_semantics: None,
                },
            }],
        };

        let json = serde_json::to_string(&meta).unwrap();
        let round: SemanticMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(meta, round);
        assert_eq!(
            round.event_type,
            Some(CanonicalEventTypeId::new("user.created"))
        );
        assert_eq!(round.fields.len(), 1);
    }

    #[test]
    fn semantic_metadata_rejects_unknown_field() {
        let json = r#"{"event_type": null, "fields": [], "bogus": 1}"#;
        let result: Result<SemanticMetadata, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn semantic_fingerprint_wraps_semantic_id_no_none_variant() {
        let id = crate::id::SemanticId::from_digest(&[9u8; 32]);
        let fp = SemanticFingerprint(id.clone());
        let json = serde_json::to_string(&fp).unwrap();
        // Transparent: serializes as the bare sem_ string, not a wrapper.
        assert_eq!(json, serde_json::to_string(&id).unwrap());
        let round: SemanticFingerprint = serde_json::from_str(&json).unwrap();
        assert_eq!(fp, round);

        // Un-annotated is expressed as Option::None at the call site, never
        // as a value this type can construct.
        let absent: Option<SemanticFingerprint> = None;
        assert_eq!(serde_json::to_string(&absent).unwrap(), "null");
    }

    #[test]
    fn code_newtypes_construct_and_expose_as_str() {
        assert_eq!(NamespaceCode::new("acme").as_str(), "acme");
        assert_eq!(
            CanonicalFieldId::new("temperature.ambient").as_str(),
            "temperature.ambient"
        );
        assert_eq!(
            CanonicalEventTypeId::new("user.created").as_str(),
            "user.created"
        );
    }

    #[test]
    fn meaning_code_round_trips_vocabulary_and_code() {
        let mc = MeaningCode {
            vocabulary: "deblob/order-status/v1".to_string(),
            code: "pending".to_string(),
        };
        let json = serde_json::to_value(&mc).unwrap();
        assert_eq!(json["vocabulary"], "deblob/order-status/v1");
        assert_eq!(json["code"], "pending");
        let round: MeaningCode = serde_json::from_value(json).unwrap();
        assert_eq!(mc, round);
    }

    #[test]
    fn temporal_round_trips_registered_epoch() {
        let t = Temporal {
            kind: Some(TemporalKind::Date),
            epoch: Some(EpochBase::Registered("acme-epoch".to_string())),
            resolution: None,
        };
        let json = serde_json::to_string(&t).unwrap();
        let round: Temporal = serde_json::from_str(&json).unwrap();
        assert_eq!(t, round);
    }
}
