//! Controlled semantic-metadata types (P2-D Task 1, spec §5 sem_ dimension).
//!
//! These are just typed wrappers over the controlled vocabulary — validating
//! a code against the actual vocabulary tables is Task 2 (a different
//! crate). This module only defines the shapes.

use crate::id::SemanticId;
use std::collections::BTreeMap;

/// A validated-elsewhere (Task 2) unit-of-measure code, e.g. `"celsius"`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct UnitCode(String);
impl UnitCode {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

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

/// A validated-elsewhere (Task 2) controlled meaning code that an enum's own
/// value maps to, e.g. `"status.active"`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct MeaningCode(String);
impl MeaningCode {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Data-sensitivity classification for a field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrivacyClass {
    Public,
    Internal,
    Pii,
    Secret,
}

/// Controlled semantic metadata for a single canonical field path. Every
/// attribute is a typed code newtype or enum — never free prose — so the
/// semantic identity of a field is always drawn from a controlled
/// vocabulary (validation of the codes themselves is Task 2).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldSemantics {
    pub unit: Option<UnitCode>,
    pub identifier_namespace: Option<NamespaceCode>,
    pub canonical_field_id: Option<CanonicalFieldId>,
    pub privacy_class: Option<PrivacyClass>,
    /// Keys are the schema's own enum VALUES (as observed in the data);
    /// values are the controlled `MeaningCode` each value maps to. A
    /// `BTreeMap` keeps this deterministic for hashing/fingerprinting.
    pub enum_semantics: Option<BTreeMap<String, MeaningCode>>,
}

/// Controlled semantic metadata for a whole schema, keyed by canonical field
/// path.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct SemanticMetadata(pub BTreeMap<String, FieldSemantics>);

/// Result of computing a schema's semantic fingerprint (Task 3): either the
/// schema carries no controlled semantic metadata at all ("un-annotated =
/// semantically unknown", distinct from an annotated-but-empty metadata
/// map), or it does and this is its `sem_` identity.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SemanticFingerprint {
    None,
    Some(SemanticId),
}

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
    fn field_semantics_rejects_unknown_field() {
        let json = r#"{"unit": "celsius", "bogus_field": true}"#;
        let result: Result<FieldSemantics, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn field_semantics_round_trips_all_attributes() {
        let mut enum_semantics = BTreeMap::new();
        enum_semantics.insert("ACTIVE".to_string(), MeaningCode::new("status.active"));

        let fs = FieldSemantics {
            unit: Some(UnitCode::new("celsius")),
            identifier_namespace: Some(NamespaceCode::new("acme.customer_id")),
            canonical_field_id: Some(CanonicalFieldId::new("temperature.ambient")),
            privacy_class: Some(PrivacyClass::Pii),
            enum_semantics: Some(enum_semantics),
        };

        let json = serde_json::to_string(&fs).unwrap();
        let round: FieldSemantics = serde_json::from_str(&json).unwrap();
        assert_eq!(fs, round);
    }

    #[test]
    fn semantic_metadata_is_transparent_over_map() {
        let mut map = BTreeMap::new();
        map.insert(
            "temperature".to_string(),
            FieldSemantics {
                unit: Some(UnitCode::new("celsius")),
                identifier_namespace: None,
                canonical_field_id: None,
                privacy_class: None,
                enum_semantics: None,
            },
        );
        let meta = SemanticMetadata(map.clone());
        let json = serde_json::to_value(&meta).unwrap();
        // Transparent: serializes as the bare map, not a wrapper object.
        assert!(json.get("temperature").is_some());
        assert!(json.get("0").is_none());

        let round: SemanticMetadata = serde_json::from_value(json).unwrap();
        assert_eq!(round.0, map);
    }

    #[test]
    fn semantic_fingerprint_round_trips_both_variants() {
        let none = SemanticFingerprint::None;
        let json = serde_json::to_string(&none).unwrap();
        let round: SemanticFingerprint = serde_json::from_str(&json).unwrap();
        assert_eq!(none, round);

        let id = crate::id::SemanticId::from_digest(&[9u8; 32]);
        let some = SemanticFingerprint::Some(id.clone());
        let json = serde_json::to_string(&some).unwrap();
        let round: SemanticFingerprint = serde_json::from_str(&json).unwrap();
        assert_eq!(some, round);
    }

    #[test]
    fn code_newtypes_construct_and_expose_as_str() {
        assert_eq!(UnitCode::new("celsius").as_str(), "celsius");
        assert_eq!(NamespaceCode::new("acme").as_str(), "acme");
        assert_eq!(
            CanonicalFieldId::new("temperature.ambient").as_str(),
            "temperature.ambient"
        );
        assert_eq!(MeaningCode::new("status.active").as_str(), "status.active");
    }
}
