//! Version-addressed, immutable vocabulary tables for `deblob-semantic-v1`,
//! plus the pure validation function that checks controlled semantic
//! metadata tokens against them.
//!
//! # Immutability contract
//!
//! Every table in this module is part of the `deblob-semantic-v1` protocol
//! version identified by [`VOCAB_VERSION`]. A code that is registered here
//! is never redefined or removed within v1: extending the vocabulary (new
//! unit codes, new meaning-code vocabularies, ...) requires a new protocol
//! version (`deblob-semantic-v2`), never an in-place edit of these tables.
//! This is what lets a `sem_` digest computed against v1 stay valid forever
//! (Task 3 mixes `VOCAB_VERSION` into the preimage).
//!
//! This module performs NO I/O and is fully synchronous.

use deblob_core::semantic::{FieldSemantics, SemanticMetadata};
use std::collections::BTreeSet;

/// Identifies the exact table set in this module. Consumed by Task 3's
/// canonical-bytes preimage; bump only by adding a new module/version, never
/// by editing the tables below in place.
pub const VOCAB_VERSION: &str = "deblob-semantic-v1";

/// UCUM (Unified Code for Units of Measure) unit codes recognized by
/// `deblob-semantic-v1`. Case-sensitive — UCUM distinguishes `"mL"` from
/// `"ML"`; never normalize case when comparing against this table.
///
/// This is a curated v1 baseline (common physical units), not the full UCUM
/// grammar/table. Extending it is a new vocabulary version.
pub const UCUM_UNITS: &[&str] = &[
    "1", "%", "By", "Cel", "K", "[degF]", "g", "kg", "L", "m", "mL", "mm", "cm", "km", "mol", "s",
    "ms", "min", "h", "d", "Hz", "V", "A", "W", "J", "N", "Pa",
];

/// ISO 4217 currency codes recognized by `deblob-semantic-v1`. Currency is
/// always expressed via this table (never a proprietary string like
/// `"currency.usd"`).
///
/// This is a curated v1 baseline of common currencies, not the full ISO
/// 4217 list. Extending it is a new vocabulary version.
pub const ISO4217_CURRENCIES: &[&str] = &[
    "USD", "EUR", "GBP", "JPY", "PLN", "CHF", "CAD", "AUD", "NZD", "CNY", "INR", "BRL", "MXN",
    "ZAR", "SEK", "NOK", "DKK", "SGD", "HKD", "KRW",
];

/// Identifier-namespace codes recognized by `deblob-semantic-v1` for
/// [`deblob_core::semantic::FieldSemantics::identifier_namespace`]. Unlike
/// `canonical_field_id`/`canonical_event_type_id` (which are per-operator
/// governance-registered — see [`CanonicalFieldIdRegistry`] /
/// [`CanonicalEventTypeIdRegistry`]), identifier namespaces are a baked,
/// version-addressed table like units and currencies.
///
/// This is a curated v1 baseline; extending it is a new vocabulary version.
pub const NAMESPACE_CODES: &[&str] = &[
    "acme.customer_id",
    "acme.order_id",
    "iso.country_code",
    "internal.uuid",
];

/// Registered `MeaningCode` vocabularies: each entry is an immutable
/// namespace+version string (e.g. `"deblob/order-status/v1"`) that a
/// `MeaningCode.vocabulary` must belong to. `deblob-semantic-v1` validates
/// only that the vocabulary *namespace* is registered — it deliberately
/// does NOT maintain a per-vocabulary code list (a code within a registered
/// vocabulary is trusted; only membership of the vocabulary itself is
/// checked). A registered vocabulary's codes are immutable and
/// version-addressed by construction: extending or reinterpreting one
/// requires minting `.../v2`, never editing `.../v1` in place.
pub const MEANING_VOCABULARIES: &[&str] = &[
    "deblob/order-status/v1",
    "deblob/user-role/v1",
    "deblob/payment-method/v1",
];

/// Errors returned by [`validate_metadata`]. Each variant carries only the
/// offending *controlled token* itself (a unit code, namespace code,
/// registered id, or vocabulary name) — never free-form user text beyond
/// that token.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum VocabError {
    #[error("unknown unit code: {0}")]
    UnknownUnit(String),
    #[error("unknown identifier namespace: {0}")]
    UnknownNamespace(String),
    #[error("unregistered canonical field id: {0}")]
    UnregisteredFieldId(String),
    #[error("unregistered canonical event type id: {0}")]
    UnregisteredEventType(String),
    #[error("unknown meaning-code vocabulary: {0}")]
    UnknownMeaningVocabulary(String),
}

/// An injectable, governance-registered set of `canonical_field_id` values.
/// Defaults to EMPTY — every `canonical_field_id` is rejected until
/// explicitly registered by the caller (there is no baked table, unlike
/// units/namespaces/vocabularies: field ids are per-operator governance,
/// not a fixed external standard).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CanonicalFieldIdRegistry(BTreeSet<String>);

impl CanonicalFieldIdRegistry {
    /// Registers `id`, returning `true` if it was newly inserted.
    pub fn register(&mut self, id: impl Into<String>) -> bool {
        self.0.insert(id.into())
    }

    pub fn contains(&self, id: &str) -> bool {
        self.0.contains(id)
    }
}

/// An injectable, governance-registered set of `canonical_event_type_id`
/// values. Defaults to EMPTY — every event-type id is rejected until
/// explicitly registered by the caller.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CanonicalEventTypeIdRegistry(BTreeSet<String>);

impl CanonicalEventTypeIdRegistry {
    /// Registers `id`, returning `true` if it was newly inserted.
    pub fn register(&mut self, id: impl Into<String>) -> bool {
        self.0.insert(id.into())
    }

    pub fn contains(&self, id: &str) -> bool {
        self.0.contains(id)
    }
}

/// The injectable registries [`validate_metadata`] checks governance-scoped
/// ids against. Both default to EMPTY.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Registries {
    pub field_ids: CanonicalFieldIdRegistry,
    pub event_type_ids: CanonicalEventTypeIdRegistry,
}

/// Validates a single field's controlled semantics, returning the first
/// offending token in field-declaration order: `canonical_field_id`,
/// `unit`, `identifier_namespace`, then each `enum_semantics` entry's
/// `MeaningCode.vocabulary`. `numeric_scale` (a plain signed integer) and
/// `temporal` (fixed enums, valid by type) need no table lookup and are not
/// checked here.
fn validate_field_semantics(
    semantics: &FieldSemantics,
    registries: &Registries,
) -> Result<(), VocabError> {
    if let Some(cfid) = &semantics.canonical_field_id {
        if !registries.field_ids.contains(cfid.as_str()) {
            return Err(VocabError::UnregisteredFieldId(cfid.as_str().to_string()));
        }
    }

    if let Some(unit) = &semantics.unit {
        validate_unit_code(unit)?;
    }

    if let Some(namespace) = &semantics.identifier_namespace {
        if !NAMESPACE_CODES.contains(&namespace.as_str()) {
            return Err(VocabError::UnknownNamespace(namespace.as_str().to_string()));
        }
    }

    if let Some(enum_semantics) = &semantics.enum_semantics {
        for mapping in enum_semantics {
            if !MEANING_VOCABULARIES.contains(&mapping.meaning.vocabulary.as_str()) {
                return Err(VocabError::UnknownMeaningVocabulary(
                    mapping.meaning.vocabulary.clone(),
                ));
            }
        }
    }

    Ok(())
}

fn validate_unit_code(unit: &deblob_core::semantic::Unit) -> Result<(), VocabError> {
    use deblob_core::semantic::UnitSystem;

    let known = match unit.system {
        UnitSystem::Ucum => UCUM_UNITS.contains(&unit.code.as_str()),
        UnitSystem::Iso4217 => ISO4217_CURRENCIES.contains(&unit.code.as_str()),
        // An operator-registered unit code outside UCUM/ISO4217: the table
        // itself is out of scope for v1 (Task 2 brief), so any non-empty
        // code is accepted.
        UnitSystem::Registered => !unit.code.is_empty(),
    };

    if known {
        Ok(())
    } else {
        Err(VocabError::UnknownUnit(unit.code.clone()))
    }
}

/// Validates every controlled semantic-metadata token in `metadata` against
/// the `deblob-semantic-v1` tables and the supplied `registries`, returning
/// the FIRST offending token as a [`VocabError`]. Checks, in order: the
/// schema-level `event_type` (if present, must be in `registries`), then
/// each field's `canonical_field_id`, `unit`, `identifier_namespace`, and
/// `enum_semantics` vocabularies.
///
/// This function validates only the controlled metadata tokens themselves —
/// never the raw payload values a schema describes.
pub fn validate_metadata(
    metadata: &SemanticMetadata,
    registries: &Registries,
) -> Result<(), VocabError> {
    if let Some(event_type) = &metadata.event_type {
        if !registries.event_type_ids.contains(event_type.as_str()) {
            return Err(VocabError::UnregisteredEventType(
                event_type.as_str().to_string(),
            ));
        }
    }

    for field in &metadata.fields {
        validate_field_semantics(&field.semantics, registries)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use deblob_core::semantic::{
        CanonicalEventTypeId, CanonicalFieldId, EnumMapping, EnumValue, FieldEntry, MeaningCode,
        NamespaceCode, PathSegment, Unit, UnitSystem,
    };

    fn field(semantics: FieldSemantics) -> FieldEntry {
        FieldEntry {
            path: vec![PathSegment::Key("f".to_string())],
            semantics,
        }
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

    fn metadata_with(fields: Vec<FieldEntry>) -> SemanticMetadata {
        SemanticMetadata {
            event_type: None,
            fields,
        }
    }

    #[test]
    fn vocab_version_is_v1() {
        assert_eq!(VOCAB_VERSION, "deblob-semantic-v1");
    }

    #[test]
    fn ucum_unit_cel_validates() {
        let meta = metadata_with(vec![field(FieldSemantics {
            unit: Some(Unit {
                system: UnitSystem::Ucum,
                code: "Cel".to_string(),
            }),
            ..empty_semantics()
        })]);
        assert!(validate_metadata(&meta, &Registries::default()).is_ok());
    }

    #[test]
    fn ucum_unknown_code_is_unknown_unit_error() {
        let meta = metadata_with(vec![field(FieldSemantics {
            unit: Some(Unit {
                system: UnitSystem::Ucum,
                code: "furlongs".to_string(),
            }),
            ..empty_semantics()
        })]);
        let err = validate_metadata(&meta, &Registries::default()).unwrap_err();
        assert_eq!(err, VocabError::UnknownUnit("furlongs".to_string()));
    }

    #[test]
    fn iso4217_usd_validates() {
        let meta = metadata_with(vec![field(FieldSemantics {
            unit: Some(Unit {
                system: UnitSystem::Iso4217,
                code: "USD".to_string(),
            }),
            ..empty_semantics()
        })]);
        assert!(validate_metadata(&meta, &Registries::default()).is_ok());
    }

    #[test]
    fn iso4217_unknown_code_is_unknown_unit_error() {
        let meta = metadata_with(vec![field(FieldSemantics {
            unit: Some(Unit {
                system: UnitSystem::Iso4217,
                code: "BITCOIN".to_string(),
            }),
            ..empty_semantics()
        })]);
        let err = validate_metadata(&meta, &Registries::default()).unwrap_err();
        assert_eq!(err, VocabError::UnknownUnit("BITCOIN".to_string()));
    }

    #[test]
    fn registered_unit_system_accepts_any_non_empty_code() {
        let meta = metadata_with(vec![field(FieldSemantics {
            unit: Some(Unit {
                system: UnitSystem::Registered,
                code: "acme-widgets".to_string(),
            }),
            ..empty_semantics()
        })]);
        assert!(validate_metadata(&meta, &Registries::default()).is_ok());
    }

    #[test]
    fn registered_unit_system_rejects_empty_code() {
        let meta = metadata_with(vec![field(FieldSemantics {
            unit: Some(Unit {
                system: UnitSystem::Registered,
                code: String::new(),
            }),
            ..empty_semantics()
        })]);
        let err = validate_metadata(&meta, &Registries::default()).unwrap_err();
        assert_eq!(err, VocabError::UnknownUnit(String::new()));
    }

    #[test]
    fn ucum_codes_are_case_sensitive() {
        // "Cel" is valid UCUM; "CEL"/"cel" are not registered codes.
        let meta = metadata_with(vec![field(FieldSemantics {
            unit: Some(Unit {
                system: UnitSystem::Ucum,
                code: "CEL".to_string(),
            }),
            ..empty_semantics()
        })]);
        let err = validate_metadata(&meta, &Registries::default()).unwrap_err();
        assert_eq!(err, VocabError::UnknownUnit("CEL".to_string()));
    }

    #[test]
    fn unregistered_canonical_field_id_is_rejected() {
        let meta = metadata_with(vec![field(FieldSemantics {
            canonical_field_id: Some(CanonicalFieldId::new("temperature.ambient")),
            ..empty_semantics()
        })]);
        let err = validate_metadata(&meta, &Registries::default()).unwrap_err();
        assert_eq!(
            err,
            VocabError::UnregisteredFieldId("temperature.ambient".to_string())
        );
    }

    #[test]
    fn registered_canonical_field_id_validates() {
        let meta = metadata_with(vec![field(FieldSemantics {
            canonical_field_id: Some(CanonicalFieldId::new("temperature.ambient")),
            ..empty_semantics()
        })]);
        let mut registries = Registries::default();
        registries.field_ids.register("temperature.ambient");
        assert!(validate_metadata(&meta, &registries).is_ok());
    }

    #[test]
    fn unregistered_event_type_is_rejected() {
        let meta = SemanticMetadata {
            event_type: Some(CanonicalEventTypeId::new("user.created")),
            fields: vec![],
        };
        let err = validate_metadata(&meta, &Registries::default()).unwrap_err();
        assert_eq!(
            err,
            VocabError::UnregisteredEventType("user.created".to_string())
        );
    }

    #[test]
    fn registered_event_type_validates() {
        let meta = SemanticMetadata {
            event_type: Some(CanonicalEventTypeId::new("user.created")),
            fields: vec![],
        };
        let mut registries = Registries::default();
        registries.event_type_ids.register("user.created");
        assert!(validate_metadata(&meta, &registries).is_ok());
    }

    #[test]
    fn registered_namespace_validates() {
        let meta = metadata_with(vec![field(FieldSemantics {
            identifier_namespace: Some(NamespaceCode::new("acme.customer_id")),
            ..empty_semantics()
        })]);
        assert!(validate_metadata(&meta, &Registries::default()).is_ok());
    }

    #[test]
    fn unknown_namespace_is_rejected() {
        let meta = metadata_with(vec![field(FieldSemantics {
            identifier_namespace: Some(NamespaceCode::new("bogus.namespace")),
            ..empty_semantics()
        })]);
        let err = validate_metadata(&meta, &Registries::default()).unwrap_err();
        assert_eq!(
            err,
            VocabError::UnknownNamespace("bogus.namespace".to_string())
        );
    }

    #[test]
    fn meaning_code_with_unregistered_vocabulary_is_rejected() {
        let enum_semantics = vec![EnumMapping {
            value: EnumValue::String("ACTIVE".to_string()),
            meaning: MeaningCode {
                vocabulary: "acme/not-registered/v1".to_string(),
                code: "pending".to_string(),
            },
        }];
        let meta = metadata_with(vec![field(FieldSemantics {
            enum_semantics: Some(enum_semantics),
            ..empty_semantics()
        })]);
        let err = validate_metadata(&meta, &Registries::default()).unwrap_err();
        assert_eq!(
            err,
            VocabError::UnknownMeaningVocabulary("acme/not-registered/v1".to_string())
        );
    }

    #[test]
    fn meaning_code_with_registered_vocabulary_validates_any_code_within_it() {
        // deblob-semantic-v1 only checks that the vocabulary *namespace* is
        // registered/immutable — it does not maintain a per-vocabulary code
        // list, so any code string under a registered vocabulary passes.
        let enum_semantics = vec![
            EnumMapping {
                value: EnumValue::String("ACTIVE".to_string()),
                meaning: MeaningCode {
                    vocabulary: "deblob/order-status/v1".to_string(),
                    code: "pending".to_string(),
                },
            },
            EnumMapping {
                value: EnumValue::String("INACTIVE".to_string()),
                meaning: MeaningCode {
                    vocabulary: "deblob/order-status/v1".to_string(),
                    code: "some-arbitrary-code-not-in-any-list".to_string(),
                },
            },
        ];
        let meta = metadata_with(vec![field(FieldSemantics {
            enum_semantics: Some(enum_semantics),
            ..empty_semantics()
        })]);
        assert!(validate_metadata(&meta, &Registries::default()).is_ok());
    }

    #[test]
    fn fully_populated_valid_metadata_validates_end_to_end() {
        let enum_semantics = vec![EnumMapping {
            value: EnumValue::String("ACTIVE".to_string()),
            meaning: MeaningCode {
                vocabulary: "deblob/order-status/v1".to_string(),
                code: "pending".to_string(),
            },
        }];

        let meta = SemanticMetadata {
            event_type: Some(CanonicalEventTypeId::new("order.created")),
            fields: vec![field(FieldSemantics {
                canonical_field_id: Some(CanonicalFieldId::new("order.total")),
                identifier_namespace: Some(NamespaceCode::new("acme.order_id")),
                unit: Some(Unit {
                    system: UnitSystem::Iso4217,
                    code: "USD".to_string(),
                }),
                numeric_scale: Some(2),
                temporal: None,
                enum_semantics: Some(enum_semantics),
            })],
        };

        let mut registries = Registries::default();
        registries.event_type_ids.register("order.created");
        registries.field_ids.register("order.total");

        assert!(validate_metadata(&meta, &registries).is_ok());
    }

    #[test]
    fn first_offending_token_wins_over_later_ones() {
        // canonical_field_id is checked before unit in field-declaration
        // order, so an unregistered field id must win even though the unit
        // is also unknown.
        let meta = metadata_with(vec![field(FieldSemantics {
            canonical_field_id: Some(CanonicalFieldId::new("not.registered")),
            unit: Some(Unit {
                system: UnitSystem::Ucum,
                code: "not-a-real-unit".to_string(),
            }),
            ..empty_semantics()
        })]);
        let err = validate_metadata(&meta, &Registries::default()).unwrap_err();
        assert_eq!(
            err,
            VocabError::UnregisteredFieldId("not.registered".to_string())
        );
    }

    #[test]
    fn canonical_field_id_registry_defaults_empty() {
        let registry = CanonicalFieldIdRegistry::default();
        assert!(!registry.contains("anything"));
    }

    #[test]
    fn canonical_event_type_id_registry_defaults_empty() {
        let registry = CanonicalEventTypeIdRegistry::default();
        assert!(!registry.contains("anything"));
    }
}
