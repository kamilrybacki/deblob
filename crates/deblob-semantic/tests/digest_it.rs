//! Golden integration tests for the P2-D Task 3 identity core: the
//! byte-level canonical protocol (`canon.rs`) plus the `sem_` digest
//! (`digest.rs`). Per `task-3-brief.md`: determinism, sensitivity,
//! typed-path anti-ambiguity, duplicate-path rejection, the `None`
//! sentinel-free empty case, and `sch_`/`sem_` domain separation are all
//! load-bearing and covered here explicitly (not just via the crate's
//! internal unit tests).

use deblob_core::id::{CandidateId, SchemaId};
use deblob_core::semantic::{
    CanonicalEventTypeId, CanonicalFieldId, EpochBase, FieldEntry, FieldSemantics, MeaningCode,
    NamespaceCode, PathSegment, SemanticMetadata, Temporal, TemporalKind, TemporalResolution, Unit,
    UnitSystem,
};
use deblob_semantic::{canonical_semantic_bytes, semantic_fingerprint, CanonError};
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

/// A metadata carrying at least one instance of every axis, used as the
/// baseline for the single-attribute-change sensitivity tests below.
fn baseline() -> SemanticMetadata {
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
        fields: vec![FieldEntry {
            path: vec![key("temperature")],
            semantics: FieldSemantics {
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
                    resolution: Some(TemporalResolution::S),
                }),
                enum_semantics: Some(enum_semantics),
            },
        }],
    }
}

fn fp(meta: &SemanticMetadata) -> deblob_core::semantic::SemanticFingerprint {
    semantic_fingerprint(meta).unwrap().unwrap()
}

// ---- determinism ----------------------------------------------------

#[test]
fn same_metadata_hashes_identically_across_repeated_calls() {
    let meta = baseline();
    assert_eq!(fp(&meta), fp(&meta));
}

#[test]
fn determinism_is_independent_of_field_entry_input_order() {
    let field_a = FieldEntry {
        path: vec![key("alpha")],
        semantics: FieldSemantics {
            numeric_scale: Some(1),
            ..empty_semantics()
        },
    };
    let field_b = FieldEntry {
        path: vec![key("beta")],
        semantics: FieldSemantics {
            numeric_scale: Some(2),
            ..empty_semantics()
        },
    };
    let field_c = FieldEntry {
        path: vec![key("gamma")],
        semantics: FieldSemantics {
            numeric_scale: Some(3),
            ..empty_semantics()
        },
    };

    let order_1 = SemanticMetadata {
        event_type: None,
        fields: vec![field_a.clone(), field_b.clone(), field_c.clone()],
    };
    let order_2 = SemanticMetadata {
        event_type: None,
        fields: vec![field_c.clone(), field_a.clone(), field_b.clone()],
    };
    let order_3 = SemanticMetadata {
        event_type: None,
        fields: vec![field_b, field_c, field_a],
    };

    let a = fp(&order_1);
    assert_eq!(a, fp(&order_2));
    assert_eq!(a, fp(&order_3));
}

// ---- sensitivity: unit change (headline case from the brief) ---------

#[test]
fn unit_code_cel_to_degf_changes_the_fingerprint() {
    let mut changed = baseline();
    changed.fields[0].semantics.unit = Some(Unit {
        system: UnitSystem::Ucum,
        code: "[degF]".to_string(),
    });
    assert_ne!(fp(&baseline()), fp(&changed));
}

// ---- sensitivity: every other single-attribute change ------------------

#[test]
fn identifier_namespace_change_changes_the_fingerprint() {
    let mut changed = baseline();
    changed.fields[0].semantics.identifier_namespace = Some(NamespaceCode::new("acme.order_id"));
    assert_ne!(fp(&baseline()), fp(&changed));
}

#[test]
fn event_type_change_changes_the_fingerprint() {
    let mut changed = baseline();
    changed.event_type = Some(CanonicalEventTypeId::new("user.deleted"));
    assert_ne!(fp(&baseline()), fp(&changed));
}

#[test]
fn numeric_scale_change_changes_the_fingerprint() {
    let mut changed = baseline();
    changed.fields[0].semantics.numeric_scale = Some(3);
    assert_ne!(fp(&baseline()), fp(&changed));
}

#[test]
fn single_enum_meaning_entry_change_changes_the_fingerprint() {
    let mut changed = baseline();
    let mut enum_semantics = BTreeMap::new();
    enum_semantics.insert(
        "ACTIVE".to_string(),
        MeaningCode {
            vocabulary: "deblob/order-status/v1".to_string(),
            // Only the code differs from baseline's "pending".
            code: "confirmed".to_string(),
        },
    );
    changed.fields[0].semantics.enum_semantics = Some(enum_semantics);
    assert_ne!(fp(&baseline()), fp(&changed));
}

#[test]
fn path_segment_change_changes_the_fingerprint() {
    let mut changed = baseline();
    changed.fields[0].path = vec![key("temperature_2")];
    assert_ne!(fp(&baseline()), fp(&changed));
}

#[test]
fn temporal_resolution_seconds_to_milliseconds_changes_the_fingerprint() {
    let mut changed = baseline();
    changed.fields[0].semantics.temporal = Some(Temporal {
        kind: Some(TemporalKind::Instant),
        epoch: Some(EpochBase::Unix),
        resolution: Some(TemporalResolution::Ms),
    });
    assert_ne!(fp(&baseline()), fp(&changed));
}

// ---- typed-path anti-ambiguity ----------------------------------------

#[test]
fn literal_dotted_key_differs_from_two_segment_path() {
    let one_segment = SemanticMetadata {
        event_type: None,
        fields: vec![FieldEntry {
            path: vec![key("a.b")],
            semantics: FieldSemantics {
                numeric_scale: Some(1),
                ..empty_semantics()
            },
        }],
    };
    let two_segments = SemanticMetadata {
        event_type: None,
        fields: vec![FieldEntry {
            path: vec![key("a"), key("b")],
            semantics: FieldSemantics {
                numeric_scale: Some(1),
                ..empty_semantics()
            },
        }],
    };
    assert_ne!(fp(&one_segment), fp(&two_segments));
    assert_ne!(
        canonical_semantic_bytes(&one_segment).unwrap(),
        canonical_semantic_bytes(&two_segments).unwrap()
    );
}

// ---- duplicate normalized paths -> error --------------------------------

#[test]
fn duplicate_normalized_paths_are_rejected() {
    let meta = SemanticMetadata {
        event_type: None,
        fields: vec![
            FieldEntry {
                path: vec![key("dup")],
                semantics: FieldSemantics {
                    numeric_scale: Some(1),
                    ..empty_semantics()
                },
            },
            FieldEntry {
                path: vec![key("dup")],
                semantics: FieldSemantics {
                    numeric_scale: Some(2),
                    ..empty_semantics()
                },
            },
        ],
    };
    assert!(matches!(
        semantic_fingerprint(&meta),
        Err(CanonError::DuplicatePath { .. })
    ));
    assert!(matches!(
        canonical_semantic_bytes(&meta),
        Err(CanonError::DuplicatePath { .. })
    ));
}

#[test]
fn duplicate_detection_applies_after_nfc_normalization() {
    // "e\u{0301}" (e + combining acute, NFD) and "\u{e9}" (precomposed é,
    // NFC) are different byte sequences pre-normalization but the same
    // canonical path once NFC-normalized — duplicate detection must catch
    // this, not just byte-identical raw keys.
    let meta = SemanticMetadata {
        event_type: None,
        fields: vec![
            FieldEntry {
                path: vec![key("e\u{0301}")],
                semantics: FieldSemantics {
                    numeric_scale: Some(1),
                    ..empty_semantics()
                },
            },
            FieldEntry {
                path: vec![key("\u{e9}")],
                semantics: FieldSemantics {
                    numeric_scale: Some(2),
                    ..empty_semantics()
                },
            },
        ],
    };
    assert!(matches!(
        semantic_fingerprint(&meta),
        Err(CanonError::DuplicatePath { .. })
    ));
}

// ---- None: no surviving assertion -----------------------------------

#[test]
fn metadata_with_no_surviving_assertion_is_none_not_a_sentinel() {
    let empty = SemanticMetadata {
        event_type: None,
        fields: vec![],
    };
    assert_eq!(semantic_fingerprint(&empty).unwrap(), None);

    // Same conclusion even when field entries are present but every one of
    // them carries zero surviving attributes (a Temporal that's all-None,
    // an empty enum_semantics map): they're removed entirely, so this is
    // still the empty case.
    let looks_populated_but_isnt = SemanticMetadata {
        event_type: None,
        fields: vec![
            FieldEntry {
                path: vec![key("a")],
                semantics: empty_semantics(),
            },
            FieldEntry {
                path: vec![key("b")],
                semantics: FieldSemantics {
                    temporal: Some(Temporal {
                        kind: None,
                        epoch: None,
                        resolution: None,
                    }),
                    enum_semantics: Some(BTreeMap::new()),
                    ..empty_semantics()
                },
            },
        ],
    };
    assert_eq!(
        semantic_fingerprint(&looks_populated_but_isnt).unwrap(),
        None
    );

    // Two different "empty" metadata values must not accidentally compare
    // equal via some shared sentinel — there is no Some(_) to compare at
    // all, which is the point: neither produces a `sem_`.
    assert_eq!(semantic_fingerprint(&empty).unwrap(), None);
}

// ---- domain separation: sem_ never parses as sch_/cand_ -----------------

#[test]
fn sem_id_never_parses_as_a_schema_or_candidate_id() {
    let id = fp(&baseline());
    let sem_str = id.0.as_str();
    assert!(sem_str.starts_with("sem_"));
    assert!(SchemaId::parse(sem_str).is_err());
    assert!(CandidateId::parse(sem_str).is_err());
}

// ---- sch_ is never part of the preimage ---------------------------------

#[test]
fn identical_semantics_hash_the_same_regardless_of_any_structural_context() {
    // SemanticMetadata carries no sch_/schema-id field at all, so this
    // holds by construction: nothing in this crate's API even accepts a
    // structural identity as an input to hashing. Demonstrate it at the
    // call boundary — two independently constructed (but semantically
    // identical) metadata values, standing in for "the same semantic
    // assertion recorded against two different physical schemas", hash
    // identically.
    let schema_a_semantics = baseline();
    let schema_b_semantics = baseline(); // a different sch_ would carry this exact same SemanticMetadata
    assert_eq!(fp(&schema_a_semantics), fp(&schema_b_semantics));
}
