//! Golden integration tests for P2-D Task 9: the PURE, path-independent
//! semantic-signature feature multiset + exact weighted-Jaccard similarity
//! core (`docs/superpowers/plans/deblob-p2d-02-hermes-similarity.md`
//! §1/§2/§3/§5). Scope is strictly the pure core — NO Redis index, NO API
//! (Task 10). Reuses Task 3's already-canonicalized `SemanticMetadata`
//! directly; nothing here re-normalizes.

use deblob_core::semantic::{
    CanonicalEventTypeId, CanonicalFieldId, EpochBase, FieldEntry, FieldSemantics, MeaningCode,
    NamespaceCode, PathSegment, SemanticMetadata, Temporal, TemporalKind, TemporalResolution, Unit,
    UnitSystem,
};
use deblob_semantic::{has_anchor, semantic_signature, similarity, strength, Score, Strength};
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

fn metadata(event_type: Option<&str>, fields: Vec<FieldEntry>) -> SemanticMetadata {
    SemanticMetadata {
        event_type: event_type.map(CanonicalEventTypeId::new),
        fields,
    }
}

// ---- path-independence: pure rename (headline case) ---------------------

#[test]
fn pure_rename_same_meanings_different_paths_yields_identical_signature() {
    let a = metadata(
        Some("sensor.reading"),
        vec![field(
            vec![key("temperature")],
            FieldSemantics {
                canonical_field_id: Some(CanonicalFieldId::new("temperature.ambient")),
                unit: Some(Unit {
                    system: UnitSystem::Ucum,
                    code: "Cel".to_string(),
                }),
                ..empty_semantics()
            },
        )],
    );
    let b = metadata(
        Some("sensor.reading"),
        vec![field(
            vec![key("temp_c"), key("value")],
            FieldSemantics {
                canonical_field_id: Some(CanonicalFieldId::new("temperature.ambient")),
                unit: Some(Unit {
                    system: UnitSystem::Ucum,
                    code: "Cel".to_string(),
                }),
                ..empty_semantics()
            },
        )],
    );

    let sig_a = semantic_signature(&a);
    let sig_b = semantic_signature(&b);
    assert_eq!(
        sig_a, sig_b,
        "a pure rename (same meanings, different paths) must be path-independent"
    );
    let score = similarity(&sig_a, &sig_b);
    assert!(score.denominator > 0);
    assert_eq!(
        score.numerator, score.denominator,
        "identical signatures must score 1/1"
    );
    assert_eq!(strength(&sig_a, &sig_b), Strength::Strong);
}

// ---- compound features discriminate association (Hermes' key case) ------

fn swapped_association_metadata() -> (SemanticMetadata, SemanticMetadata) {
    let temp_field = |code: &str, system: UnitSystem| {
        field(
            vec![key("temperature")],
            FieldSemantics {
                canonical_field_id: Some(CanonicalFieldId::new("temperature.ambient")),
                unit: Some(Unit {
                    system,
                    code: code.to_string(),
                }),
                ..empty_semantics()
            },
        )
    };
    let price_field = |code: &str, system: UnitSystem| {
        field(
            vec![key("price")],
            FieldSemantics {
                canonical_field_id: Some(CanonicalFieldId::new("price.amount")),
                unit: Some(Unit {
                    system,
                    code: code.to_string(),
                }),
                ..empty_semantics()
            },
        )
    };

    let a = metadata(
        None,
        vec![
            temp_field("USD", UnitSystem::Iso4217),
            price_field("Cel", UnitSystem::Ucum),
        ],
    );
    let b = metadata(
        None,
        vec![
            temp_field("Cel", UnitSystem::Ucum),
            price_field("USD", UnitSystem::Iso4217),
        ],
    );
    (a, b)
}

#[test]
fn compound_field_unit_features_discriminate_swapped_associations() {
    let (a, b) = swapped_association_metadata();
    let sig_a = semantic_signature(&a);
    let sig_b = semantic_signature(&b);
    assert_ne!(
        sig_a, sig_b,
        "temperature:USD,price:Cel must differ from temperature:Cel,price:USD"
    );
    let score = similarity(&sig_a, &sig_b);
    assert!(
        score.numerator < score.denominator,
        "swapped field<->unit association must not score as identical"
    );
}

#[test]
fn typed_length_prefix_encoding_prevents_delimiter_collision() {
    // A naive `format!("field-unit:{cfid}:{code}")` delimiter join would
    // collide here: "x:y" + "z" -> "field-unit:x:y:z" and "x" + "y:z" ->
    // "field-unit:x:y:z" render as the exact same string. The typed
    // length-prefixed encoding (`tag||len||value||...`) must keep them
    // distinct.
    let a = metadata(
        None,
        vec![field(
            vec![key("f")],
            FieldSemantics {
                canonical_field_id: Some(CanonicalFieldId::new("x:y")),
                unit: Some(Unit {
                    system: UnitSystem::Registered,
                    code: "z".to_string(),
                }),
                ..empty_semantics()
            },
        )],
    );
    let b = metadata(
        None,
        vec![field(
            vec![key("f")],
            FieldSemantics {
                canonical_field_id: Some(CanonicalFieldId::new("x")),
                unit: Some(Unit {
                    system: UnitSystem::Registered,
                    code: "y:z".to_string(),
                }),
                ..empty_semantics()
            },
        )],
    );
    assert_ne!(semantic_signature(&a), semantic_signature(&b));
}

// ---- two unrelated schemas -> zero score, insufficient -------------------

#[test]
fn two_unrelated_schemas_score_zero_and_are_insufficient() {
    let a = metadata(
        Some("order.created"),
        vec![field(
            vec![key("id")],
            FieldSemantics {
                canonical_field_id: Some(CanonicalFieldId::new("order.id")),
                ..empty_semantics()
            },
        )],
    );
    let b = metadata(
        Some("shipment.delivered"),
        vec![field(
            vec![key("id")],
            FieldSemantics {
                canonical_field_id: Some(CanonicalFieldId::new("shipment.tracking_number")),
                ..empty_semantics()
            },
        )],
    );
    let sig_a = semantic_signature(&a);
    let sig_b = semantic_signature(&b);
    let score = similarity(&sig_a, &sig_b);
    assert_eq!(score.numerator, 0);
    assert_eq!(strength(&sig_a, &sig_b), Strength::Insufficient);
}

// ---- weights: event_type agreement outweighs a shared unit ---------------

#[test]
fn event_type_agreement_outweighs_a_shared_unit_by_weight() {
    // Pair X: shares ONLY event_type (weight 24).
    let x1 = metadata(
        Some("order.created"),
        vec![field(
            vec![key("a")],
            FieldSemantics {
                unit: Some(Unit {
                    system: UnitSystem::Ucum,
                    code: "Cel".to_string(),
                }),
                ..empty_semantics()
            },
        )],
    );
    let x2 = metadata(
        Some("order.created"),
        vec![field(
            vec![key("b")],
            FieldSemantics {
                unit: Some(Unit {
                    system: UnitSystem::Ucum,
                    code: "[degF]".to_string(),
                }),
                ..empty_semantics()
            },
        )],
    );

    // Pair Y: shares ONLY a standalone unit (weight 4), differing event types.
    let y1 = metadata(
        Some("order.created"),
        vec![field(
            vec![key("a")],
            FieldSemantics {
                unit: Some(Unit {
                    system: UnitSystem::Ucum,
                    code: "Cel".to_string(),
                }),
                ..empty_semantics()
            },
        )],
    );
    let y2 = metadata(
        Some("shipment.delivered"),
        vec![field(
            vec![key("a")],
            FieldSemantics {
                unit: Some(Unit {
                    system: UnitSystem::Ucum,
                    code: "Cel".to_string(),
                }),
                ..empty_semantics()
            },
        )],
    );

    let score_x = similarity(&semantic_signature(&x1), &semantic_signature(&x2));
    let score_y = similarity(&semantic_signature(&y1), &semantic_signature(&y2));

    assert_eq!(
        score_x.cmp_rank(&score_y),
        std::cmp::Ordering::Greater,
        "event_type-only overlap (w=24) must outrank unit-only overlap (w=4)"
    );
}

// ---- determinism: input-order independence -------------------------------

#[test]
fn determinism_is_independent_of_field_entry_input_order() {
    let f1 = field(
        vec![key("a")],
        FieldSemantics {
            canonical_field_id: Some(CanonicalFieldId::new("field.a")),
            ..empty_semantics()
        },
    );
    let f2 = field(
        vec![key("b")],
        FieldSemantics {
            canonical_field_id: Some(CanonicalFieldId::new("field.b")),
            ..empty_semantics()
        },
    );
    let f3 = field(
        vec![key("c")],
        FieldSemantics {
            canonical_field_id: Some(CanonicalFieldId::new("field.c")),
            ..empty_semantics()
        },
    );

    let order_1 = metadata(Some("evt"), vec![f1.clone(), f2.clone(), f3.clone()]);
    let order_2 = metadata(Some("evt"), vec![f3.clone(), f1.clone(), f2.clone()]);
    let order_3 = metadata(Some("evt"), vec![f2, f3, f1]);

    let sig_1 = semantic_signature(&order_1);
    assert_eq!(sig_1.to_bytes(), semantic_signature(&order_2).to_bytes());
    assert_eq!(sig_1.to_bytes(), semantic_signature(&order_3).to_bytes());
}

// ---- no-anchor signature -> insufficient ---------------------------------

#[test]
fn no_anchor_signature_is_insufficient() {
    let a = metadata(
        None,
        vec![field(
            vec![key("t")],
            FieldSemantics {
                temporal: Some(Temporal {
                    kind: Some(TemporalKind::Instant),
                    epoch: Some(EpochBase::Unix),
                    resolution: Some(TemporalResolution::S),
                }),
                ..empty_semantics()
            },
        )],
    );
    let b = metadata(
        None,
        vec![field(
            vec![key("t2")],
            FieldSemantics {
                temporal: Some(Temporal {
                    kind: Some(TemporalKind::Instant),
                    epoch: Some(EpochBase::Unix),
                    resolution: Some(TemporalResolution::S),
                }),
                ..empty_semantics()
            },
        )],
    );
    let sig_a = semantic_signature(&a);
    let sig_b = semantic_signature(&b);
    assert!(!has_anchor(&sig_a));
    assert!(!has_anchor(&sig_b));
    assert_eq!(strength(&sig_a, &sig_b), Strength::Insufficient);
}

// ---- count-cap caps at 4 --------------------------------------------------

#[test]
fn repeated_feature_is_capped_at_four() {
    let mut enum_semantics = BTreeMap::new();
    enum_semantics.insert(
        "A".to_string(),
        MeaningCode {
            vocabulary: "deblob/order-status/v1".to_string(),
            code: "pending".to_string(),
        },
    );

    // 6 distinct fields all carrying the SAME enum meaning code: the
    // standalone `enum-meaning:` feature is field-independent, so all 6
    // collapse into one counted feature key, capped at 4.
    let fields_6: Vec<FieldEntry> = (0..6)
        .map(|i| {
            field(
                vec![key(&format!("f{i}"))],
                FieldSemantics {
                    enum_semantics: Some(enum_semantics.clone()),
                    ..empty_semantics()
                },
            )
        })
        .collect();
    let fields_4: Vec<FieldEntry> = (0..4)
        .map(|i| {
            field(
                vec![key(&format!("f{i}"))],
                FieldSemantics {
                    enum_semantics: Some(enum_semantics.clone()),
                    ..empty_semantics()
                },
            )
        })
        .collect();

    let sig_6 = semantic_signature(&metadata(None, fields_6));
    let sig_4 = semantic_signature(&metadata(None, fields_4));
    assert_eq!(
        sig_6.to_bytes(),
        sig_4.to_bytes(),
        "6 occurrences must cap to the same effective count as exactly 4"
    );
}

// ---- differing event_types caps strength at Medium ------------------------

#[test]
fn differing_event_types_caps_strength_at_medium_even_with_many_shared_fields() {
    let shared_fields = |n: usize| -> Vec<FieldEntry> {
        (0..n)
            .map(|i| {
                field(
                    vec![key(&format!("f{i}"))],
                    FieldSemantics {
                        canonical_field_id: Some(CanonicalFieldId::new(format!(
                            "shared.field.{i}"
                        ))),
                        ..empty_semantics()
                    },
                )
            })
            .collect()
    };
    let a = metadata(Some("order.created"), shared_fields(4));
    let b = metadata(Some("order.updated"), shared_fields(4));
    let sig_a = semantic_signature(&a);
    let sig_b = semantic_signature(&b);
    // Without the differing-event-types cap this would be Strong (4 shared
    // canonical_field_id, 100% coverage).
    assert_eq!(strength(&sig_a, &sig_b), Strength::Medium);
}

// ---- u128 cross-multiplication ranking (no float) -------------------------

#[test]
fn u128_cross_multiplication_ranking_matches_exact_rational_order() {
    let a = Score {
        numerator: 1,
        denominator: 3,
    };
    // 333333333333333333 / 1000000000000000000 is exactly a hair below 1/3.
    let b = Score {
        numerator: 333_333_333_333_333_333,
        denominator: 1_000_000_000_000_000_000,
    };
    assert_eq!(a.cmp_rank(&b), std::cmp::Ordering::Greater);
    assert_eq!(b.cmp_rank(&a), std::cmp::Ordering::Less);

    // Large-magnitude values near u64::MAX: a naive u64 cross-multiply would
    // overflow; the u128 widening must still produce the correct order.
    let big_1 = Score {
        numerator: u64::MAX - 1,
        denominator: u64::MAX,
    };
    let big_2 = Score {
        numerator: u64::MAX - 2,
        denominator: u64::MAX,
    };
    assert_eq!(big_1.cmp_rank(&big_2), std::cmp::Ordering::Greater);
    assert_eq!(
        big_1.cmp_rank(&big_1.clone()),
        std::cmp::Ordering::Equal,
        "a score must rank equal to itself"
    );
}

// ---- namespace not authoritative: unrelated cfid/event still low ---------

#[test]
fn shared_namespace_alone_without_another_feature_is_not_medium() {
    let a = metadata(
        None,
        vec![field(
            vec![key("a")],
            FieldSemantics {
                identifier_namespace: Some(NamespaceCode::new("acme.customer_id")),
                ..empty_semantics()
            },
        )],
    );
    let b = metadata(
        None,
        vec![field(
            vec![key("b")],
            FieldSemantics {
                identifier_namespace: Some(NamespaceCode::new("acme.customer_id")),
                ..empty_semantics()
            },
        )],
    );
    let sig_a = semantic_signature(&a);
    let sig_b = semantic_signature(&b);
    // Anchors present (namespace) on both sides, but nothing else overlaps
    // (a shared namespace alone is not one of the "units / temporal kinds /
    // enum vocab+codes" weak-tier feature classes either), so this must
    // land on Insufficient — NOT Medium ("shared namespace + another
    // feature" requires that *other* feature) and NOT Weak.
    assert_eq!(strength(&sig_a, &sig_b), Strength::Insufficient);
}
