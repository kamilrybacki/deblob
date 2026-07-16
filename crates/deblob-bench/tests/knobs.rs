//! The generator's knobs actually move the output: `distinct_schemas`
//! controls the number of distinct `deblob-canon-v1` fingerprints produced,
//! and `malformed_pct` controls the fraction of records the real Deblob
//! parser rejects.

use std::collections::HashSet;

use deblob_bench::{generate, PayloadSize, RecordKind, SyntheticConfig};
use deblob_fingerprint::{fingerprint, parse_bounded, shape_of, Limits};

#[test]
fn distinct_schemas_yields_that_many_distinct_fingerprints() {
    // No churn/drift/malformed noise: every record is a plain well-formed
    // member of its family, so the fingerprint count should equal
    // `distinct_schemas` exactly (each family's field set is unique by
    // construction — see crates/deblob-bench/src/schema.rs).
    let cfg = SyntheticConfig {
        seed: 42,
        distinct_schemas: 100,
        optional_field_churn: 0.0,
        drift_rate: 0.0,
        malformed_pct: 0.0,
        payload_bytes: PayloadSize::Small,
        // Comfortably more records than families so every family is very
        // likely sampled at least once.
        count: 4_000,
    };

    let limits = Limits::default();
    let mut fingerprints: HashSet<[u8; 32]> = HashSet::new();
    let mut families_seen: HashSet<usize> = HashSet::new();

    for record in generate(&cfg) {
        let RecordKind::WellFormed { schema_family } = record.expected else {
            panic!("malformed_pct/drift_rate are 0.0, every record must be WellFormed");
        };
        families_seen.insert(schema_family);
        let node = parse_bounded(&record.bytes, &limits).expect("well-formed record must parse");
        let shape = shape_of(&node);
        fingerprints.insert(fingerprint(&shape));
    }

    assert_eq!(
        families_seen.len(),
        100,
        "expected all 100 families to be sampled across 4000 draws"
    );
    assert_eq!(
        fingerprints.len(),
        100,
        "distinct_schemas=100 with no churn/drift/malformed noise must yield exactly 100 distinct fingerprints"
    );
}

#[test]
fn malformed_pct_controls_the_rejected_fraction() {
    let cfg = SyntheticConfig {
        seed: 123,
        distinct_schemas: 20,
        optional_field_churn: 0.2,
        drift_rate: 0.1,
        malformed_pct: 0.35,
        payload_bytes: PayloadSize::Small,
        count: 5_000,
    };
    let limits = Limits::default();

    let mut rejected = 0usize;
    let mut labeled_malformed = 0usize;
    let total = cfg.count;

    for record in generate(&cfg) {
        let is_labeled_malformed = matches!(record.expected, RecordKind::Malformed);
        if is_labeled_malformed {
            labeled_malformed += 1;
        }
        let parse_result = parse_bounded(&record.bytes, &limits);
        match (is_labeled_malformed, parse_result.is_err()) {
            (true, true) => rejected += 1,
            (true, false) => panic!("record labeled Malformed parsed successfully: {record:?}"),
            (false, true) => panic!(
                "record NOT labeled Malformed was rejected by the parser: {:?}",
                parse_result
            ),
            (false, false) => {}
        }
    }

    let observed_pct = labeled_malformed as f64 / total as f64;
    assert!(
        (observed_pct - cfg.malformed_pct).abs() < 0.05,
        "observed malformed fraction {observed_pct} too far from configured {}",
        cfg.malformed_pct
    );
    // Every record labeled Malformed was, in fact, rejected by the real
    // parser (proven inside the loop above); this just restates the count.
    assert_eq!(rejected, labeled_malformed);
}

#[test]
fn zero_malformed_pct_never_produces_a_malformed_record() {
    let cfg = SyntheticConfig {
        seed: 8,
        distinct_schemas: 10,
        optional_field_churn: 0.5,
        drift_rate: 0.5,
        malformed_pct: 0.0,
        payload_bytes: PayloadSize::Medium,
        count: 1_000,
    };
    assert!(!generate(&cfg).any(|r| matches!(r.expected, RecordKind::Malformed)));
}
