//! Proves the generator's `expected` label actually matches what the real
//! Deblob parser (`deblob_fingerprint::parse::parse_bounded`) does with the
//! bytes: well-formed/drifted records parse, malformed records are
//! rejected. This is what makes the generator trustworthy as a benchmark
//! input — the bench exercises the real parser, not a re-implementation.

use deblob_bench::{
    all_fixtures, generate, real_world_stream, PayloadSize, RealWorldKind, RecordKind,
    SyntheticConfig,
};
use deblob_fingerprint::{parse_bounded, Limits};

#[test]
fn well_formed_record_parses_via_the_real_parser() {
    let cfg = SyntheticConfig::minimal(1, 1);
    let record = generate(&cfg).next().expect("one record");
    assert!(matches!(record.expected, RecordKind::WellFormed { .. }));
    parse_bounded(&record.bytes, &Limits::default())
        .expect("a record labeled WellFormed must parse cleanly");
}

#[test]
fn malformed_record_is_rejected_by_the_real_parser() {
    let cfg = SyntheticConfig {
        seed: 55,
        distinct_schemas: 5,
        optional_field_churn: 0.0,
        drift_rate: 0.0,
        malformed_pct: 1.0,
        payload_bytes: PayloadSize::Small,
        count: 10,
    };
    for record in generate(&cfg) {
        assert!(matches!(record.expected, RecordKind::Malformed));
        assert!(
            parse_bounded(&record.bytes, &Limits::default()).is_err(),
            "a record labeled Malformed must be rejected: {:?}",
            String::from_utf8_lossy(&record.bytes)
        );
    }
}

#[test]
fn drifted_record_still_parses_cleanly() {
    let cfg = SyntheticConfig {
        seed: 77,
        distinct_schemas: 8,
        optional_field_churn: 0.0,
        drift_rate: 1.0,
        malformed_pct: 0.0,
        payload_bytes: PayloadSize::Small,
        count: 20,
    };
    for record in generate(&cfg) {
        assert!(matches!(record.expected, RecordKind::Drifted { .. }));
        parse_bounded(&record.bytes, &Limits::default())
            .expect("a compatible-drift record must still parse (it is structurally valid JSON)");
    }
}

#[test]
fn every_embedded_real_world_fixture_parses_via_the_real_parser() {
    for text in all_fixtures() {
        parse_bounded(text.as_bytes(), &Limits::default())
            .unwrap_or_else(|e| panic!("fixture failed to parse ({e:?}): {text}"));
    }
}

#[test]
fn real_world_stream_records_all_parse() {
    let kinds = [
        RealWorldKind::GitHubWebhook,
        RealWorldKind::K8sEvent,
        RealWorldKind::CloudEvent,
    ];
    for record in real_world_stream(&kinds, 15, 3) {
        assert!(matches!(record.expected, RecordKind::WellFormed { .. }));
        parse_bounded(&record.bytes, &Limits::default())
            .expect("fixture-derived record must parse");
    }
}

#[test]
fn large_payload_class_actually_produces_larger_records() {
    let small = SyntheticConfig {
        payload_bytes: PayloadSize::Small,
        ..SyntheticConfig::minimal(3, 50)
    };
    let large = SyntheticConfig {
        payload_bytes: PayloadSize::Large,
        ..SyntheticConfig::minimal(3, 50)
    };
    let small_avg: usize = generate(&small).map(|r| r.bytes.len()).sum::<usize>() / small.count;
    let large_avg: usize = generate(&large).map(|r| r.bytes.len()).sum::<usize>() / large.count;
    assert!(
        small_avg < 500,
        "small payloads should stay near ~200B, got avg {small_avg}"
    );
    assert!(
        large_avg > 15_000,
        "large payloads should approach ~20KB, got avg {large_avg}"
    );
}
