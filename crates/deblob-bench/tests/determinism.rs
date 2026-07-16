//! Determinism contract: the same `SyntheticConfig` (same seed included)
//! must produce a byte-identical stream, every time.

use deblob_bench::{generate, PayloadSize, SyntheticConfig};

fn full_config(seed: u64) -> SyntheticConfig {
    SyntheticConfig {
        seed,
        distinct_schemas: 25,
        optional_field_churn: 0.3,
        drift_rate: 0.15,
        malformed_pct: 0.1,
        payload_bytes: PayloadSize::Medium,
        count: 400,
    }
}

#[test]
fn same_seed_and_config_produce_byte_identical_streams() {
    let cfg = full_config(7);
    let run_a: Vec<Vec<u8>> = generate(&cfg).map(|r| r.bytes).collect();
    let run_b: Vec<Vec<u8>> = generate(&cfg).map(|r| r.bytes).collect();
    assert_eq!(run_a.len(), cfg.count);
    assert_eq!(run_a, run_b);
}

#[test]
fn same_seed_and_config_produce_identical_expected_labels() {
    let cfg = full_config(99);
    let labels_a: Vec<_> = generate(&cfg).map(|r| r.expected).collect();
    let labels_b: Vec<_> = generate(&cfg).map(|r| r.expected).collect();
    assert_eq!(labels_a, labels_b);
}

#[test]
fn different_seeds_diverge() {
    let a: Vec<Vec<u8>> = generate(&full_config(1)).map(|r| r.bytes).collect();
    let b: Vec<Vec<u8>> = generate(&full_config(2)).map(|r| r.bytes).collect();
    assert_ne!(
        a, b,
        "different seeds should not coincidentally agree on 400 records"
    );
}

#[test]
fn real_world_stream_is_deterministic_per_seed() {
    use deblob_bench::{real_world_stream, RealWorldKind};
    let kinds = [
        RealWorldKind::GitHubWebhook,
        RealWorldKind::K8sEvent,
        RealWorldKind::CloudEvent,
    ];
    let a: Vec<Vec<u8>> = real_world_stream(&kinds, 30, 5).map(|r| r.bytes).collect();
    let b: Vec<Vec<u8>> = real_world_stream(&kinds, 30, 5).map(|r| r.bytes).collect();
    assert_eq!(a, b);
}
