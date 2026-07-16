//! Knobs for the synthetic stream generator. Spec `docs/superpowers/specs/
//! 2026-07-16-deblob-k3s-benchmark.md` §3.1.

/// Target serialized-payload size class. Filler fields pad a record's
/// realistic base fields up to (roughly) the target byte count; the target
/// is approximate, not exact, so it stays cheap to hit deterministically.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadSize {
    /// ~200 bytes.
    Small,
    /// ~2 KB.
    Medium,
    /// ~20 KB.
    Large,
}

impl PayloadSize {
    /// Approximate target size in bytes used by the padding pass.
    pub fn target_bytes(self) -> usize {
        match self {
            PayloadSize::Small => 200,
            PayloadSize::Medium => 2_000,
            PayloadSize::Large => 20_000,
        }
    }
}

/// Parameters controlling one synthetic stream. Two calls to [`crate::generate`]
/// with equal `SyntheticConfig` values (including `seed`) MUST produce a
/// byte-identical stream.
#[derive(Debug, Clone, PartialEq)]
pub struct SyntheticConfig {
    /// Seeds the deterministic RNG driving every random choice in the
    /// stream. Same seed + same config ⇒ same bytes, always.
    pub seed: u64,
    /// Number of distinct base JSON shapes ("schema families") the
    /// generator draws from. Each family has a structurally unique set of
    /// required fields, so well-formed, unchurned, undrifted records from
    /// `distinct_schemas` families produce exactly `distinct_schemas`
    /// distinct `deblob-canon-v1` fingerprints.
    pub distinct_schemas: usize,
    /// Probability (`0.0..=1.0`) that a well-formed record adds/drops one
    /// of its family's known-optional fields, producing the "same family,
    /// optional-field subset" clustering pattern.
    pub optional_field_churn: f64,
    /// Probability (`0.0..=1.0`) that a record is a compatible-drift
    /// variant of its family (an added, previously-unseen optional field,
    /// or a widened type) rather than a plain well-formed record.
    pub drift_rate: f64,
    /// Probability (`0.0..=1.0`) that a record is malformed (duplicate
    /// JSON key, a `NaN` literal, or a truncated body) and expected to hit
    /// Deblob's quarantine path.
    pub malformed_pct: f64,
    /// Target serialized size class.
    pub payload_bytes: PayloadSize,
    /// Total number of records to emit.
    pub count: usize,
}

impl SyntheticConfig {
    /// A small, fast, deterministic default useful for tests: 10 families,
    /// no churn/drift/malformed, small payloads.
    pub fn minimal(seed: u64, count: usize) -> Self {
        Self {
            seed,
            distinct_schemas: 10,
            optional_field_churn: 0.0,
            drift_rate: 0.0,
            malformed_pct: 0.0,
            payload_bytes: PayloadSize::Small,
            count,
        }
    }
}
