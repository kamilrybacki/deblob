//! The three prequential datasets (spec §7): "round stream (chronological
//! test-then-train), development set (hyperparams/replay-ratios/promotion
//! thresholds), sealed final audit set (unseen until every model/round/
//! analysis choice frozen)". Each is an independent, deterministic
//! `deblob_eval::generate_corpus` run (distinct seed offset + distinct
//! family/variant counts) — never wall-clock- or HashMap-order-dependent.

use deblob_eval::{generate_corpus, EvalCase, GenerateConfig};

use super::prequential::PrequentialError;

const DEV_SEED_OFFSET: u64 = 0;
const ROUND_STREAM_SEED_OFFSET: u64 = 1;
const AUDIT_SEED_OFFSET: u64 = 2;

/// Sizing knobs for the three prequential datasets.
#[derive(Debug, Clone)]
pub struct PrequentialConfig {
    pub seed: u64,
    pub num_rounds: usize,
    pub round_batch_size: usize,
    pub round_stream_families: usize,
    pub round_stream_variants_per_family: usize,
    pub dev_families: usize,
    pub dev_variants_per_family: usize,
    pub audit_families: usize,
    pub audit_variants_per_family: usize,
}

impl Default for PrequentialConfig {
    fn default() -> Self {
        Self {
            seed: 100,
            num_rounds: 3,
            round_batch_size: 6,
            round_stream_families: 6,
            round_stream_variants_per_family: 8,
            dev_families: 8,
            dev_variants_per_family: 8,
            audit_families: 6,
            audit_variants_per_family: 8,
        }
    }
}

/// The sealed final audit set (spec §7): "unseen until every model/round/
/// analysis choice frozen". `cases` is `pub(super)` — reachable ONLY from
/// within `crate::continual` (specifically, `prequential::PrequentialRunner
/// ::freeze`), never from outside this crate's `continual` module, and
/// never before every round has run.
pub(super) struct SealedAuditSet {
    pub(super) cases: Vec<EvalCase>,
}

pub(super) fn round_batches(
    cfg: &PrequentialConfig,
) -> Result<Vec<Vec<EvalCase>>, PrequentialError> {
    let cases = generate_corpus(&GenerateConfig {
        families: cfg.round_stream_families,
        variants_per_family: cfg.round_stream_variants_per_family,
        seed: cfg.seed.wrapping_add(ROUND_STREAM_SEED_OFFSET),
    })
    .cases;
    let needed = cfg.num_rounds * cfg.round_batch_size;
    if cases.len() < needed {
        return Err(PrequentialError::InsufficientRoundStream {
            needed,
            available: cases.len(),
        });
    }
    Ok(cases
        .chunks(cfg.round_batch_size)
        .take(cfg.num_rounds)
        .map(|c| c.to_vec())
        .collect())
}

pub(super) fn dev_corpus(cfg: &PrequentialConfig) -> Vec<EvalCase> {
    generate_corpus(&GenerateConfig {
        families: cfg.dev_families,
        variants_per_family: cfg.dev_variants_per_family,
        seed: cfg.seed.wrapping_add(DEV_SEED_OFFSET),
    })
    .cases
}

pub(super) fn audit_set(cfg: &PrequentialConfig) -> SealedAuditSet {
    let cases = generate_corpus(&GenerateConfig {
        families: cfg.audit_families,
        variants_per_family: cfg.audit_variants_per_family,
        seed: cfg.seed.wrapping_add(AUDIT_SEED_OFFSET),
    })
    .cases;
    SealedAuditSet { cases }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> PrequentialConfig {
        PrequentialConfig {
            seed: 5,
            num_rounds: 2,
            round_batch_size: 4,
            round_stream_families: 4,
            round_stream_variants_per_family: 8,
            dev_families: 4,
            dev_variants_per_family: 8,
            audit_families: 4,
            audit_variants_per_family: 8,
        }
    }

    #[test]
    fn round_batches_are_deterministic_and_correctly_sized() {
        let cfg = cfg();
        let a = round_batches(&cfg).unwrap();
        let b = round_batches(&cfg).unwrap();
        assert_eq!(a.len(), cfg.num_rounds);
        for batch in &a {
            assert_eq!(batch.len(), cfg.round_batch_size);
        }
        let a_json = serde_json::to_string(&a.iter().flatten().collect::<Vec<_>>()).unwrap();
        let b_json = serde_json::to_string(&b.iter().flatten().collect::<Vec<_>>()).unwrap();
        assert_eq!(a_json, b_json);
    }

    #[test]
    fn insufficient_round_stream_is_a_clean_error_not_a_panic() {
        let mut cfg = cfg();
        cfg.num_rounds = 100;
        let err = round_batches(&cfg).unwrap_err();
        assert!(matches!(
            err,
            PrequentialError::InsufficientRoundStream { .. }
        ));
    }

    #[test]
    fn dev_round_stream_and_audit_corpora_are_distinct() {
        // NOTE: `EvalCase::name` is purely index-derived
        // (`gen_{family:03}_{slot:02}_{suffix}`) and does NOT depend on the
        // generator seed — only the STRUCTURAL CONTENT (sampled field
        // values, hence schema fingerprints/ids) does. So this asserts on
        // full serialized content, not names, to actually exercise the
        // seed-offset distinctness the three datasets rely on.
        let cfg = cfg();
        let dev = dev_corpus(&cfg);
        let round: Vec<EvalCase> = round_batches(&cfg).unwrap().into_iter().flatten().collect();
        let audit = audit_set(&cfg).cases;
        let dev_json = serde_json::to_string(&dev).unwrap();
        let round_json = serde_json::to_string(&round).unwrap();
        let audit_json = serde_json::to_string(&audit).unwrap();
        assert_ne!(dev_json, round_json);
        assert_ne!(dev_json, audit_json);
        assert_ne!(round_json, audit_json);
    }
}
