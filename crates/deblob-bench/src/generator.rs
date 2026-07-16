//! The seeded synthetic-stream generator itself: ties the schema pool,
//! churn/drift/malformed knobs, and payload padding together into an
//! `Iterator<Item = GeneratedRecord>`.

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde_json::Value;

use crate::config::SyntheticConfig;
use crate::fields::{CHURN_POOL, DRIFT_NOVEL_FIELDS, SIGNATURE_POOL};
use crate::malform;
use crate::padding::pad_to_target;
use crate::record::{GeneratedRecord, RecordKind};
use crate::schema::schema_for;

/// Build a deterministic synthetic stream from `cfg`. Every random choice
/// (schema family, malformed/drift rolls, churn rolls, malform strategy)
/// is drawn from a single `ChaCha8Rng` seeded with `cfg.seed`, in a fixed
/// order per record — so `generate(cfg)` called twice with an equal `cfg`
/// produces byte-identical records, every time.
pub fn generate(cfg: &SyntheticConfig) -> SyntheticGenerator {
    SyntheticGenerator {
        cfg: cfg.clone(),
        rng: ChaCha8Rng::seed_from_u64(cfg.seed),
        emitted: 0,
    }
}

/// Iterator returned by [`generate`]. See [`generate`] for the determinism
/// contract.
pub struct SyntheticGenerator {
    cfg: SyntheticConfig,
    rng: ChaCha8Rng,
    emitted: usize,
}

impl Iterator for SyntheticGenerator {
    type Item = GeneratedRecord;

    fn next(&mut self) -> Option<GeneratedRecord> {
        if self.emitted >= self.cfg.count {
            return None;
        }
        let record = self.generate_one();
        self.emitted += 1;
        Some(record)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.cfg.count.saturating_sub(self.emitted);
        (remaining, Some(remaining))
    }
}

impl SyntheticGenerator {
    fn generate_one(&mut self) -> GeneratedRecord {
        let distinct = self.cfg.distinct_schemas.max(1);
        let family = self.rng.gen_range(0..distinct);
        let schema = schema_for(family);
        let seed_hint = self.emitted;

        let is_malformed = self.rng.gen::<f64>() < self.cfg.malformed_pct;
        let is_drifted = !is_malformed && self.rng.gen::<f64>() < self.cfg.drift_rate;

        let mut obj = schema.base_object(seed_hint);

        // Churn: each churn-pool field is independently added with
        // probability `optional_field_churn`, regardless of malformed/
        // drift status, so the roll sequence never depends on those
        // branches (keeps the RNG call order simple to reason about).
        for f in CHURN_POOL {
            if self.rng.gen::<f64>() < self.cfg.optional_field_churn {
                obj.insert(f.name.to_string(), f.sample_value(seed_hint));
            }
        }

        let expected = if is_malformed {
            RecordKind::Malformed
        } else if is_drifted {
            let novel = &DRIFT_NOVEL_FIELDS[family % DRIFT_NOVEL_FIELDS.len()];
            obj.insert(novel.name.to_string(), novel.sample_value(seed_hint));
            if let Some(sig) = schema
                .fields
                .iter()
                .find(|f| SIGNATURE_POOL.iter().any(|p| p.name == f.name))
            {
                obj.insert(sig.name.to_string(), sig.widened_value(seed_hint));
            }
            RecordKind::Drifted {
                schema_family: family,
            }
        } else {
            RecordKind::WellFormed {
                schema_family: family,
            }
        };

        pad_to_target(&mut obj, self.cfg.payload_bytes.target_bytes());

        let json = serde_json::to_string(&Value::Object(obj.clone())).expect("object serializes");

        let bytes = if is_malformed {
            match self.rng.gen_range(0u8..3) {
                0 => malform::duplicate_key(&json, &obj),
                1 => malform::nan_value(&json, &obj, &schema.fields),
                _ => malform::truncated(&json),
            }
        } else {
            json.into_bytes()
        };

        GeneratedRecord { bytes, expected }
    }
}
