//! [`FakeBackend`] — the deterministic, in-process [`TrainingBackend`] used
//! by EVERY test in this crate (spec §8: "proves the pipeline data->submit
//! ->artifact->eval->gate->promote without real GPU").

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;

use super::{
    digest_hex, JobHandle, JobStatus, TrainingBackend, TrainingBackendError, TrainingJobSpec,
    QUANTIZED_WEIGHTS_KEY, TRAINING_CHECKPOINT_KEY,
};

/// Reports `Done` on the very first `poll`, with digests derived from the
/// spec's own content (via [`JobHandle`], itself content-derived) — same
/// seed + replay set always produces the same handle/digests, no
/// wall-clock/random state.
#[derive(Default)]
pub struct FakeBackend {
    submit_calls: AtomicUsize,
    poll_calls: AtomicUsize,
}

impl FakeBackend {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn submit_calls(&self) -> usize {
        self.submit_calls.load(Ordering::SeqCst)
    }

    pub fn poll_calls(&self) -> usize {
        self.poll_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl TrainingBackend for FakeBackend {
    async fn submit(&self, spec: &TrainingJobSpec) -> Result<JobHandle, TrainingBackendError> {
        self.submit_calls.fetch_add(1, Ordering::SeqCst);
        // Hashes only the CONTENT-stable fields — deliberately excludes
        // `feedback_cutoff` (a wall-clock timestamp): two calls with the
        // same base/replay/method/seed must yield the same handle
        // regardless of when `submit` happened to run, so this fake stays
        // deterministic by (base_snapshot, replay content, seed) alone —
        // the same contract a real content-addressed backend would offer.
        let stable = (
            &spec.base_bundle_digest,
            &spec.dataset_digest,
            &spec.trainer_image_digest,
            spec.method.as_str(),
            &spec.replay_manifest_digest,
            spec.seed,
        );
        let bytes = serde_json::to_vec(&stable).expect("stable tuple always serializes");
        Ok(JobHandle(digest_hex(&bytes)))
    }

    async fn poll(&self, handle: &JobHandle) -> Result<JobStatus, TrainingBackendError> {
        self.poll_calls.fetch_add(1, Ordering::SeqCst);
        let mut digests = BTreeMap::new();
        digests.insert(
            TRAINING_CHECKPOINT_KEY.to_string(),
            format!("sha256:ckpt-{}", handle.0),
        );
        digests.insert(
            QUANTIZED_WEIGHTS_KEY.to_string(),
            format!("sha256:quant-{}", handle.0),
        );
        Ok(JobStatus::Done {
            artifact_digests: digests,
        })
    }
}
