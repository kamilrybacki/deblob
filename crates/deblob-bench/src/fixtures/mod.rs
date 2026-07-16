//! Real-world JSON fixtures embedded at compile time (`include_str!`), plus
//! a deterministic cycling/sampling stream over them. Content is hand-
//! authored to be structurally faithful to the real payload shapes (nested
//! objects, arrays, mixed types) — not verbatim captures.

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::record::{GeneratedRecord, RecordKind};

const GITHUB_PUSH: &str = include_str!("data/github_push.json");
const GITHUB_PULL_REQUEST: &str = include_str!("data/github_pull_request.json");
const GITHUB_ISSUES: &str = include_str!("data/github_issues.json");

const K8S_POD_SCHEDULED: &str = include_str!("data/k8s_event_pod_scheduled.json");
const K8S_NODE_NOT_READY: &str = include_str!("data/k8s_event_node_not_ready.json");
const K8S_IMAGE_PULL_BACKOFF: &str = include_str!("data/k8s_event_image_pull_backoff.json");

const CLOUDEVENT_ORDER_CREATED: &str = include_str!("data/cloudevent_order_created.json");
const CLOUDEVENT_USER_SIGNUP: &str = include_str!("data/cloudevent_user_signup.json");
const CLOUDEVENT_SENSOR_READING: &str = include_str!("data/cloudevent_sensor_reading.json");

/// A category of real-world fixture. Each kind has its own small pool of
/// hand-authored, structurally faithful example payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RealWorldKind {
    /// GitHub webhook event bodies (push, pull_request, issues).
    GitHubWebhook,
    /// Kubernetes `Event` objects.
    K8sEvent,
    /// CloudEvents envelopes.
    CloudEvent,
}

impl RealWorldKind {
    fn pool(self) -> &'static [&'static str] {
        match self {
            RealWorldKind::GitHubWebhook => &[GITHUB_PUSH, GITHUB_PULL_REQUEST, GITHUB_ISSUES],
            RealWorldKind::K8sEvent => &[
                K8S_POD_SCHEDULED,
                K8S_NODE_NOT_READY,
                K8S_IMAGE_PULL_BACKOFF,
            ],
            RealWorldKind::CloudEvent => &[
                CLOUDEVENT_ORDER_CREATED,
                CLOUDEVENT_USER_SIGNUP,
                CLOUDEVENT_SENSOR_READING,
            ],
        }
    }
}

/// Every embedded fixture across every kind, in a fixed order — used by
/// tests that want to validate the whole corpus at once.
pub fn all_fixtures() -> Vec<&'static str> {
    [
        RealWorldKind::GitHubWebhook,
        RealWorldKind::K8sEvent,
        RealWorldKind::CloudEvent,
    ]
    .iter()
    .flat_map(|k| k.pool().iter().copied())
    .collect()
}

/// Build a deterministic stream of `count` records sampled from the
/// fixture pools of `kinds`. The sampling order is a seeded Fisher-Yates
/// shuffle of the combined pool, then cycled to reach `count` — so the
/// same `(kinds, count, seed)` always yields the same sequence of bytes.
pub fn real_world_stream(kinds: &[RealWorldKind], count: usize, seed: u64) -> RealWorldStream {
    let mut pool: Vec<&'static str> = Vec::new();
    for k in kinds {
        pool.extend_from_slice(k.pool());
    }
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    for i in (1..pool.len()).rev() {
        let j = rng.gen_range(0..=i);
        pool.swap(i, j);
    }
    RealWorldStream {
        pool,
        idx: 0,
        remaining: count,
    }
}

/// Iterator returned by [`real_world_stream`].
pub struct RealWorldStream {
    pool: Vec<&'static str>,
    idx: usize,
    remaining: usize,
}

impl Iterator for RealWorldStream {
    type Item = GeneratedRecord;

    fn next(&mut self) -> Option<GeneratedRecord> {
        if self.remaining == 0 || self.pool.is_empty() {
            return None;
        }
        self.remaining -= 1;
        let schema_family = self.idx % self.pool.len();
        let text = self.pool[schema_family];
        self.idx += 1;
        Some(GeneratedRecord {
            bytes: text.as_bytes().to_vec(),
            expected: RecordKind::WellFormed { schema_family },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_fixture_is_valid_json() {
        for text in all_fixtures() {
            assert!(
                serde_json::from_str::<serde_json::Value>(text).is_ok(),
                "fixture is not valid JSON: {text}"
            );
        }
    }

    #[test]
    fn pool_sizes_are_in_the_three_to_six_range() {
        for kind in [
            RealWorldKind::GitHubWebhook,
            RealWorldKind::K8sEvent,
            RealWorldKind::CloudEvent,
        ] {
            let n = kind.pool().len();
            assert!((3..=6).contains(&n), "{kind:?} pool has {n} fixtures");
        }
    }
}
