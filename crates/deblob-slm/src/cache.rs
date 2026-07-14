//! Decision cache for [`crate::http::HttpInferencer`] (spec §Task 2).
//!
//! Key = sha256 digest of `(model, contract_version, candidate_set, prompt)`.
//! An identical request (same model, same contract version, same retrieved
//! top-k set, same rendered prompt) is served from the cache without hitting
//! the endpoint at all — this is the "cache prompt prefix + any grammar by
//! candidate-set digest" prefill-economics requirement from the plan's
//! global constraints, applied at the decision level rather than the
//! prefill-token level (grammar/prefix caching lives in the endpoint /
//! Task-8 local-runtime path, out of scope here).
//!
//! In-memory only (`Mutex<HashMap>` with a capacity cap and naive eviction).
//! This is a single-process cache: it does not survive a restart and is not
//! shared across replicas. That's acceptable for P2 shadow mode, where the
//! cache exists to avoid redundant HTTP calls within one process's lifetime,
//! not as a durability guarantee.

use std::collections::HashMap;
use std::sync::Mutex;

use sha2::{Digest, Sha256};

use crate::contract::{FamilyCandidate, InferenceDecision};

/// Opaque cache key: hex-encoded sha256 digest.
pub type CacheKey = String;

/// Compute the decision cache key for a request.
///
/// `candidates` is the retrieved top-k set (order-sensitive — a different
/// rank ordering is, deliberately, a different cache entry, since rank is
/// part of what the model sees). `prompt` is the already-rendered prompt
/// text (see [`crate::contract::InferenceRequest::prompt`]).
pub fn cache_key(
    model: &str,
    contract_version: u32,
    candidates: &[FamilyCandidate],
    prompt: &str,
) -> CacheKey {
    let mut hasher = Sha256::new();
    hasher.update(model.as_bytes());
    hasher.update(b"\0");
    hasher.update(contract_version.to_le_bytes());
    hasher.update(b"\0");
    for candidate in candidates {
        hasher.update(candidate.schema_id.as_str().as_bytes());
        hasher.update(candidate.family_id.as_str().as_bytes());
        hasher.update(candidate.version.to_le_bytes());
        hasher.update(candidate.rank.to_le_bytes());
        hasher.update(candidate.distance.to_le_bytes());
        hasher.update(b"\0");
    }
    hasher.update(b"\0");
    hasher.update(prompt.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// A bounded in-memory cache from [`CacheKey`] to a previously validated
/// [`InferenceDecision`].
pub struct DecisionCache {
    inner: Mutex<HashMap<CacheKey, InferenceDecision>>,
    capacity: usize,
}

impl DecisionCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            capacity: capacity.max(1),
        }
    }

    /// Return a cloned cached decision, if present.
    pub fn get(&self, key: &CacheKey) -> Option<InferenceDecision> {
        let guard = self.inner.lock().expect("decision cache mutex poisoned");
        guard.get(key).cloned()
    }

    /// Insert a decision. If the cache is at capacity, evicts an arbitrary
    /// entry first (naive cap enforcement — decision caching is a
    /// best-effort optimization here, not an LRU-correctness requirement).
    pub fn put(&self, key: CacheKey, decision: InferenceDecision) {
        let mut guard = self.inner.lock().expect("decision cache mutex poisoned");
        if guard.len() >= self.capacity && !guard.contains_key(&key) {
            if let Some(evict_key) = guard.keys().next().cloned() {
                guard.remove(&evict_key);
            }
        }
        guard.insert(key, decision);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deblob_core::id::{FamilyId, SchemaId};

    fn candidate(byte: u8, rank: u32) -> FamilyCandidate {
        FamilyCandidate {
            family_id: FamilyId::new_v7(),
            schema_id: SchemaId::from_digest(&[byte; 32]),
            version: 1,
            distance: 0.1,
            rank,
        }
    }

    #[test]
    fn identical_inputs_produce_identical_keys() {
        let candidates = vec![candidate(1, 0)];
        let a = cache_key("model-x", 1, &candidates, "prompt");
        let b = cache_key("model-x", 1, &candidates, "prompt");
        assert_eq!(a, b);
    }

    #[test]
    fn differing_prompt_changes_key() {
        let candidates = vec![candidate(1, 0)];
        let a = cache_key("model-x", 1, &candidates, "prompt-a");
        let b = cache_key("model-x", 1, &candidates, "prompt-b");
        assert_ne!(a, b);
    }

    #[test]
    fn cache_roundtrips_a_decision() {
        let cache = DecisionCache::new(4);
        let key = cache_key("model-x", 1, &[], "prompt");
        assert!(cache.get(&key).is_none());

        let decision = InferenceDecision::Abstain {
            cause: crate::contract::AbstainCause::Ambiguous,
        };
        cache.put(key.clone(), decision.clone());
        assert_eq!(cache.get(&key), Some(decision));
    }
}
