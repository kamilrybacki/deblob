//! The Task-1 `SemanticInferencer` mocks (spec §8: "wire a MOCK inferencer
//! for tests (returns scripted decisions); the real adapters are Task 3").
//!
//! Two mocks, for two different needs:
//! - [`ScriptedInferencer`] — exact playback of a fixed `Vec<InferenceDecision>`
//!   in call order. Use this when a test needs to dictate precisely what
//!   B0/B1 "the model" answers for each case.
//! - [`HeuristicMockInferencer`] — a fully deterministic (seeded, no RNG
//!   state — same input always yields the same output), content-derived
//!   stand-in "SLM" for `run.rs`'s full corpus runs, where scripting one
//!   decision per case by hand isn't practical. It mostly agrees with
//!   [`crate::arms::deterministic::A1FairDeterministic`]'s structural
//!   heuristic but deterministically "disagrees" a configurable fraction
//!   of the time (a hash of the case's own retrieval geometry — never
//!   wall-clock, never an RNG stream — decides whether a given case is a
//!   disagreement), so B0/B1-vs-A1 comparisons in the runner have
//!   something real to measure.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};

use deblob::shadow::{POLICY_MAX_DISTANCE, POLICY_MIN_MARGIN, POLICY_MIN_OBSERVATIONS};
use deblob_slm::{
    AbstainCause, CandidateProfileView, EndpointStatus, FamilyCandidate, InferenceDecision,
    InferenceError, InferenceOutcome, InferenceRequest, InferenceTelemetry, Novelty, Relation,
    SemanticInferencer,
};

use crate::arms::deterministic::{margin_of, top1};

fn empty_telemetry(model_id: &str) -> InferenceTelemetry {
    InferenceTelemetry {
        request_tokens: None,
        response_tokens: None,
        ttft_ms: None,
        total_latency_ms: None,
        repair_count: 0,
        endpoint_status: EndpointStatus::Ok,
        parse_error: false,
        schema_validation_error: false,
        model_id: Some(model_id.to_string()),
    }
}

/// Plays back `script` in call order; the `n`th `classify()` call returns
/// `script[n]`. Errors (rather than panics) once the script is exhausted,
/// so a test that mis-sizes its script fails with a clear message.
pub struct ScriptedInferencer {
    script: Vec<InferenceDecision>,
    calls: AtomicUsize,
}

impl ScriptedInferencer {
    pub fn new(script: Vec<InferenceDecision>) -> Self {
        Self {
            script,
            calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait::async_trait]
impl SemanticInferencer for ScriptedInferencer {
    async fn classify(&self, _req: InferenceRequest) -> Result<InferenceOutcome, InferenceError> {
        let idx = self.calls.fetch_add(1, Ordering::SeqCst);
        let decision =
            self.script.get(idx).cloned().ok_or_else(|| {
                InferenceError::Transport(format!("script exhausted at call {idx}"))
            })?;
        Ok(InferenceOutcome {
            decision,
            telemetry: empty_telemetry("mock-scripted"),
        })
    }
}

/// Deterministic `[0.0, 1.0)` value derived from `seed` + `parts` via a
/// stable, non-cryptographic hash (`DefaultHasher`) — NOT an RNG stream:
/// calling this twice with the same arguments always returns the same
/// value, in any process, in any call order (no shared mutable state).
fn deterministic_unit_interval(seed: u64, parts: &[&str]) -> f64 {
    let mut hasher = DefaultHasher::new();
    seed.hash(&mut hasher);
    for part in parts {
        part.hash(&mut hasher);
    }
    (hasher.finish() % 1_000_000) as f64 / 1_000_000.0
}

/// See the module docs. `disagreement_rate` is the fraction of eligible
/// cases (roughly, by construction of [`Self::decide_sync`]) where this
/// mock deliberately diverges from the plain structural heuristic.
#[derive(Debug, Clone, Copy)]
pub struct HeuristicMockInferencer {
    pub seed: u64,
    pub disagreement_rate: f64,
}

impl HeuristicMockInferencer {
    pub fn new(seed: u64, disagreement_rate: f64) -> Self {
        Self {
            seed,
            disagreement_rate,
        }
    }

    fn is_disagreement(&self, top: &FamilyCandidate, observation_count: u64) -> bool {
        let key = format!("{}:{observation_count}", top.schema_id.as_str());
        deterministic_unit_interval(self.seed, &[&key]) < self.disagreement_rate
    }

    fn decide_sync(
        &self,
        candidate: &CandidateProfileView,
        retrieved: &[FamilyCandidate],
    ) -> InferenceDecision {
        let Some(top) = top1(retrieved) else {
            return InferenceDecision::Abstain {
                cause: AbstainCause::CandidateMissing,
            };
        };
        if candidate.observation_count < POLICY_MIN_OBSERVATIONS {
            return InferenceDecision::Abstain {
                cause: AbstainCause::InsufficientEvidence,
            };
        }

        let disagree = self.is_disagreement(top, candidate.observation_count);

        if top.distance > POLICY_MAX_DISTANCE {
            return if disagree {
                // Deliberately hallucinates a compatible-drift match anyway
                // — exactly the kind of raw-model error B0's metrics and
                // B1's gate exist to surface/contain.
                InferenceDecision::MatchSchema {
                    schema_id: top.schema_id.clone(),
                    relation: Relation::CompatibleDrift,
                }
            } else {
                InferenceDecision::NewCandidate {
                    novelty: Novelty::Structural,
                }
            };
        }

        if margin_of(retrieved) < POLICY_MIN_MARGIN {
            return if disagree {
                InferenceDecision::MatchSchema {
                    schema_id: top.schema_id.clone(),
                    relation: Relation::Exact,
                }
            } else {
                InferenceDecision::Abstain {
                    cause: AbstainCause::Ambiguous,
                }
            };
        }

        let near_zero = top.distance <= 1e-6;
        let relation = match (near_zero, disagree) {
            (true, false) => Relation::Exact,
            (true, true) => Relation::CompatibleDrift,
            (false, false) => Relation::CompatibleDrift,
            (false, true) => Relation::Exact,
        };
        InferenceDecision::MatchSchema {
            schema_id: top.schema_id.clone(),
            relation,
        }
    }
}

#[async_trait::async_trait]
impl SemanticInferencer for HeuristicMockInferencer {
    async fn classify(&self, req: InferenceRequest) -> Result<InferenceOutcome, InferenceError> {
        let decision = self.decide_sync(&req.candidate, &req.retrieved);
        Ok(InferenceOutcome {
            decision,
            telemetry: empty_telemetry("mock-heuristic"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deblob_core::id::{FamilyId, SchemaId};

    fn schema_id(byte: u8) -> SchemaId {
        SchemaId::from_digest(&[byte; 32])
    }

    fn fc(byte: u8, rank: u32, distance: f32) -> FamilyCandidate {
        FamilyCandidate {
            family_id: FamilyId::new_v7(),
            schema_id: schema_id(byte),
            version: 1,
            distance,
            rank,
        }
    }

    fn candidate(obs: u64) -> CandidateProfileView {
        CandidateProfileView {
            observation_count: obs,
            fields: vec![],
            truncated: false,
        }
    }

    #[tokio::test]
    async fn scripted_inferencer_plays_back_in_call_order() {
        let mock = ScriptedInferencer::new(vec![
            InferenceDecision::NewCandidate {
                novelty: Novelty::Semantic,
            },
            InferenceDecision::Abstain {
                cause: AbstainCause::Ambiguous,
            },
        ]);
        let req = InferenceRequest {
            candidate: candidate(10),
            retrieved: vec![],
            contract_version: 1,
            budget: deblob_slm::InferenceBudget {
                max_prompt_tokens: 10,
                timeout_ms: 10,
            },
            prompt: String::new(),
        };
        let first = mock.classify(req.clone()).await.unwrap();
        assert_eq!(
            first.decision,
            InferenceDecision::NewCandidate {
                novelty: Novelty::Semantic
            }
        );
        let second = mock.classify(req.clone()).await.unwrap();
        assert_eq!(
            second.decision,
            InferenceDecision::Abstain {
                cause: AbstainCause::Ambiguous
            }
        );
        assert!(mock.classify(req).await.is_err(), "script exhausted");
    }

    #[test]
    fn heuristic_mock_is_deterministic_given_the_same_seed_and_input() {
        let mock = HeuristicMockInferencer::new(42, 0.3);
        let retrieved = vec![fc(1, 1, 0.0), fc(2, 2, 0.9)];
        let a = mock.decide_sync(&candidate(500), &retrieved);
        let b = mock.decide_sync(&candidate(500), &retrieved);
        assert_eq!(a, b);
    }

    #[test]
    fn heuristic_mock_zero_disagreement_matches_a1_style_relation_choice() {
        let mock = HeuristicMockInferencer::new(7, 0.0);
        let retrieved = vec![fc(1, 1, 0.0), fc(2, 2, 0.9)];
        let decision = mock.decide_sync(&candidate(500), &retrieved);
        assert_eq!(
            decision,
            InferenceDecision::MatchSchema {
                schema_id: schema_id(1),
                relation: Relation::Exact,
            }
        );
    }

    #[test]
    fn heuristic_mock_abstains_below_observation_floor() {
        let mock = HeuristicMockInferencer::new(1, 0.0);
        let retrieved = vec![fc(1, 1, 0.0)];
        let decision = mock.decide_sync(&candidate(1), &retrieved);
        assert_eq!(
            decision,
            InferenceDecision::Abstain {
                cause: AbstainCause::InsufficientEvidence
            }
        );
    }
}
