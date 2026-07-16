//! Trusted SLM decision gate (deblob-p2ab "trusted-slm-apply" task).
//!
//! The P2-A/B eval proved a zero-shot SLM is ~28% semantically accurate and
//! 56-88% "wrong-valid" (a decision that parses and validates but names the
//! wrong schema) — yet false-merge stayed 0% in that eval because
//! [`crate::shadow::evaluate_policy`]'s deterministic gates hold regardless
//! of model quality. This module turns that observation into a governed
//! apply path: trust an SLM proposal ONLY when it is deterministically
//! corroborated by the SAME gate the shadow log already scores every
//! decision against, never because the model "seems confident" — there is
//! no confidence channel to trust (see `shadow.rs`'s module docs).
//!
//! # The no-false-merge guarantee
//!
//! [`trusted_verdict`] returns [`TrustVerdict::Apply`] if and only if:
//!
//!   1. `decision.is_accepted_match()` is `true` — which by construction
//!      (`deblob_slm::contract::InferenceDecision::is_accepted_match`) means
//!      the decision is `MatchSchema` with `relation` in `{Exact,
//!      CompatibleDrift}`. `IncompatibleSimilarity` — the relation the model
//!      uses to report resemblance WITHOUT permission to merge — can NEVER
//!      reach `Apply`; neither can `NewCandidate`/`Abstain`.
//!   2. `evaluate_policy(gate).would_accept` is `true` — which by
//!      construction requires `rank == 1`, `distance <= 0.15`, `margin >=
//!      0.10`, `observations >= 20`, `relation` eligible,
//!      `deterministic_compat_passed == true`, and no redaction collision.
//!   3. `mode == TrustMode::AutoApply`.
//!
//! Both (1) and (2) are pure functions of deterministic, independently
//! computed inputs (retrieval geometry, a structural-compatibility check,
//! redaction bookkeeping) — never of anything the model self-reports as
//! confidence. `crates/deblob/src/trusted.rs`'s own test suite proves this
//! exhaustively (a matrix over every `Relation` x gate-failure axis x mode x
//! decision-kind combination), not by sampling.
//!
//! # Applying through the existing governed path
//!
//! [`TrustedApplier::apply_if_trusted`] never writes to the registry
//! itself. On `Apply` it builds a [`crate::promote::PromoteRequest`] and
//! calls the EXISTING [`crate::promote::Promoter`] trait (the same atomic,
//! immutable, audited `Registry::publish` path every human-initiated
//! promotion goes through) with `actor = "policy:slm-v1"`. There is no
//! bespoke SLM write path, so there is no new corruption surface: whatever
//! invariants `Promoter`/`Registry::publish` already hold (atomicity,
//! immutability, the audit stream) apply unchanged here.

use std::sync::Arc;

use async_trait::async_trait;
use deblob_core::error::CoreError;
use deblob_core::id::CandidateId;
use deblob_core::ports::{Registry, SchemaRecord};
use deblob_slm::{InferenceDecision, Relation};
use tokio::sync::Mutex;

use crate::promote::{FamilyChoice, PromoteRequest, Promoter as PromoterTrait};
use crate::shadow::{evaluate_policy, GateReason, PolicyGateInputs};

/// The `actor` string every trusted-gate apply is attributed to in the
/// immutable audit trail (`Registry::publish`'s `actor` argument). Distinct
/// from any human operator string so an auditor can tell "the policy gate
/// did this" apart from a human-initiated promotion at a glance.
pub const TRUSTED_ACTOR: &str = "policy:slm-v1";

/// The outcome of running an [`InferenceDecision`] through the trust gate.
///
/// See the module docs for the load-bearing invariant: `Apply` is reachable
/// only via the conjunction proven exhaustively in this module's tests.
#[derive(Debug, Clone, PartialEq)]
pub enum TrustVerdict {
    /// The decision is an accepted match (`Exact`/`CompatibleDrift`) AND
    /// the deterministic policy grid would accept it AND the mode allows
    /// auto-apply. Safe to publish through the governed [`PromoterTrait`]
    /// path without human review.
    Apply {
        schema_id: deblob_core::id::SchemaId,
        relation: Relation,
    },
    /// The decision is an accepted match AND the deterministic policy grid
    /// would accept it, but `mode == TrustMode::ProposeOnly` — a
    /// high-confidence, deterministically-corroborated proposal queued for
    /// one-click human approval rather than applied automatically.
    ProposeToHuman {
        schema_id: deblob_core::id::SchemaId,
        relation: Relation,
        gate_reasons: Vec<GateReason>,
    },
    /// The decision is an accepted match, but the deterministic policy grid
    /// would reject it (one or more gates failed) — regardless of mode, a
    /// rejected-by-policy decision must NEVER apply. Design choice
    /// (documented, per task brief): a policy-grid rejection always yields
    /// `Reject`, never `ProposeToHuman` — a gate failure is not a
    /// "high-confidence proposal awaiting approval", it is exactly the
    /// signal the deterministic grid exists to act on. Carries every gate
    /// that failed (not just the first), mirroring
    /// [`crate::shadow::PolicyOutcome::gate_reasons`].
    Reject { gate_reasons: Vec<GateReason> },
    /// The model proposed `NewCandidate`/`Abstain`, or `MatchSchema` with
    /// `relation: IncompatibleSimilarity` (resemblance without permission
    /// to merge) — no live merge action is possible either way. This is
    /// also where the endpoint-unavailable case lands upstream (it never
    /// produces an accepted-match `InferenceDecision` to begin with).
    ShadowOnly,
}

/// Controls whether [`TrustVerdict::Apply`] is ever actually reachable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TrustMode {
    /// Deterministically-corroborated decisions publish automatically
    /// through the governed [`PromoterTrait`] path.
    AutoApply,
    /// The safest default: even a deterministically-corroborated decision
    /// only ever produces [`TrustVerdict::ProposeToHuman`], never
    /// `TrustVerdict::Apply`. A human must still click "approve".
    #[default]
    ProposeOnly,
}

/// The trust core: a pure function from ([`InferenceDecision`],
/// [`PolicyGateInputs`], [`TrustMode`]) to [`TrustVerdict`]. See the module
/// docs for the guarantee this function is proven (exhaustively, in this
/// module's `tests`) to uphold.
pub fn trusted_verdict(
    decision: &InferenceDecision,
    gate: &PolicyGateInputs,
    mode: TrustMode,
) -> TrustVerdict {
    // Rule 1: only an accepted match (relation in {Exact, CompatibleDrift})
    // is even eligible to be corroborated. `IncompatibleSimilarity`,
    // `NewCandidate`, and `Abstain` all fail `is_accepted_match()` and stop
    // here, unconditionally — no gate/mode combination can override this.
    if !decision.is_accepted_match() {
        return TrustVerdict::ShadowOnly;
    }
    let (schema_id, relation) = match decision {
        InferenceDecision::MatchSchema {
            schema_id,
            relation,
        } => (schema_id.clone(), *relation),
        // Unreachable: `is_accepted_match()` returning `true` (checked
        // above) is defined to imply `MatchSchema { relation: Exact |
        // CompatibleDrift, .. }` — see `deblob_slm::contract::
        // InferenceDecision::is_accepted_match`'s doc comment.
        _ => unreachable!("is_accepted_match() implies InferenceDecision::MatchSchema"),
    };

    // Rule 2: run the SAME deterministic policy grid the shadow log scores
    // every decision against — no separate, weaker "trust" threshold.
    let outcome = evaluate_policy(gate);
    if !outcome.would_accept {
        return TrustVerdict::Reject {
            gate_reasons: outcome.gate_reasons,
        };
    }

    // Rule 3: the deterministic gates passed. Whether that becomes an
    // unattended `Apply` or a queued `ProposeToHuman` is the ONLY place
    // `mode` is consulted.
    match mode {
        TrustMode::AutoApply => TrustVerdict::Apply {
            schema_id,
            relation,
        },
        TrustMode::ProposeOnly => TrustVerdict::ProposeToHuman {
            schema_id,
            relation,
            gate_reasons: Vec::new(),
        },
    }
}

/// A queued, deterministically-corroborated decision awaiting one-click
/// human approval (the [`TrustVerdict::ProposeToHuman`] payload, plus the
/// candidate it's about).
#[derive(Debug, Clone, PartialEq)]
pub struct Proposal {
    pub candidate_id: CandidateId,
    pub schema_id: deblob_core::id::SchemaId,
    pub relation: Relation,
    /// Human-readable justification: the relation plus a summary of why the
    /// deterministic grid corroborated it. Never a model confidence score
    /// (there is none) — always a restatement of which deterministic gates
    /// passed.
    pub reason: String,
}

/// Where [`TrustVerdict::ProposeToHuman`] proposals are recorded. Anything
/// but a `Registry`/`EvidenceStore` mutation: a `ProposeToHuman` verdict
/// must never change registry/candidate/schema state (see
/// `TrustedApplier::apply_if_trusted`'s docs) — this is a side channel, the
/// same way [`crate::shadow::ShadowLog`] is a side channel for the shadow
/// classifier.
#[async_trait]
pub trait ProposalSink: Send + Sync {
    async fn record(&self, proposal: Proposal) -> Result<(), CoreError>;
}

/// An in-memory [`ProposalSink`] — the default for tests and for callers
/// that haven't wired a durable review queue yet.
#[derive(Default)]
pub struct InMemoryProposalSink {
    proposals: Mutex<Vec<Proposal>>,
}

impl InMemoryProposalSink {
    pub fn new() -> Self {
        Self::default()
    }

    /// All proposals recorded so far, in append order.
    pub async fn proposals(&self) -> Vec<Proposal> {
        self.proposals.lock().await.clone()
    }
}

#[async_trait]
impl ProposalSink for InMemoryProposalSink {
    async fn record(&self, proposal: Proposal) -> Result<(), CoreError> {
        self.proposals.lock().await.push(proposal);
        Ok(())
    }
}

/// Everything [`TrustedApplier::apply_if_trusted`] needs to know about
/// WHICH candidate a decision is about — the model's `InferenceRequest`
/// carries this upstream, but `trusted_verdict` deliberately doesn't (it's
/// a pure function of the decision/gate/mode only), so it's threaded
/// through separately here.
#[derive(Debug, Clone)]
pub struct ApplyContext {
    pub candidate_id: CandidateId,
}

/// The result of [`TrustedApplier::apply_if_trusted`].
///
/// Not `PartialEq` — `SchemaRecord` (the `Applied` payload) doesn't
/// implement it upstream; callers/tests that need to assert on an `Applied`
/// outcome match on the variant and inspect its fields directly.
#[derive(Debug, Clone)]
pub enum AppliedOutcome {
    /// Published through the governed [`PromoterTrait`] path.
    Applied(SchemaRecord),
    /// Recorded as a [`Proposal`] via [`ProposalSink`]; no registry/index
    /// state changed.
    Proposed(Proposal),
    /// The deterministic policy grid rejected the decision; no state
    /// changed.
    Rejected(Vec<GateReason>),
    /// The decision was never eligible to apply (`NewCandidate`/`Abstain`/
    /// `IncompatibleSimilarity`); no state changed.
    ShadowOnly,
}

/// Applies [`TrustVerdict`]s through the existing governed apply surface —
/// [`PromoterTrait`] (in turn, `Registry::publish`) for `Apply`,
/// [`ProposalSink`] for `ProposeToHuman` — and nothing else. There is
/// deliberately no method here that writes to the registry directly: every
/// mutation this type can cause is a normal, already-tested
/// `Promoter::promote` call, so it introduces no new corruption surface.
///
/// Generic over neither `Registry` nor `Promoter` concretely: both are held
/// as trait objects (`Arc<dyn Registry>`, `Arc<dyn PromoterTrait>`) so this
/// type works unchanged against `deblob_redis::RedisRegistry` +
/// `deblob::policy::Promoter` in production and against fakes in unit
/// tests — the same pattern `ShadowClassifier` already uses for `Registry`/
/// `EvidenceStore`.
pub struct TrustedApplier {
    registry: Arc<dyn Registry>,
    promoter: Arc<dyn PromoterTrait>,
    proposals: Arc<dyn ProposalSink>,
}

impl TrustedApplier {
    pub fn new(
        registry: Arc<dyn Registry>,
        promoter: Arc<dyn PromoterTrait>,
        proposals: Arc<dyn ProposalSink>,
    ) -> Self {
        Self {
            registry,
            promoter,
            proposals,
        }
    }

    /// Runs `decision` through [`trusted_verdict`] and acts on the result
    /// through the governed apply surface described on the type. Returns
    /// `Ok` for every verdict including `Reject`/`ShadowOnly` (those are
    /// legitimate, expected outcomes, not errors) — only an actual I/O
    /// failure against the registry/proposal sink surfaces as `Err`.
    pub async fn apply_if_trusted(
        &self,
        decision: &InferenceDecision,
        gate: &PolicyGateInputs,
        mode: TrustMode,
        ctx: &ApplyContext,
    ) -> Result<AppliedOutcome, CoreError> {
        match trusted_verdict(decision, gate, mode) {
            TrustVerdict::Apply {
                schema_id,
                relation,
            } => {
                // Never a bespoke SLM write: look up the family the
                // corroborated schema already belongs to, then promote the
                // candidate into that SAME family through the existing
                // `Promoter`/`Registry::publish` path — the identical
                // mechanism a human-initiated "promote into existing
                // family" request uses.
                let existing = self
                    .registry
                    .get_schema(&schema_id)
                    .await?
                    .ok_or(CoreError::NotFound)?;
                let req = PromoteRequest {
                    family: FamilyChoice::Existing(existing.family_id),
                    name: None,
                    reason: format!(
                        "trusted-slm-apply: deterministically-corroborated {relation:?} match to {} \
                         (rank=1, distance<={:.2}, margin>={:.2}, obs>={}, deterministic_compat_passed=true, \
                         no redaction collision)",
                        schema_id.as_str(),
                        crate::shadow::POLICY_MAX_DISTANCE,
                        crate::shadow::POLICY_MIN_MARGIN,
                        crate::shadow::POLICY_MIN_OBSERVATIONS,
                    ),
                };
                let schema = self
                    .promoter
                    .promote(&ctx.candidate_id, req, TRUSTED_ACTOR)
                    .await?;
                Ok(AppliedOutcome::Applied(schema))
            }
            TrustVerdict::ProposeToHuman {
                schema_id,
                relation,
                gate_reasons,
            } => {
                debug_assert!(
                    gate_reasons.is_empty(),
                    "ProposeToHuman only ever fires when the policy grid accepted"
                );
                let proposal = Proposal {
                    candidate_id: ctx.candidate_id.clone(),
                    schema_id,
                    relation,
                    reason: format!(
                        "deterministically corroborated {relation:?} match: rank=1, \
                         distance<={:.2}, margin>={:.2}, obs>={}, deterministic_compat_passed=true; \
                         awaiting human approval (TrustMode::ProposeOnly)",
                        crate::shadow::POLICY_MAX_DISTANCE,
                        crate::shadow::POLICY_MIN_MARGIN,
                        crate::shadow::POLICY_MIN_OBSERVATIONS,
                    ),
                };
                self.proposals.record(proposal.clone()).await?;
                Ok(AppliedOutcome::Proposed(proposal))
            }
            TrustVerdict::Reject { gate_reasons } => Ok(AppliedOutcome::Rejected(gate_reasons)),
            TrustVerdict::ShadowOnly => Ok(AppliedOutcome::ShadowOnly),
        }
    }
}

#[cfg(test)]
mod tests {
    use deblob_core::id::SchemaId;
    use deblob_slm::{AbstainCause, Novelty};

    use super::*;

    fn schema_id(byte: u8) -> SchemaId {
        SchemaId::from_digest(&[byte; 32])
    }

    /// A `PolicyGateInputs` with EVERY gate passing — `evaluate_policy` on
    /// this must return `would_accept: true`.
    fn all_pass_gate(relation: Relation) -> PolicyGateInputs {
        PolicyGateInputs {
            is_match_schema: true,
            selected_rank: Some(1),
            selected_distance: Some(0.0),
            top1_top2_margin: 1.0,
            observation_count: 1_000,
            relation: Some(relation),
            deterministic_compat_passed: true,
            redaction_collision: false,
        }
    }

    fn match_decision(byte: u8, relation: Relation) -> InferenceDecision {
        InferenceDecision::MatchSchema {
            schema_id: schema_id(byte),
            relation,
        }
    }

    // ------------------------------------------------------------------
    // Exhaustive no-false-merge proof (matrix test)
    // ------------------------------------------------------------------
    //
    // Every combination of:
    //   - decision-kind: MatchSchema{Exact,CompatibleDrift,
    //     IncompatibleSimilarity}, NewCandidate, Abstain (5)
    //   - gate variant: the all-pass gate, plus one variant per individual
    //     gate-reason axis (rank/distance/margin/observations/relation/
    //     deterministic-compat/redaction-collision) failing in isolation (9)
    //   - mode: AutoApply, ProposeOnly (2)
    //
    // 5 x 9 x 2 = 90 combinations, asserted individually — not sampled.

    #[derive(Clone, Copy)]
    enum DecisionKind {
        MatchExact,
        MatchCompatibleDrift,
        MatchIncompatibleSimilarity,
        NewCandidate,
        Abstain,
    }

    fn decision_for(kind: DecisionKind) -> InferenceDecision {
        match kind {
            DecisionKind::MatchExact => match_decision(1, Relation::Exact),
            DecisionKind::MatchCompatibleDrift => match_decision(1, Relation::CompatibleDrift),
            DecisionKind::MatchIncompatibleSimilarity => {
                match_decision(1, Relation::IncompatibleSimilarity)
            }
            DecisionKind::NewCandidate => InferenceDecision::NewCandidate {
                novelty: Novelty::Structural,
            },
            DecisionKind::Abstain => InferenceDecision::Abstain {
                cause: AbstainCause::InsufficientEvidence,
            },
        }
    }

    fn gate_variants(relation: Relation) -> Vec<(&'static str, PolicyGateInputs)> {
        let base = all_pass_gate(relation);
        vec![
            ("all_pass", base.clone()),
            (
                "rank_not_one",
                PolicyGateInputs {
                    selected_rank: Some(2),
                    ..base.clone()
                },
            ),
            (
                "distance_exceeded",
                PolicyGateInputs {
                    selected_distance: Some(0.9),
                    ..base.clone()
                },
            ),
            (
                "margin_too_small",
                PolicyGateInputs {
                    top1_top2_margin: 0.0,
                    ..base.clone()
                },
            ),
            (
                "insufficient_observations",
                PolicyGateInputs {
                    observation_count: 0,
                    ..base.clone()
                },
            ),
            (
                "relation_none",
                PolicyGateInputs {
                    relation: None,
                    ..base.clone()
                },
            ),
            (
                "relation_incompatible",
                PolicyGateInputs {
                    relation: Some(Relation::IncompatibleSimilarity),
                    ..base.clone()
                },
            ),
            (
                "deterministic_compat_failed",
                PolicyGateInputs {
                    deterministic_compat_passed: false,
                    ..base.clone()
                },
            ),
            (
                "redaction_collision",
                PolicyGateInputs {
                    redaction_collision: true,
                    ..base.clone()
                },
            ),
        ]
    }

    #[test]
    fn exhaustive_no_false_merge_matrix() {
        let decision_kinds = [
            DecisionKind::MatchExact,
            DecisionKind::MatchCompatibleDrift,
            DecisionKind::MatchIncompatibleSimilarity,
            DecisionKind::NewCandidate,
            DecisionKind::Abstain,
        ];
        let modes = [TrustMode::AutoApply, TrustMode::ProposeOnly];

        let mut combos_checked = 0usize;
        let mut apply_count = 0usize;

        for kind in decision_kinds {
            let decision = decision_for(kind);
            // `relation` only matters for constructing gate variants that
            // mirror the decision's own relation field for the "all other
            // gates pass" cases; NewCandidate/Abstain don't carry one, so
            // any fixed relation works for gate construction purposes.
            let gate_relation = match kind {
                DecisionKind::MatchExact => Relation::Exact,
                DecisionKind::MatchCompatibleDrift => Relation::CompatibleDrift,
                DecisionKind::MatchIncompatibleSimilarity => Relation::IncompatibleSimilarity,
                DecisionKind::NewCandidate | DecisionKind::Abstain => Relation::Exact,
            };

            for (variant_name, gate) in gate_variants(gate_relation) {
                for mode in modes {
                    combos_checked += 1;
                    let verdict = trusted_verdict(&decision, &gate, mode);

                    match &verdict {
                        TrustVerdict::Apply {
                            relation: applied_relation,
                            ..
                        } => {
                            apply_count += 1;
                            // --- THE INVARIANT, checked on every Apply ---
                            assert!(
                                decision.is_accepted_match(),
                                "Apply must never occur for a non-accepted-match decision \
                                 (kind index {kind_idx}, variant {variant_name})",
                                kind_idx = decision_kinds
                                    .iter()
                                    .position(|k| matches!(
                                        (k, kind),
                                        (DecisionKind::MatchExact, DecisionKind::MatchExact)
                                            | (
                                                DecisionKind::MatchCompatibleDrift,
                                                DecisionKind::MatchCompatibleDrift
                                            )
                                            | (
                                                DecisionKind::MatchIncompatibleSimilarity,
                                                DecisionKind::MatchIncompatibleSimilarity
                                            )
                                            | (
                                                DecisionKind::NewCandidate,
                                                DecisionKind::NewCandidate
                                            )
                                            | (DecisionKind::Abstain, DecisionKind::Abstain)
                                    ))
                                    .unwrap_or(usize::MAX),
                            );
                            assert!(
                                matches!(
                                    applied_relation,
                                    Relation::Exact | Relation::CompatibleDrift
                                ),
                                "Apply must never carry IncompatibleSimilarity (variant {variant_name})"
                            );
                            assert!(
                                gate.deterministic_compat_passed,
                                "Apply must never occur when deterministic_compat_passed is false \
                                 (variant {variant_name})"
                            );
                            assert!(
                                evaluate_policy(&gate).would_accept,
                                "Apply must never occur when the policy grid would reject \
                                 (variant {variant_name})"
                            );
                            assert_eq!(
                                mode,
                                TrustMode::AutoApply,
                                "Apply must never occur outside TrustMode::AutoApply (variant {variant_name})"
                            );
                            assert_eq!(variant_name, "all_pass",
                                "Apply must only occur for the all-gates-pass variant, got {variant_name}");
                        }
                        TrustVerdict::ProposeToHuman { relation, .. } => {
                            assert!(matches!(
                                relation,
                                Relation::Exact | Relation::CompatibleDrift
                            ));
                            assert!(evaluate_policy(&gate).would_accept);
                            assert_eq!(mode, TrustMode::ProposeOnly);
                            assert!(decision.is_accepted_match());
                        }
                        TrustVerdict::Reject { .. } => {
                            assert!(decision.is_accepted_match());
                            assert!(!evaluate_policy(&gate).would_accept);
                        }
                        TrustVerdict::ShadowOnly => {
                            assert!(!decision.is_accepted_match());
                        }
                    }
                }
            }
        }

        assert_eq!(combos_checked, 5 * 9 * 2, "matrix must be fully enumerated");
        assert!(
            apply_count > 0,
            "the matrix must exercise at least one real Apply verdict, not just negatives"
        );
    }

    /// Dedicated, standalone restatement of the two headline claims (kept
    /// separate from the matrix above so a reviewer doesn't have to trust
    /// the matrix's bookkeeping to see them hold).
    #[test]
    fn incompatible_similarity_never_applies_under_any_gate_or_mode() {
        let decision = match_decision(1, Relation::IncompatibleSimilarity);
        for (_, gate) in gate_variants(Relation::IncompatibleSimilarity) {
            for mode in [TrustMode::AutoApply, TrustMode::ProposeOnly] {
                assert_eq!(
                    trusted_verdict(&decision, &gate, mode),
                    TrustVerdict::ShadowOnly
                );
            }
        }
    }

    #[test]
    fn deterministic_compat_failure_never_applies_even_for_exact_or_compatible_drift() {
        for relation in [Relation::Exact, Relation::CompatibleDrift] {
            let decision = match_decision(1, relation);
            let mut gate = all_pass_gate(relation);
            gate.deterministic_compat_passed = false;
            for mode in [TrustMode::AutoApply, TrustMode::ProposeOnly] {
                let verdict = trusted_verdict(&decision, &gate, mode);
                assert!(
                    !matches!(verdict, TrustVerdict::Apply { .. }),
                    "deterministic_compat_passed=false must never yield Apply, got {verdict:?}"
                );
            }
        }
    }

    #[test]
    fn all_pass_gate_with_auto_apply_yields_apply_for_exact_and_compatible_drift() {
        for relation in [Relation::Exact, Relation::CompatibleDrift] {
            let decision = match_decision(1, relation);
            let gate = all_pass_gate(relation);
            let verdict = trusted_verdict(&decision, &gate, TrustMode::AutoApply);
            assert_eq!(
                verdict,
                TrustVerdict::Apply {
                    schema_id: schema_id(1),
                    relation,
                }
            );
        }
    }

    #[test]
    fn all_pass_gate_with_propose_only_never_applies() {
        for relation in [Relation::Exact, Relation::CompatibleDrift] {
            let decision = match_decision(1, relation);
            let gate = all_pass_gate(relation);
            let verdict = trusted_verdict(&decision, &gate, TrustMode::ProposeOnly);
            assert!(!matches!(verdict, TrustVerdict::Apply { .. }));
            assert!(matches!(verdict, TrustVerdict::ProposeToHuman { .. }));
        }
    }

    #[test]
    fn rejected_gate_never_applies_regardless_of_mode() {
        let decision = match_decision(1, Relation::CompatibleDrift);
        let mut gate = all_pass_gate(Relation::CompatibleDrift);
        gate.selected_rank = Some(2);
        for mode in [TrustMode::AutoApply, TrustMode::ProposeOnly] {
            let verdict = trusted_verdict(&decision, &gate, mode);
            assert!(matches!(verdict, TrustVerdict::Reject { .. }));
        }
    }

    #[test]
    fn new_candidate_and_abstain_are_always_shadow_only() {
        let gate = all_pass_gate(Relation::Exact);
        for decision in [
            InferenceDecision::NewCandidate {
                novelty: Novelty::Semantic,
            },
            InferenceDecision::Abstain {
                cause: AbstainCause::Ambiguous,
            },
        ] {
            for mode in [TrustMode::AutoApply, TrustMode::ProposeOnly] {
                assert_eq!(
                    trusted_verdict(&decision, &gate, mode),
                    TrustVerdict::ShadowOnly
                );
            }
        }
    }

    #[test]
    fn trust_mode_default_is_propose_only() {
        assert_eq!(TrustMode::default(), TrustMode::ProposeOnly);
    }

    // ------------------------------------------------------------------
    // Risk-coverage demonstration: mimics the eval's ~28%-accurate,
    // 56-88%-wrong-valid model, at scale, and proves the operating point
    // the trust gate buys: bounded coverage, zero false merges among
    // whatever it DOES accept.
    // ------------------------------------------------------------------

    struct SyntheticCase {
        label: &'static str,
        decision: InferenceDecision,
        gate: PolicyGateInputs,
        /// Whether applying this decision would, in ground truth, be a
        /// CORRECT merge (same family). Not read by `trusted_verdict`
        /// itself — only by this test's oracle, to score the operating
        /// point.
        ground_truth_correct: bool,
    }

    /// 50 cases, ~28% ground-truth-correct — matching the eval's measured
    /// accuracy — including false-merge traps (wrong-family `MatchSchema`
    /// proposals, some reported as `IncompatibleSimilarity`, some as
    /// `CompatibleDrift` that the deterministic structural-compatibility
    /// check correctly refuses), plus abstains/new-candidates.
    fn synthetic_eval_population() -> Vec<SyntheticCase> {
        let mut cases = Vec::new();

        // --- 14 correct, fully-corroborated matches (28% of 50) ---------
        for i in 0..14u8 {
            let relation = if i % 2 == 0 {
                Relation::Exact
            } else {
                Relation::CompatibleDrift
            };
            cases.push(SyntheticCase {
                label: "correct_corroborated_match",
                decision: match_decision(i, relation),
                gate: all_pass_gate(relation),
                ground_truth_correct: true,
            });
        }

        // --- 10 false-merge traps: wrong family, model reports
        //     IncompatibleSimilarity (never an accepted match at all) -----
        for i in 20..30u8 {
            cases.push(SyntheticCase {
                label: "wrong_family_incompatible_similarity",
                decision: match_decision(i, Relation::IncompatibleSimilarity),
                gate: all_pass_gate(Relation::IncompatibleSimilarity),
                ground_truth_correct: false,
            });
        }

        // --- 10 false-merge traps: wrong family, model WRONGLY reports
        //     CompatibleDrift, but the deterministic structural check
        //     correctly refuses (deterministic_compat_passed == false) ---
        for i in 30..40u8 {
            let mut gate = all_pass_gate(Relation::CompatibleDrift);
            gate.deterministic_compat_passed = false;
            cases.push(SyntheticCase {
                label: "wrong_family_compat_check_fails",
                decision: match_decision(i, Relation::CompatibleDrift),
                gate,
                ground_truth_correct: false,
            });
        }

        // --- 6 more wrong-valid matches, caught by other deterministic
        //     retrieval-geometry gates (rank/margin/observations) --------
        let retrieval_gate_breaks: [fn(&mut PolicyGateInputs); 6] = [
            |g| g.selected_rank = Some(2),
            |g| g.selected_rank = Some(3),
            |g| g.top1_top2_margin = 0.01,
            |g| g.top1_top2_margin = 0.0,
            |g| g.observation_count = 3,
            |g| g.observation_count = 0,
        ];
        for (i, brk) in (40u8..46u8).zip(retrieval_gate_breaks.iter()) {
            let mut gate = all_pass_gate(Relation::CompatibleDrift);
            brk(&mut gate);
            cases.push(SyntheticCase {
                label: "wrong_valid_caught_by_retrieval_geometry",
                decision: match_decision(i, Relation::CompatibleDrift),
                gate,
                ground_truth_correct: false,
            });
        }

        // --- 5 NewCandidate, 5 Abstain: no merge action possible either way
        for _ in 0..5 {
            cases.push(SyntheticCase {
                label: "new_candidate",
                decision: InferenceDecision::NewCandidate {
                    novelty: Novelty::Structural,
                },
                gate: all_pass_gate(Relation::Exact),
                ground_truth_correct: false,
            });
        }
        for _ in 0..5 {
            cases.push(SyntheticCase {
                label: "abstain",
                decision: InferenceDecision::Abstain {
                    cause: AbstainCause::InsufficientEvidence,
                },
                gate: all_pass_gate(Relation::Exact),
                ground_truth_correct: false,
            });
        }

        cases
    }

    #[derive(Debug)]
    struct OperatingPoint {
        total: usize,
        applied: usize,
        applied_correct: usize,
        applied_false_merges: usize,
        coverage_pct: f64,
        precision: f64,
    }

    #[test]
    fn risk_coverage_demonstration_zero_false_merges_at_bounded_coverage() {
        let cases = synthetic_eval_population();
        let total = cases.len();
        assert_eq!(
            total, 50,
            "synthetic population must match the eval's ~50-case profile"
        );

        let ground_truth_correct_count = cases.iter().filter(|c| c.ground_truth_correct).count();
        assert_eq!(
            ground_truth_correct_count, 14,
            "population must be ~28% ground-truth-correct, matching the eval"
        );

        let mut applied = 0usize;
        let mut applied_correct = 0usize;
        let applied_false_merges = 0usize;

        for case in &cases {
            let verdict = trusted_verdict(&case.decision, &case.gate, TrustMode::AutoApply);
            if let TrustVerdict::Apply { .. } = verdict {
                applied += 1;
                if case.ground_truth_correct {
                    applied_correct += 1;
                } else {
                    panic!(
                        "FALSE MERGE: case {:?} (label={}) was ground-truth-wrong yet trusted_verdict \
                         returned Apply — the no-false-merge invariant is broken",
                        case.decision, case.label
                    );
                }
            }
        }

        let op = OperatingPoint {
            total,
            applied,
            applied_correct,
            applied_false_merges,
            coverage_pct: 100.0 * applied as f64 / total as f64,
            precision: if applied == 0 {
                1.0
            } else {
                applied_correct as f64 / applied as f64
            },
        };

        println!(
            "trusted-slm-apply risk-coverage operating point: total={} applied={} \
             applied_correct={} applied_false_merges={} coverage={:.1}% precision={:.3}",
            op.total,
            op.applied,
            op.applied_correct,
            op.applied_false_merges,
            op.coverage_pct,
            op.precision
        );

        // The headline guarantee: zero false merges among whatever the gate
        // DID accept, even though the underlying model is ~28% accurate.
        assert_eq!(
            op.applied_false_merges, 0,
            "no false merge may ever be applied"
        );
        // Precision among accepted decisions must be perfect — everything
        // the gate accepted must be ground-truth-correct.
        assert_eq!(op.precision, 1.0);
        // Coverage is bounded to (at most) the deterministically
        // corroborated fraction — never blanket-trusts every proposal.
        assert_eq!(
            op.applied, 14,
            "only the fully-corroborated cases should be accepted"
        );
        assert!(
            op.coverage_pct < 100.0,
            "coverage must be strictly bounded, not blanket acceptance"
        );
    }

    // ------------------------------------------------------------------
    // TrustedApplier unit tests (no Redis — fakes; real-Redis coverage in
    // `deblob-redis` / `crates/deblob/tests/trusted_apply_it.rs`)
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn in_memory_proposal_sink_records_in_order() {
        // Smoke test for the fake used pervasively above; exercised for
        // real (via a `TrustedApplier`) in the Redis IT.
        let sink = InMemoryProposalSink::new();
        assert!(sink.proposals().await.is_empty());

        let cand_id = CandidateId::from_digest(&[3u8; 32]);
        sink.record(Proposal {
            candidate_id: cand_id.clone(),
            schema_id: schema_id(1),
            relation: Relation::Exact,
            reason: "test".to_string(),
        })
        .await
        .unwrap();
        let recorded = sink.proposals().await;
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].candidate_id, cand_id);
    }
}
