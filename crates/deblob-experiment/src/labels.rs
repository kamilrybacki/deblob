//! `GoldSidecar` + `InferenceInput` construction with the leak-strip guard
//! (spec `2026-07-16-deblob-experiment.md` §2, "Ground truth EXTERNAL to
//! the gate").
//!
//! An [`EvalCase`] (from `deblob-eval`) carries the answer directly on its
//! face: `EvalCase::name` (the synthetic generator encodes the
//! transformation kind into it, e.g. `"gen_003_02_incompatible_unit_swap"`),
//! `EvalCase::category` (the ground-truth bucket), and `EvalCase::expected`
//! (the literal gold decision). None of those three fields may reach
//! anything an [`crate::arms::Arm`] or a real model sees — that is the
//! anti-tautology core of this whole experiment (an arm that could read its
//! own answer key would trivially "pass" every layer).
//!
//! [`split_case`] is the ONLY place an [`EvalCase`] is taken apart: it
//! returns an [`InferenceInput`] (candidate + retrieved top-k + the
//! rendered prompt, built via the SAME `deblob_slm::build_prompt` the
//! product's shadow lane uses — never a hand-rolled renderer) and a
//! [`GoldSidecar`] (the stripped fields, evaluator-only, never passed to an
//! `Arm`). The leak-guard test at the bottom of this file asserts that no
//! stripped field's content reaches the built `InferenceInput` or its
//! prompt text.

use deblob_core::id::SchemaId;
use deblob_eval::{Category, EvalCase, Expected};
use deblob_slm::{build_prompt, CandidateProfileView, FamilyCandidate};
use serde::Serialize;

/// Source-native / ground-truth labels held OUT of [`InferenceInput`] — the
/// evaluator-only sidecar (spec §2). Every metrics layer scores an arm's
/// [`crate::arms::ArmDecision`] against this, never the other way around;
/// nothing in `arms/` or `labels::split_case`'s `InferenceInput` output
/// ever reads from a `GoldSidecar`.
#[derive(Debug, Clone, Serialize)]
pub struct GoldSidecar {
    /// The corpus case name. Carries generator-encoded labels (e.g. the
    /// `_exact`/`_incompatible_unit_swap`/`_false_split_` suffixes emitted
    /// by `deblob_eval::generate`) — evaluator-only, never leaked.
    pub case_name: String,
    pub category: Category,
    pub expected: Expected,
}

/// Everything an [`crate::arms::Arm`] is allowed to see for one case: the
/// redacted candidate statistics, the retrieved top-k (ids/distances/ranks
/// only — never a human-readable label), the derived allow-list, and the
/// rendered PII-safe prompt built from those two fields alone.
///
/// Deliberately does NOT carry `EvalCase::name`, `EvalCase::category`, or
/// `EvalCase::expected` — there is no field here to leak them into. See
/// [`split_case`] and this module's `leak guard` tests.
#[derive(Debug, Clone, Serialize)]
pub struct InferenceInput {
    pub candidate: CandidateProfileView,
    pub retrieved: Vec<FamilyCandidate>,
    /// `retrieved`'s schema ids, in retrieval order — the exact allow-list
    /// a real `InferenceRequest`/contract validation would enforce.
    pub allowed_ids: Vec<SchemaId>,
    /// The rendered prompt text, via `deblob_slm::build_prompt` — the
    /// SAME PII-safe builder the product's shadow lane uses. Built ONLY
    /// from `candidate`/`retrieved`/`allowed_ids` above.
    pub prompt: String,
}

/// Splits one `EvalCase` into the leak-free [`InferenceInput`] an
/// [`crate::arms::Arm`] sees plus the [`GoldSidecar`] the evaluator scores
/// against. This is the ONLY function in this crate that is allowed to read
/// `EvalCase::name`/`EvalCase::category`/`EvalCase::expected` — every other
/// function downstream operates on the already-split `InferenceInput`
/// and/or `GoldSidecar`.
pub fn split_case(case: &EvalCase) -> (InferenceInput, GoldSidecar) {
    let allowed_ids: Vec<SchemaId> = case.retrieved.iter().map(|c| c.schema_id.clone()).collect();
    let prompt = build_prompt(&case.candidate, &case.retrieved, &allowed_ids).text;

    let input = InferenceInput {
        candidate: case.candidate.clone(),
        retrieved: case.retrieved.clone(),
        allowed_ids,
        prompt,
    };
    let sidecar = GoldSidecar {
        case_name: case.name.clone(),
        category: case.category,
        expected: case.expected.clone(),
    };
    (input, sidecar)
}

/// Splits a whole corpus at once, preserving order — the pairing at index
/// `i` in the two returned `Vec`s always corresponds to `corpus[i]`.
pub fn split_corpus(corpus: &[EvalCase]) -> (Vec<InferenceInput>, Vec<GoldSidecar>) {
    let mut inputs = Vec::with_capacity(corpus.len());
    let mut sidecars = Vec::with_capacity(corpus.len());
    for case in corpus {
        let (input, sidecar) = split_case(case);
        inputs.push(input);
        sidecars.push(sidecar);
    }
    (inputs, sidecars)
}

#[cfg(test)]
mod tests {
    use super::*;
    use deblob_core::id::FamilyId;
    use deblob_eval::Partition;
    use deblob_slm::{AbstainCause, CandidateProfileView as CPV, InferenceDecision};

    fn schema_id(byte: u8) -> SchemaId {
        SchemaId::from_digest(&[byte; 32])
    }

    fn family_candidate(schema_byte: u8, rank: u32, distance: f32) -> FamilyCandidate {
        FamilyCandidate {
            family_id: FamilyId::new_v7(),
            schema_id: schema_id(schema_byte),
            version: 1,
            distance,
            rank,
        }
    }

    fn empty_candidate() -> CPV {
        CPV {
            observation_count: 10,
            fields: vec![],
            truncated: false,
        }
    }

    /// A case built so that EVERY stripped field carries a distinctive,
    /// otherwise-impossible-to-coincidentally-produce marker: the case
    /// name, the category's Debug rendering, and the gold decision's
    /// AbstainCause variant name. If any of these markers turns up in the
    /// serialized `InferenceInput` or its prompt text, a leak field
    /// reached the model-facing surface.
    fn leaky_case() -> EvalCase {
        EvalCase {
            name: "LEAK_MARKER_CASE_NAME_7f3a".to_string(),
            category: Category::AmbiguousAdversarial,
            candidate: empty_candidate(),
            retrieved: vec![family_candidate(1, 1, 0.05)],
            expected: Expected {
                decision: InferenceDecision::Abstain {
                    cause: AbstainCause::CandidateMissing,
                },
                gold_schema_id: None,
                gold_rank: None,
                false_merge_trap: false,
                false_split_trap: false,
            },
            partition: Partition::Test,
        }
    }

    #[test]
    fn inference_input_never_carries_the_case_name() {
        let case = leaky_case();
        let (input, sidecar) = split_case(&case);

        let serialized = serde_json::to_string(&input).unwrap();
        assert!(
            !serialized.contains("LEAK_MARKER_CASE_NAME"),
            "case name leaked into the serialized InferenceInput: {serialized}"
        );
        assert!(
            !input.prompt.contains("LEAK_MARKER_CASE_NAME"),
            "case name leaked into the rendered prompt: {}",
            input.prompt
        );
        // The sidecar, meanwhile, is exactly where the name belongs.
        assert_eq!(sidecar.case_name, "LEAK_MARKER_CASE_NAME_7f3a");
    }

    #[test]
    fn inference_input_never_carries_the_category() {
        let case = leaky_case();
        let (input, sidecar) = split_case(&case);

        let serialized = serde_json::to_string(&input).unwrap();
        // `Category::AmbiguousAdversarial`'s snake_case serde rendering.
        assert!(
            !serialized.contains("ambiguous_adversarial"),
            "category leaked into the serialized InferenceInput: {serialized}"
        );
        assert!(
            !input.prompt.contains("ambiguous_adversarial"),
            "category leaked into the rendered prompt: {}",
            input.prompt
        );
        assert_eq!(sidecar.category, Category::AmbiguousAdversarial);
    }

    #[test]
    fn inference_input_never_carries_the_expected_decision() {
        let case = leaky_case();
        let (input, sidecar) = split_case(&case);

        let serialized = serde_json::to_string(&input).unwrap();
        // `AbstainCause::CandidateMissing`'s snake_case serde rendering —
        // distinctive enough it would only appear via a genuine leak (the
        // fixed instruction template never mentions abstain causes).
        assert!(
            !serialized.contains("candidate_missing"),
            "gold abstain cause leaked into the serialized InferenceInput: {serialized}"
        );
        assert!(
            !input.prompt.contains("candidate_missing"),
            "gold abstain cause leaked into the rendered prompt: {}",
            input.prompt
        );
        assert_eq!(
            sidecar.expected.decision,
            InferenceDecision::Abstain {
                cause: AbstainCause::CandidateMissing
            }
        );
    }

    #[test]
    fn inference_input_still_carries_the_legitimate_retrieval_surface() {
        // The retrieved top-k's ids/distances are NOT a leak — a real
        // endpoint sees exactly this. Only the gold LABEL (which one is
        // correct) is secret, not the candidate set itself.
        let case = leaky_case();
        let (input, _sidecar) = split_case(&case);

        assert_eq!(input.retrieved.len(), 1);
        assert_eq!(input.allowed_ids, vec![schema_id(1)]);
        assert!(input.prompt.contains(schema_id(1).as_str()));
    }

    #[test]
    fn split_corpus_preserves_order_and_pairing() {
        let mut a = leaky_case();
        a.name = "case_a".to_string();
        let mut b = leaky_case();
        b.name = "case_b".to_string();
        let corpus = vec![a, b];

        let (inputs, sidecars) = split_corpus(&corpus);
        assert_eq!(inputs.len(), 2);
        assert_eq!(sidecars.len(), 2);
        assert_eq!(sidecars[0].case_name, "case_a");
        assert_eq!(sidecars[1].case_name, "case_b");
    }
}
