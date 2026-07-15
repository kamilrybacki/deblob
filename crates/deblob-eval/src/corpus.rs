//! Golden corpus format + loader (deblob-p2ab Task 6; authoritative shape
//! per `docs/superpowers/plans/deblob-p2ab-hermes-review.md` §
//! "Tasks 6-7 — eval metrics + corpus", which overrides the corresponding
//! "AMEND" marker in `docs/superpowers/plans/2026-07-14-deblob-p2ab.md`).
//!
//! An [`EvalCase`] pairs a redacted [`CandidateProfileView`] + a retrieved
//! top-k (the exact shapes a real endpoint sees via
//! `deblob_slm::InferenceRequest`) with the ground-truth [`Expected`]
//! outcome the eval harness (Task 7) scores an actual decision against.
//! This module owns only the format + loader; Task 7 adds metric
//! computation (recall@k, MRR, false-merge/false-split rate, wrong-valid
//! rate, etc.) over a corpus loaded here.

use std::fs;
use std::path::Path;

use deblob_core::id::SchemaId;
use deblob_slm::{CandidateProfileView, FamilyCandidate, InferenceDecision};
use serde::{Deserialize, Serialize};

/// Hermes' 5-bucket corpus composition (spec: 25% known/exact, 20%
/// compatible drift, 15% incompatible/related-but-unsafe, 20% new family,
/// 20% ambiguous/malformed/adversarial/insufficient). The seed corpus
/// under `corpus/` covers every bucket; percentage adherence at scale is a
/// corpus-growth concern beyond Task 6's seed set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Category {
    KnownExact,
    CompatibleDrift,
    IncompatibleUnsafe,
    NewFamily,
    AmbiguousAdversarial,
}

/// Family/source/time-SEPARATED corpus split. Hermes: never randomly split
/// neighboring versions of one schema across train/test — every case
/// declares which side of the split it belongs to, and
/// [`load_corpus`]/the seed tests enforce that no schema id referenced by
/// a `Train` case is also referenced by a `Test` case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Partition {
    Train,
    Test,
}

/// The ground-truth outcome an [`EvalCase`] is scored against.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Expected {
    /// The correct 3-way decision, in the exact contract shape (Task 1)
    /// an actual endpoint's answer is compared against (match+relation /
    /// new+novelty / abstain+cause).
    pub decision: InferenceDecision,
    /// The correct schema id, for recall@k measurement — "is the gold
    /// family in the retrieved top-k, and at what rank" (Task 7). `None`
    /// when there is no single correct EXISTING schema (a `NewCandidate`
    /// case, or an `Abstain{Ambiguous|InsufficientEvidence}` case with no
    /// singular right answer). May be `Some` even when `gold_rank` is
    /// `None`: the mandatory gold-absent case records the id that SHOULD
    /// have been retrieved but wasn't, so recall@k can still count the
    /// miss.
    pub gold_schema_id: Option<SchemaId>,
    /// The rank (1-based, matching [`FamilyCandidate::rank`]) at which
    /// `gold_schema_id` appears in [`EvalCase::retrieved`], if it appears
    /// at all. `None` iff `gold_schema_id` is `None`, or the gold family
    /// was NOT retrieved (the mandatory gold-absent case).
    pub gold_rank: Option<u32>,
    /// `true` if this case specifically targets a false-MERGE failure mode
    /// — a model tempted to accept a match it should not (Hermes' hard
    /// go-live gate: false merges corrupt identity).
    pub false_merge_trap: bool,
    /// `true` if this case specifically targets a false-SPLIT failure mode
    /// — a model tempted to reject/miss a match it should accept (Hermes:
    /// false splits reduce coverage but are repairable).
    pub false_split_trap: bool,
}

/// One golden corpus case: a redacted candidate + its retrieved top-k (the
/// exact `InferenceRequest` shape a real endpoint sees) plus the ground
/// truth the eval harness (Task 7) scores an actual decision against.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvalCase {
    pub name: String,
    pub category: Category,
    pub candidate: CandidateProfileView,
    pub retrieved: Vec<FamilyCandidate>,
    pub expected: Expected,
    pub partition: Partition,
}

/// Failures loading/validating the golden corpus.
#[derive(Debug, thiserror::Error)]
pub enum EvalError {
    #[error("failed to read corpus directory {path}: {source}")]
    ReadDir {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read corpus case file {path}: {source}")]
    ReadFile {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse corpus case file {path}: {source}")]
    Parse {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("corpus case {name} ({path}) is invalid: {reason}")]
    Invalid {
        name: String,
        path: String,
        reason: String,
    },
}

impl EvalCase {
    /// Validates internal self-consistency of one case's `expected` block
    /// against its own `retrieved` top-k. This is a corpus-AUTHORING
    /// sanity check (run by [`load_corpus`] and the seed-case tests) — it
    /// does NOT validate a real model response, which is Task 7's job.
    ///
    /// Checks:
    /// - A `MatchSchema` decision's `schema_id` must be present in
    ///   `retrieved` (the real contract enforces this via the id
    ///   allow-list; a corpus case claiming otherwise is malformed).
    /// - If `gold_schema_id` is set, the decision must be an accepted
    ///   match (per [`InferenceDecision::is_accepted_match`]): `Exact` or
    ///   `CompatibleDrift`. `IncompatibleSimilarity` is never an accepted
    ///   match — there is no correct family to merge into.
    /// - Otherwise, if both a `MatchSchema` decision and `gold_schema_id`
    ///   are present, they must name the same schema.
    /// - `gold_rank`, if present, must match the rank at which
    ///   `gold_schema_id` actually appears in `retrieved` — and
    ///   `gold_rank` must be absent whenever `gold_schema_id` is absent
    ///   from `retrieved` (including when `gold_schema_id` itself is
    ///   `None`).
    pub fn validate(&self) -> Result<(), String> {
        let retrieved_rank_of = |id: &SchemaId| {
            self.retrieved
                .iter()
                .find(|c| &c.schema_id == id)
                .map(|c| c.rank)
        };

        if let InferenceDecision::MatchSchema {
            schema_id,
            relation: _,
        } = &self.expected.decision
        {
            if retrieved_rank_of(schema_id).is_none() {
                return Err(format!(
                    "expected MatchSchema({}) but that schema_id is not in `retrieved`",
                    schema_id.as_str()
                ));
            }
            if !self.expected.decision.is_accepted_match() {
                if self.expected.gold_schema_id.is_some() {
                    return Err(
                        "IncompatibleSimilarity is never an accepted match; gold_schema_id \
                         must be None"
                            .to_string(),
                    );
                }
            } else if let Some(gold_id) = &self.expected.gold_schema_id {
                if gold_id != schema_id {
                    return Err(format!(
                        "gold_schema_id ({}) disagrees with the expected MatchSchema \
                         schema_id ({})",
                        gold_id.as_str(),
                        schema_id.as_str()
                    ));
                }
            }
        }

        match (&self.expected.gold_schema_id, self.expected.gold_rank) {
            (Some(gold_id), claimed_rank) => {
                let actual_rank = retrieved_rank_of(gold_id);
                if claimed_rank != actual_rank {
                    return Err(format!(
                        "gold_rank ({claimed_rank:?}) disagrees with the rank at which \
                         gold_schema_id actually appears in `retrieved` ({actual_rank:?})"
                    ));
                }
            }
            (None, Some(claimed_rank)) => {
                return Err(format!(
                    "gold_rank ({claimed_rank}) is set but gold_schema_id is None"
                ));
            }
            (None, None) => {}
        }

        Ok(())
    }
}

/// Loads and validates every `*.json` case file directly under `dir`
/// (non-recursive), sorted by filename for a deterministic load order.
/// Fails on the first unreadable file, parse error, or
/// [`EvalCase::validate`] failure.
pub fn load_corpus(dir: impl AsRef<Path>) -> Result<Vec<EvalCase>, EvalError> {
    let dir = dir.as_ref();
    let mut paths: Vec<_> = fs::read_dir(dir)
        .map_err(|source| EvalError::ReadDir {
            path: dir.display().to_string(),
            source,
        })?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("json"))
        .collect();
    paths.sort();

    let mut cases = Vec::with_capacity(paths.len());
    for path in paths {
        let raw = fs::read_to_string(&path).map_err(|source| EvalError::ReadFile {
            path: path.display().to_string(),
            source,
        })?;
        let case: EvalCase = serde_json::from_str(&raw).map_err(|source| EvalError::Parse {
            path: path.display().to_string(),
            source,
        })?;
        if let Err(reason) = case.validate() {
            return Err(EvalError::Invalid {
                name: case.name.clone(),
                path: path.display().to_string(),
                reason,
            });
        }
        cases.push(case);
    }

    Ok(cases)
}

#[cfg(test)]
mod tests {
    use super::*;
    use deblob_slm::AbstainCause;
    use std::collections::{HashMap, HashSet};

    fn corpus_dir() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus")
    }

    #[test]
    fn loads_all_seed_cases() {
        let cases = load_corpus(corpus_dir()).expect("seed corpus should load without error");
        assert!(
            cases.len() >= 20,
            "expected the mandated breadth of seed cases (traps + mandatory + composition), \
             got {}",
            cases.len()
        );
    }

    #[test]
    fn every_case_expected_is_valid() {
        // load_corpus already runs `validate()` on every case and would
        // have failed above if any case were invalid; this test asserts
        // the same thing explicitly (and independently of load ordering)
        // so a future change to `load_corpus` that stops validating can't
        // silently defeat this guarantee.
        let cases = load_corpus(corpus_dir()).unwrap();
        for case in &cases {
            case.validate()
                .unwrap_or_else(|e| panic!("case {} failed validation: {}", case.name, e));
        }
    }

    #[test]
    fn false_merge_and_false_split_cases_present_and_distinct() {
        let cases = load_corpus(corpus_dir()).unwrap();
        let merge_names: HashSet<&str> = cases
            .iter()
            .filter(|c| c.expected.false_merge_trap)
            .map(|c| c.name.as_str())
            .collect();
        let split_names: HashSet<&str> = cases
            .iter()
            .filter(|c| c.expected.false_split_trap)
            .map(|c| c.name.as_str())
            .collect();

        assert!(
            !merge_names.is_empty(),
            "expected at least one false-merge trap case"
        );
        assert!(
            !split_names.is_empty(),
            "expected at least one false-split trap case"
        );
        assert!(
            merge_names.is_disjoint(&split_names),
            "a case should target false-merge OR false-split, not both at once: {:?}",
            merge_names.intersection(&split_names).collect::<Vec<_>>()
        );
    }

    #[test]
    fn composition_covers_all_categories() {
        let cases = load_corpus(corpus_dir()).unwrap();
        let mut counts: HashMap<Category, usize> = HashMap::new();
        for c in &cases {
            *counts.entry(c.category).or_default() += 1;
        }
        for cat in [
            Category::KnownExact,
            Category::CompatibleDrift,
            Category::IncompatibleUnsafe,
            Category::NewFamily,
            Category::AmbiguousAdversarial,
        ] {
            assert!(
                counts.get(&cat).copied().unwrap_or(0) >= 1,
                "missing seed coverage for category {cat:?}"
            );
        }
    }

    #[test]
    fn gold_rank_cases_present() {
        let cases = load_corpus(corpus_dir()).unwrap();
        let has_rank = |r: u32| cases.iter().any(|c| c.expected.gold_rank == Some(r));
        assert!(has_rank(1), "expected a seed case with gold at rank 1");
        assert!(has_rank(2), "expected a seed case with gold at rank 2");
        assert!(has_rank(3), "expected a seed case with gold at rank 3");

        let has_gold_absent = cases.iter().any(|c| {
            c.expected.gold_rank.is_none()
                && c.expected.gold_schema_id.is_some()
                && matches!(
                    c.expected.decision,
                    InferenceDecision::Abstain {
                        cause: AbstainCause::CandidateMissing
                    }
                )
        });
        assert!(
            has_gold_absent,
            "expected a gold-absent seed case expecting Abstain{{cause: CandidateMissing}}"
        );
    }

    #[test]
    fn partitions_present() {
        let cases = load_corpus(corpus_dir()).unwrap();
        assert!(
            cases.iter().any(|c| c.partition == Partition::Train),
            "expected at least one Train-partition case"
        );
        assert!(
            cases.iter().any(|c| c.partition == Partition::Test),
            "expected at least one Test-partition case"
        );

        // No exact case-name duplication.
        let mut seen_names: HashSet<&str> = HashSet::new();
        for c in &cases {
            assert!(
                seen_names.insert(c.name.as_str()),
                "duplicate case name across the corpus: {}",
                c.name
            );
        }

        // Family/source separation: no schema id referenced (via the
        // retrieved top-k or gold_schema_id) by a Train-partition case may
        // also be referenced by a Test-partition case — Hermes: never
        // randomly split neighboring versions of one schema across
        // train/test.
        let mut train_ids: HashSet<String> = HashSet::new();
        let mut test_ids: HashSet<String> = HashSet::new();
        for c in &cases {
            let target = match c.partition {
                Partition::Train => &mut train_ids,
                Partition::Test => &mut test_ids,
            };
            for r in &c.retrieved {
                target.insert(r.schema_id.as_str().to_string());
            }
            if let Some(gold) = &c.expected.gold_schema_id {
                target.insert(gold.as_str().to_string());
            }
        }
        let overlap: Vec<_> = train_ids.intersection(&test_ids).collect();
        assert!(
            overlap.is_empty(),
            "schema ids leaked across the train/test partition: {overlap:?}"
        );
    }
}
