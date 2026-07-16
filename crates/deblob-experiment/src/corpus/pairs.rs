//! Difficult-pair tagging (spec §6: "Difficult-pair categories enumerated
//! (same-family-version-change, same-structure-different-semantics,
//! different-structure-same-family, optional-field-add, rename,
//! type-widen/narrow, value-shape-drift, envelope-same-payload-different,
//! insufficient-obs, gold-absent-from-topk)").
//!
//! [`tag_cases`] tags every ingested [`EvalCase`] (from `github_archive`/
//! `wikimedia`) with whichever of those categories are MECHANICALLY
//! derivable from data this crate already computed during ingestion — the
//! case's own `expected` block, its real top-k `retrieved` (structural
//! distance, `deblob::retrieval::retrieve_topk`, reused exactly as
//! `super::retrieve_over_pool` calls it), and the [`super::FamilyPool`]'s
//! family/version lineage. It never re-reads a stripped label field, and
//! it never invents a tag from the gold answer alone — every tag traces to
//! a concrete retrieval/pool fact.
//!
//! **Honestly out of scope for this task** (documented, not silently
//! dropped, mirroring Task 1's `deterministic_compat_passed` disclosure):
//! `same-structure-different-semantics`, `rename`, `type-widen/narrow`,
//! `value-shape-drift`, and `envelope-same-payload-different` need a
//! field-level semantic diff (which NAME became which, which type widened)
//! that neither the six-component structural-distance scorer nor the
//! generalized-canonical JSON this crate reads exposes directly — that
//! signal would need a dedicated field-alignment pass, out of scope here.
//! `different-structure-same-family` is likewise not derivable: this
//! crate's family identity IS structural-fingerprint identity (§ mod.rs),
//! so "different structure, same family" cannot arise from data this
//! ingestion path produces by construction.

use std::collections::BTreeMap;

use deblob_core::id::SchemaId;
use deblob_eval::EvalCase;
use deblob_slm::CandidateProfileView;

use super::FamilyPool;

// `FamilyId`/`SchemaId` derive `Eq`/`Hash` but not `Ord` (deblob-core), so
// every set/map below keys on the id's `as_str()` string form instead —
// `String`/`&str` are `Ord`, and the string form is the id's canonical
// text representation anyway (no information lost, no risk of collision:
// distinct ids always have distinct `as_str()` text by construction).

/// One derivable difficult-pair/case category (spec §6 subset — see this
/// module's doc comment for what's covered and what's disclosed as
/// out-of-scope).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DifficultPairCategory {
    /// This case's gold family has at least one OTHER ingested case at a
    /// different schema version (same `family_id`, different
    /// `schema_id`) — spec's "same-family-version-change".
    SameFamilyVersionChange,
    /// A `SameFamilyVersionChange` pair where the later version's
    /// top-level field-name set is a strict superset of the earlier
    /// version's — spec's "optional-field-add".
    OptionalFieldAdd,
    /// `candidate.observation_count` is below
    /// `deblob::shadow::POLICY_MIN_OBSERVATIONS` — spec's "insufficient-obs".
    /// Expected to be common for single-fixture-record ingestion (a
    /// freshly observed candidate cluster starts at `observation_count:
    /// 1`), disclosed rather than filtered out.
    InsufficientObservation,
    /// The gold schema id was NOT present anywhere in `retrieved` — spec's
    /// "gold-absent-from-topk", a genuine retrieval-fault trap.
    GoldAbsentFromTopK,
}

/// One ingested case plus every difficult-pair category [`tag_cases`]
/// could mechanically derive for it. `tags` is empty for an
/// unremarkable case — most of the corpus, by construction.
#[derive(Debug, Clone)]
pub struct TaggedCase<'a> {
    pub case: &'a EvalCase,
    pub tags: Vec<DifficultPairCategory>,
}

/// Top-level object-field NAMES of a `FamilyRef::canonical`
/// (`Profile::generalized_canonical_json`) string — the
/// `{"optional":...,"types":[...],"children":{"a":{...},"b":{...}}}` shape
/// `write_generalized_field` emits. Returns an empty set for a
/// non-object-rooted or malformed canonical (defensive; every canonical
/// this crate produces IS well-formed, since it comes straight from
/// `Profile::generalized_canonical_json`).
fn top_level_field_names(canonical: &str) -> std::collections::BTreeSet<String> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(canonical) else {
        return Default::default();
    };
    value
        .get("children")
        .and_then(|c| c.as_object())
        .map(|obj| obj.keys().cloned().collect())
        .unwrap_or_default()
}

/// Tags every case in `cases` (assumed to be exactly the `EvalCase`s
/// [`super::IngestedCorpus::pool`] `pool` was built alongside) with every
/// mechanically derivable category — see this module's doc comment.
pub fn tag_cases<'a>(cases: &'a [EvalCase], pool: &FamilyPool) -> Vec<TaggedCase<'a>> {
    // Group every case's index by its gold family_id (keyed by `as_str()`
    // text — see the module-level Ord note above), so a "does this family
    // have >1 schema version among these cases" check is O(1) per case
    // instead of an O(n^2) rescan.
    let mut by_family: BTreeMap<String, Vec<(usize, SchemaId)>> = BTreeMap::new();
    for (idx, case) in cases.iter().enumerate() {
        let Some(gold) = &case.expected.gold_schema_id else {
            continue;
        };
        let Some(family_id) = pool.family_id_of(gold) else {
            continue;
        };
        by_family
            .entry(family_id.as_str().to_string())
            .or_default()
            .push((idx, gold.clone()));
    }

    // Precompute which family-key groups actually span >1 distinct
    // schema_id — only those are genuine version-change pairs.
    let multi_version_families: std::collections::BTreeSet<String> = by_family
        .iter()
        .filter(|(_, members)| {
            let distinct: std::collections::BTreeSet<&str> =
                members.iter().map(|(_, s)| s.as_str()).collect();
            distinct.len() > 1
        })
        .map(|(f, _)| f.clone())
        .collect();

    cases
        .iter()
        .map(|case| {
            let mut tags = Vec::new();

            if case.expected.gold_schema_id.is_some() && case.expected.gold_rank.is_none() {
                tags.push(DifficultPairCategory::GoldAbsentFromTopK);
            }

            if observation_count_below_floor(&case.candidate) {
                tags.push(DifficultPairCategory::InsufficientObservation);
            }

            if let Some(gold) = &case.expected.gold_schema_id {
                if let Some(family_id) = pool.family_id_of(gold) {
                    let family_key = family_id.as_str().to_string();
                    if multi_version_families.contains(&family_key) {
                        tags.push(DifficultPairCategory::SameFamilyVersionChange);

                        if is_optional_field_add(pool, &by_family[&family_key], gold) {
                            tags.push(DifficultPairCategory::OptionalFieldAdd);
                        }
                    }
                }
            }

            TaggedCase { case, tags }
        })
        .collect()
}

fn observation_count_below_floor(candidate: &CandidateProfileView) -> bool {
    candidate.observation_count < deblob::shadow::POLICY_MIN_OBSERVATIONS
}

/// `true` iff, among the OTHER schema versions this case's family has
/// (`siblings`), at least one EARLIER version's top-level field-name set is
/// a strict subset of this case's own gold version's field-name set — a
/// mechanically-detectable "optional-field-add" signature. Version
/// ordering comes from `FamilyRef::version` (assigned chronologically by
/// [`super::build_family_pool`]), never from schema_id ordering.
fn is_optional_field_add(
    pool: &FamilyPool,
    siblings: &[(usize, SchemaId)],
    this_gold: &SchemaId,
) -> bool {
    let Some(this_ref) = pool.families.iter().find(|f| &f.schema_id == this_gold) else {
        return false;
    };
    let this_fields = top_level_field_names(&this_ref.canonical);

    let distinct_schemas: std::collections::BTreeSet<&str> =
        siblings.iter().map(|(_, s)| s.as_str()).collect();
    distinct_schemas.iter().any(|&other_schema_str| {
        if other_schema_str == this_gold.as_str() {
            return false;
        }
        let Some(other_ref) = pool
            .families
            .iter()
            .find(|f| f.schema_id.as_str() == other_schema_str)
        else {
            return false;
        };
        if other_ref.version.0 >= this_ref.version.0 {
            return false; // only look at strictly EARLIER siblings
        }
        let other_fields = top_level_field_names(&other_ref.canonical);
        !other_fields.is_empty()
            && other_fields.is_subset(&this_fields)
            && other_fields != this_fields
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::corpus::{build_family_pool, profile_from_json, retrieve_over_pool, FamilyMember};
    use deblob_eval::{Category, Expected, Partition};
    use deblob_slm::{AbstainCause, InferenceDecision};

    fn profile_of(json: &str) -> deblob_monoid::Profile {
        profile_from_json(&serde_json::from_str(json).unwrap()).unwrap()
    }

    fn case_from(
        name: &str,
        candidate: CandidateProfileView,
        retrieved: Vec<deblob_slm::FamilyCandidate>,
        gold_schema_id: Option<SchemaId>,
        gold_rank: Option<u32>,
        decision: InferenceDecision,
    ) -> EvalCase {
        EvalCase {
            name: name.to_string(),
            category: Category::AmbiguousAdversarial,
            candidate,
            retrieved,
            expected: Expected {
                decision,
                gold_schema_id,
                gold_rank,
                false_merge_trap: false,
                false_split_trap: false,
            },
            partition: Partition::Train,
        }
    }

    #[test]
    fn same_family_version_change_and_optional_field_add_are_tagged() {
        let v1 = profile_of(r#"{"a": 1, "b": "x"}"#);
        let v2 = profile_of(r#"{"a": 1, "b": "x", "c": true}"#); // strict superset
        let members = vec![
            FamilyMember {
                family_name: "stream.x".to_string(),
                order_key: "1".to_string(),
                profile: v1.clone(),
            },
            FamilyMember {
                family_name: "stream.x".to_string(),
                order_key: "2".to_string(),
                profile: v2.clone(),
            },
        ];
        let pool = build_family_pool(&members);
        let schema_v1 = super::super::schema_id_of(&v1);
        let schema_v2 = super::super::schema_id_of(&v2);

        let retrieved_v1 = retrieve_over_pool(&v1, &pool.families, 5).candidates;
        let retrieved_v2 = retrieve_over_pool(&v2, &pool.families, 5).candidates;
        let rank_v1 = retrieved_v1
            .iter()
            .find(|c| c.schema_id == schema_v1)
            .map(|c| c.rank);
        let rank_v2 = retrieved_v2
            .iter()
            .find(|c| c.schema_id == schema_v2)
            .map(|c| c.rank);

        let case_v1 = case_from(
            "case_v1",
            CandidateProfileView::from_profile(&v1),
            retrieved_v1,
            Some(schema_v1.clone()),
            rank_v1,
            InferenceDecision::MatchSchema {
                schema_id: schema_v1.clone(),
                relation: deblob_slm::Relation::Exact,
            },
        );
        let case_v2 = case_from(
            "case_v2",
            CandidateProfileView::from_profile(&v2),
            retrieved_v2,
            Some(schema_v2.clone()),
            rank_v2,
            InferenceDecision::MatchSchema {
                schema_id: schema_v2.clone(),
                relation: deblob_slm::Relation::Exact,
            },
        );

        let cases = vec![case_v1, case_v2];
        let tagged = tag_cases(&cases, &pool);

        assert!(tagged[0]
            .tags
            .contains(&DifficultPairCategory::SameFamilyVersionChange));
        assert!(tagged[1]
            .tags
            .contains(&DifficultPairCategory::SameFamilyVersionChange));
        // Only v2 (the strict-superset side) is the "add" — v1 didn't add
        // anything relative to any EARLIER sibling.
        assert!(tagged[1]
            .tags
            .contains(&DifficultPairCategory::OptionalFieldAdd));
        assert!(!tagged[0]
            .tags
            .contains(&DifficultPairCategory::OptionalFieldAdd));
    }

    #[test]
    fn gold_absent_from_topk_is_tagged_when_a_small_k_excludes_the_true_family() {
        // Three structurally distinct families; the true gold ("target")
        // is the THIRD-nearest to the query, so k=2 excludes it from the
        // retrieved top-k even though it is genuinely registered.
        let query_shape = r#"{"a": 1, "b": "x", "c": 1}"#;
        let target = profile_of(r#"{"a": 1, "b": "x", "c": 1, "d": "extra_extra_extra"}"#);
        let closer_1 = profile_of(r#"{"a": 1, "b": "x", "c": 1}"#);
        let closer_2 = profile_of(r#"{"a": 1, "b": "y", "c": 2}"#);
        let members = vec![
            FamilyMember {
                family_name: "target".to_string(),
                order_key: "1".to_string(),
                profile: target.clone(),
            },
            FamilyMember {
                family_name: "closer_1".to_string(),
                order_key: "1".to_string(),
                profile: closer_1,
            },
            FamilyMember {
                family_name: "closer_2".to_string(),
                order_key: "1".to_string(),
                profile: closer_2,
            },
        ];
        let pool = build_family_pool(&members);
        let query = profile_of(query_shape);
        let retrieved = retrieve_over_pool(&query, &pool.families, 2).candidates;
        let gold_schema_id = super::super::schema_id_of(&target);
        let gold_rank = retrieved
            .iter()
            .find(|c| c.schema_id == gold_schema_id)
            .map(|c| c.rank);
        assert_eq!(
            gold_rank, None,
            "test setup must place gold outside the k=2 top-k for this assertion to be meaningful"
        );

        let case = case_from(
            "gold_absent_case",
            CandidateProfileView::from_profile(&query),
            retrieved,
            Some(gold_schema_id),
            gold_rank,
            InferenceDecision::Abstain {
                cause: AbstainCause::CandidateMissing,
            },
        );
        let cases = vec![case];
        let tagged = tag_cases(&cases, &pool);
        assert!(tagged[0]
            .tags
            .contains(&DifficultPairCategory::GoldAbsentFromTopK));
    }

    #[test]
    fn single_observation_cases_are_tagged_insufficient_observation() {
        let profile = profile_of(r#"{"a": 1}"#);
        let members = vec![FamilyMember {
            family_name: "only".to_string(),
            order_key: "1".to_string(),
            profile: profile.clone(),
        }];
        let pool = build_family_pool(&members);
        let retrieved = retrieve_over_pool(&profile, &pool.families, 3).candidates;
        let gold = super::super::schema_id_of(&profile);
        let rank = retrieved
            .iter()
            .find(|c| c.schema_id == gold)
            .map(|c| c.rank);

        let candidate = CandidateProfileView::from_profile(&profile);
        assert_eq!(candidate.observation_count, 1); // a single ingested record
        let case = case_from(
            "one_obs",
            candidate,
            retrieved,
            Some(gold.clone()),
            rank,
            InferenceDecision::MatchSchema {
                schema_id: gold,
                relation: deblob_slm::Relation::Exact,
            },
        );
        let cases = vec![case];
        let tagged = tag_cases(&cases, &pool);
        assert!(tagged[0]
            .tags
            .contains(&DifficultPairCategory::InsufficientObservation));
    }

    #[test]
    fn unremarkable_case_gets_no_tags() {
        // A single, well-observed, clearly-resolved case with no siblings.
        let profile = profile_of(r#"{"a": 1}"#);
        let members = vec![FamilyMember {
            family_name: "only".to_string(),
            order_key: "1".to_string(),
            profile: profile.clone(),
        }];
        let pool = build_family_pool(&members);
        let retrieved = retrieve_over_pool(&profile, &pool.families, 3).candidates;
        let gold = super::super::schema_id_of(&profile);
        let rank = retrieved
            .iter()
            .find(|c| c.schema_id == gold)
            .map(|c| c.rank);

        let mut candidate = CandidateProfileView::from_profile(&profile);
        candidate.observation_count = deblob::shadow::POLICY_MIN_OBSERVATIONS; // at the floor, not below it
        let case = case_from(
            "well_observed",
            candidate,
            retrieved,
            Some(gold.clone()),
            rank,
            InferenceDecision::MatchSchema {
                schema_id: gold,
                relation: deblob_slm::Relation::Exact,
            },
        );
        let cases = vec![case];
        let tagged = tag_cases(&cases, &pool);
        assert!(tagged[0].tags.is_empty());
    }
}
