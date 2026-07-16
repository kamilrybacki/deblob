//! Evaluation-tier assignment (spec §6: "Three evaluation tiers: (1)
//! in-domain temporal (old->new same source); (2) cross-source (unseen
//! repos/streams); (3) cross-corpus OOD (train GitHub/Wikimedia, audit
//! DEBS/TLC/GDELT)").
//!
//! `deblob_eval::EvalCase::partition` is a fixed `Train`/`Test` enum (owned
//! by `deblob-eval`, not extended here — additive-only, spec §"Additive;
//! cycle-free") that cannot express three tiers. [`Tier`] is the parallel,
//! richer split concept this crate needs; [`assign_tiers`] produces one
//! [`TieredCase`] per input case, carrying BOTH the tier and (for tiers 1/2,
//! where a Train/Test split is meaningful) the `EvalCase::partition` a
//! caller should use.
//!
//! **Never random.** [`assign_tiers`] splits by family + chronological
//! order + declared source — the same near-dup-before-split discipline
//! `deblob_eval::corpus`'s own seed-corpus tests enforce
//! (`partitions_present`'s "no schema id leaked across the train/test
//! partition"): every one of a family's cases (its near-dup cluster, since
//! two ingested records of the SAME family/version are near-duplicates by
//! construction) is clustered BEFORE any split decision, so a single
//! family can never straddle two tiers, and within tier 1's in-domain
//! split, a family's cases can never straddle Train and Test either.

use std::collections::BTreeMap;

use deblob_eval::{EvalCase, Partition};

use super::FamilyPool;

/// The three spec §6 evaluation tiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Tier {
    /// Same source (GitHub or Wikimedia) the corpus was pooled from,
    /// chronologically old (`Train`) vs new (`Test`) within the SAME
    /// family.
    InDomainTemporal,
    /// A family/stream never seen in tier 1's `Train` half at all —
    /// simulates an unseen repo/stream from the SAME kind of source.
    CrossSource,
    /// Reserved for genuinely out-of-corpus audit data (spec §6:
    /// "train GitHub/Wikimedia, audit DEBS/TLC/GDELT") — no loader in this
    /// crate ingests a secondary/OOD corpus yet (spec §6 lists those as
    /// "OOD audit only", not a Task 2 deliverable), so [`assign_tiers`]
    /// never itself PRODUCES this tier; it exists so a caller who DOES
    /// bring in a secondary corpus later has a slot to mark it into
    /// without inventing a fourth concept.
    CrossCorpusOod,
}

/// One case plus the [`Tier`] + `Partition` [`assign_tiers`] chose for it.
#[derive(Debug, Clone)]
pub struct TieredCase<'a> {
    pub case: &'a EvalCase,
    pub tier: Tier,
    pub partition: Partition,
    /// The source-native family the case belongs to — `source_key` from
    /// [`FamilyMember`]-derived pooling, evaluator-only bookkeeping (never
    /// re-exposed to an `Arm`), kept here so a caller can group/report by
    /// family without re-deriving it.
    pub family_key: String,
}

/// Splits `cases` (assumed built alongside `pool`, e.g.
/// [`super::IngestedCorpus::cases`]/`.pool`) into tiers 1/2 deterministically:
///
/// 1. Group cases by `family_key` (`pool.family_id_of(gold).as_str()`) —
///    every family is one atomic cluster (spec: "near-dup/sibling clusters
///    never cross train/test").
/// 2. Sort families by their EARLIEST case's `order_key` (or, if not
///    provided, source declaration order) — deterministic, not random.
/// 3. The first `cross_source_family_fraction` (rounded down, at least one
///    if any families exist and the fraction is nonzero) of families,
///    ordered LAST-discovered-first, are held out whole as [`Tier::CrossSource`]
///    (a family literally never seen in tier 1's `Train` half — simulating
///    an unseen repo/stream).
/// 4. Every remaining family's cases are tier 1 ([`Tier::InDomainTemporal`]),
///    split chronologically within the family: the earlier
///    `in_domain_train_fraction` become `Partition::Train`, the rest
///    `Partition::Test` — never splitting a family's cases across BOTH
///    tiers, only within tier 1 across Train/Test.
///
/// `order_key_of` extracts each case's chronological key (the same
/// `created_at#id` / `dt#id` string a loader used to build `pool` — callers
/// pass a closure since `EvalCase` itself carries no timestamp field).
pub fn assign_tiers<'a>(
    cases: &'a [EvalCase],
    pool: &FamilyPool,
    order_key_of: impl Fn(&EvalCase) -> String,
    cross_source_family_fraction: f64,
    in_domain_train_fraction: f64,
) -> Vec<TieredCase<'a>> {
    // Group indices by family_key, preserving first-seen order for the
    // family-level sort below.
    let mut family_order: Vec<String> = Vec::new();
    let mut by_family: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (idx, case) in cases.iter().enumerate() {
        let family_key = case
            .expected
            .gold_schema_id
            .as_ref()
            .and_then(|gold| pool.family_id_of(gold))
            .map(|f| f.as_str().to_string())
            .unwrap_or_else(|| format!("__no_gold_family__/{}", case.name));
        if !by_family.contains_key(&family_key) {
            family_order.push(family_key.clone());
        }
        by_family.entry(family_key).or_default().push(idx);
    }

    // Deterministic family ordering: by each family's earliest order_key,
    // tie-broken by the family_key string itself (never HashMap order).
    let mut families: Vec<&String> = family_order.iter().collect();
    families.sort_by(|a, b| {
        let a_min = by_family[*a]
            .iter()
            .map(|&i| order_key_of(&cases[i]))
            .min()
            .unwrap_or_default();
        let b_min = by_family[*b]
            .iter()
            .map(|&i| order_key_of(&cases[i]))
            .min()
            .unwrap_or_default();
        a_min.cmp(&b_min).then_with(|| a.cmp(b))
    });

    let cross_source_count =
        ((families.len() as f64) * cross_source_family_fraction).floor() as usize;
    let cross_source_count = cross_source_count.min(families.len());
    // Hold out the CHRONOLOGICALLY LATEST families whole — "a family never
    // seen in tier 1's Train half" reads most naturally as "the newest
    // families we haven't built history for yet", not an arbitrary slice.
    let cross_source_keys: std::collections::BTreeSet<&String> = families
        .iter()
        .rev()
        .take(cross_source_count)
        .copied()
        .collect();

    let mut out = Vec::with_capacity(cases.len());
    for family_key in &families {
        let indices = &by_family[*family_key];
        if cross_source_keys.contains(family_key) {
            for &idx in indices {
                out.push(TieredCase {
                    case: &cases[idx],
                    tier: Tier::CrossSource,
                    // Cross-source families are audited wholesale, never
                    // trained on — `Test` is the only sound `Partition`.
                    partition: Partition::Test,
                    family_key: (*family_key).clone(),
                });
            }
            continue;
        }

        // In-domain temporal: sort this family's OWN cases chronologically
        // and split old -> Train, new -> Test.
        let mut sorted_indices = indices.clone();
        sorted_indices.sort_by_key(|&i| order_key_of(&cases[i]));
        let train_count =
            ((sorted_indices.len() as f64) * in_domain_train_fraction).ceil() as usize;
        let train_count = train_count.min(sorted_indices.len());
        for (rank, &idx) in sorted_indices.iter().enumerate() {
            let partition = if rank < train_count {
                Partition::Train
            } else {
                Partition::Test
            };
            out.push(TieredCase {
                case: &cases[idx],
                tier: Tier::InDomainTemporal,
                partition,
                family_key: (*family_key).clone(),
            });
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::corpus::github_archive::{self, GithubEvent};
    use std::fs;
    use std::path::Path;

    fn fixture(name: &str) -> String {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(name);
        fs::read_to_string(&path).unwrap()
    }

    fn sample_events() -> Vec<GithubEvent> {
        github_archive::parse_fixture(&fixture("github_archive_sample.json")).unwrap()
    }

    fn order_key_of_github(case: &EvalCase) -> String {
        // Mirrors `github_archive::ingest`'s own order_key construction —
        // the event id suffix on `case.name` (`gh_<type>_<id>`) lets us
        // recover it without re-parsing the fixture.
        case.name.rsplit('_').next().unwrap_or_default().to_string()
    }

    #[test]
    fn no_family_straddles_two_tiers() {
        let events = sample_events();
        let corpus = github_archive::ingest(&events, 3).unwrap();
        let tiered = assign_tiers(&corpus.cases, &corpus.pool, order_key_of_github, 0.5, 0.5);

        let mut tier_of_family: BTreeMap<String, Tier> = BTreeMap::new();
        for tc in &tiered {
            if let Some(existing) = tier_of_family.get(&tc.family_key) {
                assert_eq!(
                    *existing, tc.tier,
                    "family {} appears in two different tiers",
                    tc.family_key
                );
            } else {
                tier_of_family.insert(tc.family_key.clone(), tc.tier);
            }
        }
    }

    #[test]
    fn no_family_straddles_train_and_test_within_cross_source() {
        let events = sample_events();
        let corpus = github_archive::ingest(&events, 3).unwrap();
        let tiered = assign_tiers(&corpus.cases, &corpus.pool, order_key_of_github, 1.0, 0.5);

        // With cross_source_family_fraction = 1.0, EVERY family is held out
        // whole as CrossSource/Test — none may appear as Train.
        assert!(tiered.iter().all(|tc| tc.tier == Tier::CrossSource));
        assert!(tiered.iter().all(|tc| tc.partition == Partition::Test));
    }

    #[test]
    fn assignment_is_deterministic_across_calls() {
        let events = sample_events();
        let corpus = github_archive::ingest(&events, 3).unwrap();
        let a = assign_tiers(&corpus.cases, &corpus.pool, order_key_of_github, 0.3, 0.5);
        let b = assign_tiers(&corpus.cases, &corpus.pool, order_key_of_github, 0.3, 0.5);
        let a_keys: Vec<(String, Tier, Partition)> = a
            .iter()
            .map(|tc| (tc.case.name.clone(), tc.tier, tc.partition))
            .collect();
        let b_keys: Vec<(String, Tier, Partition)> = b
            .iter()
            .map(|tc| (tc.case.name.clone(), tc.tier, tc.partition))
            .collect();
        assert_eq!(a_keys, b_keys);
    }

    #[test]
    fn in_domain_split_never_puts_all_of_one_familys_cases_on_one_side_when_it_has_multiple() {
        // PushEvent has 2 fixture records; with in_domain_train_fraction
        // 0.5 and cross_source_family_fraction 0.0 (nothing held out),
        // PushEvent's two cases must land as one Train + one Test — the
        // in-domain TEMPORAL split, not a family-level all-or-nothing
        // split (that's tier 2's job).
        let events = sample_events();
        let corpus = github_archive::ingest(&events, 3).unwrap();
        let tiered = assign_tiers(&corpus.cases, &corpus.pool, order_key_of_github, 0.0, 0.5);

        assert!(tiered.iter().all(|tc| tc.tier == Tier::InDomainTemporal));
        let push_family_key = tiered
            .iter()
            .find(|tc| tc.case.name.starts_with("gh_pushevent_"))
            .map(|tc| tc.family_key.clone())
            .unwrap();
        let push_partitions: std::collections::HashSet<Partition> = tiered
            .iter()
            .filter(|tc| tc.family_key == push_family_key)
            .map(|tc| tc.partition)
            .collect();
        assert_eq!(
            push_partitions.len(),
            2,
            "PushEvent's 2 records should split into one Train + one Test case"
        );
    }
}
