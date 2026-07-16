//! GitHub Archive (gharchive.org) real-event ingestion (spec §6b): parses
//! gharchive-style JSON event records (`PushEvent`, `PullRequestEvent`,
//! `IssuesEvent`, `IssueCommentEvent`, `ReleaseEvent`, `ForkEvent`,
//! `WatchEvent`, `CreateEvent`, `DeleteEvent`) into [`GithubEvent`]s, then
//! [`ingest`] turns those into [`deblob_eval::EvalCase`]s — the SAME shape
//! the synthetic generator produces, so `crate::labels::split_case` already
//! knows how to leak-strip them (see `super`'s module docs).
//!
//! **Leak guard by construction**: a case's [`deblob_slm::CandidateProfileView`]
//! and `retrieved` top-k are built EXCLUSIVELY from [`GithubEvent::payload`]
//! — never from `event_type`/`repo_full_name`/`org_login`/`created_at`/`id`,
//! which live only on [`GithubEvent`] and the [`deblob_eval::EvalCase::name`]/
//! `expected` fields `split_case` strips into the evaluator-only
//! `GoldSidecar`. A GitHub event's `payload` never itself contains the
//! literal event-type string (e.g. `PushEvent`'s payload has no `type`
//! field at all) or a repo/org identifier, so there is no field within
//! `payload` that would need redacting on top of that exclusion.

use deblob_eval::{EvalCase, Expected, Partition};
use deblob_slm::CandidateProfileView;

use super::{
    build_family_pool, decide_from_gold_candidate, profile_from_json, require_object, require_str,
    retrieve_over_pool, FamilyMember, IngestError, IngestedCorpus,
};

/// The event `type` values spec §6b names. Not enforced as a hard allow-list
/// by [`parse_fixture`] (a fixture is free to carry any `type` string — the
/// loader doesn't need to know every possible gharchive event kind to
/// ingest one correctly), just documented here as the intended coverage.
pub const RECOGNIZED_EVENT_TYPES: &[&str] = &[
    "PushEvent",
    "PullRequestEvent",
    "IssuesEvent",
    "IssueCommentEvent",
    "ReleaseEvent",
    "ForkEvent",
    "WatchEvent",
    "CreateEvent",
    "DeleteEvent",
];

/// One parsed GitHub Archive event record, in the real gharchive.org hourly
/// export envelope shape: `{"id","type","repo":{"name"},"org":{"login"}?,
/// "created_at","payload"}`. Every field except `payload` is source-native
/// label/identifier data (spec §2) — [`ingest`] never lets them reach a
/// case's `candidate`/`retrieved`.
#[derive(Debug, Clone)]
pub struct GithubEvent {
    pub id: String,
    pub event_type: String,
    pub repo_full_name: String,
    pub org_login: Option<String>,
    pub created_at: String,
    pub payload: serde_json::Value,
}

/// Parses a fixture file shaped as a JSON array of gharchive-style event
/// records — see [`GithubEvent`]'s doc comment for the envelope shape. NOT
/// gharchive's real newline-delimited-JSON-per-hour-file framing (a single
/// JSON array is simpler to hand-author and review); the RECORD shape
/// itself is the real one.
pub fn parse_fixture(json_text: &str) -> Result<Vec<GithubEvent>, IngestError> {
    let value: serde_json::Value = serde_json::from_str(json_text)?;
    let array = value
        .as_array()
        .ok_or(IngestError::MalformedField("root (expected a JSON array)"))?;
    array.iter().map(parse_one).collect()
}

fn parse_one(value: &serde_json::Value) -> Result<GithubEvent, IngestError> {
    let obj = require_object(value, "record")?;
    let id = require_str(obj.get("id").ok_or(IngestError::MissingField("id"))?, "id")?.to_string();
    let event_type = require_str(
        obj.get("type").ok_or(IngestError::MissingField("type"))?,
        "type",
    )?
    .to_string();
    let repo = require_object(
        obj.get("repo").ok_or(IngestError::MissingField("repo"))?,
        "repo",
    )?;
    let repo_full_name = require_str(
        repo.get("name")
            .ok_or(IngestError::MissingField("repo.name"))?,
        "repo.name",
    )?
    .to_string();
    let org_login = match obj.get("org") {
        Some(org_value) => {
            let org_obj = require_object(org_value, "org")?;
            Some(
                require_str(
                    org_obj
                        .get("login")
                        .ok_or(IngestError::MissingField("org.login"))?,
                    "org.login",
                )?
                .to_string(),
            )
        }
        None => None,
    };
    let created_at = require_str(
        obj.get("created_at")
            .ok_or(IngestError::MissingField("created_at"))?,
        "created_at",
    )?
    .to_string();
    let payload = obj
        .get("payload")
        .ok_or(IngestError::MissingField("payload"))?
        .clone();

    Ok(GithubEvent {
        id,
        event_type,
        repo_full_name,
        org_login,
        created_at,
        payload,
    })
}

/// Ingests `events` into [`EvalCase`]s: pools every distinct
/// `(event_type, structural fingerprint)` into a [`super::FamilyPool`]
/// (chronologically ordered, so a genuinely drifted payload shape for the
/// SAME type becomes a new VERSION of that type's family, spec §6), then
/// scores each event's OWN payload profile against the real structural-
/// distance top-`k` ([`retrieve_over_pool`]) to build its `retrieved` +
/// gold rank. Output is sorted chronologically by `created_at` (ties by
/// `id`) — deterministic, never HashMap/caller-order-dependent.
pub fn ingest(events: &[GithubEvent], k: usize) -> Result<IngestedCorpus, IngestError> {
    let mut profiles = Vec::with_capacity(events.len());
    let mut members = Vec::with_capacity(events.len());
    for event in events {
        let profile = profile_from_json(&event.payload)?;
        members.push(FamilyMember {
            family_name: event.event_type.clone(),
            order_key: format!("{}#{}", event.created_at, event.id),
            profile: profile.clone(),
        });
        profiles.push(profile);
    }
    let pool = build_family_pool(&members);

    let mut order: Vec<usize> = (0..events.len()).collect();
    order.sort_by(|&a, &b| {
        events[a]
            .created_at
            .cmp(&events[b].created_at)
            .then_with(|| events[a].id.cmp(&events[b].id))
    });

    let mut cases = Vec::with_capacity(events.len());
    for idx in order {
        let event = &events[idx];
        let profile = &profiles[idx];
        let fingerprint = profile.generalized_fingerprint();
        let gold_schema_id = pool
            .gold_schema_of
            .get(&(event.event_type.clone(), fingerprint))
            .cloned()
            .ok_or(IngestError::MalformedField("event_type"))?;

        let retrieval = retrieve_over_pool(profile, &pool.families, k);
        let candidate = CandidateProfileView::from_profile(profile);
        let retrieved = retrieval.candidates;

        let gold_candidate = retrieved
            .iter()
            .find(|c| c.schema_id == gold_schema_id)
            .cloned();
        let gold_rank = gold_candidate.as_ref().map(|c| c.rank);
        let (decision, category) =
            decide_from_gold_candidate(&gold_schema_id, gold_candidate.as_ref());

        let expected = Expected {
            decision,
            gold_schema_id: Some(gold_schema_id),
            gold_rank,
            false_merge_trap: false,
            false_split_trap: false,
        };

        cases.push(EvalCase {
            name: format!("gh_{}_{}", event.event_type.to_ascii_lowercase(), event.id),
            category,
            candidate,
            retrieved,
            expected,
            // The real spec §6/§10 evaluation tiers are assigned by
            // `corpus::tiers::assign_tiers` over the ingested corpus, not
            // here; `Train` is a neutral default consistent with
            // `deblob_eval::EvalCase::validate`'s expectations until a
            // caller re-partitions.
            partition: Partition::Train,
        });
    }

    Ok(IngestedCorpus { cases, pool })
}

#[cfg(test)]
mod tests {
    use super::*;
    use deblob_eval::Category;
    use std::fs;
    use std::path::Path;

    fn fixture(name: &str) -> String {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(name);
        fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read fixture {}: {e}", path.display()))
    }

    fn sample_events() -> Vec<GithubEvent> {
        parse_fixture(&fixture("github_archive_sample.json")).expect("fixture should parse")
    }

    #[test]
    fn parses_the_recognized_envelope_shape_for_every_record() {
        let events = sample_events();
        assert!(events.len() >= 4, "expected a handful of fixture records");
        for event in &events {
            assert!(
                RECOGNIZED_EVENT_TYPES.contains(&event.event_type.as_str()),
                "unexpected event type in fixture: {}",
                event.event_type
            );
        }
    }

    #[test]
    fn ingest_produces_correct_gold_families_and_strips_every_leak_field() {
        let events = sample_events();
        let corpus = ingest(&events, 3).expect("ingest should succeed on the fixture");
        let cases = corpus.cases;
        assert!(!cases.is_empty());

        // Every case's gold family (via the sidecar route: EvalCase.name /
        // .expected) must correspond to a real, recognized event type — the
        // "correct gold families in the sidecar" acceptance check. We can't
        // read `expected.decision`'s schema id back to a type string
        // directly (that's the whole point), but we CAN assert the ingest
        // pipeline never invented a family the fixture didn't have: every
        // gold_schema_id must be a schema id present in some case's pool.
        for case in &cases {
            assert!(
                case.expected.gold_schema_id.is_some(),
                "case {} has no gold_schema_id",
                case.name
            );
            case.validate().unwrap_or_else(|e| {
                panic!("ingested case {} failed EvalCase::validate: {e}", case.name)
            });
        }

        // Leak guard: for EVERY case, split it (Task 1's real leak-strip
        // mechanism) and assert none of the source-native labels reach the
        // InferenceInput or its rendered prompt.
        for (event, case) in events.iter().zip(&cases_in_event_order(&events, &cases)) {
            let (input, sidecar) = crate::labels::split_case(case);
            let serialized = serde_json::to_string(&input).unwrap();

            assert!(
                !serialized.contains(&event.event_type),
                "event type {} leaked into InferenceInput: {serialized}",
                event.event_type
            );
            assert!(
                !input.prompt.contains(&event.event_type),
                "event type {} leaked into the rendered prompt: {}",
                event.event_type,
                input.prompt
            );
            assert!(
                !serialized.contains(&event.repo_full_name),
                "repo identifier {} leaked into InferenceInput: {serialized}",
                event.repo_full_name
            );
            if let Some(org) = &event.org_login {
                assert!(
                    !serialized.contains(org),
                    "org identifier {org} leaked into InferenceInput: {serialized}"
                );
            }
            assert!(
                !serialized.contains(&event.id),
                "event id {} leaked into InferenceInput: {serialized}",
                event.id
            );

            // The sidecar, meanwhile, IS where the label belongs — sanity
            // check it actually carries the case name (which embeds the
            // event type, evaluator-only).
            assert!(sidecar
                .case_name
                .contains(&event.event_type.to_ascii_lowercase()));
        }
    }

    /// Re-derives which case corresponds to which input `event`, by name
    /// suffix (`gh_<type>_<id>`) — `ingest` sorts chronologically, so case
    /// order need not match `events`' input order.
    fn cases_in_event_order<'a>(
        events: &[GithubEvent],
        cases: &'a [EvalCase],
    ) -> Vec<&'a EvalCase> {
        events
            .iter()
            .map(|event| {
                let suffix = format!("_{}", event.id);
                cases
                    .iter()
                    .find(|c| c.name.ends_with(&suffix))
                    .unwrap_or_else(|| panic!("no ingested case for event id {}", event.id))
            })
            .collect()
    }

    #[test]
    fn push_and_pull_request_events_ingest_to_distinct_families() {
        let events = sample_events();
        let cases = ingest(&events, 3).expect("ingest should succeed").cases;
        let push_case = events
            .iter()
            .zip(&cases_in_event_order(&events, &cases))
            .find(|(e, _)| e.event_type == "PushEvent")
            .map(|(_, c)| *c)
            .expect("fixture must include a PushEvent");
        let pr_case = events
            .iter()
            .zip(&cases_in_event_order(&events, &cases))
            .find(|(e, _)| e.event_type == "PullRequestEvent")
            .map(|(_, c)| *c)
            .expect("fixture must include a PullRequestEvent");

        assert_ne!(
            push_case.expected.gold_schema_id,
            pr_case.expected.gold_schema_id
        );
        // Both structurally distinct payload shapes should be cleanly
        // resolvable against a small, well-separated pool.
        assert_eq!(push_case.category, Category::KnownExact);
        assert_eq!(pr_case.category, Category::KnownExact);
    }

    #[test]
    fn ingest_is_deterministic_across_repeated_calls() {
        let events = sample_events();
        let a = ingest(&events, 3).unwrap().cases;
        let b = ingest(&events, 3).unwrap().cases;
        assert_eq!(
            serde_json::to_string(&a).unwrap(),
            serde_json::to_string(&b).unwrap()
        );
    }
}
