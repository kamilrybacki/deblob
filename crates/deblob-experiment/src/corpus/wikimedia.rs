//! Wikimedia EventStreams real-event ingestion (spec §6b): parses versioned
//! JSON event records (page-create/delete, revision-create, recentchange —
//! any stream carrying a `$schema` + `meta.stream`) into [`WikimediaEvent`]s,
//! then [`ingest`] turns those into [`deblob_eval::EvalCase`]s exactly like
//! `github_archive::ingest` does (see `super`'s module docs — same leak-strip
//! reuse via `crate::labels::split_case`).
//!
//! **Gold family = schema identity, not schema VERSION.** Wikimedia's
//! `meta.stream` (e.g. `"mediawiki.page-create"`) is stable across a
//! stream's schema evolution; `$schema` (e.g.
//! `"/mediawiki/page/create/1.0.0"` vs `".../2.0.0"`) carries the explicit
//! version. [`ingest`] pools by `meta.stream` (the family) and lets
//! [`super::build_family_pool`]'s structural-fingerprint de-duplication
//! discover version boundaries automatically: two records of the SAME
//! stream whose payloads (everything except `$schema`/`meta`) are
//! structurally identical collapse to one pooled version; a genuine
//! schema-evolution step (e.g. a field added in `2.0.0`) becomes a NEW
//! version of the SAME `family_id` — the natural
//! "same-family-different-version" difficult-pair class spec §6 names.
//!
//! **Leak guard by construction**: a case's candidate/retrieved are built
//! from the event with `$schema` AND the entire `meta` object (which
//! carries `stream`/`topic`/`domain`/`uri`/...) removed — never from those
//! fields directly.

use deblob_eval::{EvalCase, Expected, Partition};
use deblob_slm::CandidateProfileView;

use super::{
    build_family_pool, decide_from_gold_candidate, profile_from_json, require_object, require_str,
    retrieve_over_pool, FamilyMember, IngestError, IngestedCorpus,
};

/// One parsed Wikimedia EventStreams record. `schema_uri`/`stream`/`topic`
/// are source-native label/lineage data (spec §2) — [`ingest`] never lets
/// them reach a case's `candidate`/`retrieved`.
#[derive(Debug, Clone)]
pub struct WikimediaEvent {
    /// The full `$schema` value, e.g. `"/mediawiki/page/create/1.0.0"`.
    pub schema_uri: String,
    /// `meta.stream` — the version-INDEPENDENT family name.
    pub stream: String,
    pub topic: String,
    /// `meta.dt` — the chronological ordering key.
    pub dt: String,
    /// `meta.id` — the event's own id (disambiguates same-`dt` records).
    pub id: String,
    /// Every top-level field except `$schema` and `meta`.
    pub payload: serde_json::Value,
}

/// Parses a fixture file shaped as a JSON array of Wikimedia EventStreams
/// records: `{"$schema","meta":{"stream","topic","dt","id",...},...rest}`
/// — the real wire shape (one JSON object per line in production; a JSON
/// array here for ease of hand-authoring/review, same framing choice as
/// `github_archive::parse_fixture`).
pub fn parse_fixture(json_text: &str) -> Result<Vec<WikimediaEvent>, IngestError> {
    let value: serde_json::Value = serde_json::from_str(json_text)?;
    let array = value
        .as_array()
        .ok_or(IngestError::MalformedField("root (expected a JSON array)"))?;
    array.iter().map(parse_one).collect()
}

fn parse_one(value: &serde_json::Value) -> Result<WikimediaEvent, IngestError> {
    let obj = require_object(value, "record")?;
    let schema_uri = require_str(
        obj.get("$schema")
            .ok_or(IngestError::MissingField("$schema"))?,
        "$schema",
    )?
    .to_string();
    let meta = require_object(
        obj.get("meta").ok_or(IngestError::MissingField("meta"))?,
        "meta",
    )?;
    let stream = require_str(
        meta.get("stream")
            .ok_or(IngestError::MissingField("meta.stream"))?,
        "meta.stream",
    )?
    .to_string();
    let topic = require_str(
        meta.get("topic")
            .ok_or(IngestError::MissingField("meta.topic"))?,
        "meta.topic",
    )?
    .to_string();
    let dt = require_str(
        meta.get("dt").ok_or(IngestError::MissingField("meta.dt"))?,
        "meta.dt",
    )?
    .to_string();
    let id = require_str(
        meta.get("id").ok_or(IngestError::MissingField("meta.id"))?,
        "meta.id",
    )?
    .to_string();

    let mut payload = obj.clone();
    payload.remove("$schema");
    payload.remove("meta");

    Ok(WikimediaEvent {
        schema_uri,
        stream,
        topic,
        dt,
        id,
        payload: serde_json::Value::Object(payload),
    })
}

/// The trailing path segment of a `$schema` URI (e.g. `"1.0.0"` out of
/// `"/mediawiki/page/create/1.0.0"`) — used only to build a human-legible
/// (evaluator-only) `EvalCase::name` suffix, never fed back into any
/// model-facing field.
fn schema_version_suffix(schema_uri: &str) -> &str {
    schema_uri.rsplit('/').next().unwrap_or(schema_uri)
}

/// Ingests `events` into [`EvalCase`]s — see this module's doc comment for
/// the family=`stream`/version=fingerprint-dedup pooling strategy. Mirrors
/// `github_archive::ingest` structurally; kept as a parallel implementation
/// (not a shared generic) because the two source envelopes' label fields
/// differ enough (nested `meta` object vs flat top-level fields) that a
/// shared abstraction would obscure more than it saves.
pub fn ingest(events: &[WikimediaEvent], k: usize) -> Result<IngestedCorpus, IngestError> {
    let mut profiles = Vec::with_capacity(events.len());
    let mut members = Vec::with_capacity(events.len());
    for event in events {
        let profile = profile_from_json(&event.payload)?;
        members.push(FamilyMember {
            family_name: event.stream.clone(),
            order_key: format!("{}#{}", event.dt, event.id),
            profile: profile.clone(),
        });
        profiles.push(profile);
    }
    let pool = build_family_pool(&members);

    let mut order: Vec<usize> = (0..events.len()).collect();
    order.sort_by(|&a, &b| {
        events[a]
            .dt
            .cmp(&events[b].dt)
            .then_with(|| events[a].id.cmp(&events[b].id))
    });

    let mut cases = Vec::with_capacity(events.len());
    for idx in order {
        let event = &events[idx];
        let profile = &profiles[idx];
        let fingerprint = profile.generalized_fingerprint();
        let gold_schema_id = pool
            .gold_schema_of
            .get(&(event.stream.clone(), fingerprint))
            .cloned()
            .ok_or(IngestError::MalformedField("stream"))?;

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

        let stream_slug = event.stream.replace(['.', '/'], "_");
        cases.push(EvalCase {
            name: format!(
                "wm_{stream_slug}_{}_{}",
                schema_version_suffix(&event.schema_uri),
                event.id
            ),
            category,
            candidate,
            retrieved,
            expected,
            // See `github_archive::ingest`'s identical note: real tiering
            // is `corpus::tiers::assign_tiers`'s job, not this loader's.
            partition: Partition::Train,
        });
    }

    Ok(IngestedCorpus { cases, pool })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;

    fn fixture(name: &str) -> String {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(name);
        fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read fixture {}: {e}", path.display()))
    }

    fn sample_events() -> Vec<WikimediaEvent> {
        parse_fixture(&fixture("wikimedia_sample.json")).expect("fixture should parse")
    }

    fn cases_in_event_order<'a>(
        events: &[WikimediaEvent],
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
    fn parses_schema_stream_and_strips_meta_into_a_flat_payload() {
        let events = sample_events();
        assert!(events.len() >= 3, "expected several fixture records");
        for event in &events {
            assert!(event.schema_uri.starts_with('/'));
            assert!(!event.stream.is_empty());
            // The parsed payload must never carry `$schema`/`meta` keys —
            // `parse_one` removes them explicitly.
            let obj = event.payload.as_object().unwrap();
            assert!(!obj.contains_key("$schema"));
            assert!(!obj.contains_key("meta"));
        }
    }

    #[test]
    fn same_stream_different_schema_version_yields_same_family_different_schema() {
        let events = sample_events();
        let corpus = ingest(&events, 5).expect("ingest should succeed on the fixture");

        // The fixture is authored with (at least) two `page-create` records
        // at different `$schema` versions with a genuinely different
        // payload shape (see the fixture file) — real schema evolution,
        // not a coincidental structural match.
        let page_create_events: Vec<&WikimediaEvent> = events
            .iter()
            .filter(|e| e.stream == "mediawiki.page-create")
            .collect();
        assert!(
            page_create_events.len() >= 2,
            "fixture must carry >=2 page-create versions to exercise version lineage"
        );

        let versions: std::collections::BTreeSet<&str> = page_create_events
            .iter()
            .map(|e| e.schema_uri.as_str())
            .collect();
        assert!(
            versions.len() >= 2,
            "fixture's page-create records must span >=2 distinct $schema versions"
        );

        let mapped = cases_in_event_order(&events, &corpus.cases);
        let page_create_cases: Vec<&EvalCase> = events
            .iter()
            .zip(&mapped)
            .filter(|(e, _)| e.stream == "mediawiki.page-create")
            .map(|(_, c)| *c)
            .collect();

        // Gold = SAME FAMILY across versions...
        let family_ids: std::collections::BTreeSet<String> = page_create_cases
            .iter()
            .map(|c| {
                let schema_id = c.expected.gold_schema_id.clone().unwrap();
                corpus
                    .pool
                    .family_id_of(&schema_id)
                    .expect("gold schema must be in the pool")
                    .as_str()
                    .to_string()
            })
            .collect();
        assert_eq!(
            family_ids.len(),
            1,
            "page-create records at different schema versions must share ONE family_id"
        );

        // ... but a DIFFERENT schema id (the version-lineage difficult pair
        // — same family, different version).
        let schema_ids: std::collections::BTreeSet<String> = page_create_cases
            .iter()
            .map(|c| {
                c.expected
                    .gold_schema_id
                    .clone()
                    .unwrap()
                    .as_str()
                    .to_string()
            })
            .collect();
        assert!(
            schema_ids.len() >= 2,
            "page-create records at different schema versions must have DIFFERENT gold schema ids"
        );
    }

    #[test]
    fn ingest_strips_schema_stream_and_topic_from_the_input_and_prompt() {
        let events = sample_events();
        let corpus = ingest(&events, 5).expect("ingest should succeed");
        let mapped = cases_in_event_order(&events, &corpus.cases);

        for (event, case) in events.iter().zip(&mapped) {
            let (input, sidecar) = crate::labels::split_case(case);
            let serialized = serde_json::to_string(&input).unwrap();

            assert!(
                !serialized.contains(&event.schema_uri),
                "$schema {} leaked into InferenceInput: {serialized}",
                event.schema_uri
            );
            assert!(
                !input.prompt.contains(&event.schema_uri),
                "$schema {} leaked into the rendered prompt: {}",
                event.schema_uri,
                input.prompt
            );
            assert!(
                !serialized.contains(&event.stream),
                "meta.stream {} leaked into InferenceInput: {serialized}",
                event.stream
            );
            assert!(
                !serialized.contains(&event.topic),
                "meta.topic {} leaked into InferenceInput: {serialized}",
                event.topic
            );
            assert!(
                !serialized.contains(&event.id),
                "meta.id {} leaked into InferenceInput: {serialized}",
                event.id
            );

            assert!(sidecar.case_name.contains(&event.id));
        }
    }

    #[test]
    fn ingest_is_deterministic_across_repeated_calls() {
        let events = sample_events();
        let a = ingest(&events, 5).unwrap().cases;
        let b = ingest(&events, 5).unwrap().cases;
        assert_eq!(
            serde_json::to_string(&a).unwrap(),
            serde_json::to_string(&b).unwrap()
        );
    }
}
