//! `GET /api/v1/stream` — the live-stream tap (Stage L1): Server-Sent
//! Events of payload-free [`deblob_kafka::StreamEvent`]s, one per hot-path
//! record outcome. Authenticated exactly like every other `/api/v1/*`
//! route (`super::router`'s `route_layer`) — never reachable without the
//! same bearer token every other management-API endpoint requires.
//!
//! Best-effort, like the underlying `tokio::sync::broadcast` channel
//! itself: a slow subscriber that falls behind the channel's fixed
//! capacity (`crate::serve::STREAM_CHANNEL_CAPACITY`) silently misses the
//! events it lagged past (`BroadcastStreamRecvError::Lagged`) rather than
//! the connection erroring out — an SSE tap for a live dashboard is a
//! lossy multicast, never a delivery guarantee, and must never fail the
//! whole stream over one skipped event.

use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use deblob_core::id::{CandidateId, SchemaId};
use deblob_core::ports::{EvidenceStore, Registry};
use deblob_kafka::{StreamEvent, StreamOutcome};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt};

use super::ApiState;

/// Best-effort `family_id` enrichment (off the hot path, this SSE consumer
/// path only — the relay's own hot path never does this lookup, see
/// `deblob_kafka::stream`'s docs on why `family_id` starts `None`): a
/// `schema_ref` that looks like a `sch_` id is resolved through the
/// registry to fill in its family, on a fresh clone of the event. Any
/// lookup miss/error simply leaves `family_id: None` — enrichment must
/// never fail the stream.
async fn enrich_family_id(registry: &Arc<dyn Registry>, event: &StreamEvent) -> StreamEvent {
    let mut event = event.clone();
    if event.family_id.is_none() && event.schema_ref.starts_with("sch_") {
        if let Ok(schema_id) = SchemaId::parse(&event.schema_ref) {
            if let Ok(Some(record)) = registry.get_schema(&schema_id).await {
                event.family_id = Some(record.family_id.as_str().to_string());
            }
        }
    }
    event
}

/// Best-effort matched-vs-new resolution (off the hot path, this SSE consumer
/// path only — the relay's hot path emits `NewCandidate` for EVERY provisional
/// classification without ever querying candidate-existence state, see
/// `deblob_kafka::StreamOutcome::NewCandidate`'s docs). Here, where an extra
/// lookup is free, a `NewCandidate` whose `cand_` ref already carries
/// accumulated evidence (`sample_count >= 2`, i.e. this was NOT its first
/// sighting) is relabelled `MatchedCandidate`. `sample_count <= 1`, a lookup
/// miss, or any store error all leave the event as `NewCandidate` — resolution
/// must never fail the stream, and "new" is the conservative default.
async fn resolve_matched(evidence: &Arc<dyn EvidenceStore>, event: &StreamEvent) -> StreamEvent {
    if event.outcome != StreamOutcome::NewCandidate || !event.schema_ref.starts_with("cand_") {
        return event.clone();
    }
    let mut event = event.clone();
    if let Ok(cand_id) = CandidateId::parse(&event.schema_ref) {
        if let Ok(Some(rec)) = evidence.get_candidate(&cand_id).await {
            if rec.sample_count >= 2 {
                event.outcome = StreamOutcome::MatchedCandidate;
            }
        }
    }
    event
}

/// Subscribes a fresh `Receiver` onto `state.stream_tx` for the lifetime of
/// this SSE connection and relays every successfully-received
/// `deblob_kafka::StreamEvent` as one `data:` JSON SSE event, best-effort
/// enriching `family_id` along the way ([`enrich_family_id`]). A
/// lagged/dropped batch of events (this subscriber fell behind) is skipped
/// rather than surfaced as an SSE error; a `StreamEvent` that somehow fails
/// to serialize (never expected — it's a plain struct of ids/strings/counts,
/// see `deblob_kafka::stream`'s own docs) is skipped the same way, for the
/// same "never fail the whole stream over one event" reason.
pub async fn get_stream(
    State(state): State<ApiState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.stream_tx.subscribe();
    let registry = state.registry.clone();
    let evidence = state.evidence.clone();
    let events = BroadcastStream::new(rx)
        .then(move |item| {
            let registry = registry.clone();
            let evidence = evidence.clone();
            async move {
                let event = item.ok()?;
                let event = resolve_matched(&evidence, &event).await;
                let event = enrich_family_id(&registry, &event).await;
                let sse_event = Event::default().json_data(&event).ok()?;
                Some(Ok(sse_event))
            }
        })
        .filter_map(|opt| opt);
    Sse::new(events).keep_alive(KeepAlive::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use deblob_core::envelope::SourceCursor;
    use deblob_core::error::CoreError;
    use deblob_core::id::{FamilyId, FamilyVersion};
    use deblob_core::ports::{
        CandidateRecord, CandidateState, FamilyRecord, FamilyRef, SchemaRecord,
    };
    use deblob_kafka::StreamOutcome;

    /// Minimal `EvidenceStore` fake: `get_candidate` answers from one fixed
    /// optional record, everything else is unreachable by `resolve_matched`.
    struct FakeEvidence(Option<CandidateRecord>);

    #[async_trait::async_trait]
    impl EvidenceStore for FakeEvidence {
        async fn get_candidate(
            &self,
            id: &CandidateId,
        ) -> Result<Option<CandidateRecord>, CoreError> {
            Ok(self.0.as_ref().filter(|r| &r.candidate_id == id).cloned())
        }
        async fn upsert_candidate(&self, _rec: CandidateRecord) -> Result<(), CoreError> {
            unimplemented!("not exercised by resolve_matched")
        }
        async fn list_candidates(
            &self,
            _state: CandidateState,
            _cursor: Option<String>,
            _limit: usize,
        ) -> Result<(Vec<CandidateRecord>, Option<String>), CoreError> {
            unimplemented!("not exercised by resolve_matched")
        }
        async fn append_evidence(
            &self,
            _id: &CandidateId,
            _stats: serde_json::Value,
        ) -> Result<(), CoreError> {
            unimplemented!("not exercised by resolve_matched")
        }
        async fn set_state(
            &self,
            _id: &CandidateId,
            _state: CandidateState,
        ) -> Result<(), CoreError> {
            unimplemented!("not exercised by resolve_matched")
        }
        async fn get_cluster(&self, _gen_fp: &str) -> Result<Option<CandidateId>, CoreError> {
            unimplemented!("not exercised by resolve_matched")
        }
        async fn set_cluster(
            &self,
            _gen_fp: &str,
            _cand_id: &CandidateId,
        ) -> Result<(), CoreError> {
            unimplemented!("not exercised by resolve_matched")
        }
        async fn add_variant(
            &self,
            _cand_id: &CandidateId,
            _bucket_key: &str,
            _fp_b32: &str,
        ) -> Result<(), CoreError> {
            unimplemented!("not exercised by resolve_matched")
        }
        async fn get_variants(
            &self,
            _cand_id: &CandidateId,
        ) -> Result<Vec<(String, String)>, CoreError> {
            unimplemented!("not exercised by resolve_matched")
        }
    }

    fn candidate(id: &CandidateId, sample_count: u64) -> CandidateRecord {
        CandidateRecord {
            candidate_id: id.clone(),
            profile: serde_json::json!({}),
            sample_count,
            first_seen_ms: 0,
            last_seen_ms: 0,
            state: CandidateState::Provisional,
            source: None,
        }
    }

    fn new_candidate_event(schema_ref: &str) -> StreamEvent {
        let mut ev = base_event(schema_ref);
        ev.outcome = StreamOutcome::NewCandidate;
        ev
    }

    /// Minimal `Registry` fake: `get_schema` answers from an optional fixed
    /// record, everything else is unreachable by `enrich_family_id`.
    struct FakeRegistry(Option<SchemaRecord>);

    #[async_trait::async_trait]
    impl Registry for FakeRegistry {
        async fn get_schema(&self, id: &SchemaId) -> Result<Option<SchemaRecord>, CoreError> {
            Ok(self.0.as_ref().filter(|r| &r.schema_id == id).cloned())
        }
        async fn resolve_structural(
            &self,
            _bucket_key: &str,
            _fingerprint: &SchemaId,
        ) -> Result<Option<SchemaId>, CoreError> {
            unimplemented!("not exercised by enrich_family_id")
        }
        async fn publish(
            &self,
            _record: SchemaRecord,
            _alias_from: &CandidateId,
            _bucket_key: &str,
            _variant_members: &[(String, String)],
            _actor: &str,
            _reason: &str,
        ) -> Result<FamilyVersion, CoreError> {
            unimplemented!("not exercised by enrich_family_id")
        }
        async fn get_alias(&self, _id: &CandidateId) -> Result<Option<SchemaId>, CoreError> {
            unimplemented!("not exercised by enrich_family_id")
        }
        async fn list_schemas(
            &self,
            _cursor: Option<String>,
            _limit: usize,
        ) -> Result<(Vec<SchemaRecord>, Option<String>), CoreError> {
            unimplemented!("not exercised by enrich_family_id")
        }
        async fn list_families_in_buckets(
            &self,
            _bucket_keys: &[String],
        ) -> Result<Vec<FamilyRef>, CoreError> {
            unimplemented!("not exercised by enrich_family_id")
        }
        async fn list_families_by_band_depth(
            &self,
            _bands: &[u32],
            _depths: &[u32],
        ) -> Result<Vec<FamilyRef>, CoreError> {
            unimplemented!("not exercised by enrich_family_id")
        }
        async fn family_version_schema(
            &self,
            _family_id: &FamilyId,
            _version: FamilyVersion,
        ) -> Result<Option<SchemaId>, CoreError> {
            unimplemented!("not exercised by enrich_family_id")
        }
        async fn get_family(
            &self,
            _family_id: &FamilyId,
        ) -> Result<Option<FamilyRecord>, CoreError> {
            unimplemented!("not exercised by enrich_family_id")
        }
        async fn list_family_versions(
            &self,
            _family_id: &FamilyId,
        ) -> Result<Vec<FamilyVersion>, CoreError> {
            unimplemented!("not exercised by enrich_family_id")
        }
    }

    fn base_event(schema_ref: &str) -> StreamEvent {
        StreamEvent {
            ts_ms: 0,
            lane: "hot",
            origin: SourceCursor {
                topic: "events.raw".to_string(),
                partition: 0,
                offset: 0,
            },
            outcome: StreamOutcome::Tagged,
            schema_ref: schema_ref.to_string(),
            family_id: None,
            reason: None,
            fields_count: 0,
            source: Some("events.raw".to_string()),
        }
    }

    #[tokio::test]
    async fn enriches_family_id_for_a_known_schema() {
        let schema_id = SchemaId::from_digest(&[9u8; 32]);
        let family_id = FamilyId::new_v7();
        let record = SchemaRecord {
            schema_id: schema_id.clone(),
            family_id: family_id.clone(),
            version: FamilyVersion(1),
            canonical: "{}".to_string(),
            canonicalizer: "deblob-canon-v1".to_string(),
            provenance: serde_json::json!({}),
            semantic: None,
            semantic_fingerprint: None,
            privacy_class: None,
            value_profile_ref: None,
            value_profile_summary: None,
        };
        let registry: Arc<dyn Registry> = Arc::new(FakeRegistry(Some(record)));
        let event = base_event(schema_id.as_str());

        let enriched = enrich_family_id(&registry, &event).await;

        assert_eq!(enriched.family_id.as_deref(), Some(family_id.as_str()));
        // Enrichment operates on a clone — the original event is untouched.
        assert!(event.family_id.is_none());
    }

    #[tokio::test]
    async fn lookup_miss_leaves_family_id_none() {
        let registry: Arc<dyn Registry> = Arc::new(FakeRegistry(None));
        let event = base_event(SchemaId::from_digest(&[1u8; 32]).as_str());

        let enriched = enrich_family_id(&registry, &event).await;

        assert!(enriched.family_id.is_none());
    }

    #[tokio::test]
    async fn non_sch_schema_ref_never_attempts_a_lookup() {
        let registry: Arc<dyn Registry> = Arc::new(FakeRegistry(None));
        let event = base_event("unresolved");

        let enriched = enrich_family_id(&registry, &event).await;

        assert!(enriched.family_id.is_none());
    }

    #[tokio::test]
    async fn re_observed_candidate_is_relabelled_matched() {
        let cand = CandidateId::from_digest(&[7u8; 32]);
        let evidence: Arc<dyn EvidenceStore> = Arc::new(FakeEvidence(Some(candidate(&cand, 5))));
        let event = new_candidate_event(cand.as_str());

        let resolved = resolve_matched(&evidence, &event).await;

        assert_eq!(resolved.outcome, StreamOutcome::MatchedCandidate);
    }

    #[tokio::test]
    async fn first_sighting_candidate_stays_new() {
        let cand = CandidateId::from_digest(&[7u8; 32]);
        let evidence: Arc<dyn EvidenceStore> = Arc::new(FakeEvidence(Some(candidate(&cand, 1))));
        let event = new_candidate_event(cand.as_str());

        let resolved = resolve_matched(&evidence, &event).await;

        assert_eq!(resolved.outcome, StreamOutcome::NewCandidate);
    }

    #[tokio::test]
    async fn unknown_candidate_stays_new() {
        let evidence: Arc<dyn EvidenceStore> = Arc::new(FakeEvidence(None));
        let event = new_candidate_event(CandidateId::from_digest(&[3u8; 32]).as_str());

        let resolved = resolve_matched(&evidence, &event).await;

        assert_eq!(resolved.outcome, StreamOutcome::NewCandidate);
    }

    #[tokio::test]
    async fn non_new_candidate_outcome_is_left_alone() {
        // A `Tagged` event (schema_ref is a sch_ id) must never be probed as
        // a candidate — resolution only ever touches NewCandidate events.
        let cand = CandidateId::from_digest(&[7u8; 32]);
        let evidence: Arc<dyn EvidenceStore> = Arc::new(FakeEvidence(Some(candidate(&cand, 99))));
        let mut event = base_event("sch_whatever");
        event.outcome = StreamOutcome::Tagged;

        let resolved = resolve_matched(&evidence, &event).await;

        assert_eq!(resolved.outcome, StreamOutcome::Tagged);
    }

    #[tokio::test]
    async fn already_populated_family_id_is_left_alone() {
        let schema_id = SchemaId::from_digest(&[9u8; 32]);
        let record = SchemaRecord {
            schema_id: schema_id.clone(),
            family_id: FamilyId::new_v7(),
            version: FamilyVersion(1),
            canonical: "{}".to_string(),
            canonicalizer: "deblob-canon-v1".to_string(),
            provenance: serde_json::json!({}),
            semantic: None,
            semantic_fingerprint: None,
            privacy_class: None,
            value_profile_ref: None,
            value_profile_summary: None,
        };
        let registry: Arc<dyn Registry> = Arc::new(FakeRegistry(Some(record)));
        let mut event = base_event(schema_id.as_str());
        event.family_id = Some("fam_already_set".to_string());

        let enriched = enrich_family_id(&registry, &event).await;

        assert_eq!(enriched.family_id.as_deref(), Some("fam_already_set"));
    }
}
