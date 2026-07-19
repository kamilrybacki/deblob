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
use deblob_core::id::SchemaId;
use deblob_core::ports::Registry;
use deblob_kafka::StreamEvent;
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
    let events = BroadcastStream::new(rx)
        .then(move |item| {
            let registry = registry.clone();
            async move {
                let event = item.ok()?;
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
    use deblob_core::id::{CandidateId, FamilyId, FamilyVersion};
    use deblob_core::ports::{FamilyRecord, FamilyRef, SchemaRecord};
    use deblob_kafka::StreamOutcome;

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
        async fn get_family(&self, _family_id: &FamilyId) -> Result<Option<FamilyRecord>, CoreError> {
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
        };
        let registry: Arc<dyn Registry> = Arc::new(FakeRegistry(Some(record)));
        let mut event = base_event(schema_id.as_str());
        event.family_id = Some("fam_already_set".to_string());

        let enriched = enrich_family_id(&registry, &event).await;

        assert_eq!(enriched.family_id.as_deref(), Some("fam_already_set"));
    }
}
