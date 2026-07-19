//! Discovery-topic consumer: the cold-lane counterpart to
//! `deblob_kafka::Relay::run`'s discovery-topic PRODUCER.
//!
//! Without this, nothing ever reads `cfg.discovery_topic` — the relay
//! happily produces a [`DiscoveryMsg`] for every `Provisional`
//! classification, but `ColdLane::ingest` never runs, so candidates never
//! accumulate and promotion has nothing to promote. [`run`] closes that
//! gap: subscribe → deserialize → parse → `ColdLane::ingest`, on a loop
//! that honors a [`CancellationToken`] for graceful shutdown, same as
//! `Relay::run`.
//!
//! Lives in the `deblob` crate (not `deblob-kafka`), because it needs
//! [`ColdLane`], which lives here. Putting this loop in `deblob-kafka`
//! would require `deblob-kafka` to depend on the `deblob` package, which
//! would reintroduce the `deblob-kafka -> deblob -> deblob-kafka` package
//! cycle Task 18's `deblob-match` split exists specifically to avoid (see
//! `crate::lib`'s docs). Adding `rdkafka` as a direct dependency of
//! `deblob` (same version/features `deblob-kafka` already uses) keeps
//! `deblob-kafka` a leaf dependency — never the reverse — so there is no
//! cycle.

use std::sync::Arc;

use deblob_core::error::CoreError;
use deblob_core::id::CandidateId;
use deblob_fingerprint::{parse_bounded, Limits};
use deblob_kafka::KafkaSasl;
use deblob_match::discovery::DiscoveryMsg;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use tokio_util::sync::CancellationToken;

use crate::coldlane::{ColdLane, IngestOutcome, SampleMeta};
use deblob_core::ports::SampleStore;

/// How many discovery messages to process between offset commits. A
/// crash between commits re-ingests up to this many messages on restart —
/// `ColdLane::ingest` is a read-merge-write accumulation of stats-only
/// evidence, not an exactly-once ledger, so re-ingesting a handful of
/// already-seen observations only nudges `sample_count`/profile stats,
/// never corrupts state (unlike the relay's own Kafka-transaction path,
/// which spec §3.1 scopes exactly-once guarantees to).
const COMMIT_EVERY: u32 = 50;

/// Configuration for one [`run`] instance.
pub struct DiscoveryConsumerCfg {
    pub brokers: String,
    /// The relay's own `group.id` (spec's `kafka.group_id`); this
    /// consumer runs under `"{group_id}-discovery"` so it never shares a
    /// consumer group with the relay's raw-topic consumer.
    pub group_id: String,
    pub discovery_topic: String,
    pub limits: Limits,
    pub sasl: Option<KafkaSasl>,
    /// Redacted troubleshooting sample capture (joint design
    /// dc-samples-dlp-1907). `None` store = capture disabled; when present,
    /// `sample_capture.enabled` + the per-source allowlist gate it. Off the
    /// hot path, fail-closed.
    pub sample_store: Option<Arc<dyn SampleStore>>,
    pub sample_capture: crate::sample_capture::SampleCaptureCfg,
}

/// Errors [`run`] can return. A malformed individual message is never one
/// of these — that's logged and skipped inside the loop (spec §10-style
/// "never silently drop the whole pipeline over one bad message") — these
/// variants are all consumer-level failures (client construction, a Kafka
/// protocol error).
#[derive(Debug, thiserror::Error)]
pub enum DiscoveryConsumerError {
    #[error("kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
}

/// Errors handling a single already-deserialized [`DiscoveryMsg`]. Both
/// `MalformedPayload` and `InvalidCandidateId` are defensive-only in
/// practice: `deblob-kafka::Relay` only ever discovery-produces a payload
/// that already passed `parse_bounded` on the hot path, and a `cand_id`
/// it minted itself via `CandidateId::from_digest` — a message that fails
/// either check here would indicate a bug upstream (or a message from an
/// untrusted producer on the discovery topic), not a normal operational
/// condition.
#[derive(Debug, thiserror::Error)]
pub enum DiscoveryHandleError {
    #[error("discovery message payload failed the bounded parse")]
    MalformedPayload,
    #[error("discovery message cand_id failed to parse")]
    InvalidCandidateId,
    #[error("cold lane ingest failed: {0}")]
    Ingest(#[from] CoreError),
}

/// Runs the discovery-consumer loop until `shutdown` is cancelled or an
/// unrecoverable [`DiscoveryConsumerError`] occurs.
pub async fn run(
    cfg: DiscoveryConsumerCfg,
    cold_lane: Arc<ColdLane>,
    shutdown: CancellationToken,
) -> Result<(), DiscoveryConsumerError> {
    let consumer: StreamConsumer = consumer_client_config(&cfg).create()?;
    consumer.subscribe(&[cfg.discovery_topic.as_str()])?;

    let mut pending_offsets = TopicPartitionList::new();
    let mut since_commit: u32 = 0;

    loop {
        let msg = tokio::select! {
            _ = shutdown.cancelled() => {
                commit_offsets(&consumer, &pending_offsets);
                return Ok(());
            }
            msg = consumer.recv() => msg?,
        };

        let topic = msg.topic().to_string();
        let partition = msg.partition();
        let offset = msg.offset();
        let payload = msg.payload().map(|p| p.to_vec());
        // Release the borrow of `consumer` this message holds before the
        // handler's `.await` and before we touch `consumer` again below —
        // same convention `deblob_kafka::relay::Relay::run` uses.
        drop(msg);

        if let Some(payload) = payload {
            match serde_json::from_slice::<DiscoveryMsg>(&payload) {
                Ok(discovery) => {
                    let capture = cfg
                        .sample_store
                        .as_ref()
                        .map(|s| (&cfg.sample_capture, s));
                    if let Err(err) =
                        handle_discovery_msg(discovery, &cold_lane, &cfg.limits, capture).await
                    {
                        // Never log the payload itself — only the error
                        // variant, which carries no message contents.
                        tracing::debug!(error = %err, "discovery consumer: skipping message");
                    }
                }
                Err(err) => {
                    tracing::debug!(
                        error = %err,
                        "discovery consumer: failed to deserialize DiscoveryMsg envelope, skipping"
                    );
                }
            }
        } else {
            tracing::debug!("discovery consumer: skipping message with no payload");
        }

        let _ = pending_offsets.add_partition_offset(&topic, partition, Offset::Offset(offset + 1));
        since_commit += 1;
        if since_commit >= COMMIT_EVERY {
            commit_offsets(&consumer, &pending_offsets);
            pending_offsets = TopicPartitionList::new();
            since_commit = 0;
        }
    }
}

/// Best-effort offset commit; a commit failure is logged, never
/// propagated — the next successful commit (or the periodic one before
/// it) still advances the group's committed position, and losing one
/// commit only widens the re-ingest window on a future restart (see
/// [`COMMIT_EVERY`]'s docs on why that's safe here).
fn commit_offsets(consumer: &StreamConsumer, offsets: &TopicPartitionList) {
    if offsets.count() == 0 {
        return;
    }
    if let Err(err) = consumer.commit(offsets, CommitMode::Async) {
        tracing::warn!(error = %err, "discovery consumer: offset commit failed");
    }
}

/// Deserialize→parse→ingest for one already-deserialized [`DiscoveryMsg`].
/// Split out from [`run`]'s loop so it's directly callable in tests
/// without a broker: feed it a `DiscoveryMsg` and a [`ColdLane`] backed by
/// a fake `EvidenceStore`, and assert the candidate accumulated.
pub async fn handle_discovery_msg(
    msg: DiscoveryMsg,
    cold_lane: &ColdLane,
    limits: &Limits,
    capture: Option<(&crate::sample_capture::SampleCaptureCfg, &Arc<dyn SampleStore>)>,
) -> Result<IngestOutcome, DiscoveryHandleError> {
    let node =
        parse_bounded(&msg.payload, limits).map_err(|_| DiscoveryHandleError::MalformedPayload)?;
    let cand_id =
        CandidateId::parse(&msg.cand_id).map_err(|_| DiscoveryHandleError::InvalidCandidateId)?;
    // Clone what the (off-path, best-effort) sample capture needs before `meta`
    // moves the source/cursor into `ingest`. `Bytes` clone is a cheap refcount.
    let cap_payload = msg.payload.clone();
    let cap_source = msg.source.clone();
    let cap_cursor = msg.cursor.clone();
    let meta = SampleMeta {
        source: msg.source,
        cursor: Some(msg.cursor),
    };
    let outcome = cold_lane
        .ingest(cand_id, &node, meta)
        .await
        .map_err(DiscoveryHandleError::Ingest)?;

    // Fail-closed sample capture (joint design dc-samples-dlp-1907): only for a
    // trusted source, keyed on the RESOLVED candidate id, DLP-redacted before
    // store. Any failure drops the sample; ingest NEVER fails on it, and no raw
    // payload is ever logged.
    if let (Some((cfg, store)), IngestOutcome::Ingested { candidate_id, .. }) = (capture, &outcome) {
        if let Some(record) = crate::sample_capture::build_sample(
            cfg,
            &cap_source,
            &cap_cursor.topic,
            cap_cursor.partition,
            cap_cursor.offset,
            candidate_id,
            &cap_payload,
            now_ms(),
        ) {
            if let Err(err) = store.put_sample(&record).await {
                tracing::warn!(target: "samples", error = %err, "sample store write failed (sample dropped)");
            }
        }
    }
    Ok(outcome)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// The consumer-side [`ClientConfig`]: `read_committed` (spec §3.2's rule
/// for every downstream consumer of a topic the relay produces to
/// transactionally) and its own consumer group, distinct from the
/// relay's.
fn consumer_client_config(cfg: &DiscoveryConsumerCfg) -> ClientConfig {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", &cfg.brokers)
        .set("group.id", format!("{}-discovery", cfg.group_id))
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("isolation.level", "read_committed");
    apply_sasl(&mut c, &cfg.sasl);
    c
}

/// Applies SASL credentials to a `ClientConfig` if present. Duplicated
/// (not shared) from `deblob_kafka::relay`'s private helper of the same
/// shape — that function isn't exported across the crate boundary, and a
/// four-line `ClientConfig` setter isn't worth widening `deblob-kafka`'s
/// public surface for.
fn apply_sasl(c: &mut ClientConfig, sasl: &Option<KafkaSasl>) {
    if let Some(sasl) = sasl {
        c.set("security.protocol", &sasl.security_protocol)
            .set("sasl.mechanism", &sasl.mechanism)
            .set("sasl.username", &sasl.username)
            .set("sasl.password", &sasl.password);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use deblob_core::envelope::SourceCursor;
    use deblob_core::ports::{CandidateRecord, CandidateState, EvidenceStore};
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    /// In-memory `EvidenceStore` fake — duplicated (not shared) from
    /// `crate::coldlane`'s own `#[cfg(test)]`-only fake of the same
    /// shape, which is private to that module's test config and not
    /// visible here. Matches the same per-file fake pattern already used
    /// throughout this workspace's test suites (coldlane.rs, api_it.rs,
    /// relay_it.rs, chaos_it.rs, promote_resolve_it.rs).
    #[derive(Default)]
    struct FakeEvidence {
        candidates: StdMutex<HashMap<CandidateId, CandidateRecord>>,
        clusters: StdMutex<HashMap<String, CandidateId>>,
        variants: StdMutex<HashMap<CandidateId, Vec<(String, String)>>>,
    }

    #[async_trait::async_trait]
    impl EvidenceStore for FakeEvidence {
        async fn upsert_candidate(&self, rec: CandidateRecord) -> Result<(), CoreError> {
            self.candidates
                .lock()
                .unwrap()
                .insert(rec.candidate_id.clone(), rec);
            Ok(())
        }

        async fn get_candidate(
            &self,
            id: &CandidateId,
        ) -> Result<Option<CandidateRecord>, CoreError> {
            Ok(self.candidates.lock().unwrap().get(id).cloned())
        }

        async fn list_candidates(
            &self,
            state: CandidateState,
            _cursor: Option<String>,
            limit: usize,
        ) -> Result<(Vec<CandidateRecord>, Option<String>), CoreError> {
            let items: Vec<_> = self
                .candidates
                .lock()
                .unwrap()
                .values()
                .filter(|c| c.state == state)
                .take(limit)
                .cloned()
                .collect();
            Ok((items, None))
        }

        async fn append_evidence(
            &self,
            _id: &CandidateId,
            _stats: serde_json::Value,
        ) -> Result<(), CoreError> {
            Ok(())
        }

        async fn set_state(
            &self,
            id: &CandidateId,
            state: CandidateState,
        ) -> Result<(), CoreError> {
            if let Some(rec) = self.candidates.lock().unwrap().get_mut(id) {
                rec.state = state;
            }
            Ok(())
        }

        async fn get_cluster(&self, gen_fp: &str) -> Result<Option<CandidateId>, CoreError> {
            Ok(self.clusters.lock().unwrap().get(gen_fp).cloned())
        }

        async fn set_cluster(&self, gen_fp: &str, cand_id: &CandidateId) -> Result<(), CoreError> {
            self.clusters
                .lock()
                .unwrap()
                .insert(gen_fp.to_string(), cand_id.clone());
            Ok(())
        }

        async fn add_variant(
            &self,
            cand_id: &CandidateId,
            bucket_key: &str,
            fp_b32: &str,
        ) -> Result<(), CoreError> {
            let mut variants = self.variants.lock().unwrap();
            let entry = variants.entry(cand_id.clone()).or_default();
            let pair = (bucket_key.to_string(), fp_b32.to_string());
            if !entry.contains(&pair) {
                entry.push(pair);
            }
            Ok(())
        }

        async fn get_variants(
            &self,
            cand_id: &CandidateId,
        ) -> Result<Vec<(String, String)>, CoreError> {
            Ok(self
                .variants
                .lock()
                .unwrap()
                .get(cand_id)
                .cloned()
                .unwrap_or_default())
        }
    }

    fn cand_id_of(json: &str) -> CandidateId {
        let node = parse_bounded(json.as_bytes(), &Limits::default()).unwrap();
        let shape = deblob_fingerprint::shape_of(&node);
        CandidateId::from_digest(&deblob_fingerprint::fingerprint(&shape))
    }

    fn discovery_msg(
        cand_id: &CandidateId,
        payload: &str,
        source: &str,
        offset: i64,
    ) -> DiscoveryMsg {
        DiscoveryMsg {
            cand_id: cand_id.as_str().to_string(),
            payload: Bytes::from(payload.as_bytes().to_vec()),
            source: source.to_string(),
            cursor: SourceCursor {
                topic: "deblob.discovery".to_string(),
                partition: 0,
                offset,
            },
        }
    }

    // Proves the full deserialize(already done)->parse->ingest path: a
    // `DiscoveryMsg` with a known JSON payload and cand_id, fed straight
    // to the handler (no broker involved), lands as a `CandidateRecord`
    // with `sample_count == 1` in the fake evidence store — i.e. `ingest`
    // actually ran, not a no-op.
    #[tokio::test]
    async fn handle_discovery_msg_ingests_and_records_candidate() {
        let evidence = Arc::new(FakeEvidence::default());
        let lane = ColdLane::new(evidence.clone());
        let limits = Limits::default();

        let payload = r#"{"a":1}"#;
        let cand_id = cand_id_of(payload);
        let msg = discovery_msg(&cand_id, payload, "events.raw", 42);

        let outcome = handle_discovery_msg(msg, &lane, &limits, None).await.unwrap();
        assert!(matches!(outcome, IngestOutcome::Ingested { .. }));

        let stored = evidence
            .get_candidate(&cand_id)
            .await
            .unwrap()
            .expect("candidate recorded");
        assert_eq!(stored.sample_count, 1);
        assert_eq!(stored.state, CandidateState::Provisional);
    }

    // A second discovery message for the same candidate must accumulate
    // (sample_count 1 -> 2), not overwrite — this is what "candidates
    // never accumulate" (the P1 bug this module fixes) actually looked
    // like before a consumer existed at all.
    #[tokio::test]
    async fn handle_discovery_msg_accumulates_across_messages() {
        let evidence = Arc::new(FakeEvidence::default());
        let lane = ColdLane::new(evidence.clone());
        let limits = Limits::default();

        let payload = r#"{"a":1}"#;
        let cand_id = cand_id_of(payload);

        handle_discovery_msg(
            discovery_msg(&cand_id, payload, "events.raw", 1),
            &lane,
            &limits,
            None,
        )
        .await
        .unwrap();
        handle_discovery_msg(
            discovery_msg(&cand_id, payload, "events.raw", 2),
            &lane,
            &limits,
            None,
        )
        .await
        .unwrap();

        let stored = evidence.get_candidate(&cand_id).await.unwrap().unwrap();
        assert_eq!(stored.sample_count, 2);
    }

    #[tokio::test]
    async fn handle_discovery_msg_rejects_invalid_cand_id() {
        let evidence = Arc::new(FakeEvidence::default());
        let lane = ColdLane::new(evidence);
        let limits = Limits::default();

        let mut msg = discovery_msg(&cand_id_of(r#"{"a":1}"#), r#"{"a":1}"#, "events.raw", 0);
        msg.cand_id = "not-a-valid-cand-id".to_string();

        let err = handle_discovery_msg(msg, &lane, &limits, None).await.unwrap_err();
        assert!(matches!(err, DiscoveryHandleError::InvalidCandidateId));
    }

    #[tokio::test]
    async fn handle_discovery_msg_rejects_malformed_payload() {
        let evidence = Arc::new(FakeEvidence::default());
        let lane = ColdLane::new(evidence);
        let limits = Limits::default();

        let cand_id = cand_id_of(r#"{"a":1}"#);
        let mut msg = discovery_msg(&cand_id, r#"{"a":1}"#, "events.raw", 0);
        msg.payload = Bytes::from(b"not json at all".to_vec());

        let err = handle_discovery_msg(msg, &lane, &limits, None).await.unwrap_err();
        assert!(matches!(err, DiscoveryHandleError::MalformedPayload));
    }
}
