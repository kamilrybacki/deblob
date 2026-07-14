//! The transactional relay (spec §3.1-3.2): consume the raw topic →
//! classify via [`HotMatcher`] → strip/rewrite `deblob-*` headers →
//! transactional produce (tagged/quarantine [+ discovery for provisional
//! shapes]) → `send_offsets_to_transaction` → commit, ONE Kafka
//! transaction per record.
//!
//! Exactly-once scope (spec §3.1): Kafka transactions cover
//! consume→produce→offset within the Kafka path only. A crash between a
//! successful produce and the commit leaves the transaction open on the
//! broker; a `read_committed` downstream consumer sees none of it, and
//! reprocessing the same source offset from a fresh relay (or after the
//! transaction fences/times out) produces byte-identical output — the
//! `deblob-schema-id`/`deblob-origin` headers are pure functions of the
//! source record and its cursor, never a freshly minted id (spec §3.2).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use deblob::coldlane::DiscoveryMsg;
use deblob::matcher::HotMatcher;
use deblob::metrics::Metrics;
use deblob_core::envelope::SourceCursor;
use deblob_core::id::SchemaRef;
use deblob_fingerprint::Limits;
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::message::{Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;
use tokio_util::sync::CancellationToken;

use crate::headers;

/// Where to inject a simulated crash inside the per-record
/// produce→commit sequence (Task 17's chaos harness). `None` (the default)
/// runs the normal consume → classify → produce → commit loop with no
/// injected fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultPoint {
    /// After the discovery record has been produced (only reachable for a
    /// `Provisional` classification), but before this record's own
    /// tagged/quarantine output is produced — simulates a crash between the
    /// two produces of the SAME transaction.
    AfterDiscoveryProduce,
    /// After every produce for this record has completed, but before
    /// `send_offsets_to_transaction`/`commit_transaction` — simulates a
    /// crash after the broker has buffered the produced records but before
    /// the transaction (and therefore the consumer offset) is committed. A
    /// `read_committed` consumer must see NONE of this transaction's
    /// records.
    AfterProduceBeforeCommit,
}

/// Configuration for one [`Relay::run`] instance.
pub struct RelayCfg {
    pub brokers: String,
    pub group_id: String,
    pub raw_topic: String,
    pub tagged_topic: String,
    pub discovery_topic: String,
    pub quarantine_topic: String,
    pub transactional_id: String,
    pub limits: Limits,
    /// Chaos-test hook (Task 17); `None` in normal operation — every test
    /// in this crate except the fault-point plumbing itself runs with
    /// `None`.
    pub fault: Option<FaultPoint>,
    /// Shared Prometheus surface (spec §11): [`Relay::run`] increments
    /// `deblob_relay_records_total` once per record read off the raw
    /// topic, and `deblob_relay_transactions_total{result}` once per
    /// transaction outcome.
    pub metrics: Arc<Metrics>,
}

/// Errors [`Relay::run`] can return. Every variant is a genuine relay
/// failure (client construction, a Kafka protocol error the relay could
/// not itself recover by aborting, or a discovery-message serialization
/// failure) — classification outcomes (`Malformed`, `Unresolved`, ...) are
/// never errors, they route to a topic (spec §10: "never silently drop").
#[derive(Debug, thiserror::Error)]
pub enum RelayError {
    #[error("kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("failed to serialize discovery message: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("consumer has no group metadata — was group.id configured?")]
    NoGroupMetadata,
}

/// [`ConsumerContext`] for the relay's own consumer. Its only job is spec
/// §3.2's rebalance rule: "on partition revoke: pause, drain/cancel
/// in-flight, abort open transaction before relinquishing. A task must
/// never commit after its partition was revoked." `pre_rebalance` runs
/// synchronously, on the thread driving the consumer's poll loop, strictly
/// BEFORE librdkafka actually relinquishes the revoked partitions — the
/// only window in which an abort is still meaningful.
#[derive(Clone)]
struct RelayConsumerContext {
    producer: FutureProducer,
    transaction_open: Arc<AtomicBool>,
    abort_timeout: Timeout,
}

impl ClientContext for RelayConsumerContext {}

impl ConsumerContext for RelayConsumerContext {
    fn pre_rebalance(&self, _base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if !matches!(rebalance, Rebalance::Revoke(_)) {
            return;
        }
        // Swap-and-check: only the thread that actually observes `true`
        // aborts, and it can never abort twice for the same open
        // transaction.
        if !self.transaction_open.swap(false, Ordering::SeqCst) {
            return;
        }
        tracing::warn!("pre_rebalance: aborting open transaction before partition revoke");
        if let Err(err) = self.producer.abort_transaction(self.abort_timeout) {
            tracing::error!(error = %err, "abort_transaction failed during pre_rebalance");
        }
    }
}

/// The transactional relay adapter (spec §3.3: "Transactional relay
/// adapter + header TagSink"). Namespacing-only — all state lives on the
/// stack inside [`Relay::run`], scoped to one running relay instance.
pub struct Relay;

impl Relay {
    /// Runs the relay loop until `shutdown` is cancelled or an
    /// unrecoverable [`RelayError`] occurs. One Kafka transaction per
    /// polled record (spec brief: "correctness over throughput for P1").
    pub async fn run(
        cfg: RelayCfg,
        matcher: Arc<HotMatcher>,
        shutdown: CancellationToken,
    ) -> Result<(), RelayError> {
        let transaction_open = Arc::new(AtomicBool::new(false));

        let producer: FutureProducer = producer_client_config(&cfg).create()?;
        producer.init_transactions(Timeout::After(Duration::from_secs(30)))?;

        let context = RelayConsumerContext {
            producer: producer.clone(),
            transaction_open: transaction_open.clone(),
            abort_timeout: Timeout::After(Duration::from_secs(10)),
        };
        let consumer: StreamConsumer<RelayConsumerContext> =
            consumer_client_config(&cfg).create_with_context(context)?;
        consumer.subscribe(&[cfg.raw_topic.as_str()])?;

        loop {
            let msg = tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                msg = consumer.recv() => msg?,
            };

            let topic = msg.topic().to_string();
            let partition = msg.partition();
            let offset = msg.offset();
            let key = msg.key().map(|k| k.to_vec());
            let payload = msg.payload().map(|p| p.to_vec());
            let inbound_headers = headers::strip_reserved(msg.headers());
            // Release the borrow of `consumer` this message holds before
            // we touch `consumer` again (group_metadata, next recv).
            drop(msg);

            cfg.metrics.inc_relay_records();
            let cursor = SourceCursor {
                topic,
                partition,
                offset,
            };

            match process_record(
                &cfg,
                &matcher,
                &producer,
                &transaction_open,
                &consumer,
                cursor,
                key,
                payload,
                inbound_headers,
            )
            .await?
            {
                ProcessOutcome::Committed => cfg.metrics.record_relay_transaction("committed"),
                ProcessOutcome::Aborted => cfg.metrics.record_relay_transaction("aborted"),
                ProcessOutcome::FaultInjected => {
                    // Simulated crash (Task 17 chaos hook): return
                    // immediately WITHOUT aborting or committing — the
                    // open transaction is simply abandoned, exactly like a
                    // real process crash. A `read_committed` consumer must
                    // see none of it.
                    return Ok(());
                }
            }
        }
    }
}

enum ProcessOutcome {
    Committed,
    Aborted,
    FaultInjected,
}

enum TransactionBody {
    Produced,
    Fault,
}

/// Runs one full begin→produce→send_offsets→commit (or abort) cycle for a
/// single polled record.
#[allow(clippy::too_many_arguments)]
async fn process_record(
    cfg: &RelayCfg,
    matcher: &HotMatcher,
    producer: &FutureProducer,
    transaction_open: &AtomicBool,
    consumer: &StreamConsumer<RelayConsumerContext>,
    cursor: SourceCursor,
    key: Option<Vec<u8>>,
    payload: Option<Vec<u8>>,
    inbound_headers: OwnedHeaders,
) -> Result<ProcessOutcome, RelayError> {
    producer.begin_transaction()?;
    transaction_open.store(true, Ordering::SeqCst);

    let body = run_transaction_body(
        cfg,
        matcher,
        producer,
        &cursor,
        key,
        payload,
        inbound_headers,
    )
    .await;

    match body {
        Ok(TransactionBody::Fault) => Ok(ProcessOutcome::FaultInjected),
        Ok(TransactionBody::Produced) => {
            let group_metadata = consumer
                .group_metadata()
                .ok_or(RelayError::NoGroupMetadata)?;
            let mut offsets = TopicPartitionList::new();
            offsets.add_partition_offset(
                &cursor.topic,
                cursor.partition,
                // The offset recorded is the NEXT message this consumer
                // group should read — one past the record just processed.
                Offset::Offset(cursor.offset + 1),
            )?;

            match producer.send_offsets_to_transaction(
                &offsets,
                &group_metadata,
                Timeout::After(Duration::from_secs(10)),
            ) {
                Ok(()) => {
                    producer.commit_transaction(Timeout::After(Duration::from_secs(30)))?;
                    transaction_open.store(false, Ordering::SeqCst);
                    Ok(ProcessOutcome::Committed)
                }
                Err(err) => {
                    tracing::warn!(error = %err, "send_offsets_to_transaction failed, aborting");
                    producer.abort_transaction(Timeout::After(Duration::from_secs(10)))?;
                    transaction_open.store(false, Ordering::SeqCst);
                    Ok(ProcessOutcome::Aborted)
                }
            }
        }
        Err(err) => {
            tracing::warn!(error = %err, "produce failed inside transaction, aborting");
            producer.abort_transaction(Timeout::After(Duration::from_secs(10)))?;
            transaction_open.store(false, Ordering::SeqCst);
            Ok(ProcessOutcome::Aborted)
        }
    }
}

/// Classifies (or tombstone-passes-through) one record and produces its
/// output(s) inside the already-open transaction. Never calls
/// begin/commit/abort itself — that's `process_record`'s job — so a fault
/// injection here can cleanly signal "stop, leave the transaction open"
/// without this function needing to know about transaction bookkeeping.
#[allow(clippy::too_many_arguments)]
async fn run_transaction_body(
    cfg: &RelayCfg,
    matcher: &HotMatcher,
    producer: &FutureProducer,
    cursor: &SourceCursor,
    key: Option<Vec<u8>>,
    payload: Option<Vec<u8>>,
    inbound_headers: OwnedHeaders,
) -> Result<TransactionBody, RelayError> {
    let queue_timeout = Timeout::After(Duration::from_secs(10));

    let Some(payload) = payload else {
        // Kafka tombstone: null value. NOT malformed — no parse attempted
        // at all (spec §3.2), pass through with the reserved tombstone
        // tag, preserving the key so compaction semantics hold.
        let out_headers = headers::with_tag(inbound_headers, &SchemaRef::Tombstone, cursor);
        produce(
            producer,
            &cfg.tagged_topic,
            Some(cursor.partition),
            key.as_deref(),
            None,
            out_headers,
            queue_timeout,
        )
        .await?;
        return Ok(TransactionBody::Produced);
    };

    let classification = matcher.classify(&payload, &cfg.limits).await;

    if let SchemaRef::Provisional(ref cand_id) = classification.schema_ref {
        let discovery = DiscoveryMsg {
            cand_id: cand_id.as_str().to_string(),
            payload: Bytes::from(payload.clone()),
            // The relay has no per-producer identity from the raw Kafka
            // record itself (no reserved header carries one, and
            // inventing one would violate "IDs only, never model output"
            // header hygiene) — the raw topic name is the closest stable
            // "source" identity the cold lane's per-source rate limiter
            // can key on.
            source: cfg.raw_topic.clone(),
            cursor: cursor.clone(),
        };
        let discovery_bytes = serde_json::to_vec(&discovery)?;
        produce(
            producer,
            &cfg.discovery_topic,
            // No source-partition mapping requirement for the discovery
            // topic (spec §3.2's p→p rule is scoped to "derived topic",
            // i.e. the tagged topic); route by candidate id instead so a
            // given candidate's discovery evidence lands on one partition.
            None,
            Some(cand_id.as_str().as_bytes()),
            Some(&discovery_bytes),
            OwnedHeaders::new(),
            queue_timeout,
        )
        .await?;

        if cfg.fault == Some(FaultPoint::AfterDiscoveryProduce) {
            return Ok(TransactionBody::Fault);
        }
    }

    let (target_topic, out_headers) = match &classification.schema_ref {
        SchemaRef::Malformed => {
            let reason = classification
                .quarantine
                .expect("Malformed classification always carries a quarantine reason");
            let h = headers::with_tag(inbound_headers, &classification.schema_ref, cursor);
            let h = headers::with_quarantine_reason(h, reason);
            (&cfg.quarantine_topic, h)
        }
        _ => {
            let h = headers::with_tag(inbound_headers, &classification.schema_ref, cursor);
            (&cfg.tagged_topic, h)
        }
    };

    produce(
        producer,
        target_topic,
        // Derived topic has the same partition count as the raw topic;
        // produce source partition p -> derived partition p, explicitly
        // (never key routing) — spec §3.2.
        Some(cursor.partition),
        key.as_deref(),
        Some(&payload),
        out_headers,
        queue_timeout,
    )
    .await?;

    if cfg.fault == Some(FaultPoint::AfterProduceBeforeCommit) {
        return Ok(TransactionBody::Fault);
    }

    Ok(TransactionBody::Produced)
}

/// Produces one record as part of the currently-open transaction.
async fn produce(
    producer: &FutureProducer,
    topic: &str,
    partition: Option<i32>,
    key: Option<&[u8]>,
    payload: Option<&[u8]>,
    headers: OwnedHeaders,
    queue_timeout: Timeout,
) -> Result<(), RelayError> {
    let mut record = FutureRecord::<[u8], [u8]>::to(topic).headers(headers);
    if let Some(p) = partition {
        record = record.partition(p);
    }
    if let Some(k) = key {
        record = record.key(k);
    }
    if let Some(p) = payload {
        record = record.payload(p);
    }
    producer
        .send(record, queue_timeout)
        .await
        .map_err(|(err, _owned_msg)| RelayError::Kafka(err))?;
    Ok(())
}

/// The consumer-side [`ClientConfig`] (pub(crate) so relay.rs's own unit
/// tests can assert the cooperative-sticky/read-uncommitted settings
/// without spinning up a broker — spec §3.2's rebalance-config rule).
pub(crate) fn consumer_client_config(cfg: &RelayCfg) -> ClientConfig {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", &cfg.brokers)
        .set("group.id", &cfg.group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        // Spec §3.2: cooperative-sticky assignment; on revoke, abort the
        // open transaction before relinquishing (RelayConsumerContext).
        .set("partition.assignment.strategy", "cooperative-sticky")
        .set("session.timeout.ms", "10000")
        // The relay's OWN consumer reads the raw topic, which the relay
        // itself does not produce transactionally — uncommitted-by-the-
        // relay is a non-issue here since nothing upstream of the raw
        // topic is transactional either. `read_committed` is what
        // DOWNSTREAM consumers of tagged/discovery/quarantine must set.
        .set("isolation.level", "read_uncommitted");
    c
}

/// The producer-side [`ClientConfig`]: idempotence + `transactional.id`
/// from `cfg`, required for [`Producer::init_transactions`].
fn producer_client_config(cfg: &RelayCfg) -> ClientConfig {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", &cfg.brokers)
        .set("transactional.id", &cfg.transactional_id)
        .set("enable.idempotence", "true")
        .set("message.timeout.ms", "10000")
        .set("transaction.timeout.ms", "30000");
    c
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> RelayCfg {
        RelayCfg {
            brokers: "localhost:9092".to_string(),
            group_id: "deblob-relay-test".to_string(),
            raw_topic: "raw".to_string(),
            tagged_topic: "tagged".to_string(),
            discovery_topic: "discovery".to_string(),
            quarantine_topic: "quarantine".to_string(),
            transactional_id: "deblob-relay-test-txn".to_string(),
            limits: Limits::default(),
            fault: None,
            metrics: Metrics::new(),
        }
    }

    // Spec §3.2: "Cooperative-sticky assignment" — asserted at the config
    // level without a broker (the Docker-backed relay_it.rs test #7 covers
    // the full "clean run commits normally" behavior end to end).
    #[test]
    fn consumer_config_sets_cooperative_sticky_assignment() {
        let c = consumer_client_config(&cfg());
        assert_eq!(
            c.get("partition.assignment.strategy"),
            Some("cooperative-sticky")
        );
    }

    #[test]
    fn consumer_config_disables_auto_commit() {
        // Offset commits happen exclusively via
        // `send_offsets_to_transaction`, never librdkafka's auto-commit.
        let c = consumer_client_config(&cfg());
        assert_eq!(c.get("enable.auto.commit"), Some("false"));
    }

    #[test]
    fn producer_config_carries_transactional_id_and_idempotence() {
        let c = producer_client_config(&cfg());
        assert_eq!(c.get("transactional.id"), Some("deblob-relay-test-txn"));
        assert_eq!(c.get("enable.idempotence"), Some("true"));
    }
}
