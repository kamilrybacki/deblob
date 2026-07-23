//! The transactional relay (spec §3.1-3.2, batching spec
//! `docs/superpowers/specs/2026-07-16-relay-batching.md` §1-2): consume the
//! raw topic → classify via [`HotMatcher`] → strip/rewrite `deblob-*`
//! headers → transactional produce (tagged/quarantine [+ discovery for
//! provisional shapes]) → `send_offsets_to_transaction` → commit, ONE Kafka
//! transaction per BATCH of up to `max_batch_records` records (or whatever
//! accumulated within `max_batch_linger_ms`) — amortising the per-commit
//! latency across many records instead of paying it once per record.
//!
//! Exactly-once scope (spec §3.1, batching spec §2): Kafka transactions
//! cover consume→produce→offset within the Kafka path only. A crash between
//! the batch's last successful produce and its commit leaves the
//! transaction open on the broker; a `read_committed` downstream consumer
//! sees NONE of the batch's records, and reprocessing the same source
//! offsets from a fresh relay (or after the transaction fences/times out)
//! reproduces the whole batch byte-identically — the
//! `deblob-schema-id`/`deblob-origin` headers are pure functions of the
//! source record and its cursor, never a freshly minted id (spec §3.2).
//! Batching changes only the GRANULARITY of the guarantee (per-batch
//! instead of per-record), never the guarantee itself: a batch is committed
//! or reprocessed as one atomic unit, never partially.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use deblob_core::envelope::SourceCursor;
use deblob_core::error::QuarantineReason;
use deblob_core::id::SchemaRef;
use deblob_fingerprint::Limits;
use deblob_match::discovery::DiscoveryMsg;
use deblob_match::matcher::HotMatcher;
use deblob_match::metrics::Metrics;
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::message::{Message, OwnedHeaders};
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;
use tokio::sync::broadcast;
use tokio::time::{sleep_until, Instant as TokioInstant};
use tokio_util::sync::CancellationToken;

use crate::headers;
use crate::stream::{StreamEvent, StreamOutcome};

/// The Redpanda / librdkafka default single-message ceiling (1 MiB) —
/// [`RelayCfg::max_message_bytes`]'s default when no explicit value is set.
pub const DEFAULT_MAX_MESSAGE_BYTES: usize = 1024 * 1024;

/// Headroom reserved below [`RelayCfg::max_message_bytes`] for a produced
/// record's headers, key, and Kafka framing when size-guarding the payload —
/// so `payload.len() <= max_message_bytes - RELAY_PRODUCE_MARGIN` guarantees
/// the assembled message stays under the broker/producer limit. Generous
/// (16 KiB) relative to the handful of small bounded relay headers.
pub const RELAY_PRODUCE_MARGIN: usize = 16 * 1024;

/// Where to inject a simulated crash inside the per-batch
/// produce→commit sequence (Task 17's chaos harness, extended for batching
/// per `docs/superpowers/specs/2026-07-16-relay-batching.md` §4). `None`
/// (the default) runs the normal consume → classify → produce → commit
/// loop with no injected fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultPoint {
    /// After the discovery record has been produced (only reachable for a
    /// `Provisional` classification), but before that SAME record's own
    /// tagged/quarantine output is produced — simulates a crash between the
    /// two produces of one record, mid-batch. Any records already produced
    /// earlier in the batch, and this record's discovery produce, remain
    /// part of the still-open (never committed) transaction.
    AfterDiscoveryProduce,
    /// After every produce for the WHOLE BATCH has completed, but before
    /// `send_offsets_to_transaction`/`commit_transaction` for the batch —
    /// simulates a crash after the broker has buffered every produced
    /// record in the batch but before the transaction (and therefore the
    /// consumer offsets) is committed. A `read_committed` consumer must see
    /// NONE of this transaction's records — i.e. none of the batch's
    /// records, not just the last one.
    AfterProduceBeforeCommit,
}

/// Configuration for one [`Relay::run`] instance.
pub struct RelayCfg {
    pub brokers: String,
    pub group_id: String,
    pub raw_topic: String,
    /// Every topic the relay consumes from (Hermes review gap 1: multi-topic
    /// subscribe), IN ADDITION to `raw_topic` staying around for back-compat.
    /// [`Relay::run`] subscribes to this list when non-empty; when empty
    /// (every pre-existing call site/test — the zero-value default of a
    /// `Vec`), it falls back to `[raw_topic]` alone, reproducing the exact
    /// pre-this-field single-topic behavior.
    pub raw_topics: Vec<String>,
    pub tagged_topic: String,
    pub discovery_topic: String,
    pub quarantine_topic: String,
    pub transactional_id: String,
    pub limits: Limits,
    /// Flush the accumulated batch and commit ONE transaction once it
    /// reaches this many records (batching spec §3). Clamped to at least 1
    /// by [`Relay::run`] — `1` reproduces the exact pre-batching
    /// per-record-transaction behaviour, a documented escape hatch.
    pub max_batch_records: usize,
    /// Flush the accumulated batch once this many milliseconds have
    /// elapsed since the FIRST record was added to it, even if
    /// `max_batch_records` hasn't been reached — bounds the added latency
    /// of a partially-full batch (batching spec §3). The linger timer does
    /// not start until the batch holds at least one record: [`Relay::run`]
    /// blocks indefinitely for the first record of a new batch.
    pub max_batch_linger_ms: u64,
    /// Flush the accumulated batch once its buffered payload+key bytes reach
    /// this many, EVEN if `max_batch_records` hasn't — so the in-memory batch is
    /// a fixed memory reservoir regardless of per-record size (a batch of 500
    /// near-`max_message_bytes` records would otherwise hold hundreds of MiB;
    /// jr-deblob-stability-231518). Clamped to at least 1 by [`Relay::run`].
    pub max_batch_bytes: usize,
    /// Hard ceiling on a single produced Kafka message (value + key +
    /// headers + framing), mirrored onto the producer's `message.max.bytes`
    /// and enforced BEFORE produce so one oversized record can never abort a
    /// whole batch of good ones (the silent-data-loss bug: an enqueue
    /// `MessageSizeTooLarge` used to abort the transaction, skipping every
    /// record batched with the offender). A record whose payload would not
    /// fit (minus [`RELAY_PRODUCE_MARGIN`] headroom for headers/key/framing)
    /// is routed to the quarantine topic as a compact, PAYLOAD-FREE
    /// `SizeExceeded` marker instead — observable, offset-advancing, never a
    /// batch abort. Defaults to the Redpanda/librdkafka 1 MiB default; raise
    /// it only in lockstep with the broker's `max.message.bytes`.
    pub max_message_bytes: usize,
    /// Chaos-test hook (Task 17); `None` in normal operation — every test
    /// in this crate except the fault-point plumbing itself runs with
    /// `None`.
    pub fault: Option<FaultPoint>,
    /// Shared Prometheus surface (spec §11): [`Relay::run`] increments
    /// `deblob_relay_records_total` once per record read off the raw
    /// topic, and `deblob_relay_transactions_total{result}` once per
    /// transaction outcome.
    pub metrics: Arc<Metrics>,
    /// Optional SASL credentials (spec §9: "rdkafka TLS/SASL supported").
    /// `None` (the default in every existing call site) leaves both the
    /// consumer and producer `ClientConfig` exactly as before this field
    /// existed — plaintext/PLAINTEXT brokers, unchanged. Task 18's
    /// `main.rs` is the only caller that ever constructs `Some`, from the
    /// env-only `DEBLOB_KAFKA_SASL_*` secrets — SASL credentials must never
    /// live in the TOML config file.
    pub sasl: Option<KafkaSasl>,
    /// Live-stream tap (Stage L1, payload-free): `Some` makes
    /// [`process_batch`] broadcast a [`StreamEvent`] per record via a
    /// NON-BLOCKING send once that record's outcome is decided. `None` —
    /// every pre-existing call site/test — skips this entirely: zero
    /// behavior change from before this field existed. The exactly-once
    /// relay's correctness never depends on this channel having a
    /// receiver, room, or even existing (see `crate::stream` docs).
    pub stream_tx: Option<broadcast::Sender<StreamEvent>>,
}

/// SASL credentials for the relay's Kafka clients (spec §9). Never
/// `Debug`/`Display`-derived with the raw fields exposed — see the
/// hand-written [`std::fmt::Debug`] impl below, which redacts `password`.
#[derive(Clone)]
pub struct KafkaSasl {
    /// `sasl.mechanism` (e.g. `PLAIN`, `SCRAM-SHA-512`).
    pub mechanism: String,
    /// `security.protocol` (e.g. `SASL_SSL`, `SASL_PLAINTEXT`).
    pub security_protocol: String,
    pub username: String,
    pub password: String,
}

impl std::fmt::Debug for KafkaSasl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSasl")
            .field("mechanism", &self.mechanism)
            .field("security_protocol", &self.security_protocol)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
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
    /// unrecoverable [`RelayError`] occurs. One Kafka transaction per BATCH
    /// of up to `cfg.max_batch_records` records — or fewer, if
    /// `cfg.max_batch_linger_ms` elapses first, or on shutdown (the
    /// in-flight batch is flushed, then the loop exits) — amortising the
    /// commit latency across the whole batch (batching spec §1).
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
        // Hermes review gap 1: subscribe to every topic in `raw_topics` when
        // non-empty; every pre-existing call site leaves `raw_topics` empty,
        // so this falls back to the single `raw_topic` exactly as before.
        let subscribe_topics: Vec<&str> = if cfg.raw_topics.is_empty() {
            vec![cfg.raw_topic.as_str()]
        } else {
            cfg.raw_topics.iter().map(String::as_str).collect()
        };
        consumer.subscribe(&subscribe_topics)?;

        // `0` would mean "never flush" — clamp to 1, which reproduces the
        // exact pre-batching per-record-transaction behaviour (batching
        // spec §3's documented escape hatch).
        let max_batch_records = cfg.max_batch_records.max(1);
        let max_batch_bytes = cfg.max_batch_bytes.max(1);
        let linger = Duration::from_millis(cfg.max_batch_linger_ms);

        loop {
            let mut batch: Vec<PendingRecord> = Vec::new();
            let mut batch_bytes: usize = 0;
            // `None` until the first record lands — the linger timer must
            // not start (and must not fire) before there is anything to
            // flush (batching spec §1: "Block for the first record").
            let mut deadline: Option<TokioInstant> = None;

            while batch.len() < max_batch_records {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = until_deadline(deadline) => break,
                    msg = consumer.recv() => {
                        let msg = msg?;
                        let topic = msg.topic().to_string();
                        let partition = msg.partition();
                        let offset = msg.offset();
                        let key = msg.key().map(|k| k.to_vec());
                        let payload = msg.payload().map(|p| p.to_vec());
                        let inbound_headers = headers::strip_reserved(msg.headers());
                        // Release the borrow of `consumer` this message
                        // holds before we touch `consumer` again
                        // (group_metadata, next recv).
                        drop(msg);

                        cfg.metrics.inc_relay_records();
                        if deadline.is_none() {
                            deadline = Some(TokioInstant::now() + linger);
                        }
                        let rec_bytes = payload.as_ref().map_or(0, |p| p.len())
                            + key.as_ref().map_or(0, |k| k.len());
                        batch.push(PendingRecord {
                            cursor: SourceCursor { topic, partition, offset },
                            key,
                            payload,
                            headers: inbound_headers,
                        });
                        batch_bytes += rec_bytes;
                        // Byte-bound flush: cap the batch's resident memory
                        // independent of per-record size (jr-deblob-stability-231518).
                        if batch_bytes >= max_batch_bytes {
                            break;
                        }
                    }
                }
            }

            if batch.is_empty() {
                // Only reachable via shutdown firing before any record was
                // accumulated — nothing to flush.
                return Ok(());
            }

            let shutting_down = shutdown.is_cancelled();

            match process_batch(
                &cfg,
                &matcher,
                &producer,
                &transaction_open,
                &consumer,
                batch,
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
                    // see none of the whole batch.
                    return Ok(());
                }
            }

            if shutting_down {
                // The in-flight batch was flushed above; nothing more to
                // accumulate.
                return Ok(());
            }
        }
    }
}

/// Resolves once `deadline` has passed; pends forever if `deadline` is
/// `None` — the vehicle for "no linger timer until the batch holds at
/// least one record" inside `tokio::select!`.
async fn until_deadline(deadline: Option<TokioInstant>) {
    match deadline {
        Some(d) => sleep_until(d).await,
        None => std::future::pending::<()>().await,
    }
}

/// One record accumulated into a batch, holding everything needed to run
/// [`run_transaction_body`] once the batch's transaction is open.
struct PendingRecord {
    cursor: SourceCursor,
    key: Option<Vec<u8>>,
    payload: Option<Vec<u8>>,
    headers: OwnedHeaders,
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

/// Runs one full begin→enqueue(×N)→await-all-deliveries→send_offsets→commit
/// (or abort) cycle for a whole BATCH of records (batching spec §1-§2, and
/// the produce-pipelining fix on top of it): begins one transaction, runs
/// [`run_transaction_body`] for every record in `batch` (in accumulation
/// order) to ENQUEUE its produce(s) — `run_transaction_body` never awaits a
/// delivery itself, it only hands back the [`DeliveryFuture`]s it created —
/// then, once every record in the batch has been enqueued, awaits every
/// collected delivery ONCE, all together. Only after every delivery in the
/// whole batch has confirmed does this send ONE `send_offsets_to_transaction`
/// covering `MAX(offset)+1` for every `(topic, partition)` touched anywhere
/// in the batch, then commits. Any record's enqueue error, any delivery's
/// failure, or the offset-send itself failing, aborts the WHOLE transaction
/// — no partial-batch commit, ever (batching spec §2 "Abort on any error").
///
/// This is what turns ~1000 sequential awaited broker round-trips per
/// 500-record batch (the pre-fix bottleneck: `produce()` used to await each
/// delivery inline) into ~1000 non-blocking local enqueues followed by ONE
/// bounded await-all — the enqueues and their network round-trips overlap
/// instead of serializing, matching the bench producer's `send_result` +
/// batched-drain pattern.
#[allow(clippy::too_many_arguments)]
async fn process_batch(
    cfg: &RelayCfg,
    matcher: &HotMatcher,
    producer: &FutureProducer,
    transaction_open: &AtomicBool,
    consumer: &StreamConsumer<RelayConsumerContext>,
    batch: Vec<PendingRecord>,
) -> Result<ProcessOutcome, RelayError> {
    producer.begin_transaction()?;
    transaction_open.store(true, Ordering::SeqCst);

    // Per-partition MAX(offset) across the batch (batching spec §2): a
    // batch may span multiple raw-topic partitions on one consumer, and
    // the offset commit must cover every one of them, not just the last
    // record processed.
    let mut max_offsets: BTreeMap<(String, i32), i64> = BTreeMap::new();

    // Every DeliveryFuture enqueued across the whole batch (bounded by
    // `max_batch_records` × produces-per-record — at most 2 per record,
    // tagged/quarantine + optional discovery), awaited ONCE after the loop
    // below instead of inline per record.
    let mut deliveries: Vec<DeliveryFuture> = Vec::new();

    for record in batch {
        let PendingRecord {
            cursor,
            key,
            payload,
            headers: inbound_headers,
        } = record;

        max_offsets
            .entry((cursor.topic.clone(), cursor.partition))
            .and_modify(|max| *max = (*max).max(cursor.offset))
            .or_insert(cursor.offset);

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
            Ok((TransactionBody::Produced, mut record_deliveries)) => {
                deliveries.append(&mut record_deliveries);
                continue;
            }
            Ok((TransactionBody::Fault, _record_deliveries)) => {
                // Simulated crash mid-batch (AfterDiscoveryProduce, chaos
                // hook): stop processing the rest of the batch and leave
                // the transaction open — the caller returns immediately
                // without committing or aborting. Any deliveries already
                // enqueued (including this record's discovery produce) are
                // dropped here — they're already part of the still-open
                // transaction on the broker regardless of whether this
                // process ever awaits their local delivery future.
                return Ok(ProcessOutcome::FaultInjected);
            }
            Err(err) => {
                tracing::warn!(error = %err, "enqueue failed inside transaction, aborting batch");
                producer.abort_transaction(Timeout::After(Duration::from_secs(10)))?;
                transaction_open.store(false, Ordering::SeqCst);
                return Ok(ProcessOutcome::Aborted);
            }
        }
    }

    // Every record in the batch enqueued cleanly — now await every
    // delivery for the WHOLE BATCH, once, in one pass. This is the actual
    // pipelining: all the enqueues above ran without blocking on a broker
    // round-trip, so their underlying network I/O already overlapped; this
    // loop just collects the outcomes. ANY delivery failure aborts the
    // whole batch — exactly once means no partial commit, ever.
    for delivery in deliveries {
        match delivery.await {
            Ok(Ok(_partition_offset)) => {}
            Ok(Err((err, _owned_msg))) => {
                tracing::warn!(error = %err, "delivery failed inside transaction, aborting batch");
                producer.abort_transaction(Timeout::After(Duration::from_secs(10)))?;
                transaction_open.store(false, Ordering::SeqCst);
                return Ok(ProcessOutcome::Aborted);
            }
            Err(_canceled) => {
                // The producer was dropped before this delivery's report
                // arrived — never expected in practice (the producer
                // outlives `process_batch`) but must abort rather than
                // silently treating a never-confirmed delivery as success.
                tracing::warn!("delivery future canceled inside transaction, aborting batch");
                producer.abort_transaction(Timeout::After(Duration::from_secs(10)))?;
                transaction_open.store(false, Ordering::SeqCst);
                return Ok(ProcessOutcome::Aborted);
            }
        }
    }

    // Batching spec §1/§4: AfterProduceBeforeCommit now fires after the
    // BATCH's produces AND their deliveries have ALL been awaited above,
    // before the batch's send_offsets_to_transaction/commit_transaction.
    if cfg.fault == Some(FaultPoint::AfterProduceBeforeCommit) {
        return Ok(ProcessOutcome::FaultInjected);
    }

    let group_metadata = consumer
        .group_metadata()
        .ok_or(RelayError::NoGroupMetadata)?;
    let mut offsets = TopicPartitionList::new();
    for ((topic, partition), max_offset) in &max_offsets {
        offsets.add_partition_offset(
            topic,
            *partition,
            // The offset recorded is the NEXT message this consumer group
            // should read — one past the highest offset processed on this
            // partition anywhere in the batch.
            Offset::Offset(max_offset + 1),
        )?;
    }

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

/// Classifies (or tombstone-passes-through) one record and ENQUEUES its
/// output(s) inside the already-open (batch) transaction, returning the
/// [`DeliveryFuture`](s) it created WITHOUT awaiting them — `process_batch`
/// collects every record's deliveries across the whole batch and awaits
/// them all together, once, after the last record here has enqueued (the
/// produce-pipelining fix: this used to `.await` each delivery inline,
/// which serialized every record behind its own broker round-trip). Never
/// calls begin/commit/abort itself — that's `process_batch`'s job — so a
/// fault injection here can cleanly signal "stop, leave the transaction
/// open" without this function needing to know about transaction
/// bookkeeping.
#[allow(clippy::too_many_arguments)]
async fn run_transaction_body(
    cfg: &RelayCfg,
    matcher: &HotMatcher,
    producer: &FutureProducer,
    cursor: &SourceCursor,
    key: Option<Vec<u8>>,
    payload: Option<Vec<u8>>,
    inbound_headers: OwnedHeaders,
) -> Result<(TransactionBody, Vec<DeliveryFuture>), RelayError> {
    let Some(payload) = payload else {
        // Kafka tombstone: null value. NOT malformed — no parse attempted
        // at all (spec §3.2), pass through with the reserved tombstone
        // tag, preserving the key so compaction semantics hold.
        let out_headers = headers::with_tag(inbound_headers, &SchemaRef::Tombstone, cursor);
        let delivery = produce(
            producer,
            &cfg.tagged_topic,
            Some(cursor.partition),
            key.as_deref(),
            None,
            out_headers,
        )?;

        // Live-stream tap (Stage L1): a tombstone never reaches
        // `HotMatcher::classify` (no payload to parse), so its `StreamEvent`
        // is built directly here rather than from a `Classification`.
        emit_stream_event(
            cfg,
            cursor,
            StreamOutcome::Tagged,
            SchemaRef::Tombstone.header_value(),
            None,
            0,
        );

        // AfterProduceBeforeCommit is now checked once per BATCH in
        // `process_batch`, after every record's deliveries (including this
        // tombstone's) have been awaited — not per record.
        return Ok((TransactionBody::Produced, vec![delivery]));
    };

    // Produce-size guard (fix for the MessageSizeTooLarge batch-abort /
    // silent-data-loss bug): a payload that would not fit under the producer's
    // `message.max.bytes` once headers/key/framing are added is routed to
    // quarantine as a compact, PAYLOAD-FREE `SizeExceeded` marker instead of
    // being produced whole. A whole-payload produce that overflows returns
    // `MessageSizeTooLarge` at enqueue, which aborts the ENTIRE batch (every
    // good record with it) and — because the in-memory consumer position has
    // already advanced — silently skips them. The marker always fits, so the
    // offset commits and the batch survives; the oversized record stays
    // observable in quarantine (its full payload is still retained in the raw
    // topic, addressable by this cursor).
    let produce_cap = cfg.max_message_bytes.saturating_sub(RELAY_PRODUCE_MARGIN);
    if payload.len() > produce_cap {
        let out_headers = headers::with_tag(inbound_headers, &SchemaRef::Malformed, cursor);
        let out_headers =
            headers::with_quarantine_reason(out_headers, QuarantineReason::SizeExceeded);
        let delivery = produce(
            producer,
            &cfg.quarantine_topic,
            Some(cursor.partition),
            key.as_deref(),
            None, // PAYLOAD-FREE marker: the oversized payload can't be produced whole
            out_headers,
        )?;
        tracing::warn!(
            topic = %cursor.topic,
            partition = cursor.partition,
            offset = cursor.offset,
            payload_bytes = payload.len(),
            cap = produce_cap,
            "record too large to produce; quarantined as size_exceeded (payload not forwarded)"
        );
        emit_stream_event(
            cfg,
            cursor,
            StreamOutcome::Quarantined,
            SchemaRef::Malformed.header_value(),
            Some(headers::quarantine_reason_value(QuarantineReason::SizeExceeded).to_string()),
            0,
        );
        return Ok((TransactionBody::Produced, vec![delivery]));
    }

    let mut deliveries = Vec::with_capacity(2);

    // `cursor.topic` (Hermes lineage gap 3, spec §4/§9) is the ACTUAL
    // consumed topic — folded into any freshly minted `Provisional`
    // candidate id by `HotMatcher::classify` so two sources sharing the
    // exact same raw shape never collide on one candidate.
    let classification = matcher.classify(&cursor.topic, &payload, &cfg.limits).await;

    // Live-stream tap (Stage L1): emitted immediately once the
    // classification is decided — the SAME timing `HotMatcher::classify`
    // already uses for its own `deblob_messages_total`/`deblob_tag_latency_
    // seconds` metrics (recorded inside `classify` itself, unconditional on
    // whether this record's produce/transaction ever commits). Non-blocking,
    // best-effort: see `emit_stream_event` docs.
    let stream_outcome = match &classification.schema_ref {
        SchemaRef::Malformed => StreamOutcome::Quarantined,
        SchemaRef::Provisional(_) => StreamOutcome::NewCandidate,
        SchemaRef::Known(_) | SchemaRef::Unresolved | SchemaRef::Tombstone => StreamOutcome::Tagged,
    };
    let stream_reason = classification
        .quarantine
        .map(|reason| headers::quarantine_reason_value(reason).to_string());
    emit_stream_event(
        cfg,
        cursor,
        stream_outcome,
        classification.schema_ref.header_value(),
        stream_reason,
        classification.fields_count,
    );

    if let SchemaRef::Provisional(ref cand_id) = classification.schema_ref {
        let discovery = DiscoveryMsg {
            cand_id: cand_id.as_str().to_string(),
            payload: Bytes::from(payload.clone()),
            // The relay has no per-producer identity from the raw Kafka
            // record itself (no reserved header carries one, and
            // inventing one would violate "IDs only, never model output"
            // header hygiene) — the ACTUAL topic this record was consumed
            // from (Hermes review gap 1 fix: was `cfg.raw_topic`, which lied
            // once multi-topic subscribe existed — now the record's own
            // `cursor.topic`) is the closest stable "source" identity the
            // cold lane's per-source rate limiter can key on.
            source: cursor.topic.clone(),
            cursor: cursor.clone(),
        };
        let discovery_bytes = serde_json::to_vec(&discovery)?;
        // The discovery record embeds the payload (as JSON/base64 ≈ 1.3× the
        // raw bytes), so for a near-limit provisional record it overflows the
        // produce ceiling BEFORE the tagged produce does. Skip only the
        // discovery evidence when it won't fit — best-effort by design — and
        // still tag the record below; never abort the batch for it.
        if discovery_bytes.len() > produce_cap {
            tracing::warn!(
                topic = %cursor.topic,
                partition = cursor.partition,
                offset = cursor.offset,
                discovery_bytes = discovery_bytes.len(),
                cap = produce_cap,
                "discovery evidence too large to produce; skipped (record still tagged)"
            );
        } else {
            let delivery = produce(
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
            )?;
            deliveries.push(delivery);

            if cfg.fault == Some(FaultPoint::AfterDiscoveryProduce) {
                return Ok((TransactionBody::Fault, deliveries));
            }
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

    let delivery = produce(
        producer,
        target_topic,
        // Derived topic has the same partition count as the raw topic;
        // produce source partition p -> derived partition p, explicitly
        // (never key routing) — spec §3.2.
        Some(cursor.partition),
        key.as_deref(),
        Some(&payload),
        out_headers,
    )?;
    deliveries.push(delivery);

    // AfterProduceBeforeCommit is now checked once per BATCH in
    // `process_batch`, after every record's deliveries have all been
    // awaited — not per record.
    Ok((TransactionBody::Produced, deliveries))
}

/// Enqueues one record as part of the currently-open transaction via
/// [`FutureProducer::send_result`] and returns its [`DeliveryFuture`]
/// WITHOUT awaiting it. `send_result` enqueues onto the producer's local
/// queue and returns immediately — it does NOT wait for the broker's
/// delivery ack the way the old `FutureProducer::send` call this replaces
/// did. Awaiting every delivery inline (the bug this fixes) serialized
/// every produce inside a transaction behind its own network round-trip:
/// a 500-record batch with up to 2 produces per record meant ~1000
/// sequential awaited round-trips per transaction (~10s), even though
/// batching had already amortised the commit itself. The caller
/// ([`process_batch`], via [`run_transaction_body`]) collects every
/// record's `DeliveryFuture` across the whole batch and awaits them all
/// together, once, before `send_offsets_to_transaction`/commit — so the
/// enqueues (and their underlying network I/O) overlap instead of
/// serializing.
///
/// The immediate-enqueue error case (e.g. the local queue is full) is
/// mapped to a [`RelayError`] here, exactly like the old inline delivery
/// failure was — the caller's existing `Err(err)` arm aborts the whole
/// batch, unchanged.
fn produce(
    producer: &FutureProducer,
    topic: &str,
    partition: Option<i32>,
    key: Option<&[u8]>,
    payload: Option<&[u8]>,
    headers: OwnedHeaders,
) -> Result<DeliveryFuture, RelayError> {
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
        .send_result(record)
        .map_err(|(err, _record)| RelayError::Kafka(err))
}

/// Builds and broadcasts one payload-free [`StreamEvent`] for the live-
/// stream tap (Stage L1) — a NO-OP when `cfg.stream_tx` is `None` (every
/// pre-Stage-L1 call site/test). `broadcast::Sender::send` is itself
/// synchronous/non-blocking (it never awaits a receiver — it only fails
/// when there are currently zero receivers, or is silently lossy for a
/// receiver lagging behind the channel's fixed capacity), matching this
/// tap's "never block or fail the hot path" contract; the `Result` is
/// deliberately ignored — the exactly-once relay's correctness never
/// depends on this succeeding.
fn emit_stream_event(
    cfg: &RelayCfg,
    cursor: &SourceCursor,
    outcome: StreamOutcome,
    schema_ref: String,
    reason: Option<String>,
    fields_count: u32,
) {
    let Some(tx) = &cfg.stream_tx else {
        return;
    };
    let event = StreamEvent {
        ts_ms: now_ms(),
        lane: "hot",
        origin: cursor.clone(),
        outcome,
        schema_ref,
        // Not populated at Stage L1 — see `StreamEvent::family_id` docs.
        family_id: None,
        reason,
        fields_count,
        source: Some(cursor.topic.clone()),
    };
    let _ = tx.send(event);
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
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
        .set("isolation.level", "read_uncommitted")
        // BOUND THE PREFETCH so the relay's memory is independent of backlog
        // size and partition count (diagnosed 2026-07-23: 35 OOM restarts while
        // draining a 9388-message lag). librdkafka's default per-partition fetch
        // queue (`queued.max.messages.kbytes` ~1 GiB) times the multi-topic
        // subscribe (~40 raw topics x 4 partitions = ~160 partitions) can buffer
        // many GiB under a lag backlog and OOM the process. Cap it to ~2 MiB per
        // partition (~320 MiB total worst-case) — still ample in-flight for the
        // batched relay loop, but a lag spike can no longer balloon RSS.
        .set("queued.max.messages.kbytes", "2048")
        .set("queued.min.messages", "2000")
        .set("fetch.message.max.bytes", "1048576")
        .set("fetch.max.bytes", "8388608");
    apply_sasl(&mut c, &cfg.sasl);
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
        .set("transaction.timeout.ms", "30000")
        // Mirror the relay's own produce-size guard onto the client so the two
        // agree on the ceiling (the guard routes anything larger to quarantine
        // BEFORE it ever reaches this limit).
        .set("message.max.bytes", cfg.max_message_bytes.to_string())
        // Bound the producer send queue so it is another fixed memory reservoir,
        // not a ~1 GiB one (librdkafka `queue.buffering.max.kbytes` default,
        // jr-deblob-stability-231518). 64 MiB is ample for the batched
        // transactional relay; if the queue fills, produce back-pressures (a
        // bounded wait) rather than growing RSS.
        .set("queue.buffering.max.kbytes", "65536")
        .set("queue.buffering.max.messages", "200000");
    apply_sasl(&mut c, &cfg.sasl);
    c
}

/// Applies SASL credentials to a `ClientConfig` if present — shared by
/// both the consumer and producer builders (and, outside this module,
/// [`crate::discovery_producer`]'s standalone producer) so every Kafka
/// client this crate builds never drifts on how
/// `security.protocol`/`sasl.mechanism` get set. `pub(crate)` rather than
/// private so the discovery-producer module can reuse it without
/// duplicating the SASL wiring.
pub(crate) fn apply_sasl(c: &mut ClientConfig, sasl: &Option<KafkaSasl>) {
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

    fn cfg() -> RelayCfg {
        RelayCfg {
            brokers: "localhost:9092".to_string(),
            group_id: "deblob-relay-test".to_string(),
            raw_topic: "raw".to_string(),
            raw_topics: Vec::new(),
            tagged_topic: "tagged".to_string(),
            discovery_topic: "discovery".to_string(),
            quarantine_topic: "quarantine".to_string(),
            transactional_id: "deblob-relay-test-txn".to_string(),
            limits: Limits::default(),
            max_batch_records: 500,
            max_batch_linger_ms: 100,
            max_batch_bytes: 32 * 1024 * 1024,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            fault: None,
            metrics: Metrics::new(),
            sasl: None,
            stream_tx: None,
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

    // Spec §9: "rdkafka TLS/SASL supported" — `sasl: None` (every existing
    // call site) leaves both client configs exactly as before this field
    // existed; `Some` applies identically to consumer and producer.
    #[test]
    fn no_sasl_by_default() {
        let c = consumer_client_config(&cfg());
        assert_eq!(c.get("security.protocol"), None);
        assert_eq!(c.get("sasl.mechanism"), None);
    }

    #[test]
    fn sasl_applies_to_both_consumer_and_producer_configs() {
        let mut with_sasl = cfg();
        with_sasl.sasl = Some(KafkaSasl {
            mechanism: "SCRAM-SHA-512".to_string(),
            security_protocol: "SASL_SSL".to_string(),
            username: "deblob".to_string(),
            password: "s3cr3t".to_string(),
        });

        let consumer = consumer_client_config(&with_sasl);
        assert_eq!(consumer.get("security.protocol"), Some("SASL_SSL"));
        assert_eq!(consumer.get("sasl.mechanism"), Some("SCRAM-SHA-512"));
        assert_eq!(consumer.get("sasl.username"), Some("deblob"));
        assert_eq!(consumer.get("sasl.password"), Some("s3cr3t"));

        let producer = producer_client_config(&with_sasl);
        assert_eq!(producer.get("security.protocol"), Some("SASL_SSL"));
        assert_eq!(producer.get("sasl.mechanism"), Some("SCRAM-SHA-512"));
    }

    #[test]
    fn kafka_sasl_debug_redacts_password() {
        let sasl = KafkaSasl {
            mechanism: "PLAIN".to_string(),
            security_protocol: "SASL_PLAINTEXT".to_string(),
            username: "deblob".to_string(),
            password: "s3cr3t".to_string(),
        };
        let rendered = format!("{sasl:?}");
        assert!(!rendered.contains("s3cr3t"), "rendered: {rendered}");
        assert!(rendered.contains("<redacted>"));
    }
}
