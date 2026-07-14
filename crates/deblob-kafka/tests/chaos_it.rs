//! Crash-consistency + rebalance chaos suite for the transactional relay
//! (Task 17), backed by a real single-node KRaft `apache/kafka` container
//! (testcontainers). Complements `relay_it.rs`'s clean-path behaviors with
//! the failure-mode guarantees spec §3.1-3.2 actually depends on:
//!
//! - a crash between produce and commit must leave NO trace under
//!   `read_committed` (this closes the gap the Task 16 review flagged —
//!   `abort_visibility_read_committed_sees_nothing` is the load-bearing
//!   test in this file);
//! - reprocessing after such a crash is exactly-once, byte-identical to a
//!   clean run;
//! - replaying the same source range through independent fresh relays is
//!   idempotent (tags are pure functions of shape + cursor, never freshly
//!   minted);
//! - a real consumer-group rebalance (cooperative-sticky, mid-stream)
//!   loses nothing and duplicates nothing.
//!
//! Every verification consumer here sets `isolation.level=read_committed`,
//! same rationale as `relay_it.rs`. This file is its own integration-test
//! binary (Rust compiles each `tests/*.rs` file separately), so the
//! container/topic/consumer helpers below intentionally mirror
//! `relay_it.rs`'s rather than importing them — there is nothing to import
//! from.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use deblob_core::error::CoreError;
use deblob_core::id::{CandidateId, FamilyVersion, SchemaId};
use deblob_core::ports::{Registry, SchemaRecord};
use deblob_fingerprint::Limits;
use deblob_kafka::{FaultPoint, Relay, RelayCfg, RelayError};
use deblob_match::matcher::HotMatcher;
use deblob_match::metrics::Metrics;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{Headers, Message, OwnedHeaders, OwnedMessage};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use testcontainers_modules::kafka::apache;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tokio_util::sync::CancellationToken;

/// Starts a single-node KRaft `apache/kafka` container with the internal
/// `__transaction_state` topic's replication factor forced down to 1 (see
/// `relay_it.rs::start_kafka` for the full rationale).
async fn start_kafka() -> ContainerAsync<apache::Kafka> {
    apache::Kafka::default()
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .start()
        .await
        .expect("kafka container must start")
}

async fn broker_addr(kafka: &ContainerAsync<apache::Kafka>) -> String {
    format!(
        "127.0.0.1:{}",
        kafka
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("mapped kafka port")
    )
}

/// Registry fake: every lookup misses, so an unseen shape always tags
/// `Provisional` — these tests exercise the relay's crash/replay/rebalance
/// behavior, not classification outcomes, so a constant miss is enough
/// (same rationale as `relay_it.rs::MissRegistry`).
struct MissRegistry;

#[async_trait::async_trait]
impl Registry for MissRegistry {
    async fn get_schema(&self, _id: &SchemaId) -> Result<Option<SchemaRecord>, CoreError> {
        Ok(None)
    }

    async fn resolve_structural(
        &self,
        _bucket_key: &str,
        _fingerprint: &SchemaId,
    ) -> Result<Option<SchemaId>, CoreError> {
        Ok(None)
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
        unimplemented!("hot-path relay never publishes")
    }

    async fn get_alias(&self, _id: &CandidateId) -> Result<Option<SchemaId>, CoreError> {
        Ok(None)
    }

    async fn list_schemas(
        &self,
        _cursor: Option<String>,
        _limit: usize,
    ) -> Result<(Vec<SchemaRecord>, Option<String>), CoreError> {
        Ok((vec![], None))
    }

    async fn list_families_in_buckets(
        &self,
        _bucket_keys: &[String],
    ) -> Result<Vec<deblob_core::ports::FamilyRef>, CoreError> {
        Ok(vec![])
    }
}

fn matcher() -> Arc<HotMatcher> {
    Arc::new(HotMatcher::new(Arc::new(MissRegistry), 16, Metrics::new()))
}

struct TestTopics {
    raw: String,
    tagged: String,
    discovery: String,
    quarantine: String,
}

/// One topic set, all four names sharing `prefix`.
fn topics(prefix: &str) -> TestTopics {
    TestTopics {
        raw: format!("{prefix}-raw"),
        tagged: format!("{prefix}-tagged"),
        discovery: format!("{prefix}-discovery"),
        quarantine: format!("{prefix}-quarantine"),
    }
}

/// A topic set that reuses an EXISTING raw topic name but mints fresh
/// tagged/discovery/quarantine topics under `prefix` — used whenever two
/// (or more) independent relay runs must consume the SAME source records
/// (crash-then-recover, or clean-vs-recovery comparison) without their
/// outputs colliding on one topic.
fn topics_sharing_raw(raw: &str, prefix: &str) -> TestTopics {
    TestTopics {
        raw: raw.to_string(),
        tagged: format!("{prefix}-tagged"),
        discovery: format!("{prefix}-discovery"),
        quarantine: format!("{prefix}-quarantine"),
    }
}

/// Creates every topic in `names` with `partitions` partitions, replication
/// factor 1. `partitions` must be >= the raw topic's partition count for
/// every derived topic (tagged/quarantine are produced to explicitly by
/// source partition index, spec §3.2's p -> p rule), so callers pass one
/// consistent count for a whole topic set.
async fn create_topics(brokers: &str, names: &[&str], partitions: i32) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("admin client");
    let new_topics: Vec<NewTopic> = names
        .iter()
        .map(|n| NewTopic::new(n, partitions, TopicReplication::Fixed(1)))
        .collect();
    let refs: Vec<&NewTopic> = new_topics.iter().collect();
    let results = admin
        .create_topics(refs, &AdminOptions::new())
        .await
        .expect("create_topics call");
    for r in results {
        r.expect("topic creation must succeed");
    }
}

fn relay_cfg(
    brokers: &str,
    t: &TestTopics,
    group_id: &str,
    txn_id: &str,
    fault: Option<FaultPoint>,
) -> RelayCfg {
    RelayCfg {
        brokers: brokers.to_string(),
        group_id: group_id.to_string(),
        raw_topic: t.raw.clone(),
        tagged_topic: t.tagged.clone(),
        discovery_topic: t.discovery.clone(),
        quarantine_topic: t.quarantine.clone(),
        transactional_id: txn_id.to_string(),
        limits: Limits::default(),
        fault,
        metrics: Metrics::new(),
        sasl: None,
    }
}

fn raw_producer(brokers: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "10000")
        .create()
        .expect("raw producer")
}

/// A verification consumer: `isolation.level=read_committed` so it can
/// NEVER observe a record from an aborted or still-open transaction.
fn committed_consumer(brokers: &str, group_id: &str, topic: &str) -> StreamConsumer {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("isolation.level", "read_committed")
        .create()
        .expect("verification consumer");
    consumer.subscribe(&[topic]).expect("subscribe");
    consumer
}

async fn recv_owned(consumer: &StreamConsumer, timeout: Duration) -> OwnedMessage {
    let msg = tokio::time::timeout(timeout, consumer.recv())
        .await
        .expect("message within timeout")
        .expect("no kafka error");
    msg.detach()
}

/// Bounded-wait recv that returns `None` on a timed-out deadline instead of
/// panicking — the vehicle for "assert read_committed sees NOTHING" and for
/// draining a stream until a deadline. Deterministic, not a race: an open
/// or aborted transaction's records are categorically invisible to a
/// `read_committed` consumer no matter how long the deadline is, so a short
/// deadline is sufficient and non-flaky for the "sees nothing" assertions.
async fn try_recv_owned(consumer: &StreamConsumer, timeout: Duration) -> Option<OwnedMessage> {
    match tokio::time::timeout(timeout, consumer.recv()).await {
        Ok(Ok(msg)) => Some(msg.detach()),
        Ok(Err(err)) => panic!("kafka error while polling: {err}"),
        Err(_) => None,
    }
}

fn header_map(headers: Option<&OwnedHeaders>) -> HashMap<String, Option<Vec<u8>>> {
    let mut map = HashMap::new();
    if let Some(headers) = headers {
        for h in headers.iter() {
            map.insert(h.key.to_string(), h.value.map(|v| v.to_vec()));
        }
    }
    map
}

/// Cancels `shutdown` and awaits `handle`, asserting the relay task
/// actually returned `Ok(())` within the deadline — used wherever a test's
/// correctness claim ("no post-revoke commit error corrupts state")
/// depends on the relay having shut down cleanly, not just on the test
/// loop giving up waiting for it.
async fn stop_checked(
    shutdown: CancellationToken,
    handle: tokio::task::JoinHandle<Result<(), RelayError>>,
    label: &str,
) {
    shutdown.cancel();
    let outcome = tokio::time::timeout(Duration::from_secs(15), handle).await;
    match outcome {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(err))) => panic!("{label}: relay returned an error on shutdown: {err}"),
        Ok(Err(join_err)) => panic!("{label}: relay task panicked: {join_err}"),
        Err(_) => panic!("{label}: relay did not shut down within the deadline"),
    }
}

async fn produce_raw(producer: &FutureProducer, topic: &str, partition: i32, payload: &[u8]) {
    producer
        .send(
            FutureRecord::<[u8], [u8]>::to(topic)
                .partition(partition)
                .payload(payload),
            Duration::from_secs(5),
        )
        .await
        .expect("produce raw record");
}

fn origin(topic: &str, partition: i32, offset: i64) -> String {
    format!("{topic}/{partition}/{offset}")
}

// ---------------------------------------------------------------------
// Test 1 (closes the Task 16 review gap): a crash between produce and
// commit leaves NOTHING visible under read_committed on either the tagged
// or the discovery topic — then a fresh relay reprocessing the same
// source offset produces the record exactly once, with the correct tag.
// ---------------------------------------------------------------------
#[tokio::test]
async fn abort_visibility_read_committed_sees_nothing() {
    let kafka = start_kafka().await;
    let brokers = broker_addr(&kafka).await;
    let raw = "abort-raw".to_string();
    create_topics(&brokers, &[&raw], 1).await;
    let t = topics_sharing_raw(&raw, "abort");
    create_topics(&brokers, &[&t.tagged, &t.discovery, &t.quarantine], 1).await;

    let producer = raw_producer(&brokers);
    // An unknown shape: MissRegistry always tags Provisional, so this
    // record's transaction produces to BOTH the discovery and the tagged
    // topic — a genuinely multi-produce transaction, not a trivial one.
    let payload = br#"{"abort_field_xyz":true}"#.to_vec();
    produce_raw(&producer, &raw, 0, &payload).await;

    // Relay A: fault AFTER the produce(s) complete but BEFORE
    // send_offsets_to_transaction/commit_transaction — simulates a crash
    // with the transaction left open on the broker. `Relay::run` returns
    // `Ok(())` itself as soon as the fault fires (spec: "return
    // immediately WITHOUT aborting or committing").
    let shutdown_a = CancellationToken::new();
    let handle_a = tokio::spawn(Relay::run(
        relay_cfg(
            &brokers,
            &t,
            "abort-group",
            "abort-txn",
            Some(FaultPoint::AfterProduceBeforeCommit),
        ),
        matcher(),
        shutdown_a.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(30), handle_a)
        .await
        .expect("relay A must return promptly after fault injection")
        .expect("relay A task must not panic")
        .expect("relay A must return Ok(()) after a simulated crash");

    // The transaction was never committed (and even if the relay's own
    // shutdown path opportunistically aborted it via pre_rebalance, it was
    // never COMMITTED) — a read_committed consumer must see NOTHING on
    // either downstream topic. No partial output: the discovery produce
    // that happened before the fault point is just as invisible as the
    // tagged produce that triggered it.
    let tagged_peek = committed_consumer(&brokers, "abort-tagged-peek", &t.tagged);
    assert!(
        try_recv_owned(&tagged_peek, Duration::from_secs(8))
            .await
            .is_none(),
        "read_committed must see NOTHING on the tagged topic while the transaction is unresolved"
    );
    let discovery_peek = committed_consumer(&brokers, "abort-discovery-peek", &t.discovery);
    assert!(
        try_recv_owned(&discovery_peek, Duration::from_secs(8))
            .await
            .is_none(),
        "read_committed must see NOTHING on the discovery topic — no partial output"
    );
    drop(tagged_peek);
    drop(discovery_peek);

    // Recovery: a FRESH relay, SAME transactional_id (so init_transactions
    // fences relay A's incarnation and aborts any still-dangling
    // transaction — real crash-recovery semantics) and SAME group_id (so
    // it resumes from the never-committed offset), no fault.
    let shutdown_b = CancellationToken::new();
    let handle_b = tokio::spawn(Relay::run(
        relay_cfg(&brokers, &t, "abort-group", "abort-txn", None),
        matcher(),
        shutdown_b.clone(),
    ));

    let verify = committed_consumer(&brokers, "abort-verify-final", &t.tagged);
    let out = recv_owned(&verify, Duration::from_secs(30)).await;
    let headers = header_map(out.headers());
    let schema_id = String::from_utf8(
        headers
            .get("deblob-schema-id")
            .expect("schema id header present")
            .clone()
            .expect("schema id header has a value"),
    )
    .expect("schema id is utf8");
    assert!(
        schema_id.starts_with("cand_"),
        "reprocessed record must tag Provisional: {schema_id}"
    );
    assert_eq!(
        headers.get("deblob-origin").unwrap().as_deref(),
        Some(origin(&raw, 0, 0).as_bytes())
    );

    // Exactly once: no second copy shows up (the aborted attempt left no
    // trace to be double-delivered from).
    assert!(
        try_recv_owned(&verify, Duration::from_secs(5))
            .await
            .is_none(),
        "reprocessing must be exactly once — no duplicate from the aborted attempt"
    );

    stop_checked(shutdown_b, handle_b, "relay B").await;
}

// ---------------------------------------------------------------------
// Test 2: a crash mid-batch (fault on the first record) is recovered by a
// fresh relay restart (same group + transactional_id); every input record
// ends up committed EXACTLY ONCE, with headers byte-identical to an
// independent clean run over the same raw records.
// ---------------------------------------------------------------------
#[tokio::test]
async fn kill_between_produce_and_commit_reprocess_exactly_once() {
    let kafka = start_kafka().await;
    let brokers = broker_addr(&kafka).await;
    let raw = "kill-raw".to_string();
    create_topics(&brokers, &[&raw], 2).await;
    let recovery = topics_sharing_raw(&raw, "kill-recovery");
    let clean = topics_sharing_raw(&raw, "kill-clean");
    create_topics(
        &brokers,
        &[
            &recovery.tagged,
            &recovery.discovery,
            &recovery.quarantine,
            &clean.tagged,
            &clean.discovery,
            &clean.quarantine,
        ],
        2,
    )
    .await;

    let producer = raw_producer(&brokers);
    let mut expected = Vec::new();
    for i in 0..4i64 {
        let partition = (i % 2) as i32;
        let payload = format!(r#"{{"n":{i}}}"#).into_bytes();
        produce_raw(&producer, &raw, partition, &payload).await;
        expected.push(origin(&raw, partition, i / 2));
    }
    let expected_origins: HashSet<String> = expected.into_iter().collect();
    assert_eq!(expected_origins.len(), 4, "4 distinct source offsets");

    // Relay A: fault on the FIRST record it happens to read (order across
    // the 2 partitions is not guaranteed) — simulates a crash mid-batch.
    // `Relay::run` returns as soon as the fault fires, so the other 3
    // records were never even consumed (no offset committed for them
    // either).
    let shutdown_a = CancellationToken::new();
    let handle_a = tokio::spawn(Relay::run(
        relay_cfg(
            &brokers,
            &recovery,
            "kill-group",
            "kill-txn",
            Some(FaultPoint::AfterProduceBeforeCommit),
        ),
        matcher(),
        shutdown_a.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(30), handle_a)
        .await
        .expect("relay A must return promptly after fault injection")
        .expect("relay A task must not panic")
        .expect("relay A must return Ok(()) after a simulated crash");

    // Recovery: fresh relay, SAME group + transactional_id, no fault —
    // processes all 4 records (starting over from the never-committed
    // offset) exactly once each.
    let shutdown_b = CancellationToken::new();
    let handle_b = tokio::spawn(Relay::run(
        relay_cfg(&brokers, &recovery, "kill-group", "kill-txn", None),
        matcher(),
        shutdown_b.clone(),
    ));

    let recovered = drain_by_origin(&brokers, "kill-recovery-verify", &recovery.tagged, 4).await;
    stop_checked(shutdown_b, handle_b, "relay B (recovery)").await;

    assert_eq!(
        recovered.keys().cloned().collect::<HashSet<_>>(),
        expected_origins,
        "every source offset must appear — no loss"
    );
    // `drain_by_origin` itself asserts no duplicate origin keys arrive
    // (see below) — restated here for clarity of intent.
    assert_eq!(recovered.len(), 4, "no duplicates from the aborted attempt");

    // Independent clean run: fresh group + transactional_id, no fault,
    // over the SAME raw records — the reference "what would a normal run
    // have produced" baseline.
    let shutdown_c = CancellationToken::new();
    let handle_c = tokio::spawn(Relay::run(
        relay_cfg(&brokers, &clean, "kill-clean-group", "kill-clean-txn", None),
        matcher(),
        shutdown_c.clone(),
    ));
    let clean_out = drain_by_origin(&brokers, "kill-clean-verify", &clean.tagged, 4).await;
    stop_checked(shutdown_c, handle_c, "relay C (clean)").await;

    assert_eq!(
        clean_out.keys().cloned().collect::<HashSet<_>>(),
        expected_origins,
        "clean reference run must also cover every source offset"
    );

    for o in &expected_origins {
        assert_eq!(
            recovered.get(o),
            clean_out.get(o),
            "recovered headers for {o} must be byte-identical to the clean run"
        );
    }
}

/// Drains `count` records from `topic` under `read_committed`, keyed by
/// their `deblob-origin` header. Panics if the same origin is delivered
/// twice (duplicate delivery) before `count` distinct origins are seen, or
/// if `count` is not reached within the deadline (loss).
async fn drain_by_origin(
    brokers: &str,
    group_id: &str,
    topic: &str,
    count: usize,
) -> HashMap<String, HashMap<String, Option<Vec<u8>>>> {
    let consumer = committed_consumer(brokers, group_id, topic);
    let mut out = HashMap::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while out.len() < count {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out draining {topic}: got {}/{count} records ({:?})",
            out.len(),
            out.keys().collect::<Vec<_>>()
        );
        let msg = recv_owned(&consumer, remaining.min(Duration::from_secs(15))).await;
        let headers = header_map(msg.headers());
        let origin = String::from_utf8(
            headers
                .get("deblob-origin")
                .expect("origin header present")
                .clone()
                .expect("origin header has a value"),
        )
        .expect("origin header is utf8");
        assert!(
            out.insert(origin.clone(), headers).is_none(),
            "duplicate delivery of {origin} on {topic}"
        );
    }
    out
}

// ---------------------------------------------------------------------
// Test 3: replaying the same source range through two fully independent
// fresh relays (different groups, different transactional ids) is
// idempotent — byte-identical headers both times. Tags are pure functions
// of shape + cursor, never freshly minted (spec §3.2).
// ---------------------------------------------------------------------
#[tokio::test]
async fn duplicate_delivery_idempotent() {
    let kafka = start_kafka().await;
    let brokers = broker_addr(&kafka).await;
    let raw = "dup-raw".to_string();
    create_topics(&brokers, &[&raw], 1).await;
    let first = topics_sharing_raw(&raw, "dup-first");
    let second = topics_sharing_raw(&raw, "dup-second");
    create_topics(
        &brokers,
        &[
            &first.tagged,
            &first.discovery,
            &first.quarantine,
            &second.tagged,
            &second.discovery,
            &second.quarantine,
        ],
        1,
    )
    .await;

    let producer = raw_producer(&brokers);
    let mut expected_origins = HashSet::new();
    for i in 0..3i64 {
        let payload = format!(r#"{{"dup_n":{i}}}"#).into_bytes();
        produce_raw(&producer, &raw, 0, &payload).await;
        expected_origins.insert(origin(&raw, 0, i));
    }

    let shutdown_1 = CancellationToken::new();
    let handle_1 = tokio::spawn(Relay::run(
        relay_cfg(&brokers, &first, "dup-group-1", "dup-txn-1", None),
        matcher(),
        shutdown_1.clone(),
    ));
    let delivery_1 = drain_by_origin(&brokers, "dup-verify-1", &first.tagged, 3).await;
    stop_checked(shutdown_1, handle_1, "relay 1").await;

    let shutdown_2 = CancellationToken::new();
    let handle_2 = tokio::spawn(Relay::run(
        relay_cfg(&brokers, &second, "dup-group-2", "dup-txn-2", None),
        matcher(),
        shutdown_2.clone(),
    ));
    let delivery_2 = drain_by_origin(&brokers, "dup-verify-2", &second.tagged, 3).await;
    stop_checked(shutdown_2, handle_2, "relay 2").await;

    assert_eq!(
        delivery_1.keys().cloned().collect::<HashSet<_>>(),
        expected_origins
    );
    assert_eq!(
        delivery_2.keys().cloned().collect::<HashSet<_>>(),
        expected_origins
    );
    for o in &expected_origins {
        assert_eq!(
            delivery_1.get(o),
            delivery_2.get(o),
            "replaying {o} through a fresh relay must be byte-identical: same \
             deblob-schema-id, same deblob-origin, every time"
        );
    }
}

// ---------------------------------------------------------------------
// Test 4: a real consumer-group rebalance (cooperative-sticky, two
// instances in the same group, one cancelled mid-stream) loses nothing
// and duplicates nothing. Exercises the cooperative-sticky assignment +
// RelayConsumerContext::pre_rebalance wiring under REAL rebalance timing
// (not a synthetic fault point — there is none for this scenario).
// ---------------------------------------------------------------------
#[tokio::test]
async fn rebalance_mid_stream_no_loss_no_dup() {
    let kafka = start_kafka().await;
    let brokers = broker_addr(&kafka).await;
    let t = topics("rebal-mid");
    create_topics(
        &brokers,
        &[&t.raw, &t.tagged, &t.discovery, &t.quarantine],
        4,
    )
    .await;

    let producer = raw_producer(&brokers);
    let n = 20i64;
    let mut expected_origins = HashSet::new();
    for i in 0..n {
        let partition = (i % 4) as i32;
        let payload = format!(r#"{{"n":{i}}}"#).into_bytes();
        produce_raw(&producer, &t.raw, partition, &payload).await;
        expected_origins.insert(origin(&t.raw, partition, i / 4));
    }
    assert_eq!(expected_origins.len(), n as usize);

    let group = "rebal-mid-group";
    let shutdown_1 = CancellationToken::new();
    let handle_1 = tokio::spawn(Relay::run(
        relay_cfg(&brokers, &t, group, "rebal-mid-txn-1", None),
        matcher(),
        shutdown_1.clone(),
    ));
    let shutdown_2 = CancellationToken::new();
    let handle_2 = tokio::spawn(Relay::run(
        relay_cfg(&brokers, &t, group, "rebal-mid-txn-2", None),
        matcher(),
        shutdown_2.clone(),
    ));

    // Let both members join the group (cooperative-sticky's initial
    // assignment round) and make some real progress on the stream before
    // triggering the rebalance.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Trigger a mid-stream rebalance: cancel instance 1's token. Its
    // `Relay::run` loop notices on its next iteration, returns, and drops
    // its consumer — which (per `RelayConsumerContext::pre_rebalance`)
    // aborts any transaction still open at that moment before relinquishing
    // its partitions, and the group rebalances the revoked partitions onto
    // the surviving instance.
    stop_checked(shutdown_1, handle_1, "relay 1 (cancelled mid-stream)").await;

    // The survivor picks up the reassigned partitions and finishes the
    // stream. Drain the tagged topic under read_committed until every
    // input origin has been seen, or the deadline elapses.
    let verify = committed_consumer(&brokers, "rebal-mid-verify", &t.tagged);
    let mut seen: HashMap<String, u32> = HashMap::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    while (seen.len() as i64) < n && tokio::time::Instant::now() < deadline {
        if let Some(msg) = try_recv_owned(&verify, Duration::from_secs(5)).await {
            let headers = header_map(msg.headers());
            let origin = String::from_utf8(
                headers
                    .get("deblob-origin")
                    .expect("origin header present")
                    .clone()
                    .expect("origin header has a value"),
            )
            .expect("origin header is utf8");
            *seen.entry(origin).or_insert(0) += 1;
        }
    }

    stop_checked(shutdown_2, handle_2, "relay 2 (survivor)").await;

    let seen_origins: HashSet<String> = seen.keys().cloned().collect();
    assert_eq!(
        seen_origins, expected_origins,
        "every input record must appear exactly once across the rebalance — no loss"
    );
    for (o, count) in &seen {
        assert_eq!(
            *count, 1,
            "record {o} must not be duplicated across the rebalance, got {count} deliveries"
        );
    }
}
