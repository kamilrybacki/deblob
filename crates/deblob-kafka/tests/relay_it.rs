//! Integration tests for the transactional relay (spec §3.1-3.2), backed
//! by a real single-node KRaft `apache/kafka` container (testcontainers).
//!
//! Every verification consumer here sets `isolation.level=read_committed`
//! — the whole point of the transactional relay is that a downstream
//! reader configured this way NEVER observes a partially-produced or
//! aborted transaction's records. The relay's OWN consumer (built inside
//! `Relay::run`) reads the raw topic at the default `read_uncommitted`,
//! which is correct: nothing upstream of the raw topic is transactional.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use deblob_core::error::CoreError;
use deblob_core::id::{CandidateId, FamilyVersion, SchemaId};
use deblob_core::ports::{CandidateRecord, CandidateState, Registry, SchemaRecord};
use deblob_fingerprint::Limits;
use deblob_kafka::{Relay, RelayCfg};
use deblob_match::discovery::DiscoveryMsg;
use deblob_match::matcher::HotMatcher;
use deblob_match::metrics::Metrics;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{Header, Headers, Message, OwnedHeaders, OwnedMessage};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use std::sync::Arc;
use testcontainers_modules::kafka::apache;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tokio_util::sync::CancellationToken;

/// Starts a single-node KRaft `apache/kafka` container with the internal
/// `__transaction_state` topic's replication factor forced down to 1.
///
/// Without this override, `Producer::init_transactions` times out: the
/// broker default (`transaction.state.log.replication.factor=3`,
/// `transaction.state.log.min.isr=2`) can never be satisfied by a
/// single-broker cluster, so the internal transaction-coordinator topic
/// never finishes creating. `testcontainers_modules::kafka::apache::Kafka`
/// already forces `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1` for the same
/// reason on `__consumer_offsets`, but not (yet) for `__transaction_state`.
async fn start_kafka() -> ContainerAsync<apache::Kafka> {
    apache::Kafka::default()
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .start()
        .await
        .expect("kafka container must start")
}

/// Registry fake: every lookup misses (so an unseen shape always tags
/// `Provisional`) — the relay's own behavior under test (header hygiene,
/// partitioning, transactions, quarantine, tombstones, replay determinism)
/// doesn't depend on ever observing a real `Known` classification, only on
/// `HotMatcher::classify` reaching a decision at all. `publish` is never
/// reachable from the hot path and panics if called, matching the pattern
/// `deblob-match`'s own unit tests use for the same reason.
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

    async fn list_families_by_band_depth(
        &self,
        _bands: &[u32],
        _depths: &[u32],
    ) -> Result<Vec<deblob_core::ports::FamilyRef>, CoreError> {
        Ok(vec![])
    }

    async fn family_version_schema(
        &self,
        _family_id: &deblob_core::id::FamilyId,
        _version: FamilyVersion,
    ) -> Result<Option<SchemaId>, CoreError> {
        Ok(None)
    }

    async fn get_family(
        &self,
        _family_id: &deblob_core::id::FamilyId,
    ) -> Result<Option<deblob_core::ports::FamilyRecord>, CoreError> {
        Ok(None)
    }

    async fn list_family_versions(
        &self,
        _family_id: &deblob_core::id::FamilyId,
    ) -> Result<Vec<FamilyVersion>, CoreError> {
        Ok(vec![])
    }
}

// Silence "unused" on evidence/candidate-shaped items pulled in only to
// keep this file's imports symmetric with the rest of the workspace's
// registry-fake test pattern.
#[allow(dead_code)]
fn _unused_candidate_state_shape() -> CandidateState {
    CandidateState::Provisional
}
#[allow(dead_code)]
fn _unused_candidate_record_shape(_r: CandidateRecord) {}

fn matcher() -> Arc<HotMatcher> {
    Arc::new(HotMatcher::new(Arc::new(MissRegistry), 16, Metrics::new()))
}

struct TestTopics {
    raw: String,
    tagged: String,
    discovery: String,
    quarantine: String,
}

fn topics(prefix: &str) -> TestTopics {
    TestTopics {
        raw: format!("{prefix}-raw"),
        tagged: format!("{prefix}-tagged"),
        discovery: format!("{prefix}-discovery"),
        quarantine: format!("{prefix}-quarantine"),
    }
}

/// Creates every topic in `names` with 2 partitions, replication factor 1.
async fn create_topics(brokers: &str, names: &[&str]) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("admin client");
    let new_topics: Vec<NewTopic> = names
        .iter()
        .map(|n| NewTopic::new(n, 2, TopicReplication::Fixed(1)))
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

fn relay_cfg(brokers: &str, t: &TestTopics, group_id: &str, txn_id: &str) -> RelayCfg {
    RelayCfg {
        brokers: brokers.to_string(),
        group_id: group_id.to_string(),
        raw_topic: t.raw.clone(),
        raw_topics: Vec::new(),
        tagged_topic: t.tagged.clone(),
        discovery_topic: t.discovery.clone(),
        quarantine_topic: t.quarantine.clone(),
        transactional_id: txn_id.to_string(),
        limits: Limits::default(),
        // Batching spec (`docs/superpowers/specs/2026-07-16-relay-batching.md`)
        // §3's production defaults — these behavior tests care about
        // per-record OUTCOMES (headers, partitioning, quarantine,
        // tombstones, replay determinism), not batch granularity, so
        // running them against the real default batch size is the most
        // faithful re-validation that batching didn't change any of that.
        max_batch_records: 500,
        max_batch_linger_ms: 100,
        max_batch_bytes: 32 * 1024 * 1024,
        max_message_bytes: deblob_kafka::DEFAULT_MAX_MESSAGE_BYTES,
        fault: None,
        metrics: Metrics::new(),
        sasl: None,
        stream_tx: None,
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

fn header_map(headers: Option<&OwnedHeaders>) -> HashMap<String, Option<Vec<u8>>> {
    let mut map = HashMap::new();
    if let Some(headers) = headers {
        for h in headers.iter() {
            map.insert(h.key.to_string(), h.value.map(|v| v.to_vec()));
        }
    }
    map
}

async fn stop(
    shutdown: CancellationToken,
    handle: tokio::task::JoinHandle<Result<(), deblob_kafka::RelayError>>,
) {
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
}

// ---------------------------------------------------------------------
// Behavior 1: header hygiene — strip ALL inbound `deblob-*` headers,
// write exactly one `deblob-schema-id` + `deblob-origin`.
// ---------------------------------------------------------------------
#[tokio::test]
async fn header_hygiene_strips_spoofed_headers_and_writes_canonical_tag() {
    let kafka = start_kafka().await;
    let brokers = format!(
        "127.0.0.1:{}",
        kafka
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("mapped kafka port")
    );
    let t = topics("hdr");
    create_topics(&brokers, &[&t.raw, &t.tagged, &t.discovery, &t.quarantine]).await;

    let producer = raw_producer(&brokers);
    let spoofed = OwnedHeaders::new()
        .insert(Header {
            key: "deblob-schema-id",
            value: Some(b"cand_spoofed".as_slice()),
        })
        .insert(Header {
            key: "DEBLOB-ORIGIN",
            value: Some(b"evil-topic/9/9".as_slice()),
        })
        .insert(Header {
            key: "content-type",
            value: Some(b"application/json".as_slice()),
        });
    let payload = br#"{"a":1}"#.to_vec();
    producer
        .send(
            FutureRecord::<[u8], [u8]>::to(&t.raw)
                .partition(0)
                .payload(payload.as_slice())
                .headers(spoofed),
            Duration::from_secs(5),
        )
        .await
        .expect("produce raw record");

    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(Relay::run(
        relay_cfg(&brokers, &t, "hdr-group", "hdr-txn"),
        matcher(),
        shutdown.clone(),
    ));

    let consumer = committed_consumer(&brokers, "hdr-verify", &t.tagged);
    let out = recv_owned(&consumer, Duration::from_secs(30)).await;
    let headers = header_map(out.headers());

    // Exactly one of each canonical header — never a duplicate.
    let all = out.headers().expect("headers present");
    assert_eq!(
        all.iter().filter(|h| h.key == "deblob-schema-id").count(),
        1
    );
    assert_eq!(all.iter().filter(|h| h.key == "deblob-origin").count(), 1);

    // The spoofed values are gone; the relay's own coordinates win.
    assert_eq!(
        headers.get("deblob-origin").unwrap().as_deref(),
        Some(format!("{}/0/0", t.raw).as_bytes())
    );
    assert_ne!(
        headers.get("deblob-schema-id").unwrap().as_deref(),
        Some(b"cand_spoofed".as_slice())
    );

    // A genuine, non-reserved header survives untouched.
    assert_eq!(
        headers.get("content-type").unwrap().as_deref(),
        Some(b"application/json".as_slice())
    );

    stop(shutdown, handle).await;
}

// ---------------------------------------------------------------------
// Behavior 2: partition p -> p, explicit (not key-routed).
// ---------------------------------------------------------------------
#[tokio::test]
async fn produces_to_the_same_partition_index_as_the_source() {
    let kafka = start_kafka().await;
    let brokers = format!(
        "127.0.0.1:{}",
        kafka
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("mapped kafka port")
    );
    let t = topics("part");
    create_topics(&brokers, &[&t.raw, &t.tagged, &t.discovery, &t.quarantine]).await;

    let producer = raw_producer(&brokers);
    for p in [0i32, 1i32] {
        let payload = format!(r#"{{"p":{p}}}"#).into_bytes();
        producer
            .send(
                FutureRecord::<[u8], [u8]>::to(&t.raw)
                    .partition(p)
                    .payload(payload.as_slice()),
                Duration::from_secs(5),
            )
            .await
            .expect("produce raw record");
    }

    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(Relay::run(
        relay_cfg(&brokers, &t, "part-group", "part-txn"),
        matcher(),
        shutdown.clone(),
    ));

    let consumer = committed_consumer(&brokers, "part-verify", &t.tagged);
    let mut seen_partitions = HashSet::new();
    for _ in 0..2 {
        let msg = recv_owned(&consumer, Duration::from_secs(30)).await;
        let headers = header_map(msg.headers());
        let origin = String::from_utf8(headers.get("deblob-origin").unwrap().clone().unwrap())
            .expect("origin header is utf8");
        // deblob-origin is "<topic>/<partition>/<offset>" — the SOURCE
        // partition must equal the output record's own partition.
        let source_partition: i32 = origin
            .rsplit('/')
            .nth(1)
            .expect("origin has partition segment")
            .parse()
            .expect("origin partition segment is numeric");
        assert_eq!(source_partition, msg.partition());
        seen_partitions.insert(msg.partition());
    }
    assert_eq!(seen_partitions, HashSet::from([0, 1]));

    stop(shutdown, handle).await;
}

// ---------------------------------------------------------------------
// Behavior 3: transactional EOS — unknown shape produces a tagged record
// AND a DiscoveryMsg, in the SAME committed transaction.
// ---------------------------------------------------------------------
#[tokio::test]
async fn provisional_classification_produces_tagged_and_discovery_transactionally() {
    let kafka = start_kafka().await;
    let brokers = format!(
        "127.0.0.1:{}",
        kafka
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("mapped kafka port")
    );
    let t = topics("eos");
    create_topics(&brokers, &[&t.raw, &t.tagged, &t.discovery, &t.quarantine]).await;

    let producer = raw_producer(&brokers);
    let payload = br#"{"unseen_field_xyz":true}"#.to_vec();
    producer
        .send(
            FutureRecord::<[u8], [u8]>::to(&t.raw)
                .partition(0)
                .payload(payload.as_slice()),
            Duration::from_secs(5),
        )
        .await
        .expect("produce raw record");

    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(Relay::run(
        relay_cfg(&brokers, &t, "eos-group", "eos-txn"),
        matcher(),
        shutdown.clone(),
    ));

    let tagged_consumer = committed_consumer(&brokers, "eos-tagged-verify", &t.tagged);
    let tagged = recv_owned(&tagged_consumer, Duration::from_secs(30)).await;
    let tagged_headers = header_map(tagged.headers());
    let schema_id_bytes = tagged_headers
        .get("deblob-schema-id")
        .expect("schema id header present")
        .clone()
        .expect("schema id header has a value");
    let schema_id = String::from_utf8(schema_id_bytes).expect("schema id is utf8");
    assert!(
        schema_id.starts_with("cand_"),
        "unseen shape must tag Provisional: {schema_id}"
    );

    let discovery_consumer = committed_consumer(&brokers, "eos-discovery-verify", &t.discovery);
    let discovery_msg = recv_owned(&discovery_consumer, Duration::from_secs(30)).await;
    let discovery: DiscoveryMsg =
        serde_json::from_slice(discovery_msg.payload().expect("discovery has a payload"))
            .expect("discovery message deserializes");

    assert_eq!(discovery.cand_id, schema_id);
    assert_eq!(discovery.cursor.topic, t.raw);
    assert_eq!(discovery.cursor.partition, 0);
    assert_eq!(discovery.cursor.offset, 0);
    assert_eq!(discovery.payload.as_ref(), payload.as_slice());

    stop(shutdown, handle).await;
}

// ---------------------------------------------------------------------
// Behavior 4: malformed -> quarantine topic, with reason header, never
// silently dropped.
// ---------------------------------------------------------------------
#[tokio::test]
async fn malformed_payload_routes_to_quarantine_with_reason_header() {
    let kafka = start_kafka().await;
    let brokers = format!(
        "127.0.0.1:{}",
        kafka
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("mapped kafka port")
    );
    let t = topics("mal");
    create_topics(&brokers, &[&t.raw, &t.tagged, &t.discovery, &t.quarantine]).await;

    let producer = raw_producer(&brokers);
    // Duplicate JSON object key -> deblob-fingerprint rejects with
    // QuarantineReason::DuplicateKey (spec §4).
    let payload = br#"{"a":1,"a":2}"#.to_vec();
    producer
        .send(
            FutureRecord::<[u8], [u8]>::to(&t.raw)
                .partition(0)
                .payload(payload.as_slice()),
            Duration::from_secs(5),
        )
        .await
        .expect("produce raw record");

    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(Relay::run(
        relay_cfg(&brokers, &t, "mal-group", "mal-txn"),
        matcher(),
        shutdown.clone(),
    ));

    let consumer = committed_consumer(&brokers, "mal-verify", &t.quarantine);
    let msg = recv_owned(&consumer, Duration::from_secs(30)).await;
    let headers = header_map(msg.headers());

    assert_eq!(
        headers.get("deblob-schema-id").unwrap().as_deref(),
        Some(b"malformed".as_slice())
    );
    assert_eq!(
        headers.get("deblob-quarantine-reason").unwrap().as_deref(),
        Some(b"duplicate_key".as_slice())
    );
    // Never silently dropped: the original payload bytes are preserved.
    assert_eq!(msg.payload(), Some(payload.as_slice()));

    stop(shutdown, handle).await;
}

// ---------------------------------------------------------------------
// Behavior 4b: an oversized record is quarantined (payload-free
// size_exceeded marker), NEVER aborting the batch it shares with good
// records. Regression for the MessageSizeTooLarge silent-data-loss bug.
// ---------------------------------------------------------------------
#[tokio::test]
async fn oversize_record_quarantines_without_aborting_its_batch() {
    let kafka = start_kafka().await;
    let brokers = format!(
        "127.0.0.1:{}",
        kafka
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("mapped kafka port")
    );
    let t = topics("big");
    create_topics(&brokers, &[&t.raw, &t.tagged, &t.discovery, &t.quarantine]).await;

    let producer = raw_producer(&brokers);
    // A small GOOD record and an OVERSIZE record, both on partition 0 so they
    // land in ONE batch/transaction. Before the guard, the oversize record's
    // produce failed with MessageSizeTooLarge and aborted the whole batch —
    // silently dropping the good record with it.
    let good = br#"{"a":1}"#.to_vec();
    let oversize = format!(r#"{{"big":"{}"}}"#, "x".repeat(60_000)).into_bytes();
    for payload in [&good, &oversize] {
        producer
            .send(
                FutureRecord::<[u8], [u8]>::to(&t.raw)
                    .partition(0)
                    .payload(payload.as_slice()),
                Duration::from_secs(5),
            )
            .await
            .expect("produce raw record");
    }

    // Cap well below the oversize record (60 KB) but above the good one.
    let mut cfg = relay_cfg(&brokers, &t, "big-group", "big-txn");
    cfg.max_message_bytes = 64 * 1024;

    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(Relay::run(cfg, matcher(), shutdown.clone()));

    // The GOOD record still reaches the tagged topic — proving the batch was
    // committed, not aborted by its oversize batch-mate.
    let tagged_consumer = committed_consumer(&brokers, "big-tagged-verify", &t.tagged);
    let tagged = recv_owned(&tagged_consumer, Duration::from_secs(30)).await;
    assert_eq!(tagged.payload(), Some(good.as_slice()));

    // The OVERSIZE record lands in quarantine as a PAYLOAD-FREE size_exceeded
    // marker — observable, never a silent drop, never the full payload.
    let q_consumer = committed_consumer(&brokers, "big-q-verify", &t.quarantine);
    let q = recv_owned(&q_consumer, Duration::from_secs(30)).await;
    let q_headers = header_map(q.headers());
    assert_eq!(
        q_headers.get("deblob-schema-id").unwrap().as_deref(),
        Some(b"malformed".as_slice())
    );
    assert_eq!(
        q_headers
            .get("deblob-quarantine-reason")
            .unwrap()
            .as_deref(),
        Some(b"size_exceeded".as_slice())
    );
    assert!(
        q.payload().map_or(true, |p| p.is_empty()),
        "oversize marker must carry no payload"
    );

    stop(shutdown, handle).await;
}

// ---------------------------------------------------------------------
// Behavior 5: Kafka tombstone (null payload) is NOT malformed — pass
// through untouched, reserved tombstone tag, no parse attempted.
// ---------------------------------------------------------------------
#[tokio::test]
async fn tombstone_passes_through_without_parsing() {
    let kafka = start_kafka().await;
    let brokers = format!(
        "127.0.0.1:{}",
        kafka
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("mapped kafka port")
    );
    let t = topics("tomb");
    create_topics(&brokers, &[&t.raw, &t.tagged, &t.discovery, &t.quarantine]).await;

    let producer = raw_producer(&brokers);
    let key = b"tombstone-key".to_vec();
    producer
        .send(
            // Deliberately no `.payload(...)` call -> a NULL value, i.e. a
            // genuine Kafka tombstone.
            FutureRecord::<[u8], [u8]>::to(&t.raw)
                .partition(0)
                .key(key.as_slice()),
            Duration::from_secs(5),
        )
        .await
        .expect("produce tombstone record");

    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(Relay::run(
        relay_cfg(&brokers, &t, "tomb-group", "tomb-txn"),
        matcher(),
        shutdown.clone(),
    ));

    let consumer = committed_consumer(&brokers, "tomb-verify", &t.tagged);
    let msg = recv_owned(&consumer, Duration::from_secs(30)).await;

    assert_eq!(msg.payload(), None, "tombstone value must stay null");
    assert_eq!(msg.key(), Some(key.as_slice()), "key must be preserved");
    let headers = header_map(msg.headers());
    assert_eq!(
        headers.get("deblob-schema-id").unwrap().as_deref(),
        Some(b"tombstone".as_slice())
    );

    stop(shutdown, handle).await;
}

// ---------------------------------------------------------------------
// Behavior 6: replay determinism — reprocessing the same source offset
// through a FRESH relay (new consumer group) mints byte-identical
// deblob-schema-id / deblob-origin headers. Never a fresh cand_/UUID.
// ---------------------------------------------------------------------
#[tokio::test]
async fn replaying_the_same_offset_through_a_fresh_relay_is_byte_identical() {
    let kafka = start_kafka().await;
    let brokers = format!(
        "127.0.0.1:{}",
        kafka
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("mapped kafka port")
    );
    let t = topics("replay");
    create_topics(&brokers, &[&t.raw, &t.tagged, &t.discovery, &t.quarantine]).await;

    let producer = raw_producer(&brokers);
    let payload = br#"{"replay_field":42}"#.to_vec();
    producer
        .send(
            FutureRecord::<[u8], [u8]>::to(&t.raw)
                .partition(0)
                .payload(payload.as_slice()),
            Duration::from_secs(5),
        )
        .await
        .expect("produce raw record");

    // One long-lived verification consumer on the tagged topic, from
    // `earliest`: its FIRST recv() picks up relay-A's tagged copy, its
    // SECOND picks up relay-B's — both derived from the SAME raw offset.
    let verify = committed_consumer(&brokers, "replay-verify", &t.tagged);

    let shutdown_a = CancellationToken::new();
    let handle_a = tokio::spawn(Relay::run(
        relay_cfg(&brokers, &t, "replay-group-a", "replay-txn-a"),
        matcher(),
        shutdown_a.clone(),
    ));
    let first = recv_owned(&verify, Duration::from_secs(30)).await;
    let first_headers = header_map(first.headers());
    stop(shutdown_a, handle_a).await;

    let shutdown_b = CancellationToken::new();
    let handle_b = tokio::spawn(Relay::run(
        relay_cfg(&brokers, &t, "replay-group-b", "replay-txn-b"),
        matcher(),
        shutdown_b.clone(),
    ));
    let second = recv_owned(&verify, Duration::from_secs(30)).await;
    let second_headers = header_map(second.headers());
    stop(shutdown_b, handle_b).await;

    assert_eq!(
        first_headers.get("deblob-schema-id"),
        second_headers.get("deblob-schema-id"),
        "replay must mint the identical cand_ id, never a fresh one"
    );
    assert_eq!(
        first_headers.get("deblob-origin"),
        second_headers.get("deblob-origin"),
        "replay must record the identical origin coordinates"
    );
}

// ---------------------------------------------------------------------
// Behavior 7 (config-asserted per the brief; the full chaos/abort-
// visibility test lands in Task 17): cooperative-sticky is wired via
// RelayConsumerContext, and a clean multi-record, multi-partition run
// commits normally end to end.
// ---------------------------------------------------------------------
#[tokio::test]
async fn clean_multi_record_run_with_cooperative_sticky_context_commits_normally() {
    let kafka = start_kafka().await;
    let brokers = format!(
        "127.0.0.1:{}",
        kafka
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("mapped kafka port")
    );
    let t = topics("rebal");
    create_topics(&brokers, &[&t.raw, &t.tagged, &t.discovery, &t.quarantine]).await;

    let producer = raw_producer(&brokers);
    for i in 0..4i32 {
        let payload = format!(r#"{{"n":{i}}}"#).into_bytes();
        producer
            .send(
                FutureRecord::<[u8], [u8]>::to(&t.raw)
                    .partition(i % 2)
                    .payload(payload.as_slice()),
                Duration::from_secs(5),
            )
            .await
            .expect("produce raw record");
    }

    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(Relay::run(
        relay_cfg(&brokers, &t, "rebal-group", "rebal-txn"),
        matcher(),
        shutdown.clone(),
    ));

    let consumer = committed_consumer(&brokers, "rebal-verify", &t.tagged);
    for _ in 0..4 {
        let _ = recv_owned(&consumer, Duration::from_secs(30)).await;
    }

    stop(shutdown, handle).await;
}
