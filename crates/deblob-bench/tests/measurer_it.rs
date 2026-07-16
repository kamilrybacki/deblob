//! Reproduces (and guards against regressing) the live k3s smoke-run bug:
//! `measure_topic` captured 0-of-20000 tagged messages even though the
//! topic demonstrably held them, because the idle-timeout clock started at
//! consumer construction rather than after the consumer group actually
//! finished joining. A brand-new (never-before-seen) consumer group's
//! FIRST rebalance is deliberately delayed by the broker's
//! `group.initial.rebalance.delay.ms` (Kafka default: 3000ms, and this
//! `testcontainers_modules::kafka::apache::Kafka` image doesn't override
//! it) — every real Kafka cluster does this, it's not a testcontainers
//! quirk. This test exploits that deterministic delay: it produces every
//! message BEFORE `measure_topic` even subscribes (the exact live-run
//! shape: messages already sitting in the topic, consumer joins late),
//! then runs `measure_topic` with a fresh group and an idle timeout well
//! SHORTER than the broker's mandatory join delay. Pre-fix, the idle clock
//! (started at loop entry) would expire mid-join and the run would stop at
//! `received == 0`. Post-fix, the idle clock only starts once the consumer
//! is actually assigned, so the backlog is captured in full.

use std::time::Duration;

use deblob_bench::header::{encode_produce_ns, now_ns, PRODUCE_NS_HEADER};
use deblob_bench::measurer::measure_topic;
use deblob_bench::outcome::SCHEMA_ID_HEADER;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use testcontainers_modules::kafka::apache;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

async fn create_topic(brokers: &str, name: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("admin client");
    let new_topic = NewTopic::new(name, 1, TopicReplication::Fixed(1));
    let results = admin
        .create_topics([&new_topic], &AdminOptions::new())
        .await
        .expect("create_topics call");
    for r in results {
        r.expect("topic creation must succeed");
    }
}

fn tagged_producer(brokers: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "10000")
        .create()
        .expect("tagged producer")
}

/// Produces `count` bench-tagged messages onto `topic`, mimicking what the
/// real relay would have already written: a `bench-produce-ns` latency
/// header plus a `deblob-schema-id` tag header. Every send is awaited, so
/// by the time this returns every message is durably committed on the
/// broker — exactly the "topic already holds the backlog" precondition the
/// live bug needs to reproduce.
async fn produce_tagged_backlog(brokers: &str, topic: &str, count: usize) {
    let producer = tagged_producer(brokers);
    for i in 0..count {
        let ns_bytes = encode_produce_ns(now_ns());
        let headers = OwnedHeaders::new()
            .insert(Header {
                key: PRODUCE_NS_HEADER,
                value: Some(&ns_bytes[..]),
            })
            .insert(Header {
                key: SCHEMA_ID_HEADER,
                value: Some(b"sch_backlog_fixture".as_slice()),
            });
        let payload = format!(r#"{{"n":{i}}}"#).into_bytes();
        producer
            .send(
                FutureRecord::<[u8], [u8]>::to(topic)
                    .payload(payload.as_slice())
                    .headers(headers),
                Duration::from_secs(5),
            )
            .await
            .expect("produce backlog record");
    }
}

#[tokio::test]
async fn late_joining_measurer_captures_a_backlog_despite_a_short_idle_timeout() {
    let kafka = apache::Kafka::default()
        .start()
        .await
        .expect("kafka container must start");
    let brokers = format!(
        "127.0.0.1:{}",
        kafka
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("mapped kafka port")
    );

    let topic = "measurer-late-join-backlog";
    create_topic(&brokers, topic).await;

    let count = 25usize;
    // The full backlog is on the broker BEFORE `measure_topic` is even
    // called — the consumer group hasn't been created yet, so its first
    // rebalance is guaranteed to hit the broker's
    // `group.initial.rebalance.delay.ms` (3000ms default).
    produce_tagged_backlog(&brokers, topic, count).await;

    // Fresh, never-before-seen group id: this run's join is genuinely the
    // group's FIRST rebalance, so the ~3s broker-mandated delay applies.
    let group_id = format!("measurer-late-join-{}", now_ns());

    // Short enough that the pre-fix code (idle clock started at consumer
    // construction) reliably stops at `received == 0` before the group
    // finishes joining; long enough that once assignment lands, reading
    // 25 already-committed messages off a local broker comfortably fits.
    let idle_timeout = Duration::from_millis(1500);
    // Generous overall backstop — covers the ~3s join plus the read with
    // room to spare, without masking a genuine hang.
    let deadline = Duration::from_secs(30);

    let acc = measure_topic(
        &brokers,
        &group_id,
        topic,
        count as u64,
        deadline,
        idle_timeout,
    )
    .await
    .expect("measure_topic must not error");

    assert_eq!(
        acc.received, count as u64,
        "a late-joining measurer must still capture every already-committed \
         message in the backlog, not stop at 0 because assignment hadn't \
         finished before the idle clock started"
    );
    assert_eq!(
        acc.missing_latency, 0,
        "every produced record carried a bench-produce-ns header"
    );
    assert_eq!(acc.histogram.len(), count as u64);
    assert_eq!(acc.outcomes.known, count as u64);
}
