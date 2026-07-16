//! The bench producer (spec §3.1): an `rdkafka` `FutureProducer` that
//! drives a `GeneratedRecord` stream onto the ingest topic (`events.raw` by
//! default), stamping each record with a fresh `bench-produce-ns` header
//! (see `crate::header`) at either a target rate or max throughput.
//!
//! Needs a LIVE broker to actually run — [`produce_stream`] is exercised
//! by the controller's Docker-backed integration test, not by this crate's
//! unit suite. The pure parts ([`KeyDistribution::key_for`], the
//! `ClientConfig` builder) are unit-tested below without a broker, the
//! same split `deblob_kafka::relay` uses for its own consumer/producer
//! `ClientConfig`s.

use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord};

use crate::header::{encode_produce_ns, now_ns, PRODUCE_NS_HEADER};
use crate::record::GeneratedRecord;

/// How the producer assigns a Kafka message key to each record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyDistribution {
    /// No key (null) — the broker's own partitioner picks the partition.
    None,
    /// Cycles a fixed pool of `n` synthetic keys (`"bench-key-<i % n>"`),
    /// so records land on a small, stable set of partitions — useful for
    /// exercising the relay's source-partition→derived-partition mapping
    /// (spec §3.2) across a known key set. `n == 0` behaves like `None`.
    RoundRobin(u32),
}

impl KeyDistribution {
    /// The key bytes for the `index`-th produced record, `None` for
    /// [`KeyDistribution::None`] (or a zero-sized pool). Pure —
    /// unit-testable without a broker.
    pub fn key_for(self, index: u64) -> Option<Vec<u8>> {
        match self {
            KeyDistribution::None => None,
            KeyDistribution::RoundRobin(0) => None,
            KeyDistribution::RoundRobin(n) => {
                Some(format!("bench-key-{}", index % u64::from(n)).into_bytes())
            }
        }
    }
}

/// Target production pace.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RateLimit {
    /// Produce as fast as the client/broker accept — backpressure comes
    /// from `FutureProducer::send`'s own await, exactly like the relay's
    /// own produce path (`deblob_kafka::relay::produce`).
    MaxThroughput,
    /// Pace sends so the `index`-th record targets `index / rate` seconds
    /// after the stream started.
    PerSecond(f64),
}

/// [`produce_stream`]'s outcome.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProduceStats {
    pub sent: u64,
    pub send_errors: u64,
    /// Wall-clock time the whole `produce_stream` call took — the
    /// denominator for `throughput_msgs_per_sec` in the report
    /// (`crate::report`).
    pub wall_time: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum ProducerError {
    #[error("kafka client construction failed: {0}")]
    ClientConfig(#[from] rdkafka::error::KafkaError),
}

/// The producer-side [`ClientConfig`]: idempotent, but deliberately NOT
/// transactional — the bench never needs the relay's exactly-once
/// transaction machinery on the way IN, only reliable at-least-once
/// delivery onto the raw topic (the relay itself owns exactly-once from
/// raw → tagged).
pub fn producer_client_config(brokers: &str) -> ClientConfig {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", brokers)
        .set("enable.idempotence", "true")
        .set("message.timeout.ms", "30000");
    c
}

/// Builds the [`FutureProducer`] [`produce_stream`] sends through.
pub fn build_producer(brokers: &str) -> Result<FutureProducer, ProducerError> {
    Ok(producer_client_config(brokers).create()?)
}

/// Caps how many in-flight [`DeliveryFuture`]s `produce_stream` accumulates
/// before draining them. Bounds memory on very large `--count` runs while
/// still letting sends run far ahead of their broker acks — the point of
/// pipelining at all.
const INFLIGHT_BATCH: usize = 5_000;

/// Whether the in-flight delivery-future buffer has grown large enough to
/// drain now. Pulled out as a pure predicate so the pipelining threshold
/// is unit-testable without a broker.
fn should_drain(inflight_len: usize, batch_size: usize) -> bool {
    batch_size > 0 && inflight_len >= batch_size
}

/// Drives `records` onto `topic`, each stamped with a fresh
/// `bench-produce-ns` header (spec §3.1) and an optional key from `keys`.
/// Paces sends when `rate` is [`RateLimit::PerSecond`]; otherwise sends as
/// fast as the local queue accepts.
///
/// Pipelined via [`FutureProducer::send_result`]: that call enqueues onto
/// the producer's local queue and returns a [`DeliveryFuture`]
/// immediately — it does NOT await the broker's delivery ack the way
/// `FutureProducer::send` does. Awaiting every delivery ack inline (the
/// bug this replaces) serializes each record behind its own network
/// round-trip, so the measured "throughput" is really the client's
/// one-at-a-time latency, not the broker/relay's real capacity. Here the
/// futures are collected and drained in bounded batches (`INFLIGHT_BATCH`)
/// so many sends are in flight at once, sends stay memory-bounded on large
/// `--count` runs, and every delivery outcome still lands in
/// `stats.sent`/`stats.send_errors`.
pub async fn produce_stream(
    producer: &FutureProducer,
    topic: &str,
    records: impl Iterator<Item = GeneratedRecord>,
    keys: KeyDistribution,
    rate: RateLimit,
) -> ProduceStats {
    let mut stats = ProduceStats::default();
    let start = tokio::time::Instant::now();
    let interval_ns: Option<f64> = match rate {
        RateLimit::MaxThroughput => None,
        RateLimit::PerSecond(r) if r > 0.0 => Some(1_000_000_000.0 / r),
        RateLimit::PerSecond(_) => None,
    };

    let mut inflight: Vec<DeliveryFuture> = Vec::with_capacity(INFLIGHT_BATCH);

    for (index, record) in records.enumerate() {
        if let Some(interval_ns) = interval_ns {
            let target_ns = (index as f64) * interval_ns;
            let target = start + Duration::from_nanos(target_ns as u64);
            let now = tokio::time::Instant::now();
            if target > now {
                tokio::time::sleep(target - now).await;
            }
        }

        let key = keys.key_for(index as u64);
        let ns_bytes = encode_produce_ns(now_ns());
        let headers = OwnedHeaders::new().insert(Header {
            key: PRODUCE_NS_HEADER,
            value: Some(&ns_bytes[..]),
        });

        let mut future_record = FutureRecord::<[u8], [u8]>::to(topic)
            .payload(&record.bytes)
            .headers(headers);
        if let Some(k) = key.as_deref() {
            future_record = future_record.key(k);
        }

        match producer.send_result(future_record) {
            Ok(delivery) => {
                inflight.push(delivery);
                if should_drain(inflight.len(), INFLIGHT_BATCH) {
                    drain_inflight(&mut inflight, &mut stats).await;
                }
            }
            // The local queue rejected the record outright (e.g. full) —
            // it was never handed to the broker, so it's a send error
            // exactly like a delivery failure below.
            Err(_queue_full_or_similar) => stats.send_errors += 1,
        }
    }

    // Drain whatever's left after the loop — the tail batch is almost
    // always smaller than `INFLIGHT_BATCH`.
    drain_inflight(&mut inflight, &mut stats).await;

    stats.wall_time = start.elapsed();
    stats
}

/// Awaits every pending future in `inflight`, folding each delivery
/// outcome into `stats`, then empties `inflight`. The single drain point
/// both the batch boundary and the post-loop tail call into, so the
/// success/error accounting only lives in one place.
async fn drain_inflight(inflight: &mut Vec<DeliveryFuture>, stats: &mut ProduceStats) {
    for delivery in inflight.drain(..) {
        match delivery.await {
            Ok(Ok(_partition_offset)) => stats.sent += 1,
            Ok(Err((_err, _owned_msg))) => stats.send_errors += 1,
            // The producer was dropped before its delivery report
            // arrived. Never expected in practice (the producer outlives
            // the whole `produce_stream` call) but must count as an
            // error rather than silently vanishing from the totals.
            Err(_canceled) => stats.send_errors += 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_key_distribution_never_keys_a_record() {
        assert_eq!(KeyDistribution::None.key_for(0), None);
        assert_eq!(KeyDistribution::None.key_for(999), None);
    }

    #[test]
    fn zero_sized_pool_behaves_like_no_key() {
        assert_eq!(KeyDistribution::RoundRobin(0).key_for(0), None);
        assert_eq!(KeyDistribution::RoundRobin(0).key_for(5), None);
    }

    #[test]
    fn round_robin_cycles_the_pool_deterministically() {
        let dist = KeyDistribution::RoundRobin(3);
        assert_eq!(dist.key_for(0), Some(b"bench-key-0".to_vec()));
        assert_eq!(dist.key_for(1), Some(b"bench-key-1".to_vec()));
        assert_eq!(dist.key_for(2), Some(b"bench-key-2".to_vec()));
        assert_eq!(dist.key_for(3), Some(b"bench-key-0".to_vec()));
        assert_eq!(dist.key_for(103), Some(b"bench-key-1".to_vec()));
    }

    #[test]
    fn producer_client_config_sets_idempotence_and_bootstrap_servers() {
        let cfg = producer_client_config("localhost:9092");
        assert_eq!(cfg.get("bootstrap.servers"), Some("localhost:9092"));
        assert_eq!(cfg.get("enable.idempotence"), Some("true"));
    }

    #[test]
    fn should_drain_is_false_below_the_batch_size() {
        assert!(!should_drain(0, 5_000));
        assert!(!should_drain(4_999, 5_000));
    }

    #[test]
    fn should_drain_is_true_at_and_above_the_batch_size() {
        assert!(should_drain(5_000, 5_000));
        assert!(should_drain(5_001, 5_000));
    }

    #[test]
    fn should_drain_never_fires_for_a_zero_batch_size() {
        // A zero threshold must never be interpreted as "always drain" —
        // it would turn every single send into its own batch (right back
        // to the pre-fix serial-await behavior).
        assert!(!should_drain(0, 0));
        assert!(!should_drain(100, 0));
    }

    #[test]
    fn producer_client_config_is_not_transactional() {
        // Unlike the relay's own producer config
        // (`deblob_kafka::relay::producer_client_config`), the bench
        // producer never sets `transactional.id` — it's a plain,
        // idempotent, at-least-once producer onto the raw topic.
        let cfg = producer_client_config("localhost:9092");
        assert_eq!(cfg.get("transactional.id"), None);
    }
}
