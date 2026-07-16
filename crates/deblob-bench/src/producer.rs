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
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

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

/// Drives `records` onto `topic`, one `FutureProducer::send` per record,
/// each stamped with a fresh `bench-produce-ns` header (spec §3.1) and an
/// optional key from `keys`. Paces sends when `rate` is
/// [`RateLimit::PerSecond`]; otherwise sends as fast as the queue accepts.
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

        match producer
            .send(future_record, Timeout::After(Duration::from_secs(10)))
            .await
        {
            Ok(_delivery) => stats.sent += 1,
            Err((_err, _owned_msg)) => stats.send_errors += 1,
        }
    }

    stats.wall_time = start.elapsed();
    stats
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
    fn producer_client_config_is_not_transactional() {
        // Unlike the relay's own producer config
        // (`deblob_kafka::relay::producer_client_config`), the bench
        // producer never sets `transactional.id` — it's a plain,
        // idempotent, at-least-once producer onto the raw topic.
        let cfg = producer_client_config("localhost:9092");
        assert_eq!(cfg.get("transactional.id"), None);
    }
}
