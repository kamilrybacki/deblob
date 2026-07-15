//! A [`DiscoverySink`] backed by `deblob-kafka`'s standalone discovery
//! producer (Task 3, spec §3.2): wraps
//! [`deblob_kafka::discovery_producer::DiscoveryProducer`] so HTTP-ingested
//! unknowns reach the SAME discovery topic — and therefore the same cold
//! lane — the Kafka relay's own transactional produce feeds.
//!
//! Placement note (spec §3.3, Task 3): this type lives in `deblob-http`,
//! NOT `deblob-kafka`, precisely so `deblob-kafka` never has to know the
//! `DiscoverySink` trait (or `deblob-http` itself) exists. The dependency
//! is one-directional — `deblob-http -> deblob-kafka` (added to this
//! crate's `Cargo.toml` for this task) — and `deblob-kafka`'s own
//! `Cargo.toml` gained no new dependency at all. No cycle: `deblob-kafka`
//! depends on `deblob-core`/`deblob-fingerprint`/`deblob-match` only, none
//! of which depend back on `deblob-http`.

use deblob_kafka::discovery_producer::DiscoveryProducer;
use deblob_match::discovery::DiscoveryMsg;

use crate::proxy::{DiscoveryError, DiscoverySink};

/// Adapts [`DiscoveryProducer`] to the [`DiscoverySink`] trait the ingest
/// handler calls against.
pub struct KafkaDiscoverySink {
    producer: DiscoveryProducer,
}

impl KafkaDiscoverySink {
    pub fn new(producer: DiscoveryProducer) -> Self {
        Self { producer }
    }
}

#[async_trait::async_trait]
impl DiscoverySink for KafkaDiscoverySink {
    /// Never carries a payload byte in its error path (spec §9): a
    /// produce failure is mapped to [`DiscoveryError::Unavailable`]
    /// carrying only `DiscoveryProducerError`'s own bounded `Display`
    /// text (an rdkafka error code or a serde error message) — never the
    /// `msg.payload` bytes themselves.
    async fn enqueue(&self, msg: DiscoveryMsg) -> Result<(), DiscoveryError> {
        self.producer
            .produce(&msg)
            .await
            .map_err(|error| DiscoveryError::Unavailable(error.to_string()))
    }
}
