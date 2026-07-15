//! A standalone, NON-transactional Kafka producer for the discovery topic
//! (Task 3 of the P2-C HTTP proxy, spec §3.2 reuse): lets any ingest
//! transport other than the relay itself — concretely, `deblob-http`'s
//! `KafkaDiscoverySink` — publish a [`DiscoveryMsg`] onto the SAME
//! discovery topic [`crate::relay::Relay::run`] produces to, so the cold
//! lane sees HTTP-ingested unknowns exactly like Kafka-ingested ones.
//!
//! Deliberately NOT the relay's transactional producer
//! (`relay::producer_client_config` + `Producer::init_transactions`): the
//! relay wraps a produce in a Kafka transaction because it MUST commit
//! that produce atomically with its own consumer offset (spec §3.1's
//! exactly-once scope). An HTTP-ingested discovery message has no
//! consumer offset to commit atomically with — there's nothing to wrap in
//! a transaction — so a bare idempotent produce (`enable.idempotence`,
//! same as the relay's producer, for at-least-once-without-duplication
//! delivery to the broker) is the correct, simpler primitive here.
//!
//! This module has no idea `deblob-http` or its `DiscoverySink` trait
//! exist — the dependency runs one way, `deblob-http -> deblob-kafka`,
//! never back, so this crate stays the workspace's only Kafka-aware
//! adapter without being coupled to any particular caller.

use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

use deblob_match::discovery::DiscoveryMsg;

use crate::relay::{apply_sasl, KafkaSasl};

/// Configuration for one [`DiscoveryProducer::new`] instance.
#[derive(Debug, Clone)]
pub struct DiscoveryProducerCfg {
    pub brokers: String,
    pub discovery_topic: String,
    /// Optional SASL credentials (spec §9), applied identically to how the
    /// relay's own clients apply them — see [`crate::relay::apply_sasl`].
    pub sasl: Option<KafkaSasl>,
}

/// Every way [`DiscoveryProducer`] can fail to build or produce. Never
/// carries a payload byte — only the bounded, derived `rdkafka`/`serde`
/// error text.
#[derive(Debug, thiserror::Error)]
pub enum DiscoveryProducerError {
    #[error("kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("failed to serialize discovery message: {0}")]
    Serialize(#[from] serde_json::Error),
}

/// A bare, idempotent (non-transactional) producer bound to one discovery
/// topic.
pub struct DiscoveryProducer {
    producer: FutureProducer,
    topic: String,
}

impl DiscoveryProducer {
    /// Builds the underlying `rdkafka` producer client. Fallible only the
    /// way `ClientConfig::create` is fallible (e.g. malformed broker
    /// list) — no network round-trip happens here, matching
    /// `relay::producer_client_config`'s own lazy-connect behavior.
    pub fn new(cfg: DiscoveryProducerCfg) -> Result<Self, DiscoveryProducerError> {
        let client_config = producer_client_config(&cfg);
        let producer: FutureProducer = client_config.create()?;
        Ok(Self {
            producer,
            topic: cfg.discovery_topic,
        })
    }

    /// Produces `msg` onto this producer's discovery topic, keyed by its
    /// `cand_id` — the SAME partitioning rule
    /// `relay::run_transaction_body` uses for the relay's own discovery
    /// produce (spec §3.2: "route by candidate id instead so a given
    /// candidate's discovery evidence lands on one partition"), so the
    /// cold lane sees one candidate's evidence ordered on one partition
    /// regardless of which transport ingested it.
    pub async fn produce(&self, msg: &DiscoveryMsg) -> Result<(), DiscoveryProducerError> {
        let bytes = serde_json::to_vec(msg)?;
        let record = FutureRecord::<[u8], [u8]>::to(&self.topic)
            .key(msg.cand_id.as_bytes())
            .payload(&bytes);
        self.producer
            .send(record, Timeout::After(Duration::from_secs(10)))
            .await
            .map_err(|(err, _owned_msg)| DiscoveryProducerError::Kafka(err))?;
        Ok(())
    }
}

/// The `pub(crate)` [`ClientConfig`] builder — idempotent, non-
/// transactional (no `transactional.id`, no `init_transactions` call
/// site), unlike `relay::producer_client_config`. `pub(crate)` so this
/// module's own unit tests can assert on it without a broker, mirroring
/// `relay.rs`'s own test pattern for `consumer_client_config`/
/// `producer_client_config`.
pub(crate) fn producer_client_config(cfg: &DiscoveryProducerCfg) -> ClientConfig {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", &cfg.brokers)
        .set("enable.idempotence", "true")
        .set("message.timeout.ms", "10000");
    apply_sasl(&mut c, &cfg.sasl);
    c
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> DiscoveryProducerCfg {
        DiscoveryProducerCfg {
            brokers: "localhost:9092".to_string(),
            discovery_topic: "discovery".to_string(),
            sasl: None,
        }
    }

    #[test]
    fn producer_config_is_idempotent_and_non_transactional() {
        let c = producer_client_config(&cfg());
        assert_eq!(c.get("enable.idempotence"), Some("true"));
        // No `transactional.id` — this producer never opens a Kafka
        // transaction, unlike `relay::producer_client_config`.
        assert_eq!(c.get("transactional.id"), None);
    }

    #[test]
    fn no_sasl_by_default() {
        let c = producer_client_config(&cfg());
        assert_eq!(c.get("security.protocol"), None);
        assert_eq!(c.get("sasl.mechanism"), None);
    }

    #[test]
    fn sasl_applies_to_the_producer_config() {
        let mut with_sasl = cfg();
        with_sasl.sasl = Some(KafkaSasl {
            mechanism: "SCRAM-SHA-512".to_string(),
            security_protocol: "SASL_SSL".to_string(),
            username: "deblob".to_string(),
            password: "s3cr3t".to_string(),
        });

        let c = producer_client_config(&with_sasl);
        assert_eq!(c.get("security.protocol"), Some("SASL_SSL"));
        assert_eq!(c.get("sasl.mechanism"), Some("SCRAM-SHA-512"));
        assert_eq!(c.get("sasl.username"), Some("deblob"));
    }

    #[test]
    fn new_builds_without_a_broker_round_trip() {
        // `FutureProducer::create` never dials the broker — construction
        // is lazy, matching `relay::producer_client_config`'s own
        // documented behavior. Building against an unreachable broker
        // list must still succeed synchronously.
        let result = DiscoveryProducer::new(cfg());
        assert!(result.is_ok());
    }
}
