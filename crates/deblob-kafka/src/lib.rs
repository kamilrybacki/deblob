//! `deblob-kafka` — the transactional relay adapter (spec §3.1-3.2, §3.3).
//!
//! Reads the raw source topic, classifies each record via
//! [`deblob_match::matcher::HotMatcher`], strips/rewrites `deblob-*` headers, and
//! transactionally produces the tagged (or quarantined) record — plus, for
//! provisional shapes, a `DiscoveryMsg` on the discovery topic — in the SAME
//! Kafka transaction as the consumer offset commit. This is the only crate
//! in the workspace that talks to Kafka; everything else stays adapter-free
//! (spec §3.3).

pub mod discovery_producer;
pub mod headers;
pub mod relay;
pub mod stream;

pub use discovery_producer::{DiscoveryProducer, DiscoveryProducerCfg, DiscoveryProducerError};
pub use relay::{
    FaultPoint, KafkaSasl, Relay, RelayCfg, RelayError, DEFAULT_MAX_MESSAGE_BYTES,
    RELAY_PRODUCE_MARGIN,
};
pub use stream::{StreamEvent, StreamOutcome};
