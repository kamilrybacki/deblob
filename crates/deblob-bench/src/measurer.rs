//! The bench measurer (spec §3.1): a `read_committed` `StreamConsumer` on
//! the tagged topic. Reads each message's `bench-produce-ns` header (see
//! `crate::header`) to compute end-to-end latency, and its
//! `deblob-schema-id` header (see `crate::outcome`) to classify the tag
//! outcome, folding both into a running [`MeasureAccumulator`].
//!
//! [`process_headers`]/[`MeasureAccumulator::record`] are pure — the
//! per-message extraction and folding logic is unit-tested below against
//! real `rdkafka` header types, without a broker (the same pattern
//! `deblob_kafka::headers`'s own tests use). [`measure_topic`] itself needs
//! a LIVE broker and is exercised by the controller's Docker-backed
//! integration run.

use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{BorrowedHeaders, Headers, Message};

use crate::header::{decode_produce_ns, now_ns, PRODUCE_NS_HEADER};
use crate::histogram::LatencyHistogram;
use crate::outcome::{classify, TagOutcome, TagOutcomeCounts, SCHEMA_ID_HEADER};

/// One tagged message's extracted, pre-aggregation facts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessedMessage {
    /// End-to-end latency in nanoseconds; `None` if the message carried no
    /// (or an undecodable) `bench-produce-ns` header — e.g. a record this
    /// harness didn't itself produce.
    pub latency_ns: Option<u64>,
    /// The tag outcome; `None` if no `deblob-schema-id` header was present
    /// at all — should never happen for a real tagged/quarantine record,
    /// but the measurer must never panic over it.
    pub outcome: Option<TagOutcome>,
}

/// Extracts latency + tag outcome from one message's headers.
/// `observed_at_ns` is passed in (rather than read internally) so this
/// function is deterministic and testable without a wall clock.
pub fn process_headers(headers: Option<&BorrowedHeaders>, observed_at_ns: u64) -> ProcessedMessage {
    let mut latency_ns = None;
    let mut outcome = None;
    if let Some(headers) = headers {
        for h in headers.iter() {
            match h.key {
                PRODUCE_NS_HEADER => {
                    if let Some(v) = h.value {
                        if let Some(produce_ns) = decode_produce_ns(v) {
                            latency_ns = Some(observed_at_ns.saturating_sub(produce_ns));
                        }
                    }
                }
                SCHEMA_ID_HEADER => {
                    if let Some(v) = h.value {
                        if let Ok(s) = std::str::from_utf8(v) {
                            outcome = Some(classify(s));
                        }
                    }
                }
                _ => {}
            }
        }
    }
    ProcessedMessage {
        latency_ns,
        outcome,
    }
}

/// Running aggregate the measurer folds every [`ProcessedMessage`] into.
#[derive(Debug, Default)]
pub struct MeasureAccumulator {
    pub histogram: LatencyHistogram,
    pub outcomes: TagOutcomeCounts,
    /// Every message observed on the tagged topic, regardless of whether
    /// it carried a usable latency header.
    pub received: u64,
    /// Messages observed with no (or undecodable) `bench-produce-ns`
    /// header — excluded from `histogram`, tracked separately so the
    /// report can surface it rather than silently under-counting.
    pub missing_latency: u64,
}

impl MeasureAccumulator {
    pub fn record(&mut self, msg: ProcessedMessage) {
        self.received += 1;
        match msg.latency_ns {
            Some(ns) => self.histogram.record_ns(ns),
            None => self.missing_latency += 1,
        }
        if let Some(outcome) = msg.outcome {
            self.outcomes.record(outcome);
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MeasurerError {
    #[error("kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
}

/// The consumer-side [`ClientConfig`]: `read_committed` (spec §3.1 —
/// "a `read_committed` consumer on the tagged topic"), so this measurer can
/// never observe a record from an aborted or still-open relay transaction,
/// matching every other verification consumer in this workspace (e.g.
/// `crates/deblob/tests/e2e_it.rs::committed_consumer`).
pub fn consumer_client_config(brokers: &str, group_id: &str) -> ClientConfig {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("isolation.level", "read_committed");
    c
}

/// Consumes from `topic` until either `expected` messages have been
/// processed or `deadline` elapses (an overall deadline, not a per-message
/// timeout — a slow first message doesn't eat into the budget for the
/// rest), folding every message into a fresh [`MeasureAccumulator`].
pub async fn measure_topic(
    brokers: &str,
    group_id: &str,
    topic: &str,
    expected: u64,
    deadline: Duration,
) -> Result<MeasureAccumulator, MeasurerError> {
    let consumer: StreamConsumer = consumer_client_config(brokers, group_id).create()?;
    consumer.subscribe(&[topic])?;

    let mut acc = MeasureAccumulator::default();
    let end = tokio::time::Instant::now() + deadline;
    while expected == 0 || acc.received < expected {
        let remaining = end.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, consumer.recv()).await {
            Ok(Ok(msg)) => {
                let processed = process_headers(msg.headers(), now_ns());
                acc.record(processed);
            }
            // A genuine Kafka error or the overall deadline firing both end
            // the measurement window early — the caller sees whatever was
            // accumulated so far via `Ok(acc)`, never loses it to a hard
            // error over a partial run.
            Ok(Err(_kafka_err)) => break,
            Err(_elapsed) => break,
        }
    }
    Ok(acc)
}

#[cfg(test)]
mod tests {
    use rdkafka::message::{Header, OwnedHeaders};

    use super::*;
    use crate::header::encode_produce_ns;

    fn owned(pairs: &[(&str, &[u8])]) -> OwnedHeaders {
        let mut h = OwnedHeaders::new();
        for (k, v) in pairs {
            h = h.insert(Header {
                key: k,
                value: Some(*v),
            });
        }
        h
    }

    #[test]
    fn process_headers_extracts_latency_and_outcome_together() {
        let produce_ns = 1_000_000_000u64;
        let ns_bytes = encode_produce_ns(produce_ns);
        let headers = owned(&[
            (PRODUCE_NS_HEADER, &ns_bytes[..]),
            (SCHEMA_ID_HEADER, b"sch_abcdef"),
        ]);

        let observed_at = produce_ns + 5_000_000; // +5ms
        let processed = process_headers(Some(headers.as_borrowed()), observed_at);

        assert_eq!(processed.latency_ns, Some(5_000_000));
        assert_eq!(processed.outcome, Some(TagOutcome::Known));
    }

    #[test]
    fn process_headers_handles_missing_produce_ns_header() {
        let headers = owned(&[(SCHEMA_ID_HEADER, b"unresolved")]);
        let processed = process_headers(Some(headers.as_borrowed()), 42);
        assert_eq!(processed.latency_ns, None);
        assert_eq!(processed.outcome, Some(TagOutcome::Unresolved));
    }

    #[test]
    fn process_headers_handles_missing_schema_id_header() {
        let ns_bytes = encode_produce_ns(10);
        let headers = owned(&[(PRODUCE_NS_HEADER, &ns_bytes[..])]);
        let processed = process_headers(Some(headers.as_borrowed()), 20);
        assert_eq!(processed.latency_ns, Some(10));
        assert_eq!(processed.outcome, None);
    }

    #[test]
    fn process_headers_of_none_is_empty() {
        let processed = process_headers(None, 100);
        assert_eq!(processed.latency_ns, None);
        assert_eq!(processed.outcome, None);
    }

    #[test]
    fn process_headers_ignores_undecodable_produce_ns_bytes() {
        let headers = owned(&[(PRODUCE_NS_HEADER, b"not-8-bytes")]);
        let processed = process_headers(Some(headers.as_borrowed()), 100);
        assert_eq!(processed.latency_ns, None);
    }

    #[test]
    fn accumulator_folds_latency_and_outcome_and_tracks_missing_latency() {
        let mut acc = MeasureAccumulator::default();
        acc.record(ProcessedMessage {
            latency_ns: Some(5_000_000),
            outcome: Some(TagOutcome::Known),
        });
        acc.record(ProcessedMessage {
            latency_ns: None,
            outcome: Some(TagOutcome::Unresolved),
        });

        assert_eq!(acc.received, 2);
        assert_eq!(acc.missing_latency, 1);
        assert_eq!(acc.histogram.len(), 1);
        assert_eq!(acc.outcomes.known, 1);
        assert_eq!(acc.outcomes.unresolved, 1);
    }

    #[test]
    fn consumer_config_sets_read_committed_isolation() {
        let cfg = consumer_client_config("localhost:9092", "bench-group");
        assert_eq!(cfg.get("isolation.level"), Some("read_committed"));
        assert_eq!(cfg.get("enable.auto.commit"), Some("false"));
        assert_eq!(cfg.get("group.id"), Some("bench-group"));
    }
}
