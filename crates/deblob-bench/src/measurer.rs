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

/// Default "give up waiting for the next message" window used by
/// [`measure_topic`] when the caller doesn't override it. Chosen to comfortably
/// exceed the relay's own tag latency (spec evidence: `resolve_structural`
/// completed in ≤5ms per message even under the buggy smoke run) while still
/// ending a run promptly once production has genuinely finished.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(10);

/// How long [`measure_topic`] will wait for the consumer group to receive
/// its partition assignment before it starts the idle-timeout clock at all.
///
/// This is the fix for the live k3s run that captured 0-of-20000 tagged
/// messages: a fresh consumer group's join (coordinator discovery +
/// `JoinGroup`/`SyncGroup` rounds) can easily take several seconds, and the
/// old code started `last_progress` at loop entry — so with a short
/// `--measure-timeout-secs`/idle window, the loop could hit its stop
/// condition before the consumer was ever actually assigned a partition,
/// let alone had a chance to poll a real message. Bounded generously here
/// (and separately capped by the caller's overall `deadline`, so a
/// genuinely unreachable broker still can't hang the bench forever) so a
/// cold group join is tolerated even when the caller passes a tight idle
/// timeout.
pub const DEFAULT_ASSIGNMENT_TIMEOUT: Duration = Duration::from_secs(30);

/// Why [`measure_topic`]'s consume loop stopped. Exists so the stop
/// decision is directly observable/testable, not just an implicit `break`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    /// Saw `expected` messages. `expected == 0` (no target set) never
    /// produces this variant — the loop then runs until idle/deadline.
    ReachedExpected,
    /// No new message arrived for `idle_timeout` — production (or the
    /// relay) has gone quiet; waiting longer won't surface more.
    Idle,
    /// The overall deadline elapsed regardless of progress — a hard
    /// backstop so a stalled broker can't hang the bench forever.
    Deadline,
}

/// Pure stop-condition check for [`measure_topic`]'s consume loop.
/// `idle_elapsed` is the time since the last message was received (or
/// since the loop started, if none yet); `deadline_elapsed` is whether the
/// overall wall-clock deadline has passed. This is the fix for the smoke
/// run's truncated 927-of-2000 capture: rather than a single overall
/// deadline that can expire mid-backlog (e.g. eaten by a slow initial
/// group join), the loop now keeps consuming as long as messages keep
/// arriving, only stopping on genuine completion, genuine silence, or the
/// hard backstop.
pub fn measure_stop_reason(
    received: u64,
    expected: u64,
    idle_elapsed: Duration,
    idle_timeout: Duration,
    deadline_elapsed: bool,
) -> Option<StopReason> {
    if expected > 0 && received >= expected {
        return Some(StopReason::ReachedExpected);
    }
    if idle_elapsed >= idle_timeout {
        return Some(StopReason::Idle);
    }
    if deadline_elapsed {
        return Some(StopReason::Deadline);
    }
    None
}

/// Consumes from `topic` until [`measure_stop_reason`] says to stop:
/// `expected` messages seen, `idle_timeout` elapsed since the last
/// message, or the overall `deadline` elapsed. Folds every message into a
/// fresh [`MeasureAccumulator`], which the caller compares against
/// `expected`/the producer's own sent count to report received-vs-expected
/// explicitly rather than silently under-counting.
pub async fn measure_topic(
    brokers: &str,
    group_id: &str,
    topic: &str,
    expected: u64,
    deadline: Duration,
    idle_timeout: Duration,
) -> Result<MeasureAccumulator, MeasurerError> {
    let consumer: StreamConsumer = consumer_client_config(brokers, group_id).create()?;
    consumer.subscribe(&[topic])?;

    let mut acc = MeasureAccumulator::default();
    let overall_deadline = tokio::time::Instant::now() + deadline;

    // Phase 1: wait for the consumer group to actually be assigned
    // partitions before starting the idle clock. Calling `consumer.recv()`
    // is what drives rdkafka's internal join/rebalance machinery, so this
    // polls it — bounded by `DEFAULT_ASSIGNMENT_TIMEOUT` and by the overall
    // deadline (never waits past either) — until `consumer.assignment()`
    // is non-empty. If a message happens to arrive during this window (it
    // can: assignment completing and the first message being handed back
    // can land in the same `recv()`), it's folded into `acc` immediately
    // rather than discarded.
    let assignment_deadline =
        overall_deadline.min(tokio::time::Instant::now() + DEFAULT_ASSIGNMENT_TIMEOUT);
    loop {
        let assigned = consumer
            .assignment()
            .map(|tpl| tpl.count() > 0)
            .unwrap_or(false);
        if assigned {
            break;
        }
        let now = tokio::time::Instant::now();
        if now >= assignment_deadline {
            break;
        }
        let wait = assignment_deadline.saturating_duration_since(now);
        match tokio::time::timeout(wait, consumer.recv()).await {
            Ok(Ok(msg)) => {
                let processed = process_headers(msg.headers(), now_ns());
                acc.record(processed);
                break;
            }
            // A genuine Kafka error this early ends the run — fall through
            // to the main loop, which will immediately observe the
            // deadline/idle condition and return whatever's in `acc`
            // (nothing, in this case) rather than propagating a hard
            // error over a partial run.
            Ok(Err(_kafka_err)) => break,
            // Still waiting on assignment; loop and recheck.
            Err(_elapsed) => {}
        }
    }

    // The idle clock starts here — AFTER assignment (or after giving up
    // waiting for it), never at consumer construction. A slow group join
    // no longer eats into the idle budget.
    let mut last_progress = tokio::time::Instant::now();

    loop {
        let now = tokio::time::Instant::now();
        let idle_elapsed = now.saturating_duration_since(last_progress);
        let deadline_elapsed = now >= overall_deadline;
        if measure_stop_reason(
            acc.received,
            expected,
            idle_elapsed,
            idle_timeout,
            deadline_elapsed,
        )
        .is_some()
        {
            break;
        }

        let remaining_deadline = overall_deadline.saturating_duration_since(now);
        let remaining_idle = idle_timeout.saturating_sub(idle_elapsed);
        let wait = remaining_deadline.min(remaining_idle);
        if wait.is_zero() {
            break;
        }

        match tokio::time::timeout(wait, consumer.recv()).await {
            Ok(Ok(msg)) => {
                let processed = process_headers(msg.headers(), now_ns());
                acc.record(processed);
                last_progress = tokio::time::Instant::now();
            }
            // A genuine Kafka error ends the measurement window early —
            // the caller sees whatever was accumulated so far via
            // `Ok(acc)`, never loses it to a hard error over a partial
            // run.
            Ok(Err(_kafka_err)) => break,
            // Either the idle window or the overall deadline elapsed
            // first (`wait` was bounded by whichever is smaller) — loop
            // back to `measure_stop_reason` to determine which and stop
            // cleanly on the next iteration rather than treating a
            // `tokio::time::timeout` timeout as a hard error.
            Err(_elapsed) => {}
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
    fn stop_reason_fires_on_reaching_expected_before_idle_or_deadline() {
        let reason = measure_stop_reason(
            2000,
            2000,
            Duration::from_millis(1),
            Duration::from_secs(10),
            false,
        );
        assert_eq!(reason, Some(StopReason::ReachedExpected));
    }

    #[test]
    fn stop_reason_is_none_while_short_of_expected_and_not_idle_or_expired() {
        let reason = measure_stop_reason(
            927,
            2000,
            Duration::from_millis(1),
            Duration::from_secs(10),
            false,
        );
        assert_eq!(reason, None);
    }

    #[test]
    fn stop_reason_fires_idle_even_when_short_of_expected() {
        // This is the truncation bug from the smoke run, made explicit:
        // 927 of 2000 received must NOT silently stop unless the run has
        // genuinely gone idle or hit the hard deadline.
        let reason = measure_stop_reason(
            927,
            2000,
            Duration::from_secs(10),
            Duration::from_secs(10),
            false,
        );
        assert_eq!(reason, Some(StopReason::Idle));
    }

    #[test]
    fn stop_reason_fires_deadline_as_the_last_resort_backstop() {
        let reason = measure_stop_reason(
            927,
            2000,
            Duration::from_millis(1),
            Duration::from_secs(10),
            true,
        );
        assert_eq!(reason, Some(StopReason::Deadline));
    }

    #[test]
    fn stop_reason_with_no_expected_target_runs_until_idle_or_deadline() {
        // `expected == 0` (no target set) must never match
        // `ReachedExpected` — the loop should run until idle/deadline
        // regardless of how many messages have been received.
        assert_eq!(
            measure_stop_reason(
                0,
                0,
                Duration::from_millis(1),
                Duration::from_secs(10),
                false
            ),
            None
        );
        assert_eq!(
            measure_stop_reason(
                500,
                0,
                Duration::from_secs(10),
                Duration::from_secs(10),
                false
            ),
            Some(StopReason::Idle)
        );
    }

    #[test]
    fn stop_reason_prefers_reached_expected_over_a_simultaneous_idle_or_deadline() {
        let reason = measure_stop_reason(
            2000,
            2000,
            Duration::from_secs(10),
            Duration::from_secs(10),
            true,
        );
        assert_eq!(reason, Some(StopReason::ReachedExpected));
    }

    #[test]
    fn consumer_config_sets_read_committed_isolation() {
        let cfg = consumer_client_config("localhost:9092", "bench-group");
        assert_eq!(cfg.get("isolation.level"), Some("read_committed"));
        assert_eq!(cfg.get("enable.auto.commit"), Some("false"));
        assert_eq!(cfg.get("group.id"), Some("bench-group"));
    }
}
