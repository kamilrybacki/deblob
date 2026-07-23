//! Prometheus metrics + structured-tracing init (spec §11, design book §6).
//!
//! `Metrics` wraps a private [`prometheus::Registry`] and is the single
//! place that knows how to turn a domain outcome ([`SchemaRef`],
//! [`QuarantineReason`], ...) into a metric label. Every label set is
//! **bounded** by construction: the only strings that ever reach a label
//! value are the fixed `fate`/`reason`/`result`/`operation` enums below —
//! never a schema id, candidate id, producer/source identifier, topic
//! name, or error message (spec §11, design-book §6: "no IDs/topics/
//! messages in labels"). Metric names follow the design book's naming
//! rules: `deblob_` prefix, base units, counters end `_total`, durations
//! end `_seconds`.
//!
//! Canonical names (design-book §6 / spec §11) this module registers:
//! `deblob_relay_records_total`, `deblob_relay_transactions_total{result}`,
//! `deblob_schema_matches_total{result}`, `deblob_candidates_active`,
//! `deblob_candidate_promotions_total{result}`, `deblob_cold_lane_lag_records`,
//! `deblob_registry_operation_duration_seconds{operation}`,
//! `deblob_quarantine_records_total{reason}` — plus two P1-specific
//! additions the design book doesn't itemize but this task's brief
//! requires: `deblob_messages_total{fate}`, `deblob_cache_hits_total`, and
//! `deblob_tag_latency_seconds` (the end-to-end `HotMatcher::classify`
//! latency, distinct from the registry-call-only
//! `registry_operation_duration_seconds`).
//!
//! P2-D Task 7 additions (`deblob-p2d-hermes-review.md` §5, `deblob::
//! semantic_drift`): `deblob_semantic_drift_total` (no labels — one family
//! gaining a structurally-compatible version whose active `sem_` changed)
//! and `deblob_semantic_collision_total{strength}` (bounded `strength` ∈
//! `strong`/`medium`/`weak` — one `sem_` shared by ≥2 `sch_`s). Both are
//! PROPOSAL-ONLY diagnostic counters: nothing in `deblob::semantic_drift`
//! ever aliases, merges, or mutates a family/schema/`sem_` off the back of
//! either signal — see that module's docs.
//!
//! This crate's own code (the hot-path matcher and the cold lane) only
//! ever emits a subset of the canonical set directly; `deblob_relay_
//! records_total` and `deblob_relay_transactions_total{result}` are
//! incremented from `deblob-kafka::Relay::run` (Task 16) via the `pub`
//! `inc_relay_records`/`record_relay_transaction` methods below, against
//! the SAME `Metrics` instance passed in through `RelayCfg`. The remainder
//! (`deblob_cold_lane_lag_records`, `deblob_candidate_promotions_total`)
//! are registered up front so the `/metrics` exposition surface is stable
//! across phases, even though nothing increments them yet.
//! `deblob_slm_decisions_total` (P2, shadow-mode SLM) is deliberately NOT
//! registered here — P1 has no SLM lane, and a metric nothing ever emits
//! would misrepresent what this binary tracks.

use std::sync::Arc;
use std::time::Duration;

use deblob_core::error::QuarantineReason;
use deblob_core::id::SchemaRef;
use prometheus::{
    Counter, CounterVec, Encoder, Gauge, Histogram, HistogramOpts, HistogramVec, Opts, Registry,
    TextEncoder,
};

/// Bounded `fate` label values for `deblob_messages_total`.
const FATES: [&str; 5] = [
    "known",
    "provisional",
    "unresolved",
    "malformed",
    "tombstone",
];

/// Bounded `result` label values for `deblob_schema_matches_total` — the
/// subset of [`FATES`] that a completed *match* (as opposed to a rejected
/// parse) can land on.
const MATCH_RESULTS: [&str; 3] = ["known", "provisional", "unresolved"];

/// Bounded `reason` label values for `deblob_quarantine_records_total`:
/// exactly the 8 [`QuarantineReason`] variants, snake_case (spec §4).
const QUARANTINE_REASONS: [&str; 8] = [
    "duplicate_key",
    "non_finite_number",
    "depth_exceeded",
    "size_exceeded",
    "field_count_exceeded",
    "key_length_exceeded",
    "parse_error",
    "utf8_error",
];

/// Bounded `operation` label values for
/// `deblob_registry_operation_duration_seconds`.
const REGISTRY_OPERATIONS: [&str; 1] = ["resolve_structural"];

/// Bounded `strength` label values for `deblob_semantic_collision_total`
/// (P2-D Task 7, `deblob-p2d-hermes-review.md` §5) — the annotation-coverage
/// classification `deblob::semantic_drift::CollisionStrength` produces.
/// Never a `sem_`/`sch_` id, family id, or vocabulary code.
const COLLISION_STRENGTHS: [&str; 3] = ["strong", "medium", "weak"];

/// Bounded `operation` label values for `deblob_redis_write_refusals_total` —
/// the fixed set of Redis WRITE sites (in `deblob-redis`) that classify a
/// `noeviction`/`maxmemory` OOM refusal. Never a Redis key, id, or error
/// message.
const REDIS_WRITE_OPERATIONS: [&str; 7] = [
    "schema_publish",
    "set_name",
    "evidence_append",
    "candidate_state",
    "state_index",
    "sample",
    "value_profile",
];

/// Bounded `outcome` label values for `deblob_ollama_requests_total` — the
/// decision-lane SLM caller-boundary classification of one `classify` call.
const OLLAMA_OUTCOMES: [&str; 3] = ["ok", "timeout", "error"];

/// `fate` label for one classification outcome. Never derived from the
/// carried id — only the discriminant.
pub(crate) fn fate_label(schema_ref: &SchemaRef) -> &'static str {
    match schema_ref {
        SchemaRef::Known(_) => "known",
        SchemaRef::Provisional(_) => "provisional",
        SchemaRef::Unresolved => "unresolved",
        SchemaRef::Malformed => "malformed",
        SchemaRef::Tombstone => "tombstone",
    }
}

/// `reason` label for one quarantine event. Also reused verbatim as the
/// `reason` field on the matching `tracing::debug!` quarantine log line
/// (`matcher::classify`) — one label-string source of truth for both.
pub(crate) fn quarantine_reason_label(reason: QuarantineReason) -> &'static str {
    match reason {
        QuarantineReason::DuplicateKey => "duplicate_key",
        QuarantineReason::NonFiniteNumber => "non_finite_number",
        QuarantineReason::DepthExceeded => "depth_exceeded",
        QuarantineReason::SizeExceeded => "size_exceeded",
        QuarantineReason::FieldCountExceeded => "field_count_exceeded",
        QuarantineReason::KeyLengthExceeded => "key_length_exceeded",
        QuarantineReason::ParseError => "parse_error",
        QuarantineReason::Utf8Error => "utf8_error",
    }
}

/// The process-wide Prometheus surface. Cheap to increment (atomic ops on
/// the hot path); build once via [`Metrics::new`] and share behind an
/// `Arc` across the matcher, cold lane, and management API.
pub struct Metrics {
    registry: Registry,

    messages_total: CounterVec,
    schema_matches_total: CounterVec,
    cache_hits_total: Counter,
    quarantine_records_total: CounterVec,
    tag_latency_seconds: Histogram,
    registry_operation_duration_seconds: HistogramVec,

    candidates_active: Gauge,
    candidate_promotions_total: CounterVec,

    relay_records_total: Counter,
    relay_transactions_total: CounterVec,
    cold_lane_lag_records: Gauge,

    /// P2-D Task 7: `deblob_semantic_drift_total` — no labels by design
    /// (the drift record's `family_id`/`sch_`/`sem_` fields carry the
    /// detail; the metric itself is just "how often does this fire").
    semantic_drift_total: Counter,
    /// P2-D Task 7: `deblob_semantic_collision_total{strength}`.
    semantic_collision_total: CounterVec,

    /// `deblob_redis_write_refusals_total{operation}` — Redis write commands
    /// refused by the `noeviction`/`maxmemory 1700mb` ceiling (an `OOM`-coded
    /// `redis::RedisError`), by the write operation that hit it. Incremented
    /// from `deblob-redis`'s write paths via [`Metrics::inc_redis_write_refusal`].
    redis_write_refusals_total: CounterVec,

    /// `deblob_ollama_inflight` — in-flight SLM inference requests on the
    /// decision lane. Ollama exposes no usable `/metrics`, so this brackets
    /// the caller-side `classify` call (`deblob::shadow`) instead.
    ollama_inflight: Gauge,
    /// `deblob_ollama_requests_total{outcome}` — decision-lane SLM inference
    /// requests by `outcome` (`ok`/`timeout`/`error`).
    ollama_requests_total: CounterVec,
}

impl Metrics {
    /// Builds every metric, registers it against a fresh private
    /// [`prometheus::Registry`], and pre-touches every bounded label
    /// combination that P1 can emit so `/metrics` shows a stable set of
    /// series (at `0`) from the very first scrape rather than only after
    /// the first matching event of each kind.
    pub fn new() -> Arc<Self> {
        let registry = Registry::new();

        let messages_total = CounterVec::new(
            Opts::new(
                "deblob_messages_total",
                "Total messages classified on the hot path, by fate.",
            ),
            &["fate"],
        )
        .expect("valid metric opts");

        let schema_matches_total = CounterVec::new(
            Opts::new(
                "deblob_schema_matches_total",
                "Total schema-match attempts that reached a decision, by result.",
            ),
            &["result"],
        )
        .expect("valid metric opts");

        let cache_hits_total = Counter::with_opts(Opts::new(
            "deblob_cache_hits_total",
            "Total exact-match LRU cache hits on the hot path (zero registry round-trips).",
        ))
        .expect("valid metric opts");

        let quarantine_records_total = CounterVec::new(
            Opts::new(
                "deblob_quarantine_records_total",
                "Total messages quarantined, by reason.",
            ),
            &["reason"],
        )
        .expect("valid metric opts");

        let tag_latency_seconds = Histogram::with_opts(HistogramOpts::new(
            "deblob_tag_latency_seconds",
            "End-to-end HotMatcher::classify latency in seconds.",
        ))
        .expect("valid metric opts");

        let registry_operation_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "deblob_registry_operation_duration_seconds",
                "Registry backend call duration in seconds, by operation.",
            ),
            &["operation"],
        )
        .expect("valid metric opts");

        let candidates_active = Gauge::with_opts(Opts::new(
            "deblob_candidates_active",
            "Number of distinct candidates currently tracked by the cold lane.",
        ))
        .expect("valid metric opts");

        let candidate_promotions_total = CounterVec::new(
            Opts::new(
                "deblob_candidate_promotions_total",
                "Total candidate promotion attempts, by result.",
            ),
            &["result"],
        )
        .expect("valid metric opts");

        let relay_records_total = Counter::with_opts(Opts::new(
            "deblob_relay_records_total",
            "Total records read off the raw relay topic.",
        ))
        .expect("valid metric opts");

        let relay_transactions_total = CounterVec::new(
            Opts::new(
                "deblob_relay_transactions_total",
                "Total relay transactions, by result.",
            ),
            &["result"],
        )
        .expect("valid metric opts");

        let cold_lane_lag_records = Gauge::with_opts(Opts::new(
            "deblob_cold_lane_lag_records",
            "Cold-lane consumer lag, in records.",
        ))
        .expect("valid metric opts");

        let semantic_drift_total = Counter::with_opts(Opts::new(
            "deblob_semantic_drift_total",
            "Total semantic-drift diagnostics fired: a family gained a \
             structurally-compatible version whose active sem_ changed. \
             Proposal-only — never splits a family.",
        ))
        .expect("valid metric opts");

        let semantic_collision_total = CounterVec::new(
            Opts::new(
                "deblob_semantic_collision_total",
                "Total same-sem_/different-sch_ diagnostics fired, by \
                 annotation-coverage strength. Proposal-only — never \
                 aliases or merges a family.",
            ),
            &["strength"],
        )
        .expect("valid metric opts");

        let redis_write_refusals_total = CounterVec::new(
            Opts::new(
                "deblob_redis_write_refusals_total",
                "Redis write commands refused (OOM/noeviction) by operation",
            ),
            &["operation"],
        )
        .expect("valid metric opts");

        let ollama_inflight = Gauge::with_opts(Opts::new(
            "deblob_ollama_inflight",
            "In-flight SLM inference requests (decision lane)",
        ))
        .expect("valid metric opts");

        let ollama_requests_total = CounterVec::new(
            Opts::new(
                "deblob_ollama_requests_total",
                "Total SLM inference requests (decision lane), by outcome",
            ),
            &["outcome"],
        )
        .expect("valid metric opts");

        for metric in [
            Box::new(messages_total.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(schema_matches_total.clone()),
            Box::new(cache_hits_total.clone()),
            Box::new(quarantine_records_total.clone()),
            Box::new(tag_latency_seconds.clone()),
            Box::new(registry_operation_duration_seconds.clone()),
            Box::new(candidates_active.clone()),
            Box::new(candidate_promotions_total.clone()),
            Box::new(relay_records_total.clone()),
            Box::new(relay_transactions_total.clone()),
            Box::new(cold_lane_lag_records.clone()),
            Box::new(semantic_drift_total.clone()),
            Box::new(semantic_collision_total.clone()),
            Box::new(redis_write_refusals_total.clone()),
            Box::new(ollama_inflight.clone()),
            Box::new(ollama_requests_total.clone()),
        ] {
            registry.register(metric).expect("unique metric name");
        }

        // Pre-touch every bounded label value P1 can actually emit so the
        // exposition surface is stable from the first scrape (spec §11).
        for fate in FATES {
            messages_total.with_label_values(&[fate]);
        }
        for result in MATCH_RESULTS {
            schema_matches_total.with_label_values(&[result]);
        }
        for reason in QUARANTINE_REASONS {
            quarantine_records_total.with_label_values(&[reason]);
        }
        for operation in REGISTRY_OPERATIONS {
            registry_operation_duration_seconds.with_label_values(&[operation]);
        }
        for strength in COLLISION_STRENGTHS {
            semantic_collision_total.with_label_values(&[strength]);
        }
        for operation in REDIS_WRITE_OPERATIONS {
            redis_write_refusals_total.with_label_values(&[operation]);
        }
        for outcome in OLLAMA_OUTCOMES {
            ollama_requests_total.with_label_values(&[outcome]);
        }

        Arc::new(Self {
            registry,
            messages_total,
            schema_matches_total,
            cache_hits_total,
            quarantine_records_total,
            tag_latency_seconds,
            registry_operation_duration_seconds,
            candidates_active,
            candidate_promotions_total,
            relay_records_total,
            relay_transactions_total,
            cold_lane_lag_records,
            semantic_drift_total,
            semantic_collision_total,
            redis_write_refusals_total,
            ollama_inflight,
            ollama_requests_total,
        })
    }

    /// The underlying [`prometheus::Registry`], for callers that want to
    /// gather it themselves (e.g. a future combined-registry setup).
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Renders the current state as Prometheus text exposition format
    /// (version 0.0.4) — what the `/metrics` HTTP handler returns verbatim.
    pub fn gather_text(&self) -> Result<String, prometheus::Error> {
        let families = self.registry.gather();
        let encoder = TextEncoder::new();
        let mut buf = Vec::new();
        encoder.encode(&families, &mut buf)?;
        Ok(String::from_utf8(buf)
            .unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned()))
    }

    /// Records one hot-path classification outcome: increments
    /// `deblob_messages_total{fate}` always, and additionally
    /// `deblob_schema_matches_total{result}` when the outcome is a real
    /// match decision (known/provisional/unresolved) rather than a parse
    /// rejection (malformed) or a tombstone.
    pub(crate) fn record_classification(&self, schema_ref: &SchemaRef) {
        let fate = fate_label(schema_ref);
        self.messages_total.with_label_values(&[fate]).inc();
        if MATCH_RESULTS.contains(&fate) {
            self.schema_matches_total.with_label_values(&[fate]).inc();
        }
    }

    /// Records one quarantine event: increments
    /// `deblob_quarantine_records_total{reason}`.
    pub(crate) fn record_quarantine(&self, reason: QuarantineReason) {
        self.quarantine_records_total
            .with_label_values(&[quarantine_reason_label(reason)])
            .inc();
    }

    /// Increments `deblob_cache_hits_total` — call exactly once per
    /// exact-match LRU hit on the hot path.
    pub(crate) fn record_cache_hit(&self) {
        self.cache_hits_total.inc();
    }

    /// Observes `deblob_tag_latency_seconds` — the full
    /// `HotMatcher::classify` wall-clock duration.
    pub(crate) fn observe_tag_latency(&self, elapsed: Duration) {
        self.tag_latency_seconds.observe(elapsed.as_secs_f64());
    }

    /// Observes `deblob_registry_operation_duration_seconds{operation}`.
    pub(crate) fn observe_registry_op(&self, operation: &'static str, elapsed: Duration) {
        self.registry_operation_duration_seconds
            .with_label_values(&[operation])
            .observe(elapsed.as_secs_f64());
    }

    /// Increments `deblob_candidates_active` — call exactly once per
    /// genuinely new candidate the cold lane creates. `pub` (not
    /// `pub(crate)`): Task 18 moved `deblob::coldlane::ColdLane` (the only
    /// caller) into the `deblob` crate while this `Metrics` type stayed in
    /// `deblob-match`, so this is now a cross-crate call.
    pub fn inc_candidates_active(&self) {
        self.candidates_active.inc();
    }

    /// Registered but not yet incremented anywhere in P1 (no code path
    /// promotes candidates today) — kept `pub(crate)` so a future
    /// promotion-policy task can start emitting into this series without
    /// touching this module's public shape.
    #[allow(dead_code)]
    pub(crate) fn record_promotion(&self, result: &str) {
        self.candidate_promotions_total
            .with_label_values(&[result])
            .inc();
    }

    /// Increments `deblob_relay_records_total` — call once per record read
    /// off the raw relay topic (Task 16's `deblob-kafka::Relay::run`).
    pub fn inc_relay_records(&self) {
        self.relay_records_total.inc();
    }

    /// Increments `deblob_relay_transactions_total{result}` — call once per
    /// relay transaction outcome, `result` one of `"committed"`/
    /// `"aborted"` (Task 16).
    pub fn record_relay_transaction(&self, result: &str) {
        self.relay_transactions_total
            .with_label_values(&[result])
            .inc();
    }

    #[allow(dead_code)]
    pub(crate) fn set_cold_lane_lag(&self, records: f64) {
        self.cold_lane_lag_records.set(records);
    }

    /// Increments `deblob_semantic_drift_total` — call once per
    /// `deblob::semantic_drift::SemanticDrift` fired (P2-D Task 7). `pub`:
    /// cross-crate caller, same reason as `inc_candidates_active`.
    pub fn record_semantic_drift(&self) {
        self.semantic_drift_total.inc();
    }

    /// Increments `deblob_semantic_collision_total{strength}` — call once
    /// per same-`sem_`/different-`sch_` finding classified (P2-D Task 7),
    /// `strength` one of `"strong"`/`"medium"`/`"weak"`
    /// (`CollisionStrength::as_str`). `pub`: cross-crate caller.
    pub fn record_semantic_collision(&self, strength: &str) {
        self.semantic_collision_total
            .with_label_values(&[strength])
            .inc();
    }

    /// Increments `deblob_redis_write_refusals_total{operation}` — call once
    /// per Redis WRITE command refused by the `noeviction`/`maxmemory`
    /// ceiling (an `OOM`-coded `redis::RedisError`), `operation` one of the
    /// fixed `REDIS_WRITE_OPERATIONS` write-site names. `pub`: cross-crate
    /// caller (`deblob-redis`'s write paths).
    pub fn inc_redis_write_refusal(&self, operation: &str) {
        self.redis_write_refusals_total
            .with_label_values(&[operation])
            .inc();
    }

    /// Increments `deblob_ollama_inflight` — call once immediately BEFORE a
    /// decision-lane SLM inference call. `pub`: cross-crate caller
    /// (`deblob::shadow`).
    pub fn inc_ollama_inflight(&self) {
        self.ollama_inflight.inc();
    }

    /// Decrements `deblob_ollama_inflight` — call once immediately AFTER a
    /// decision-lane SLM inference call returns, on every path.
    pub fn dec_ollama_inflight(&self) {
        self.ollama_inflight.dec();
    }

    /// Increments `deblob_ollama_requests_total{outcome}` — call once per
    /// completed decision-lane SLM inference call, `outcome` one of
    /// `"ok"`/`"timeout"`/`"error"`. `pub`: cross-crate caller
    /// (`deblob::shadow`).
    pub fn record_ollama_request(&self, outcome: &str) {
        self.ollama_requests_total
            .with_label_values(&[outcome])
            .inc();
    }
}

/// Initializes the process-wide `tracing` subscriber: JSON-formatted
/// structured logs, level controlled by `RUST_LOG` (env-filter, defaults
/// to `info`). Idempotent-safe to call once at binary startup (Task 18's
/// `main.rs`).
///
/// CRITICAL (spec §11): nothing in this crate may pass payload bytes,
/// parsed-node contents, or canonicalized text into a `tracing::` field —
/// only bounded/derived values (fate labels, reasons, byte counts,
/// fingerprints). `scripts/lint-no-payload-logs.sh` enforces this in CI as
/// a best-effort grep guard, not a substitute for review.
pub fn init_tracing() {
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .with_target(true)
        .init();
}

#[cfg(test)]
pub(crate) mod test_support {
    //! Test-only helpers for reading a metric value back out of a gathered
    //! [`prometheus::proto::MetricFamily`] list — used by this module's own
    //! tests and by `matcher`/`coldlane`'s unit tests (same crate, so these
    //! `#[cfg(test)]` items are visible there too under `cargo test`).

    use prometheus::proto::MetricFamily;

    /// The counter/gauge value of the family `name`, optionally filtered to
    /// the metric instance carrying label `(key, value)`. Panics if the
    /// family doesn't exist at all (a real bug in the test), returns `0.0`
    /// if the family exists but no instance matches the label filter (a
    /// legitimate "never happened yet" reading).
    pub(crate) fn value_of(
        families: &[MetricFamily],
        name: &str,
        label: Option<(&str, &str)>,
    ) -> f64 {
        let family = families
            .iter()
            .find(|f| f.get_name() == name)
            .unwrap_or_else(|| panic!("metric family {name:?} not found in gathered output"));

        for m in family.get_metric() {
            let matches = match label {
                None => m.get_label().is_empty(),
                Some((k, v)) => m
                    .get_label()
                    .iter()
                    .any(|lp| lp.get_name() == k && lp.get_value() == v),
            };
            if !matches {
                continue;
            }
            if m.has_counter() {
                return m.get_counter().get_value();
            }
            if m.has_gauge() {
                return m.get_gauge().get_value();
            }
            if m.has_histogram() {
                return m.get_histogram().get_sample_count() as f64;
            }
        }
        0.0
    }

    /// The full sorted list of label *names* (not values) declared on
    /// family `name` — used to assert a metric's label set is exactly the
    /// bounded set this module documents (no stray id/topic/message label
    /// ever sneaks in).
    pub(crate) fn label_names_of(families: &[MetricFamily], name: &str) -> Vec<String> {
        let family = families
            .iter()
            .find(|f| f.get_name() == name)
            .unwrap_or_else(|| panic!("metric family {name:?} not found in gathered output"));
        let mut names: Vec<String> = family
            .get_metric()
            .iter()
            .flat_map(|m| m.get_label().iter().map(|lp| lp.get_name().to_string()))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        names.sort();
        names
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registers_the_full_canonical_p1_surface() {
        let metrics = Metrics::new();
        let text = metrics.gather_text().unwrap();

        for name in [
            "deblob_messages_total",
            "deblob_schema_matches_total",
            "deblob_cache_hits_total",
            "deblob_quarantine_records_total",
            "deblob_tag_latency_seconds",
            "deblob_registry_operation_duration_seconds",
            "deblob_candidates_active",
        ] {
            assert!(text.contains(name), "missing metric {name} in:\n{text}");
        }
    }

    #[test]
    fn no_id_labels_label_sets_are_the_documented_bounded_set() {
        // Guards against a future edit accidentally widening a label set to
        // carry a schema/candidate/producer id, a topic name, or an error
        // message (spec §11 / design-book §6: "no IDs/topics/messages in
        // labels").
        let metrics = Metrics::new();
        let families = metrics.registry.gather();

        assert_eq!(
            test_support::label_names_of(&families, "deblob_messages_total"),
            vec!["fate".to_string()]
        );
        assert_eq!(
            test_support::label_names_of(&families, "deblob_quarantine_records_total"),
            vec!["reason".to_string()]
        );
        assert_eq!(
            test_support::label_names_of(&families, "deblob_schema_matches_total"),
            vec!["result".to_string()]
        );
        assert!(
            test_support::label_names_of(&families, "deblob_cache_hits_total").is_empty(),
            "cache_hits_total must carry no labels at all"
        );
    }

    #[test]
    fn registers_the_p2d_task7_semantic_diagnostic_surface() {
        let metrics = Metrics::new();
        let text = metrics.gather_text().unwrap();
        for name in [
            "deblob_semantic_drift_total",
            "deblob_semantic_collision_total",
        ] {
            assert!(text.contains(name), "missing metric {name} in:\n{text}");
        }
    }

    #[test]
    fn semantic_collision_total_label_set_is_bounded_to_strength_only() {
        // Same drift-guard shape as `no_id_labels_...` above: this metric
        // must never grow a sem_/sch_/family-id label.
        let metrics = Metrics::new();
        let families = metrics.registry.gather();
        assert_eq!(
            test_support::label_names_of(&families, "deblob_semantic_collision_total"),
            vec!["strength".to_string()]
        );
        assert!(
            test_support::label_names_of(&families, "deblob_semantic_drift_total").is_empty(),
            "semantic_drift_total must carry no labels at all"
        );
    }

    #[test]
    fn record_semantic_drift_and_collision_increment_expected_series() {
        let metrics = Metrics::new();
        metrics.record_semantic_drift();
        metrics.record_semantic_collision("strong");
        metrics.record_semantic_collision("strong");
        metrics.record_semantic_collision("weak");

        let families = metrics.registry.gather();
        assert_eq!(
            test_support::value_of(&families, "deblob_semantic_drift_total", None),
            1.0
        );
        assert_eq!(
            test_support::value_of(
                &families,
                "deblob_semantic_collision_total",
                Some(("strength", "strong"))
            ),
            2.0
        );
        assert_eq!(
            test_support::value_of(
                &families,
                "deblob_semantic_collision_total",
                Some(("strength", "weak"))
            ),
            1.0
        );
        assert_eq!(
            test_support::value_of(
                &families,
                "deblob_semantic_collision_total",
                Some(("strength", "medium"))
            ),
            0.0
        );
    }

    #[test]
    fn quarantine_reason_labels_cover_every_variant_exactly() {
        // Every QuarantineReason variant must round-trip to one of the 8
        // pre-touched bounded label values — this is what keeps
        // `quarantine_reason_label` and `QUARANTINE_REASONS` from drifting
        // apart if a 9th reason is ever added to deblob-core without a
        // matching update here.
        let all_reasons = [
            QuarantineReason::DuplicateKey,
            QuarantineReason::NonFiniteNumber,
            QuarantineReason::DepthExceeded,
            QuarantineReason::SizeExceeded,
            QuarantineReason::FieldCountExceeded,
            QuarantineReason::KeyLengthExceeded,
            QuarantineReason::ParseError,
            QuarantineReason::Utf8Error,
        ];
        let mut labels: Vec<&'static str> = all_reasons
            .iter()
            .copied()
            .map(quarantine_reason_label)
            .collect();
        labels.sort_unstable();
        let mut expected = QUARANTINE_REASONS.to_vec();
        expected.sort_unstable();
        assert_eq!(labels, expected);
    }
}
