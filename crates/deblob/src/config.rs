//! Runtime configuration (spec §9, Task 18): non-secret operational knobs
//! come from a TOML file (default path `deblob.toml`, overridable via
//! `--config`) plus a small env overlay; SECRETS ARE ENV-ONLY and validated
//! present at startup by [`validate_secrets`] — never deserialized out of
//! the TOML file, never logged.
//!
//! `Config` intentionally has no field for `DEBLOB_API_TOKEN`,
//! `DEBLOB_REDIS_URL`, `DEBLOB_KAFKA_BROKERS`, or any `DEBLOB_KAFKA_SASL_*`
//! credential — those exist only in [`Secrets`], built exclusively from
//! environment variables. See `deblob.example.toml` at the repo root for
//! the canonical TOML shape/defaults this module parses.

use std::fmt;
use std::path::{Path, PathBuf};

use deblob_kafka::KafkaSasl;
use deblob_redis::RedisOpts;
use serde::Deserialize;

/// Non-secret operational configuration loaded from a TOML file.
///
/// `deny_unknown_fields` (on this and every nested config struct below):
/// a typo'd TOML key (e.g. `[kafak]` or `discovry_topic`) errors loudly at
/// startup instead of silently falling back to a default the operator
/// never intended.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub kafka: KafkaConfig,
    #[serde(default)]
    pub limits: LimitsConfig,
    #[serde(default)]
    pub promotion: PromotionConfig,
    #[serde(default)]
    pub management: ManagementConfig,
    /// `[slm]` — SLM shadow-lane wiring (P2-A/B Task 5b). Absent from a
    /// TOML file entirely, or present with `enabled` unset, both fall back
    /// to [`SlmConfig::default`] (`enabled: false`) — the shadow lane is
    /// OFF unless an operator explicitly opts in.
    #[serde(default)]
    pub slm: SlmConfig,
    /// `[http_proxy]` — HTTP push reverse-proxy wiring (P2-C Task 4).
    /// Absent from a TOML file entirely, or present with `enabled` unset,
    /// both fall back to [`HttpProxyConfig::default`] (`enabled: false`) —
    /// same "off unless explicitly opted in" contract as `[slm]`.
    #[serde(default)]
    pub http_proxy: HttpProxyConfig,
    /// `[semantic]` — governance-registered `canonical_field_id`/
    /// `canonical_event_type_id` vocabularies (P2-D Task 8 follow-up A1).
    /// Absent from a TOML file entirely defaults to
    /// [`SemanticConfig::default`] (both lists empty) — every strong-axis
    /// annotation then still `422`s, exactly Task 6's original behavior.
    #[serde(default)]
    pub semantic: SemanticConfig,
    /// `[umbrella]` — gold-umbrella consolidation policy (joint design
    /// `dc-umbrella-signals-1907`). Absent, or present with fields unset,
    /// defaults to [`UmbrellaConfig::default`] — the value guard runs in
    /// SHADOW (records/logs, never enforces) unless an operator explicitly
    /// opts into enforcement, same "off unless opted in" contract as `[slm]`.
    #[serde(default)]
    pub umbrella: UmbrellaConfig,
    /// `[samples]` — redacted troubleshooting sample capture (joint design
    /// `dc-samples-dlp-1907`). Absent, or `enabled` unset, defaults OFF —
    /// deblob's §9 "never store payloads" invariant holds unless an operator
    /// explicitly opts in per trusted source.
    #[serde(default)]
    pub samples: SamplesConfig,
    /// `[auto_promote]` — automatic candidate promotion (opt-in). Absent, or
    /// `enabled` unset, defaults OFF: promotion stays human-driven exactly as
    /// before. When enabled, a periodic sweep publishes any provisional
    /// candidate that clears the deterministic
    /// [`crate::policy::AutoPromotePolicy`] bar (samples, age, and a settled
    /// required-field backbone) to a NEW family with no human in the loop.
    #[serde(default)]
    pub auto_promote: AutoPromoteConfig,
    /// `[settle]` — settle-and-sample (jr-deblob-stability-231518). Absent, or
    /// `enabled` unset, defaults OFF: every record classifies exactly as before.
    /// When enabled per allow-listed HOMOGENEOUS source, a source that settles on
    /// a stable schema fast-paths its records past the expensive classify,
    /// sampling 1-in-`sample_rate` for drift.
    #[serde(default)]
    pub settle: SettleConfig,
}

/// `[settle]` — settle-and-sample policy (jr-deblob-stability-231518). Maps to
/// [`deblob_kafka::SettleCfg`] via [`SettleConfig::to_settle_cfg`].
#[derive(Debug, Clone, serde::Deserialize)]
pub struct SettleConfig {
    /// Master switch — OFF by default (every record classifies).
    #[serde(default)]
    pub enabled: bool,
    /// Consecutive same-`Known`-schema hits before a source settles.
    #[serde(default = "default_settle_after")]
    pub settle_after: u32,
    /// While settled, classify 1-in-this for drift; the rest fast-path.
    #[serde(default = "default_settle_sample_rate")]
    pub sample_rate: u32,
    /// Source topics allowed to settle (opt-in; only homogeneous sources should
    /// be listed — a heterogeneous source never settles anyway).
    #[serde(default)]
    pub sources: Vec<String>,
}

impl Default for SettleConfig {
    fn default() -> Self {
        SettleConfig {
            enabled: false,
            settle_after: default_settle_after(),
            sample_rate: default_settle_sample_rate(),
            sources: Vec::new(),
        }
    }
}

impl SettleConfig {
    pub fn to_settle_cfg(&self) -> deblob_kafka::SettleCfg {
        deblob_kafka::SettleCfg {
            enabled: self.enabled,
            settle_after: self.settle_after,
            sample_rate: self.sample_rate,
            sources: self.sources.clone(),
        }
    }
}

fn default_settle_after() -> u32 {
    1000
}
fn default_settle_sample_rate() -> u32 {
    1000
}

/// `[auto_promote]` automatic-promotion policy. Every field defaults so an
/// absent section is fully OFF (promotion stays manual).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AutoPromoteConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_auto_min_samples")]
    pub min_samples: u64,
    #[serde(default = "default_auto_min_age_ms")]
    pub min_age_ms: i64,
    #[serde(default = "default_auto_min_required_fields")]
    pub min_required_fields: usize,
    #[serde(default = "default_auto_min_required_ratio")]
    pub min_required_ratio: f64,
    /// How often (ms) the auto-promote sweep re-scans provisional candidates.
    #[serde(default = "default_auto_sweep_interval_ms")]
    pub sweep_interval_ms: u64,
    /// Relay-bound source identities (topic names) whose candidates MAY be
    /// auto-promoted. DEFAULT-DENY: an empty list promotes nothing even when
    /// `enabled` — an operator must explicitly name the sources trusted enough
    /// for unattended publication, so a lower-trust or attacker-reachable
    /// producer cannot grind a crafted shape into a governed schema. Matched
    /// against `CandidateRecord.source` (the relay-bound topic, never a
    /// producer-supplied header).
    #[serde(default)]
    pub allowed_sources: Vec<String>,
    /// Hard cap on how many candidates a single sweep tick may publish. Bounds
    /// the blast radius of first-enable against a large provisional backlog (a
    /// misconfiguration or post-outage pileup) — the remainder is promoted on
    /// later ticks, not all at once.
    #[serde(default = "default_auto_max_per_tick")]
    pub max_promotions_per_tick: usize,
}

fn default_auto_min_samples() -> u64 {
    crate::policy::DEFAULT_AUTO_MIN_SAMPLES
}
fn default_auto_min_age_ms() -> i64 {
    crate::policy::DEFAULT_AUTO_MIN_AGE_MS
}
fn default_auto_min_required_fields() -> usize {
    crate::policy::DEFAULT_AUTO_MIN_REQUIRED_FIELDS
}
fn default_auto_min_required_ratio() -> f64 {
    crate::policy::DEFAULT_AUTO_MIN_REQUIRED_RATIO
}
fn default_auto_sweep_interval_ms() -> u64 {
    30_000
}
fn default_auto_max_per_tick() -> usize {
    20
}

impl Default for AutoPromoteConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            min_samples: default_auto_min_samples(),
            min_age_ms: default_auto_min_age_ms(),
            min_required_fields: default_auto_min_required_fields(),
            min_required_ratio: default_auto_min_required_ratio(),
            sweep_interval_ms: default_auto_sweep_interval_ms(),
            allowed_sources: Vec::new(),
            max_promotions_per_tick: default_auto_max_per_tick(),
        }
    }
}

impl AutoPromoteConfig {
    /// Expands the thresholds into the [`crate::policy::AutoPromotePolicy`] the
    /// sweep enforces (the source allowlist / per-tick cap are applied by the
    /// sweep itself, not the policy).
    pub fn to_policy(&self) -> crate::policy::AutoPromotePolicy {
        crate::policy::AutoPromotePolicy {
            min_samples: self.min_samples,
            min_age_ms: self.min_age_ms,
            min_required_fields: self.min_required_fields,
            min_required_ratio: self.min_required_ratio,
        }
    }

    /// Fail-closed startup validation, run only when `enabled`. Rejects
    /// nonsensical values (a `0` interval would panic `tokio::time::interval`;
    /// an out-of-range ratio would silently disable the shape guard) and
    /// refuses thresholds LOOSER than the manual [`PromotionConfig`] guards the
    /// shared promoter re-applies (else auto-eligible candidates would fail the
    /// manual check inside `promote` and warn-loop forever). `Err` is a
    /// human-readable message the caller maps to a startup error.
    pub fn validate(&self, promotion: &PromotionConfig) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }
        if self.sweep_interval_ms == 0 {
            return Err("[auto_promote].sweep_interval_ms must be > 0".to_string());
        }
        if !self.min_required_ratio.is_finite() || !(0.0..=1.0).contains(&self.min_required_ratio) {
            return Err(format!(
                "[auto_promote].min_required_ratio must be in 0.0..=1.0, got {}",
                self.min_required_ratio
            ));
        }
        if self.min_required_fields == 0 {
            return Err("[auto_promote].min_required_fields must be >= 1".to_string());
        }
        if self.max_promotions_per_tick == 0 {
            return Err("[auto_promote].max_promotions_per_tick must be >= 1".to_string());
        }
        if self.min_samples < promotion.min_samples {
            return Err(format!(
                "[auto_promote].min_samples ({}) must be >= [promotion].min_samples ({}) — \
                 auto thresholds cannot be looser than the manual guards the promoter re-applies",
                self.min_samples, promotion.min_samples
            ));
        }
        if self.min_age_ms < promotion.min_age_ms {
            return Err(format!(
                "[auto_promote].min_age_ms ({}) must be >= [promotion].min_age_ms ({})",
                self.min_age_ms, promotion.min_age_ms
            ));
        }
        if self.allowed_sources.is_empty() {
            return Err(
                "[auto_promote].enabled is true but [auto_promote].allowed_sources is empty — \
                 name at least one trusted source, or disable auto-promotion (default-deny)"
                    .to_string(),
            );
        }
        Ok(())
    }
}

/// `[samples]` capture policy. Every field defaults so an absent section is
/// fully OFF.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SamplesConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Trusted source strings authorized for capture (the relay-bound source
    /// identity — a topic name / configured origin, NEVER a producer header).
    #[serde(default)]
    pub capture_sources: Vec<String>,
    #[serde(default = "default_max_per_candidate")]
    pub max_per_candidate: usize,
    #[serde(default = "default_retention_secs")]
    pub retention_secs: u64,
    #[serde(default = "default_key_ttl_secs")]
    pub key_ttl_secs: u64,
    #[serde(default = "default_max_input_bytes")]
    pub max_input_bytes: usize,
    #[serde(default = "default_max_sample_bytes")]
    pub max_sample_bytes: usize,
}

impl Default for SamplesConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            capture_sources: Vec::new(),
            max_per_candidate: default_max_per_candidate(),
            retention_secs: default_retention_secs(),
            key_ttl_secs: default_key_ttl_secs(),
            max_input_bytes: default_max_input_bytes(),
            max_sample_bytes: default_max_sample_bytes(),
        }
    }
}

fn default_max_per_candidate() -> usize {
    20
}
fn default_retention_secs() -> u64 {
    7 * 24 * 60 * 60 // 7 days
}
fn default_key_ttl_secs() -> u64 {
    8 * 24 * 60 * 60 // 8 days safety-net (> retention)
}
fn default_max_input_bytes() -> usize {
    65536
}
fn default_max_sample_bytes() -> usize {
    8192
}

/// `[umbrella]` consolidation policy.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UmbrellaConfig {
    /// When `true`, a proposed umbrella that has ANY field the value guard
    /// judges `CONTRADICTORY` (disjoint bucket masks among comparable units,
    /// above minimum support) is SUPPRESSED from auto-proposal and logged for
    /// human review instead of being persisted as provisional. When `false`
    /// (the default), the guard runs in shadow: the verdict is computed and
    /// logged, but every proposal is persisted exactly as before. Enforcement
    /// should only be enabled after shadow observation on real cohorts.
    #[serde(default)]
    pub enforce_value_guard: bool,
    /// Minimum numeric observations a leaf must carry before its bucket mask
    /// may participate in a `CONTRADICTORY` verdict (guards early-sample bias).
    /// Defaults to [`default_min_value_support`] (30). Low-volume deployments
    /// (few observations per poll) may lower it so the guard is not
    /// permanently `UNKNOWN`; below it, a mismatch never blocks.
    #[serde(default = "default_min_value_support")]
    pub min_value_support: u64,
}

/// Default `[umbrella].min_value_support` (Hermes review: guard early-sample
/// bias). 30 observations.
fn default_min_value_support() -> u64 {
    30
}

impl Default for UmbrellaConfig {
    fn default() -> Self {
        Self {
            enforce_value_guard: false,
            min_value_support: default_min_value_support(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaConfig {
    pub raw_topic: String,
    /// Every topic the relay consumes from, in addition to `raw_topic`
    /// (Hermes review gap 1: multi-topic subscribe). `#[serde(default)]` so
    /// every pre-existing TOML file (which never had this key) still
    /// parses, defaulting to an empty list — [`KafkaConfig::
    /// effective_raw_topics`] is what actually falls back to `[raw_topic]`
    /// alone in that case (a plain `#[serde(default)]` on this field can't
    /// itself reach across to a sibling field during deserialization).
    #[serde(default)]
    pub raw_topics: Vec<String>,
    pub tagged_topic: String,
    pub discovery_topic: String,
    pub quarantine_topic: String,
    pub group_id: String,
    pub transactional_id: String,
    /// Relay transaction batching (`docs/superpowers/specs/2026-07-16-relay-batching.md`
    /// §3): flush and commit ONE Kafka transaction once the batch reaches
    /// this many records. Defaults to
    /// [`default_max_batch_records`] (500) — batching is ON by default,
    /// the whole point of the change. `1` reproduces the exact
    /// pre-batching per-record-transaction behaviour, a documented escape
    /// hatch. Absent from a TOML file entirely still parses (the serde
    /// default), so every pre-batching config file keeps working.
    #[serde(default = "default_max_batch_records")]
    pub max_batch_records: usize,
    /// Relay transaction batching (spec §3): flush the accumulated batch
    /// once this many milliseconds have elapsed since its first record,
    /// even if `max_batch_records` hasn't been reached — bounds the added
    /// latency of a partially-full batch. Defaults to
    /// [`default_max_batch_linger_ms`] (100ms).
    #[serde(default = "default_max_batch_linger_ms")]
    pub max_batch_linger_ms: u64,
    /// Flush the relay batch once its buffered bytes reach this, bounding the
    /// batch's resident memory independent of per-record size
    /// (jr-deblob-stability-231518). Defaults to 32 MiB.
    #[serde(default = "default_max_batch_bytes")]
    pub max_batch_bytes: usize,
    /// Hard ceiling (bytes) on a single produced Kafka message; mirrored onto
    /// the relay producer's `message.max.bytes` and enforced BEFORE produce so
    /// one oversized record is quarantined (payload-free `size_exceeded`
    /// marker) rather than aborting its whole batch. Defaults to the
    /// Redpanda/librdkafka 1 MiB default; raise only in lockstep with the
    /// broker's `max.message.bytes`.
    #[serde(default = "default_max_message_bytes")]
    pub max_message_bytes: usize,
}

impl KafkaConfig {
    /// The full topic list [`crate::serve::serve`] threads into
    /// `deblob_kafka::RelayCfg::raw_topics`: `raw_topics` verbatim when
    /// non-empty, else `[raw_topic]` alone (Hermes review gap 1) — the same
    /// fallback [`deblob_kafka::relay::Relay::run`] itself applies, kept
    /// here too so `serve()`'s wiring is unit-testable without spinning up
    /// Kafka.
    pub fn effective_raw_topics(&self) -> Vec<String> {
        if self.raw_topics.is_empty() {
            vec![self.raw_topic.clone()]
        } else {
            self.raw_topics.clone()
        }
    }
}

/// Batching spec §3: "max_batch_records: usize (default 500)".
fn default_max_batch_records() -> usize {
    500
}

/// Batching spec §3: "max_batch_linger_ms: u64 (default 100)".
fn default_max_batch_linger_ms() -> u64 {
    100
}

/// The Redpanda/librdkafka 1 MiB default single-message ceiling —
/// [`deblob_kafka::DEFAULT_MAX_MESSAGE_BYTES`].
fn default_max_message_bytes() -> usize {
    1024 * 1024
}

/// Default relay batch byte-budget (32 MiB, jr-deblob-stability-231518).
fn default_max_batch_bytes() -> usize {
    32 * 1024 * 1024
}

/// Bounds enforced by the bounded parser (spec §4). Mirrors the subset of
/// `deblob_fingerprint::Limits` the TOML config exposes as operator-tunable
/// knobs — [`LimitsConfig::to_limits`] fills in the rest
/// (`max_key_len`/`max_string_len`/`max_array_inspect`) from
/// `Limits::default()`.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LimitsConfig {
    pub max_bytes: usize,
    pub max_depth: u32,
    pub max_fields_per_object: usize,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        let d = deblob_fingerprint::Limits::default();
        Self {
            max_bytes: d.max_bytes,
            max_depth: d.max_depth,
            max_fields_per_object: d.max_fields_per_object,
        }
    }
}

impl LimitsConfig {
    /// Expands into a full [`deblob_fingerprint::Limits`], borrowing the
    /// ceilings this config doesn't expose from `Limits::default()`.
    pub fn to_limits(self) -> deblob_fingerprint::Limits {
        deblob_fingerprint::Limits {
            max_bytes: self.max_bytes,
            max_depth: self.max_depth,
            max_fields_per_object: self.max_fields_per_object,
            ..deblob_fingerprint::Limits::default()
        }
    }
}

/// Promotion guard thresholds (spec §5/§6). Mirrors
/// `crate::policy::PromotionPolicy`.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromotionConfig {
    pub min_samples: u64,
    pub min_age_ms: i64,
}

impl Default for PromotionConfig {
    fn default() -> Self {
        let d = crate::policy::PromotionPolicy::default();
        Self {
            min_samples: d.min_samples,
            min_age_ms: d.min_age_ms,
        }
    }
}

impl PromotionConfig {
    pub fn to_policy(self) -> crate::policy::PromotionPolicy {
        crate::policy::PromotionPolicy {
            min_samples: self.min_samples,
            min_age_ms: self.min_age_ms,
        }
    }
}

/// The management API's listen address (spec §8) — a SEPARATE port from
/// the Kafka ingest path, never reachable from the producer network path.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagementConfig {
    pub addr: String,
}

impl Default for ManagementConfig {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:9615".to_string(),
        }
    }
}

/// `[slm]` — SLM shadow-lane configuration (P2-A/B Task 5b, deferred
/// follow-up to Task 5): the `ShadowClassifier` (`crate::shadow`) was
/// built + unit-tested in Task 5, but nothing in the running binary drove
/// it until this task's periodic sweep (`crate::shadow::run_shadow_sweep`,
/// wired into `crate::serve::serve`). `enabled` DEFAULTS TO `false` —
/// unless a TOML file explicitly sets `enabled = true`, `serve()`
/// constructs no `HttpInferencer`, no `RedisShadowLog`, and spawns no
/// sweep task, so every P1/pre-Task-5b behavior and test is unaffected.
///
/// The SLM API token is deliberately NOT a field here — it is env-only
/// (`DEBLOB_SLM_API_TOKEN`, see [`Secrets::slm_api_token`] /
/// [`validate_secrets`]), same secrets-never-in-TOML rule as
/// `DEBLOB_API_TOKEN`/`DEBLOB_REDIS_URL`/`DEBLOB_KAFKA_*`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlmConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Base URL of an OpenAI-compatible endpoint, e.g.
    /// `http://localhost:8000/v1` — passed straight through to
    /// `deblob_slm::SlmHttpConfig::base_url`. Only read when `enabled`.
    #[serde(default)]
    pub base_url: String,
    #[serde(default)]
    pub model: String,
    #[serde(default = "default_slm_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_slm_max_concurrency")]
    pub max_concurrency: usize,
    /// How often (milliseconds) `crate::shadow::run_shadow_sweep` re-scans
    /// provisional candidates.
    #[serde(default = "default_slm_sweep_interval_ms")]
    pub sweep_interval_ms: u64,
    /// Minimum `sample_count` a candidate needs before the periodic sweep
    /// offers it to `ShadowClassifier::maybe_classify` — mirrors
    /// `PromotionPolicy::min_samples` in shape, but is configured as an
    /// INDEPENDENT threshold (`ShadowClassifier`'s own docs: shadow
    /// eligibility is allowed to diverge from promotion eligibility,
    /// typically lower/earlier, to build labeled precision samples
    /// sooner).
    #[serde(default = "default_slm_min_samples")]
    pub min_samples: u64,
    /// Minimum observed age (`last_seen_ms - first_seen_ms`, milliseconds)
    /// — mirrors `PromotionPolicy::min_age_ms`.
    #[serde(default = "default_slm_min_window_ms")]
    pub min_window_ms: u64,
}

fn default_slm_timeout_ms() -> u64 {
    8_000
}

fn default_slm_max_concurrency() -> usize {
    2
}

fn default_slm_sweep_interval_ms() -> u64 {
    30_000
}

/// Deliberately lower than `PromotionPolicy::DEFAULT_MIN_SAMPLES` (10) —
/// the shadow lane exists to build labeled precision data BEFORE a
/// candidate is promotion-eligible, so its default stability bar is
/// looser.
fn default_slm_min_samples() -> u64 {
    5
}

/// Deliberately lower than `PromotionPolicy::DEFAULT_MIN_AGE_MS` (5
/// minutes) — same rationale as [`default_slm_min_samples`].
fn default_slm_min_window_ms() -> u64 {
    60_000
}

impl Default for SlmConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            base_url: String::new(),
            model: String::new(),
            timeout_ms: default_slm_timeout_ms(),
            max_concurrency: default_slm_max_concurrency(),
            sweep_interval_ms: default_slm_sweep_interval_ms(),
            min_samples: default_slm_min_samples(),
            min_window_ms: default_slm_min_window_ms(),
        }
    }
}

/// `[http_proxy]` — HTTP push reverse-proxy configuration (P2-C Task 4):
/// the `HttpProxy` ingest listener (`deblob-http::proxy::HttpProxy::run`)
/// was built + hardened in Tasks 1-3, but nothing in the running binary
/// drove it until this task's wiring (`crate::serve::serve`). `enabled`
/// DEFAULTS TO `false` — unless a TOML file explicitly sets
/// `enabled = true`, `serve()` constructs no `HttpProxyCfg`, no
/// `KafkaDiscoverySink`, and spawns no proxy listener, so every
/// pre-Task-4 behavior and test is unaffected.
///
/// The HTTP ingest auth token is deliberately NOT a field here — it is
/// env-only (`DEBLOB_HTTP_INGEST_TOKEN`, see
/// [`Secrets::http_ingest_token`] / [`validate_secrets`]), same
/// secrets-never-in-TOML rule as `DEBLOB_API_TOKEN`/`DEBLOB_SLM_API_TOKEN`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpProxyConfig {
    #[serde(default)]
    pub enabled: bool,
    /// The ingest listener address — SEPARATE from the management API
    /// port (spec §8) and from the Kafka relay's own listen concerns.
    #[serde(default = "default_http_listen_addr")]
    pub listen_addr: String,
    /// The fixed upstream allowlist (SSRF prevention, spec §4). `route`
    /// MUST be a member of this list — `serve()`'s wiring validates that
    /// before spawning the listener, and `HttpProxy::run` re-validates it
    /// at construction as defense-in-depth.
    #[serde(default)]
    pub upstream_allowlist: Vec<String>,
    /// The single upstream every request is forwarded to (Task 1). A
    /// later task may promote this to a real path -> upstream route map.
    #[serde(default)]
    pub route: String,
    #[serde(default = "default_http_max_body_bytes")]
    pub max_body_bytes: usize,
    #[serde(default = "default_http_max_header_bytes")]
    pub max_header_bytes: usize,
    #[serde(default = "default_http_max_header_count")]
    pub max_header_count: usize,
    #[serde(default = "default_http_request_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "default_http_header_read_timeout_ms")]
    pub header_read_timeout_ms: u64,
    #[serde(default = "default_http_upstream_timeout_ms")]
    pub upstream_timeout_ms: u64,
    /// See `HttpProxyCfg::discovery_enqueue_timeout` (Task 4 Part 2) in
    /// `deblob-http`.
    #[serde(default = "default_http_discovery_enqueue_timeout_ms")]
    pub discovery_enqueue_timeout_ms: u64,
    /// Whether `DEBLOB_HTTP_INGEST_TOKEN` is REQUIRED at startup — `false`
    /// (the default) never requires it, matching "the HTTP proxy is off
    /// unless explicitly configured" (see [`validate_secrets`]).
    #[serde(default)]
    pub require_auth: bool,
}

fn default_http_listen_addr() -> String {
    "127.0.0.1:9600".to_string()
}

fn default_http_max_body_bytes() -> usize {
    1_048_576
}

fn default_http_max_header_bytes() -> usize {
    65_536
}

fn default_http_max_header_count() -> usize {
    200
}

fn default_http_request_timeout_ms() -> u64 {
    10_000
}

fn default_http_header_read_timeout_ms() -> u64 {
    10_000
}

fn default_http_upstream_timeout_ms() -> u64 {
    10_000
}

fn default_http_discovery_enqueue_timeout_ms() -> u64 {
    500
}

impl Default for HttpProxyConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            listen_addr: default_http_listen_addr(),
            upstream_allowlist: Vec::new(),
            route: String::new(),
            max_body_bytes: default_http_max_body_bytes(),
            max_header_bytes: default_http_max_header_bytes(),
            max_header_count: default_http_max_header_count(),
            request_timeout_ms: default_http_request_timeout_ms(),
            header_read_timeout_ms: default_http_header_read_timeout_ms(),
            upstream_timeout_ms: default_http_upstream_timeout_ms(),
            discovery_enqueue_timeout_ms: default_http_discovery_enqueue_timeout_ms(),
            require_auth: false,
        }
    }
}

/// `[semantic]` — governance-registered `canonical_field_id`/
/// `canonical_event_type_id` vocabularies (P2-D Task 8 follow-up A1,
/// `docs/superpowers/plans/deblob-p2d-hermes-review.md` §2/§7): the two
/// lists an operator maintains so `PUT /api/v1/schemas/{id}/semantic` can
/// validate the "strong axes" (`canonical_field_id`/`canonical_event_type_id`
/// — governance-registered, unlike the baked UCUM/ISO4217/namespace/
/// meaning-vocabulary tables `deblob_semantic::vocab` ships with) against
/// something other than an always-empty set. Task 6 wired the API surface
/// but left `ApiState.semantic_registries` permanently
/// `Registries::default()` (empty) with no registration endpoint — this
/// section, plus [`SemanticConfig::to_registries`] and `serve()`'s use of
/// it, is what actually seeds it. NOT secrets: these are plain, versioned,
/// reviewable governance identifiers (no credential, no connection string),
/// same posture as `[promotion]`'s numeric thresholds — env-only secrets
/// stay exclusively in [`Secrets`].
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticConfig {
    /// Registered `canonical_field_id` values (e.g. `"cfid_temperature_ambient"`).
    /// An annotation naming any OTHER field id still `422`s.
    #[serde(default)]
    pub canonical_field_ids: Vec<String>,
    /// Registered `canonical_event_type_id` values (e.g. `"order.created"`).
    /// An annotation naming any OTHER event type still `422`s.
    #[serde(default)]
    pub event_types: Vec<String>,
    /// `[semantic].domain_gate_enforce` — when `true`, the source-domain
    /// coherence gate (`jr-deblob-domain-gate-221052`) drops a semantic-neighbor
    /// candidate whose ingest domain is proven-disjoint from the query's (e.g. a
    /// compute/GPU schema vs an energy/carbon schema). When `false` (default) it
    /// runs in SHADOW: every candidate is annotated with its domain + the gate
    /// decision and a would-veto count is logged, but nothing is dropped — so the
    /// veto set can be validated against the diagnostic response before enforcing.
    #[serde(default)]
    pub domain_gate_enforce: bool,
}

impl SemanticConfig {
    /// Builds the injectable [`deblob_semantic::Registries`]
    /// `serve()` threads into `ApiState.semantic_registries` — a pure,
    /// no-I/O transform (unit-testable without Redis/HTTP), mirroring
    /// [`LimitsConfig::to_limits`]/[`PromotionConfig::to_policy`]'s own
    /// "`Config` field -> domain type" pattern. Duplicate entries collapse
    /// (the registries are sets); order is never significant.
    pub fn to_registries(&self) -> deblob_semantic::Registries {
        let mut registries = deblob_semantic::Registries::default();
        for id in &self.canonical_field_ids {
            registries.field_ids.register(id.clone());
        }
        for id in &self.event_types {
            registries.event_type_ids.register(id.clone());
        }
        registries
    }
}

/// Errors loading/parsing the TOML file or validating startup secrets.
/// Never carries a secret VALUE — [`ConfigError::MissingEnvVar`] names only
/// the variable, and [`std::fmt::Display`]/[`std::fmt::Debug`] on every
/// variant is safe to log verbatim.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse config TOML: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("missing required environment variable {0}")]
    MissingEnvVar(&'static str),
}

impl Config {
    /// Parses `Config` straight out of a TOML string (no file I/O) — the
    /// primitive [`Config::load`] and unit tests both build on.
    pub fn parse_toml(s: &str) -> Result<Self, ConfigError> {
        toml::from_str(s).map_err(ConfigError::Parse)
    }

    /// Reads and parses the TOML config file at `path`.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let contents = std::fs::read_to_string(path).map_err(|e| ConfigError::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
        Self::parse_toml(&contents)
    }
}

/// Applies a small, explicit env overlay to non-secret [`Config`] fields —
/// currently just `management.addr`, the one operational knob ops most
/// commonly want to override per-environment without editing the TOML
/// file. `env` is injected as a closure (rather than this function calling
/// `std::env::var` itself) so it's unit-testable without mutating real
/// process environment — see [`process_env`] for the real-process adapter
/// callers pass in production.
pub fn apply_env_overlay(mut config: Config, env: &impl Fn(&str) -> Option<String>) -> Config {
    if let Some(addr) = env(ENV_MANAGEMENT_ADDR) {
        config.management.addr = addr;
    }
    config
}

pub const ENV_MANAGEMENT_ADDR: &str = "DEBLOB_MANAGEMENT_ADDR";
pub const ENV_API_TOKEN: &str = "DEBLOB_API_TOKEN";
pub const ENV_REDIS_URL: &str = "DEBLOB_REDIS_URL";
pub const ENV_KAFKA_BROKERS: &str = "DEBLOB_KAFKA_BROKERS";
pub const ENV_KAFKA_SASL_USERNAME: &str = "DEBLOB_KAFKA_SASL_USERNAME";
pub const ENV_KAFKA_SASL_PASSWORD: &str = "DEBLOB_KAFKA_SASL_PASSWORD";
pub const ENV_KAFKA_SASL_MECHANISM: &str = "DEBLOB_KAFKA_SASL_MECHANISM";
pub const ENV_KAFKA_SECURITY_PROTOCOL: &str = "DEBLOB_KAFKA_SECURITY_PROTOCOL";
/// The SLM shadow lane's API token — env-only, required IFF `[slm].enabled`
/// is `true` (see [`validate_secrets`]); never read at all when disabled.
pub const ENV_SLM_API_TOKEN: &str = "DEBLOB_SLM_API_TOKEN";
/// The HTTP push reverse-proxy's ingest auth token — env-only, required
/// IFF `[http_proxy].require_auth` is `true` (see [`validate_secrets`]);
/// never required when unset/`false`, same "off unless explicitly
/// configured" contract as [`ENV_SLM_API_TOKEN`].
pub const ENV_HTTP_INGEST_TOKEN: &str = "DEBLOB_HTTP_INGEST_TOKEN";
pub const ENV_SAMPLES_REDIS_URL: &str = "DEBLOB_SAMPLES_REDIS_URL";
pub const ENV_SAMPLES_READ_TOKEN: &str = "DEBLOB_SAMPLES_READ_TOKEN";

const DEFAULT_SASL_MECHANISM: &str = "PLAIN";
const DEFAULT_SECURITY_PROTOCOL: &str = "SASL_SSL";

/// The env-only secrets (spec §9): `DEBLOB_API_TOKEN`, `DEBLOB_REDIS_URL`,
/// `DEBLOB_KAFKA_BROKERS`, and optional SASL credentials. Never
/// constructed from the TOML config file — [`validate_secrets`] is the
/// only constructor, and it reads exclusively from environment variables.
pub struct Secrets {
    pub api_token: String,
    pub redis_url: String,
    pub kafka_brokers: String,
    pub kafka_sasl: Option<KafkaSasl>,
    /// `DEBLOB_SLM_API_TOKEN` (P2-A/B Task 5b). `Some` iff the variable was
    /// present in the environment; [`validate_secrets`] additionally
    /// REQUIRES it (errors if absent) when `[slm].enabled` is `true`, but
    /// otherwise leaves it optional — reading it never depends on whether
    /// the SLM lane is enabled, only whether calling `serve()` treats its
    /// absence as fatal.
    pub slm_api_token: Option<String>,
    /// `DEBLOB_HTTP_INGEST_TOKEN` (P2-C Task 4). `Some` iff the variable
    /// was present in the environment; [`validate_secrets`] additionally
    /// REQUIRES it (errors if absent) when `[http_proxy].require_auth` is
    /// `true`, but otherwise leaves it optional — same shape as
    /// `slm_api_token` above.
    pub http_ingest_token: Option<String>,
    /// `DEBLOB_SAMPLES_REDIS_URL` (joint design dc-samples-dlp-1907). The
    /// DEDICATED, VOLATILE Redis for the redacted sample store — MUST be a
    /// separate instance from `redis_url` (the permanent vault), whose
    /// RDB/AOF/backups would outlive the retention TTL. `Some` iff the variable
    /// was present; sample capture stays off without it even if `[samples]
    /// .enabled` is true.
    pub samples_redis_url: Option<String>,
    /// `DEBLOB_SAMPLES_READ_TOKEN` — the separate `samples:read` capability
    /// required to VIEW redacted samples (distinct from the ordinary
    /// `api_token`; DLP is probabilistic so raw-derived content is
    /// need-to-know). `Some` iff present; the read endpoint 404s/403s without
    /// it configured.
    pub samples_read_token: Option<String>,
}

/// Hand-written (not derived): every field here is a secret value, so the
/// `Debug` impl redacts all of them rather than risk a future derive
/// accidentally logging one (spec §9: secrets are never logged).
impl fmt::Debug for Secrets {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Secrets")
            .field("api_token", &"<redacted>")
            .field("redis_url", &"<redacted>")
            .field("kafka_brokers", &"<redacted>")
            .field(
                "kafka_sasl",
                &self.kafka_sasl.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "slm_api_token",
                &self.slm_api_token.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "http_ingest_token",
                &self.http_ingest_token.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "samples_redis_url",
                &self.samples_redis_url.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "samples_read_token",
                &self.samples_read_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// Validates every required secret is present in the environment, per
/// spec §9. `env` is injected as a closure — production passes
/// [`process_env`] (a thin wrapper over `std::env::var`); tests pass a
/// fake lookup so this is fully unit-testable without touching real
/// process env. Returns [`ConfigError::MissingEnvVar`] NAMING the first
/// missing variable on failure; never includes a secret's VALUE anywhere
/// in an error (there's nothing to leak — a missing variable has no
/// value).
///
/// SASL is optional as a whole group: if `DEBLOB_KAFKA_SASL_USERNAME` is
/// unset, `kafka_sasl` is `None` and the relay connects without SASL. If
/// it IS set, `DEBLOB_KAFKA_SASL_PASSWORD` becomes required (mechanism/
/// security-protocol fall back to sane defaults if unset).
///
/// `slm_enabled` (the caller's already-parsed `[slm].enabled`, P2-A/B Task
/// 5b) gates whether `DEBLOB_SLM_API_TOKEN` is REQUIRED: `true` and the
/// variable is absent → [`ConfigError::MissingEnvVar`] naming
/// [`ENV_SLM_API_TOKEN`], same as every other required secret. `false`
/// (the default) never fails on it — the token is read if present
/// (harmless either way) but its absence is not an error, matching "the
/// shadow lane is off unless explicitly configured" (see [`SlmConfig`]).
///
/// `http_ingest_required` (the caller's already-parsed
/// `[http_proxy].require_auth`, P2-C Task 4) gates whether
/// `DEBLOB_HTTP_INGEST_TOKEN` is REQUIRED, identically to how
/// `slm_enabled` gates `DEBLOB_SLM_API_TOKEN` above.
pub fn validate_secrets(
    env: &impl Fn(&str) -> Option<String>,
    slm_enabled: bool,
    http_ingest_required: bool,
) -> Result<Secrets, ConfigError> {
    let api_token = env(ENV_API_TOKEN).ok_or(ConfigError::MissingEnvVar(ENV_API_TOKEN))?;
    let redis_url = env(ENV_REDIS_URL).ok_or(ConfigError::MissingEnvVar(ENV_REDIS_URL))?;
    let kafka_brokers =
        env(ENV_KAFKA_BROKERS).ok_or(ConfigError::MissingEnvVar(ENV_KAFKA_BROKERS))?;

    let kafka_sasl = match env(ENV_KAFKA_SASL_USERNAME) {
        None => None,
        Some(username) => {
            let password = env(ENV_KAFKA_SASL_PASSWORD)
                .ok_or(ConfigError::MissingEnvVar(ENV_KAFKA_SASL_PASSWORD))?;
            let mechanism =
                env(ENV_KAFKA_SASL_MECHANISM).unwrap_or_else(|| DEFAULT_SASL_MECHANISM.to_string());
            let security_protocol = env(ENV_KAFKA_SECURITY_PROTOCOL)
                .unwrap_or_else(|| DEFAULT_SECURITY_PROTOCOL.to_string());
            Some(KafkaSasl {
                mechanism,
                security_protocol,
                username,
                password,
            })
        }
    };

    let slm_api_token = env(ENV_SLM_API_TOKEN);
    if slm_enabled && slm_api_token.is_none() {
        return Err(ConfigError::MissingEnvVar(ENV_SLM_API_TOKEN));
    }

    let http_ingest_token = env(ENV_HTTP_INGEST_TOKEN);
    if http_ingest_required && http_ingest_token.is_none() {
        return Err(ConfigError::MissingEnvVar(ENV_HTTP_INGEST_TOKEN));
    }

    // Sample capture is OPTIONAL: absent env vars simply leave it off (even if
    // `[samples].enabled`), never a startup error — fail-safe toward NOT
    // storing payloads.
    let samples_redis_url = env(ENV_SAMPLES_REDIS_URL);
    let samples_read_token = env(ENV_SAMPLES_READ_TOKEN);

    Ok(Secrets {
        api_token,
        redis_url,
        kafka_brokers,
        kafka_sasl,
        slm_api_token,
        http_ingest_token,
        samples_redis_url,
        samples_read_token,
    })
}

/// The real-process-env adapter for [`apply_env_overlay`]/
/// [`validate_secrets`] — production's only caller of `std::env::var`
/// for these purposes, kept in one place so it's obvious where the real
/// environment is actually read.
pub fn process_env(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

/// Maps the `--unsafe-volatile` CLI flag onto [`RedisOpts`] (spec §6:
/// "refuse non-persistent Redis unless `--unsafe-volatile`"). A pure,
/// one-line function so main's wiring is unit-testable without a real
/// Redis: the default (flag absent) must always be `allow_volatile:
/// false`.
pub fn redis_opts(unsafe_volatile: bool) -> RedisOpts {
    RedisOpts {
        allow_volatile: unsafe_volatile,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    const EXAMPLE_TOML: &str = include_str!("../../../deblob.example.toml");

    fn fake_env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn lookup(map: HashMap<String, String>) -> impl Fn(&str) -> Option<String> {
        move |key| map.get(key).cloned()
    }

    #[test]
    fn config_parses_toml() {
        let config = Config::parse_toml(EXAMPLE_TOML).expect("example TOML must parse");

        assert_eq!(config.kafka.raw_topic, "events.raw");
        assert_eq!(config.kafka.tagged_topic, "events.tagged");
        assert_eq!(config.kafka.discovery_topic, "deblob.discovery");
        assert_eq!(config.kafka.quarantine_topic, "deblob.quarantine");
        assert_eq!(config.kafka.group_id, "deblob");
        assert_eq!(config.kafka.transactional_id, "deblob-relay-1");
        // The example TOML leaves batching commented out — defaults apply
        // (batching spec §3: 500 records / 100ms linger).
        assert_eq!(config.kafka.max_batch_records, 500);
        assert_eq!(config.kafka.max_batch_linger_ms, 100);

        assert_eq!(config.limits.max_bytes, 1_048_576);
        assert_eq!(config.limits.max_depth, 32);
        assert_eq!(config.limits.max_fields_per_object, 1024);

        assert_eq!(config.promotion.min_samples, 10);
        assert_eq!(config.promotion.min_age_ms, 300_000);

        assert_eq!(config.management.addr, "127.0.0.1:9615");

        assert!(!config.slm.enabled);
        assert_eq!(config.slm.base_url, "http://localhost:8000/v1");
        assert_eq!(config.slm.model, "granite-4.0-nano-1b");
        assert_eq!(config.slm.timeout_ms, 8000);
        assert_eq!(config.slm.max_concurrency, 2);
        assert_eq!(config.slm.sweep_interval_ms, 30000);
        assert_eq!(config.slm.min_samples, 5);
        assert_eq!(config.slm.min_window_ms, 60000);

        assert!(config.semantic.canonical_field_ids.is_empty());
        assert!(config.semantic.event_types.is_empty());
    }

    #[test]
    fn config_parses_from_a_real_file_via_load() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deblob.example.toml");
        let config = Config::load(&path).expect("Config::load must read + parse the example file");
        assert_eq!(config.kafka.group_id, "deblob");
    }

    #[test]
    fn missing_config_sections_fall_back_to_documented_defaults() {
        let minimal = r#"
            [kafka]
            raw_topic = "r"
            tagged_topic = "t"
            discovery_topic = "d"
            quarantine_topic = "q"
            group_id = "g"
            transactional_id = "x"
        "#;
        let config = Config::parse_toml(minimal).expect("minimal config must parse");
        assert_eq!(config.management.addr, "127.0.0.1:9615");
        assert_eq!(config.promotion.min_samples, 10);
        assert_eq!(config.limits.max_bytes, 1_048_576);
        // [kafka] present but WITHOUT the batching keys — serde defaults
        // fill them in (batching spec §3: existing configs must still
        // parse).
        assert_eq!(config.kafka.max_batch_records, 500);
        assert_eq!(config.kafka.max_batch_linger_ms, 100);

        // No `[slm]` section at all — the shadow lane must default to
        // disabled, same as every other pre-Task-5b config.
        assert!(!config.slm.enabled);
        assert_eq!(config.slm.timeout_ms, 8000);
        assert_eq!(config.slm.max_concurrency, 2);
        assert_eq!(config.slm.sweep_interval_ms, 30000);
        assert_eq!(config.slm.min_samples, 5);
        assert_eq!(config.slm.min_window_ms, 60000);
    }

    #[test]
    fn kafka_section_parses_explicit_batching_values() {
        let toml = r#"
            [kafka]
            raw_topic = "r"
            tagged_topic = "t"
            discovery_topic = "d"
            quarantine_topic = "q"
            group_id = "g"
            transactional_id = "x"
            max_batch_records = 1000
            max_batch_linger_ms = 250
        "#;
        let config = Config::parse_toml(toml).expect("explicit batching keys must parse");
        assert_eq!(config.kafka.max_batch_records, 1000);
        assert_eq!(config.kafka.max_batch_linger_ms, 250);
    }

    /// Batching spec §3: "max_batch_records = 1 reproduces the exact
    /// current per-record behaviour (a documented escape hatch)".
    #[test]
    fn kafka_section_max_batch_records_one_is_a_valid_escape_hatch() {
        let toml = r#"
            [kafka]
            raw_topic = "r"
            tagged_topic = "t"
            discovery_topic = "d"
            quarantine_topic = "q"
            group_id = "g"
            transactional_id = "x"
            max_batch_records = 1
        "#;
        let config = Config::parse_toml(toml).expect("max_batch_records = 1 must parse");
        assert_eq!(config.kafka.max_batch_records, 1);
    }

    /// Hermes review gap 1: `raw_topics` absent from the TOML entirely
    /// deserializes to an empty `Vec` (the `#[serde(default)]`), and
    /// `effective_raw_topics` then falls back to `[raw_topic]` alone — the
    /// exact pre-multi-topic subscribe behavior. When explicitly set,
    /// `effective_raw_topics` returns it verbatim, ignoring `raw_topic`.
    #[test]
    fn raw_topics_defaults_empty_and_falls_back_to_raw_topic() {
        let config = Config::parse_toml(EXAMPLE_TOML).expect("example TOML must parse");
        assert!(config.kafka.raw_topics.is_empty());
        assert_eq!(
            config.kafka.effective_raw_topics(),
            vec![config.kafka.raw_topic.clone()]
        );

        let toml = r#"
            [kafka]
            raw_topic = "r"
            raw_topics = ["r", "r2", "r3"]
            tagged_topic = "t"
            discovery_topic = "d"
            quarantine_topic = "q"
            group_id = "g"
            transactional_id = "x"
        "#;
        let config = Config::parse_toml(toml).expect("explicit raw_topics must parse");
        assert_eq!(
            config.kafka.raw_topics,
            vec!["r".to_string(), "r2".to_string(), "r3".to_string()]
        );
        assert_eq!(
            config.kafka.effective_raw_topics(),
            vec!["r".to_string(), "r2".to_string(), "r3".to_string()]
        );
    }

    #[test]
    fn config_parses_slm_section() {
        let toml = r#"
            [kafka]
            raw_topic = "r"
            tagged_topic = "t"
            discovery_topic = "d"
            quarantine_topic = "q"
            group_id = "g"
            transactional_id = "x"

            [slm]
            enabled = true
            base_url = "http://slm.internal:8000/v1"
            model = "test-model"
            timeout_ms = 1234
            max_concurrency = 7
            sweep_interval_ms = 5000
            min_samples = 3
            min_window_ms = 10000
        "#;
        let config = Config::parse_toml(toml).expect("[slm] section must parse");
        assert!(config.slm.enabled);
        assert_eq!(config.slm.base_url, "http://slm.internal:8000/v1");
        assert_eq!(config.slm.model, "test-model");
        assert_eq!(config.slm.timeout_ms, 1234);
        assert_eq!(config.slm.max_concurrency, 7);
        assert_eq!(config.slm.sweep_interval_ms, 5000);
        assert_eq!(config.slm.min_samples, 3);
        assert_eq!(config.slm.min_window_ms, 10000);
    }

    #[test]
    fn slm_section_partial_fields_fall_back_to_defaults() {
        let toml = r#"
            [kafka]
            raw_topic = "r"
            tagged_topic = "t"
            discovery_topic = "d"
            quarantine_topic = "q"
            group_id = "g"
            transactional_id = "x"

            [slm]
            enabled = true
            base_url = "http://slm.internal:8000/v1"
            model = "test-model"
        "#;
        let config = Config::parse_toml(toml).expect("partial [slm] section must parse");
        assert!(config.slm.enabled);
        // Omitted fields fall back to the documented defaults.
        assert_eq!(config.slm.timeout_ms, 8000);
        assert_eq!(config.slm.max_concurrency, 2);
        assert_eq!(config.slm.sweep_interval_ms, 30000);
        assert_eq!(config.slm.min_samples, 5);
        assert_eq!(config.slm.min_window_ms, 60000);
    }

    #[test]
    fn slm_section_rejects_unknown_field() {
        let toml = r#"
            [kafka]
            raw_topic = "r"
            tagged_topic = "t"
            discovery_topic = "d"
            quarantine_topic = "q"
            group_id = "g"
            transactional_id = "x"

            [slm]
            enabled = true
            base_url = "http://slm.internal:8000/v1"
            model = "test-model"
            api_token = "should-never-be-in-toml"
        "#;
        let err = Config::parse_toml(toml).expect_err("a typo'd/secret field must be rejected");
        assert!(matches!(err, ConfigError::Parse(_)));
    }

    #[test]
    fn auto_promote_defaults_off_and_validates() {
        let toml = r#"
            [kafka]
            raw_topic = "r"
            tagged_topic = "t"
            discovery_topic = "d"
            quarantine_topic = "q"
            group_id = "g"
            transactional_id = "x"
        "#;
        let config = Config::parse_toml(toml).unwrap();
        assert!(
            !config.auto_promote.enabled,
            "absent [auto_promote] must be OFF"
        );
        // A disabled section validates trivially.
        assert!(config.auto_promote.validate(&config.promotion).is_ok());
    }

    #[test]
    fn auto_promote_validate_rejects_thresholds_looser_than_manual() {
        let promotion = PromotionConfig {
            min_samples: 10,
            min_age_ms: 300_000,
        };
        let cfg = AutoPromoteConfig {
            enabled: true,
            min_samples: 5, // looser than manual 10
            allowed_sources: vec!["events.grid".to_string()],
            ..AutoPromoteConfig::default()
        };
        let err = cfg.validate(&promotion).unwrap_err();
        assert!(err.contains("min_samples"), "was: {err}");
    }

    #[test]
    fn auto_promote_validate_requires_allowed_sources_when_enabled() {
        // Default thresholds are >= manual defaults, so the allowlist is what
        // fails: enabling with no trusted source is refused (default-deny).
        let cfg = AutoPromoteConfig {
            enabled: true,
            ..AutoPromoteConfig::default()
        };
        let err = cfg.validate(&PromotionConfig::default()).unwrap_err();
        assert!(err.contains("allowed_sources"), "was: {err}");
    }

    #[test]
    fn auto_promote_validate_rejects_zero_interval_and_bad_ratio() {
        let base = AutoPromoteConfig {
            enabled: true,
            allowed_sources: vec!["events.grid".to_string()],
            ..AutoPromoteConfig::default()
        };
        let zero_interval = AutoPromoteConfig {
            sweep_interval_ms: 0,
            ..base.clone()
        };
        assert!(zero_interval
            .validate(&PromotionConfig::default())
            .unwrap_err()
            .contains("sweep_interval_ms"));
        let bad_ratio = AutoPromoteConfig {
            min_required_ratio: 1.5,
            ..base
        };
        assert!(bad_ratio
            .validate(&PromotionConfig::default())
            .unwrap_err()
            .contains("min_required_ratio"));
    }

    #[test]
    fn auto_promote_validates_when_fully_configured() {
        let cfg = AutoPromoteConfig {
            enabled: true,
            allowed_sources: vec!["events.grid.carbonintensity".to_string()],
            ..AutoPromoteConfig::default()
        };
        assert!(cfg.validate(&PromotionConfig::default()).is_ok());
    }

    #[test]
    fn auto_promote_section_rejects_unknown_field() {
        let toml = r#"
            [kafka]
            raw_topic = "r"
            tagged_topic = "t"
            discovery_topic = "d"
            quarantine_topic = "q"
            group_id = "g"
            transactional_id = "x"

            [auto_promote]
            enabled = true
            bogus_knob = 1
        "#;
        let err =
            Config::parse_toml(toml).expect_err("unknown [auto_promote] key must be rejected");
        assert!(matches!(err, ConfigError::Parse(_)));
    }

    #[test]
    fn env_overlay_applies() {
        // Part 1: the overlay overrides a non-secret Config field.
        let config = Config::parse_toml(EXAMPLE_TOML).unwrap();
        assert_eq!(config.management.addr, "127.0.0.1:9615");

        let env = lookup(fake_env(&[(ENV_MANAGEMENT_ADDR, "0.0.0.0:9999")]));
        let overlaid = apply_env_overlay(config, &env);
        assert_eq!(overlaid.management.addr, "0.0.0.0:9999");

        // Part 2: without the override, the TOML value survives untouched.
        let config2 = Config::parse_toml(EXAMPLE_TOML).unwrap();
        let no_override = lookup(fake_env(&[]));
        let unchanged = apply_env_overlay(config2, &no_override);
        assert_eq!(unchanged.management.addr, "127.0.0.1:9615");

        // Part 3: TOML parse + env-sourced secrets combine into a full
        // runtime configuration — Config from the file, Secrets from env,
        // neither leaking into the other's source.
        let secrets_env = lookup(fake_env(&[
            (ENV_API_TOKEN, "test-token"),
            (ENV_REDIS_URL, "redis://localhost:6379"),
            (ENV_KAFKA_BROKERS, "localhost:9092"),
        ]));
        let secrets =
            validate_secrets(&secrets_env, false, false).expect("all required secrets present");
        assert_eq!(secrets.api_token, "test-token");
        assert_eq!(secrets.redis_url, "redis://localhost:6379");
        assert_eq!(secrets.kafka_brokers, "localhost:9092");
        assert!(secrets.kafka_sasl.is_none());
        // The combined runtime state has both halves available together.
        assert_eq!(unchanged.kafka.group_id, "deblob");
    }

    #[test]
    fn missing_api_token_fails_startup_naming_var() {
        let env = lookup(fake_env(&[
            (ENV_REDIS_URL, "redis://localhost:6379"),
            (ENV_KAFKA_BROKERS, "localhost:9092"),
        ]));

        let err =
            validate_secrets(&env, false, false).expect_err("missing DEBLOB_API_TOKEN must fail");
        let message = err.to_string();
        assert!(
            message.contains(ENV_API_TOKEN),
            "error must name the missing variable: {message}"
        );
    }

    #[test]
    fn missing_redis_url_fails_startup_naming_var() {
        let env = lookup(fake_env(&[
            (ENV_API_TOKEN, "test-token"),
            (ENV_KAFKA_BROKERS, "localhost:9092"),
        ]));

        let err =
            validate_secrets(&env, false, false).expect_err("missing DEBLOB_REDIS_URL must fail");
        assert!(err.to_string().contains(ENV_REDIS_URL));
    }

    #[test]
    fn missing_kafka_brokers_fails_startup_naming_var() {
        let env = lookup(fake_env(&[
            (ENV_API_TOKEN, "test-token"),
            (ENV_REDIS_URL, "redis://localhost:6379"),
        ]));

        let err = validate_secrets(&env, false, false)
            .expect_err("missing DEBLOB_KAFKA_BROKERS must fail");
        assert!(err.to_string().contains(ENV_KAFKA_BROKERS));
    }

    #[test]
    fn sasl_username_without_password_fails_naming_password_var() {
        let env = lookup(fake_env(&[
            (ENV_API_TOKEN, "test-token"),
            (ENV_REDIS_URL, "redis://localhost:6379"),
            (ENV_KAFKA_BROKERS, "localhost:9092"),
            (ENV_KAFKA_SASL_USERNAME, "deblob"),
        ]));

        let err = validate_secrets(&env, false, false)
            .expect_err("SASL username without password must fail");
        assert!(err.to_string().contains(ENV_KAFKA_SASL_PASSWORD));
    }

    #[test]
    fn sasl_credentials_parsed_when_fully_present() {
        let env = lookup(fake_env(&[
            (ENV_API_TOKEN, "test-token"),
            (ENV_REDIS_URL, "redis://localhost:6379"),
            (ENV_KAFKA_BROKERS, "localhost:9092"),
            (ENV_KAFKA_SASL_USERNAME, "deblob"),
            (ENV_KAFKA_SASL_PASSWORD, "s3cr3t"),
        ]));

        let secrets =
            validate_secrets(&env, false, false).expect("full SASL credentials must validate");
        let sasl = secrets.kafka_sasl.expect("sasl must be Some");
        assert_eq!(sasl.username, "deblob");
        assert_eq!(sasl.password, "s3cr3t");
        assert_eq!(sasl.mechanism, DEFAULT_SASL_MECHANISM);
        assert_eq!(sasl.security_protocol, DEFAULT_SECURITY_PROTOCOL);
    }

    #[test]
    fn secrets_debug_never_prints_values() {
        let env = lookup(fake_env(&[
            (ENV_API_TOKEN, "super-secret-token"),
            (ENV_REDIS_URL, "redis://user:pass@localhost:6379"),
            (ENV_KAFKA_BROKERS, "broker.internal:9092"),
            (ENV_KAFKA_SASL_USERNAME, "deblob"),
            (ENV_KAFKA_SASL_PASSWORD, "s3cr3t"),
            (ENV_SLM_API_TOKEN, "slm-super-secret"),
        ]));
        let secrets = validate_secrets(&env, true, false).unwrap();
        let rendered = format!("{secrets:?}");
        assert!(!rendered.contains("super-secret-token"));
        assert!(!rendered.contains("pass@localhost"));
        assert!(!rendered.contains("broker.internal"));
        assert!(!rendered.contains("s3cr3t"));
        assert!(!rendered.contains("slm-super-secret"));
    }

    #[test]
    fn slm_enabled_requires_token() {
        // slm.enabled=true, no DEBLOB_SLM_API_TOKEN → clear error naming
        // the variable.
        let env_missing = lookup(fake_env(&[
            (ENV_API_TOKEN, "test-token"),
            (ENV_REDIS_URL, "redis://localhost:6379"),
            (ENV_KAFKA_BROKERS, "localhost:9092"),
        ]));
        let err = validate_secrets(&env_missing, true, false)
            .expect_err("slm.enabled=true with no DEBLOB_SLM_API_TOKEN must fail");
        assert!(
            err.to_string().contains(ENV_SLM_API_TOKEN),
            "error must name the missing variable: {err}"
        );

        // slm.enabled=true, DEBLOB_SLM_API_TOKEN present → ok, captured.
        let env_present = lookup(fake_env(&[
            (ENV_API_TOKEN, "test-token"),
            (ENV_REDIS_URL, "redis://localhost:6379"),
            (ENV_KAFKA_BROKERS, "localhost:9092"),
            (ENV_SLM_API_TOKEN, "slm-token-value"),
        ]));
        let secrets = validate_secrets(&env_present, true, false)
            .expect("slm.enabled=true with DEBLOB_SLM_API_TOKEN present must succeed");
        assert_eq!(secrets.slm_api_token.as_deref(), Some("slm-token-value"));

        // slm.enabled=false, no DEBLOB_SLM_API_TOKEN → ok, not required.
        let env_disabled = lookup(fake_env(&[
            (ENV_API_TOKEN, "test-token"),
            (ENV_REDIS_URL, "redis://localhost:6379"),
            (ENV_KAFKA_BROKERS, "localhost:9092"),
        ]));
        let secrets = validate_secrets(&env_disabled, false, false)
            .expect("slm.enabled=false must not require DEBLOB_SLM_API_TOKEN");
        assert!(secrets.slm_api_token.is_none());
    }

    #[test]
    fn config_parses_http_proxy_section() {
        // A TOML with `[http_proxy]` parses into `Config` with the right
        // fields.
        let toml = r#"
            [kafka]
            raw_topic = "r"
            tagged_topic = "t"
            discovery_topic = "d"
            quarantine_topic = "q"
            group_id = "g"
            transactional_id = "x"

            [http_proxy]
            enabled = true
            listen_addr = "127.0.0.1:9600"
            upstream_allowlist = ["https://upstream.internal:8443"]
            route = "https://upstream.internal:8443/ingest"
            max_body_bytes = 2097152
            max_header_bytes = 32768
            max_header_count = 100
            request_timeout_ms = 5000
            header_read_timeout_ms = 6000
            upstream_timeout_ms = 7000
            discovery_enqueue_timeout_ms = 250
            require_auth = true
        "#;
        let config = Config::parse_toml(toml).expect("[http_proxy] section must parse");
        assert!(config.http_proxy.enabled);
        assert_eq!(config.http_proxy.listen_addr, "127.0.0.1:9600");
        assert_eq!(
            config.http_proxy.upstream_allowlist,
            vec!["https://upstream.internal:8443".to_string()]
        );
        assert_eq!(
            config.http_proxy.route,
            "https://upstream.internal:8443/ingest"
        );
        assert_eq!(config.http_proxy.max_body_bytes, 2_097_152);
        assert_eq!(config.http_proxy.max_header_bytes, 32_768);
        assert_eq!(config.http_proxy.max_header_count, 100);
        assert_eq!(config.http_proxy.request_timeout_ms, 5000);
        assert_eq!(config.http_proxy.header_read_timeout_ms, 6000);
        assert_eq!(config.http_proxy.upstream_timeout_ms, 7000);
        assert_eq!(config.http_proxy.discovery_enqueue_timeout_ms, 250);
        assert!(config.http_proxy.require_auth);

        // A TOML WITHOUT `[http_proxy]` at all still parses — defaults to
        // disabled, same "off unless explicitly configured" contract as
        // `[slm]`.
        let minimal = r#"
            [kafka]
            raw_topic = "r"
            tagged_topic = "t"
            discovery_topic = "d"
            quarantine_topic = "q"
            group_id = "g"
            transactional_id = "x"
        "#;
        let config = Config::parse_toml(minimal).expect("config without [http_proxy] must parse");
        assert!(!config.http_proxy.enabled);
        assert_eq!(config.http_proxy.listen_addr, "127.0.0.1:9600");
        assert!(config.http_proxy.upstream_allowlist.is_empty());
        assert_eq!(config.http_proxy.route, "");
        assert_eq!(config.http_proxy.max_body_bytes, 1_048_576);
        assert_eq!(config.http_proxy.max_header_bytes, 65_536);
        assert_eq!(config.http_proxy.max_header_count, 200);
        assert_eq!(config.http_proxy.request_timeout_ms, 10_000);
        assert_eq!(config.http_proxy.header_read_timeout_ms, 10_000);
        assert_eq!(config.http_proxy.upstream_timeout_ms, 10_000);
        assert_eq!(config.http_proxy.discovery_enqueue_timeout_ms, 500);
        assert!(!config.http_proxy.require_auth);

        // `deny_unknown_fields` rejects a typo'd/unexpected key.
        let typo = r#"
            [kafka]
            raw_topic = "r"
            tagged_topic = "t"
            discovery_topic = "d"
            quarantine_topic = "q"
            group_id = "g"
            transactional_id = "x"

            [http_proxy]
            enabled = true
            api_token = "should-never-be-in-toml"
        "#;
        let err = Config::parse_toml(typo).expect_err("a typo'd/secret field must be rejected");
        assert!(matches!(err, ConfigError::Parse(_)));
    }

    #[test]
    fn http_ingest_token_required_only_when_require_auth() {
        // require_auth=true, no DEBLOB_HTTP_INGEST_TOKEN → clear error
        // naming the variable.
        let env_missing = lookup(fake_env(&[
            (ENV_API_TOKEN, "test-token"),
            (ENV_REDIS_URL, "redis://localhost:6379"),
            (ENV_KAFKA_BROKERS, "localhost:9092"),
        ]));
        let err = validate_secrets(&env_missing, false, true)
            .expect_err("require_auth=true with no DEBLOB_HTTP_INGEST_TOKEN must fail");
        assert!(
            err.to_string().contains(ENV_HTTP_INGEST_TOKEN),
            "error must name the missing variable: {err}"
        );

        // require_auth=true, DEBLOB_HTTP_INGEST_TOKEN present → ok.
        let env_present = lookup(fake_env(&[
            (ENV_API_TOKEN, "test-token"),
            (ENV_REDIS_URL, "redis://localhost:6379"),
            (ENV_KAFKA_BROKERS, "localhost:9092"),
            (ENV_HTTP_INGEST_TOKEN, "http-token-value"),
        ]));
        let secrets = validate_secrets(&env_present, false, true)
            .expect("require_auth=true with DEBLOB_HTTP_INGEST_TOKEN present must succeed");
        assert_eq!(
            secrets.http_ingest_token.as_deref(),
            Some("http-token-value")
        );

        // require_auth=false, no DEBLOB_HTTP_INGEST_TOKEN → ok, not
        // required.
        let env_disabled = lookup(fake_env(&[
            (ENV_API_TOKEN, "test-token"),
            (ENV_REDIS_URL, "redis://localhost:6379"),
            (ENV_KAFKA_BROKERS, "localhost:9092"),
        ]));
        let secrets = validate_secrets(&env_disabled, false, false)
            .expect("require_auth=false must not require DEBLOB_HTTP_INGEST_TOKEN");
        assert!(secrets.http_ingest_token.is_none());
    }

    /// A TOML with `[semantic]` parses into `Config` with both lists
    /// populated; a TOML WITHOUT the section at all defaults to both lists
    /// empty (same "absent means the safe default" contract as `[slm]`/
    /// `[http_proxy]`); `deny_unknown_fields` rejects a typo'd key.
    #[test]
    fn config_parses_semantic_section() {
        let toml = r#"
            [kafka]
            raw_topic = "r"
            tagged_topic = "t"
            discovery_topic = "d"
            quarantine_topic = "q"
            group_id = "g"
            transactional_id = "x"

            [semantic]
            canonical_field_ids = ["cfid_temperature_ambient", "cfid_order_total"]
            event_types = ["order.created"]
        "#;
        let config = Config::parse_toml(toml).expect("[semantic] section must parse");
        assert_eq!(
            config.semantic.canonical_field_ids,
            vec![
                "cfid_temperature_ambient".to_string(),
                "cfid_order_total".to_string()
            ]
        );
        assert_eq!(
            config.semantic.event_types,
            vec!["order.created".to_string()]
        );

        let minimal = r#"
            [kafka]
            raw_topic = "r"
            tagged_topic = "t"
            discovery_topic = "d"
            quarantine_topic = "q"
            group_id = "g"
            transactional_id = "x"
        "#;
        let config = Config::parse_toml(minimal).expect("config without [semantic] must parse");
        assert!(config.semantic.canonical_field_ids.is_empty());
        assert!(config.semantic.event_types.is_empty());

        let typo = r#"
            [kafka]
            raw_topic = "r"
            tagged_topic = "t"
            discovery_topic = "d"
            quarantine_topic = "q"
            group_id = "g"
            transactional_id = "x"

            [semantic]
            canonical_field_ids = []
            unregistered_typo_field = true
        "#;
        let err = Config::parse_toml(typo).expect_err("a typo'd field must be rejected");
        assert!(matches!(err, ConfigError::Parse(_)));
    }

    /// [`SemanticConfig::to_registries`] is the pure `Config` -> domain-type
    /// transform `serve()` calls — this proves the seeding itself, without
    /// Redis/HTTP: every listed id is registered, and an id NOT listed is
    /// still rejected.
    #[test]
    fn semantic_config_to_registries_seeds_configured_ids_only() {
        let semantic = SemanticConfig {
            canonical_field_ids: vec!["cfid_temperature_ambient".to_string()],
            event_types: vec!["order.created".to_string()],
            domain_gate_enforce: false,
        };
        let registries = semantic.to_registries();

        assert!(registries.field_ids.contains("cfid_temperature_ambient"));
        assert!(!registries.field_ids.contains("cfid_unregistered"));
        assert!(registries.event_type_ids.contains("order.created"));
        assert!(!registries.event_type_ids.contains("order.unregistered"));

        // The documented default (no [semantic] section) seeds nothing —
        // every strong-axis annotation still 422s, Task 6's original
        // behavior.
        let empty = SemanticConfig::default().to_registries();
        assert!(!empty.field_ids.contains("cfid_temperature_ambient"));
        assert!(!empty.event_type_ids.contains("order.created"));
    }

    #[test]
    fn volatile_without_flag_is_rejected() {
        // Default (no --unsafe-volatile) must map to allow_volatile: false
        // — RedisRegistry/RedisEvidence::connect then reject a
        // non-persistent Redis instance.
        let default_opts = redis_opts(false);
        assert!(!default_opts.allow_volatile);

        // The flag being passed is the ONLY way to get allow_volatile: true.
        let flagged_opts = redis_opts(true);
        assert!(flagged_opts.allow_volatile);
    }
}
