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
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub kafka: KafkaConfig,
    #[serde(default)]
    pub limits: LimitsConfig,
    #[serde(default)]
    pub promotion: PromotionConfig,
    #[serde(default)]
    pub management: ManagementConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaConfig {
    pub raw_topic: String,
    pub tagged_topic: String,
    pub discovery_topic: String,
    pub quarantine_topic: String,
    pub group_id: String,
    pub transactional_id: String,
}

/// Bounds enforced by the bounded parser (spec §4). Mirrors the subset of
/// `deblob_fingerprint::Limits` the TOML config exposes as operator-tunable
/// knobs — [`LimitsConfig::to_limits`] fills in the rest
/// (`max_key_len`/`max_string_len`/`max_array_inspect`) from
/// `Limits::default()`.
#[derive(Debug, Clone, Copy, Deserialize)]
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
pub fn validate_secrets(env: &impl Fn(&str) -> Option<String>) -> Result<Secrets, ConfigError> {
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

    Ok(Secrets {
        api_token,
        redis_url,
        kafka_brokers,
        kafka_sasl,
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

        assert_eq!(config.limits.max_bytes, 1_048_576);
        assert_eq!(config.limits.max_depth, 32);
        assert_eq!(config.limits.max_fields_per_object, 1024);

        assert_eq!(config.promotion.min_samples, 10);
        assert_eq!(config.promotion.min_age_ms, 300_000);

        assert_eq!(config.management.addr, "127.0.0.1:9615");
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
        let secrets = validate_secrets(&secrets_env).expect("all required secrets present");
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

        let err = validate_secrets(&env).expect_err("missing DEBLOB_API_TOKEN must fail");
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

        let err = validate_secrets(&env).expect_err("missing DEBLOB_REDIS_URL must fail");
        assert!(err.to_string().contains(ENV_REDIS_URL));
    }

    #[test]
    fn missing_kafka_brokers_fails_startup_naming_var() {
        let env = lookup(fake_env(&[
            (ENV_API_TOKEN, "test-token"),
            (ENV_REDIS_URL, "redis://localhost:6379"),
        ]));

        let err = validate_secrets(&env).expect_err("missing DEBLOB_KAFKA_BROKERS must fail");
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

        let err = validate_secrets(&env).expect_err("SASL username without password must fail");
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

        let secrets = validate_secrets(&env).expect("full SASL credentials must validate");
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
        ]));
        let secrets = validate_secrets(&env).unwrap();
        let rendered = format!("{secrets:?}");
        assert!(!rendered.contains("super-secret-token"));
        assert!(!rendered.contains("pass@localhost"));
        assert!(!rendered.contains("broker.internal"));
        assert!(!rendered.contains("s3cr3t"));
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
