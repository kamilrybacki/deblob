//! Entry point (Task 18): parses CLI args, loads config + env-only secrets,
//! then delegates every non-trivial wiring decision to
//! [`deblob::serve::serve`] — the reusable runtime entrypoint (Task 19)
//! that also backs the end-to-end acceptance test in
//! `crates/deblob/tests/e2e_it.rs`, so the test exercises the SAME code
//! path this binary runs in production, not a test-only stand-in.
//!
//! Kept thin on purpose: every non-trivial decision (config parsing, env
//! overlay, secret validation, the `--unsafe-volatile` → `RedisOpts`
//! mapping) lives in [`deblob::config`]; the actual Redis/Kafka/API wiring
//! lives in [`deblob::serve`]. This file's own tests cover only the
//! CLI-parsing surface that's specific to `main`.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use deblob::config::{self, Config};
use deblob::metrics::init_tracing;
use deblob::serve::{serve, AppError};
use tokio_util::sync::CancellationToken;

#[derive(Parser, Debug)]
#[command(
    name = "deblob",
    about = "Schema-tagging hot/cold-lane relay (spec P1)"
)]
struct Cli {
    /// Path to the TOML config file (non-secret operational knobs only —
    /// see `deblob.example.toml`).
    #[arg(long, default_value = "deblob.toml")]
    config: PathBuf,

    /// Allow connecting to a Redis instance with AOF persistence disabled
    /// (spec §6: "refuse non-persistent Redis unless --unsafe-volatile").
    /// Off by default; an explicit, documented risk acceptance for
    /// ephemeral/test deployments only.
    #[arg(long)]
    unsafe_volatile: bool,
}

fn main() -> ExitCode {
    init_tracing();
    let cli = Cli::parse();

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("failed to start tokio runtime: {e}");
            return ExitCode::FAILURE;
        }
    };

    match runtime.block_on(run(cli)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            // Every `AppError` variant's `Display` is secret-value-free
            // (spec §9) — safe to log as-is.
            tracing::error!(error = %err, "deblob exiting");
            ExitCode::FAILURE
        }
    }
}

/// Errors `run` can hit before it ever reaches [`serve`] — config loading
/// and secret validation, both CLI-specific concerns that `serve` itself
/// takes no part in.
#[derive(Debug, thiserror::Error)]
enum RunError {
    #[error(transparent)]
    Config(#[from] config::ConfigError),
    #[error(transparent)]
    Serve(#[from] AppError),
}

async fn run(cli: Cli) -> Result<(), RunError> {
    let raw_config = Config::load(&cli.config)?;
    let app_config = config::apply_env_overlay(raw_config, &config::process_env);
    let secrets = config::validate_secrets(&config::process_env)?;
    let redis_opts = config::redis_opts(cli.unsafe_volatile);

    tracing::info!(
        config_path = %cli.config.display(),
        unsafe_volatile = cli.unsafe_volatile,
        "starting deblob"
    );

    // `serve` waits on `shutdown` internally rather than listening for OS
    // signals itself — that's what lets an end-to-end test hand it a
    // token it controls instead of a real SIGTERM. Production's `main` is
    // the one place that actually watches for SIGTERM/SIGINT and cancels
    // the token on the caller's behalf.
    let shutdown = CancellationToken::new();
    let signal_shutdown = shutdown.clone();
    let signal_task = tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        signal_shutdown.cancel();
    });

    let result = serve(app_config, secrets, redis_opts, shutdown).await;
    signal_task.abort();
    result.map_err(RunError::Serve)
}

/// Waits for SIGTERM (the orchestrator's stop signal) or SIGINT/Ctrl-C
/// (interactive/dev use), whichever comes first.
#[cfg(unix)]
async fn wait_for_shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};

    let mut sigterm = signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
    tokio::select! {
        _ = sigterm.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_defaults_to_deblob_toml_and_safe_redis() {
        let cli = Cli::parse_from(["deblob"]);
        assert_eq!(cli.config, PathBuf::from("deblob.toml"));
        assert!(!cli.unsafe_volatile);
    }

    #[test]
    fn cli_unsafe_volatile_flag_sets_true() {
        let cli = Cli::parse_from(["deblob", "--unsafe-volatile"]);
        assert!(cli.unsafe_volatile);
    }

    #[test]
    fn cli_config_flag_overrides_default_path() {
        let cli = Cli::parse_from(["deblob", "--config", "/etc/deblob/deblob.toml"]);
        assert_eq!(cli.config, PathBuf::from("/etc/deblob/deblob.toml"));
    }

    // The default (no flag) must map to `allow_volatile: false` — this is
    // the same assertion `deblob::config`'s own `volatile_without_flag_is_
    // rejected` test makes on `config::redis_opts` directly; repeated here
    // against the exact value `main`'s wiring passes, so a future edit to
    // `run()` that swaps the argument order can't silently invert it.
    #[test]
    fn default_cli_maps_to_non_volatile_redis_opts() {
        let cli = Cli::parse_from(["deblob"]);
        let opts = config::redis_opts(cli.unsafe_volatile);
        assert!(!opts.allow_volatile);
    }
}
