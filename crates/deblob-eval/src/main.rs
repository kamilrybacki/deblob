//! `deblob-eval` binary — the offline eval harness CLI (Task 8).
//!
//! Loads the golden corpus (Task 6), builds an `HttpInferencer` pointed at
//! a CONFIGURED OpenAI-compatible endpoint (Task 2), drives the full
//! corpus through it (Task 7's `run_eval`/`compute_metrics`), and prints
//! the human + machine report. This binary never talks to the cold lane,
//! the registry, or any live Deblob state — see `crate` module docs.
//!
//! ## Retrieval-k ablation (`--k`)
//!
//! Hermes' Task 3 review: "the eval evaluates k = 1, 3, 5." The golden
//! corpus already bakes in a fixed `retrieved` top-k per case (produced by
//! Task 3's structural retrieval, `top_k = 3` default — no seed case
//! carries more than 3 retrieved candidates). `--k` truncates every case's
//! `retrieved` to `rank <= k` before building the `InferenceRequest`,
//! simulating "what does the endpoint decide if the retrieval budget were
//! only k" — a genuine ablation, not just a report-label change. Passing
//! `--k` runs ONE truncation; omitting it runs all three (1, 3, 5) and
//! prints three labeled reports. Because no seed case exceeds 3 retrieved
//! candidates, `k=3` and `k=5` are currently equivalent to the untruncated
//! corpus; `k=1` is the only truncation that changes what the endpoint
//! sees today. `recall_at_1`/`recall_at_3`/`recall_at_5`/`mrr` in the
//! printed [`deblob_eval::Metrics`] are unaffected by `--k`: they are
//! computed from the corpus's own `expected.gold_rank` field (Task 3's
//! retrieval result), not from what was actually sent to the endpoint this
//! run — see `crate::metrics::compute_metrics`'s "Retrieval quality"
//! block.
//!
//! ## Secrets
//!
//! `DEBLOB_SLM_API_TOKEN` is read ONLY from the environment, never from a
//! CLI flag (a flag would land in shell history / `ps`), and is never
//! printed or included in the JSON report — see [`resolve_slm_config`].

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use deblob_eval::{compute_metrics, load_corpus, report, run_eval, EvalCase, Metrics};
use deblob_slm::{HttpInferencer, SlmHttpConfig};

const ENV_BASE_URL: &str = "DEBLOB_SLM_BASE_URL";
const ENV_MODEL: &str = "DEBLOB_SLM_MODEL";
const ENV_API_TOKEN: &str = "DEBLOB_SLM_API_TOKEN";

/// Deblob offline eval harness — scores a configured `SemanticInferencer`
/// endpoint against the golden corpus. See `docs/eval-runbook.md` for the
/// full operator procedure (which model to target first and why).
///
/// With no subcommand, runs the eval-scoring flow below (unchanged CLI
/// shape). `deblob-eval generate ...` (spec:
/// `docs/superpowers/specs/2026-07-16-slm-corpus-generator.md`) instead
/// runs the synthetic ground-truth corpus generator — see
/// [`Command::Generate`].
#[derive(Debug, Parser)]
#[command(name = "deblob-eval", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// Directory containing golden corpus `*.json` case files. Run this
    /// binary from `crates/deblob-eval/` (where the default `corpus/`
    /// directory lives), or pass an explicit path.
    #[arg(long, default_value = "corpus")]
    corpus: PathBuf,

    /// Base URL of an OpenAI-compatible endpoint, e.g.
    /// `http://localhost:8000/v1`. Falls back to `DEBLOB_SLM_BASE_URL` if
    /// omitted. Required (via flag or env).
    #[arg(long)]
    base_url: Option<String>,

    /// Model id to request from the endpoint. Falls back to
    /// `DEBLOB_SLM_MODEL` if omitted. Required (via flag or env).
    #[arg(long)]
    model: Option<String>,

    /// Retrieval-k ablation: 1, 3, or 5. Omit to run all three. See the
    /// module docs for exactly what this does and does not affect.
    #[arg(long)]
    k: Option<u32>,

    /// Write the machine-readable JSON report to this file (in addition to
    /// printing the human report to stdout).
    #[arg(long)]
    json_out: Option<PathBuf>,

    /// Per-call timeout, milliseconds.
    #[arg(long, default_value_t = 30_000)]
    timeout_ms: u64,

    /// Max concurrent in-flight calls to the endpoint.
    #[arg(long, default_value_t = 4)]
    max_concurrency: usize,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Generate a deterministic, ground-truth-labeled synthetic corpus
    /// (spec: `docs/superpowers/specs/2026-07-16-slm-corpus-generator.md`)
    /// — expands the eval and produces fine-tune training data. NO LLM in
    /// the loop; every label comes from a known deterministic
    /// transformation.
    Generate(GenerateArgs),
}

#[derive(Debug, Parser)]
struct GenerateArgs {
    /// Directory to write generated `EvalCase` JSON files into (created if
    /// missing).
    #[arg(long)]
    out: PathBuf,

    /// Number of distinct base family schemas to generate.
    #[arg(long, default_value_t = 20)]
    families: usize,

    /// Number of variant cases to generate per family.
    #[arg(long = "variants-per-family", default_value_t = 8)]
    variants_per_family: usize,

    /// RNG seed — same seed always produces byte-identical output.
    #[arg(long, default_value_t = 1)]
    seed: u64,

    /// Optional path to also write the fine-tune JSONL export (spec §4):
    /// one `{case_name, partition, prompt, gold_tool_call}` line per case.
    #[arg(long = "finetune-jsonl")]
    finetune_jsonl: Option<PathBuf>,
}

/// Runs `deblob-eval generate`: builds the synthetic corpus, writes it to
/// `args.out`, prints the case-mix + partition summary (spec §6), and
/// optionally writes the fine-tune JSONL export.
fn run_generate(args: &GenerateArgs) -> ExitCode {
    let cfg = deblob_eval::GenerateConfig {
        families: args.families,
        variants_per_family: args.variants_per_family,
        seed: args.seed,
    };
    let generated = deblob_eval::generate_corpus(&cfg);

    if let Err(err) = deblob_eval::write_corpus(&args.out, &generated.cases) {
        eprintln!(
            "failed to write generated corpus to {}: {err}",
            args.out.display()
        );
        return ExitCode::FAILURE;
    }
    println!(
        "wrote {} generated case(s) to {}\n",
        generated.cases.len(),
        args.out.display()
    );
    println!("{}", deblob_eval::format_summary(&generated.summary));

    if let Some(path) = &args.finetune_jsonl {
        let jsonl = deblob_eval::render_finetune_jsonl(&generated.cases);
        if let Err(err) = std::fs::write(path, jsonl) {
            eprintln!(
                "failed to write fine-tune JSONL to {}: {err}",
                path.display()
            );
            return ExitCode::FAILURE;
        }
        println!(
            "wrote fine-tune JSONL ({} lines) to {}",
            generated.cases.len(),
            path.display()
        );
    }

    ExitCode::SUCCESS
}

/// The resolved endpoint configuration this binary will run against.
/// Separate from [`Cli`] so [`resolve_slm_config`] stays a pure,
/// independently-testable function — it never reads `std::env` itself; the
/// caller (`main`) supplies the env fallbacks explicitly.
///
/// `Debug` redacts `api_token` (same pattern as
/// `deblob_slm::SlmHttpConfig`) so a `{:?}`/`unwrap_err` in a test failure
/// message can never leak it.
struct ResolvedSlmConfig {
    base_url: String,
    model: String,
    api_token: Option<String>,
    timeout_ms: u64,
    max_concurrency: usize,
}

impl std::fmt::Debug for ResolvedSlmConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedSlmConfig")
            .field("base_url", &self.base_url)
            .field("model", &self.model)
            .field("api_token", &self.api_token.as_ref().map(|_| "<redacted>"))
            .field("timeout_ms", &self.timeout_ms)
            .field("max_concurrency", &self.max_concurrency)
            .finish()
    }
}

/// Resolves the endpoint config from CLI flags with env-var fallback for
/// `base_url`/`model` (`env_base_url`/`env_model`, the caller's
/// `std::env::var(ENV_BASE_URL)`/`std::env::var(ENV_MODEL)`). `api_token`
/// is accepted ONLY via `env_api_token` — there is no `--api-token` CLI
/// flag, and this function never reads `std::env` directly, so it is
/// exercised deterministically by unit tests below without touching
/// process-global environment state.
///
/// Errors with a clear, actionable message (naming both the flag and the
/// env var) when `base_url` or `model` end up unset from either source.
/// Never touches `api_token`'s value in the error message or anywhere
/// else — an absent token is not an error here (an endpoint may not
/// require auth); the caller's HTTP layer surfaces an auth failure if the
/// endpoint does require one.
fn resolve_slm_config(
    cli: &Cli,
    env_base_url: Option<String>,
    env_model: Option<String>,
    env_api_token: Option<String>,
) -> Result<ResolvedSlmConfig, String> {
    let base_url = cli.base_url.clone().or(env_base_url).ok_or_else(|| {
        format!(
            "missing SLM endpoint base URL: pass --base-url or set {ENV_BASE_URL} \
             (e.g. http://localhost:8000/v1 for a local llama.cpp-server/vLLM/Ollama/LM Studio \
             endpoint). See docs/eval-runbook.md."
        )
    })?;
    let model = cli.model.clone().or(env_model).ok_or_else(|| {
        format!("missing SLM model id: pass --model or set {ENV_MODEL}. See docs/eval-runbook.md.")
    })?;
    Ok(ResolvedSlmConfig {
        base_url,
        model,
        api_token: env_api_token,
        timeout_ms: cli.timeout_ms,
        max_concurrency: cli.max_concurrency,
    })
}

impl From<ResolvedSlmConfig> for SlmHttpConfig {
    fn from(cfg: ResolvedSlmConfig) -> Self {
        SlmHttpConfig {
            base_url: cfg.base_url,
            model: cfg.model,
            api_token: cfg.api_token,
            timeout_ms: cfg.timeout_ms,
            max_concurrency: cfg.max_concurrency,
        }
    }
}

/// The retrieval-k values to ablate over: `Some(k)` runs exactly that one
/// (validated to be 1, 3, or 5 — the values Hermes' Task 3 review names),
/// `None` runs all three.
fn k_values(cli_k: Option<u32>) -> Result<Vec<u32>, String> {
    match cli_k {
        None => Ok(vec![1, 3, 5]),
        Some(k) if [1, 3, 5].contains(&k) => Ok(vec![k]),
        Some(other) => Err(format!(
            "--k must be 1, 3, or 5 (got {other}); omit --k to run all three"
        )),
    }
}

/// Truncates every case's `retrieved` top-k to `rank <= k`, simulating a
/// smaller retrieval budget. Does not touch `expected` — see the module
/// docs' "Retrieval-k ablation" section for what this does and does not
/// change about the resulting metrics.
fn truncate_to_k(corpus: &[EvalCase], k: u32) -> Vec<EvalCase> {
    corpus
        .iter()
        .map(|case| {
            let mut truncated = case.clone();
            truncated.retrieved.retain(|c| c.rank <= k);
            truncated
        })
        .collect()
}

/// Runs the full Task 6→7 pipeline (already-loaded corpus →
/// `run_eval` → `compute_metrics`) for one `k` value and returns the
/// computed [`Metrics`] alongside the truncated corpus it was scored
/// against (the truncated corpus, not the original, is what
/// `compute_metrics` requires — see its docs on the `run`/`corpus` length
/// invariant).
async fn eval_at_k(inferencer: &HttpInferencer, corpus: &[EvalCase], k: u32) -> Metrics {
    let truncated = truncate_to_k(corpus, k);
    let run = run_eval(inferencer, &truncated).await;
    compute_metrics(&run, &truncated)
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    if let Some(Command::Generate(args)) = &cli.command {
        return run_generate(args);
    }

    let corpus = match load_corpus(&cli.corpus) {
        Ok(cases) => cases,
        Err(err) => {
            eprintln!("failed to load corpus from {}: {err}", cli.corpus.display());
            return ExitCode::FAILURE;
        }
    };

    let ks = match k_values(cli.k) {
        Ok(ks) => ks,
        Err(err) => {
            eprintln!("{err}");
            return ExitCode::FAILURE;
        }
    };

    let resolved = match resolve_slm_config(
        &cli,
        std::env::var(ENV_BASE_URL).ok(),
        std::env::var(ENV_MODEL).ok(),
        std::env::var(ENV_API_TOKEN).ok(),
    ) {
        Ok(resolved) => resolved,
        Err(err) => {
            eprintln!("{err}");
            return ExitCode::FAILURE;
        }
    };

    println!(
        "loaded {} golden corpus case(s) from {}",
        corpus.len(),
        cli.corpus.display()
    );
    println!(
        "scoring endpoint model={:?} (token {})...\n",
        resolved.model,
        if std::env::var(ENV_API_TOKEN).is_ok() {
            "configured, not logged"
        } else {
            "not set"
        }
    );

    let inferencer = HttpInferencer::new(resolved.into());

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(err) => {
            eprintln!("failed to start async runtime: {err}");
            return ExitCode::FAILURE;
        }
    };

    let mut json_runs = Vec::with_capacity(ks.len());
    for k in ks {
        let metrics = runtime.block_on(eval_at_k(&inferencer, &corpus, k));
        let (human, json) = report(&metrics);
        println!("=== retrieval-k = {k} ===");
        println!("{human}");
        json_runs.push(serde_json::json!({ "k": k, "metrics": json }));
    }

    if let Some(path) = &cli.json_out {
        let out = serde_json::json!({ "runs": json_runs });
        let rendered = match serde_json::to_string_pretty(&out) {
            Ok(s) => s,
            Err(err) => {
                eprintln!("failed to render JSON report: {err}");
                return ExitCode::FAILURE;
            }
        };
        if let Err(err) = std::fs::write(path, rendered) {
            eprintln!("failed to write JSON report to {}: {err}", path.display());
            return ExitCode::FAILURE;
        }
        println!("wrote JSON report to {}", path.display());
    }

    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;
    use deblob_core::id::FamilyId;
    use deblob_eval::{Category, Expected, Partition};
    use deblob_slm::{AbstainCause, CandidateProfileView, FamilyCandidate, InferenceDecision};

    fn base_cli() -> Cli {
        Cli {
            command: None,
            corpus: PathBuf::from("corpus"),
            base_url: None,
            model: None,
            k: None,
            json_out: None,
            timeout_ms: 30_000,
            max_concurrency: 4,
        }
    }

    // -- resolve_slm_config ---------------------------------------------

    #[test]
    fn missing_base_url_errors_clearly() {
        let cli = base_cli();
        let err = resolve_slm_config(&cli, None, Some("m".to_string()), None).unwrap_err();
        assert!(
            err.contains("base URL") && err.contains(ENV_BASE_URL) && err.contains("--base-url"),
            "error should name both the flag and the env var: {err}"
        );
    }

    #[test]
    fn missing_model_errors_clearly() {
        let cli = base_cli();
        let err = resolve_slm_config(&cli, Some("http://x".to_string()), None, None).unwrap_err();
        assert!(
            err.contains("model") && err.contains(ENV_MODEL) && err.contains("--model"),
            "error should name both the flag and the env var: {err}"
        );
    }

    #[test]
    fn cli_flags_take_precedence_over_env() {
        let mut cli = base_cli();
        cli.base_url = Some("http://flag".to_string());
        cli.model = Some("flag-model".to_string());
        let resolved = resolve_slm_config(
            &cli,
            Some("http://env".to_string()),
            Some("env-model".to_string()),
            None,
        )
        .unwrap();
        assert_eq!(resolved.base_url, "http://flag");
        assert_eq!(resolved.model, "flag-model");
    }

    #[test]
    fn env_fallback_used_when_flags_absent() {
        let cli = base_cli();
        let resolved = resolve_slm_config(
            &cli,
            Some("http://env".to_string()),
            Some("env-model".to_string()),
            Some("shh".to_string()),
        )
        .unwrap();
        assert_eq!(resolved.base_url, "http://env");
        assert_eq!(resolved.model, "env-model");
        assert_eq!(resolved.api_token.as_deref(), Some("shh"));
    }

    #[test]
    fn api_token_has_no_cli_flag_env_only() {
        // Compile-time proof by construction: `Cli` has no `api_token`
        // field at all (see the struct above) — there is no flag to even
        // pass one via. This test documents that invariant and would fail
        // to compile (not just fail at runtime) if a field were added.
        let cli = base_cli();
        let _ = cli; // Cli has: command, corpus, base_url, model, k, json_out, timeout_ms, max_concurrency.
    }

    // -- k_values ----------------------------------------------------------

    #[test]
    fn no_k_flag_runs_all_three() {
        assert_eq!(k_values(None).unwrap(), vec![1, 3, 5]);
    }

    #[test]
    fn valid_k_runs_one() {
        assert_eq!(k_values(Some(3)).unwrap(), vec![3]);
    }

    #[test]
    fn invalid_k_errors_clearly() {
        let err = k_values(Some(2)).unwrap_err();
        assert!(err.contains('2'), "error should name the bad value: {err}");
    }

    // -- truncate_to_k -------------------------------------------------------

    fn schema_id(byte: u8) -> deblob_core::id::SchemaId {
        deblob_core::id::SchemaId::from_digest(&[byte; 32])
    }

    fn fc(byte: u8, rank: u32) -> FamilyCandidate {
        FamilyCandidate {
            family_id: FamilyId::new_v7(),
            schema_id: schema_id(byte),
            version: 1,
            distance: 0.1,
            rank,
        }
    }

    fn case_with_retrieved(retrieved: Vec<FamilyCandidate>) -> EvalCase {
        EvalCase {
            name: "t".to_string(),
            category: Category::KnownExact,
            candidate: CandidateProfileView {
                observation_count: 1,
                fields: vec![],
                truncated: false,
            },
            retrieved,
            expected: Expected {
                decision: InferenceDecision::Abstain {
                    cause: AbstainCause::Ambiguous,
                },
                gold_schema_id: None,
                gold_rank: None,
                false_merge_trap: false,
                false_split_trap: false,
            },
            partition: Partition::Test,
        }
    }

    #[test]
    fn truncate_to_k_keeps_only_low_enough_ranks() {
        let corpus = vec![case_with_retrieved(vec![
            fc(1, 1),
            fc(2, 2),
            fc(3, 3),
            fc(4, 4),
            fc(5, 5),
        ])];

        let k1 = truncate_to_k(&corpus, 1);
        assert_eq!(k1[0].retrieved.len(), 1);
        assert_eq!(k1[0].retrieved[0].rank, 1);

        let k3 = truncate_to_k(&corpus, 3);
        assert_eq!(k3[0].retrieved.len(), 3);
        assert!(k3[0].retrieved.iter().all(|c| c.rank <= 3));

        let k5 = truncate_to_k(&corpus, 5);
        assert_eq!(k5[0].retrieved.len(), 5);
    }

    #[test]
    fn truncate_to_k_leaves_original_corpus_untouched() {
        let corpus = vec![case_with_retrieved(vec![fc(1, 1), fc(2, 2)])];
        let _ = truncate_to_k(&corpus, 1);
        assert_eq!(
            corpus[0].retrieved.len(),
            2,
            "truncate_to_k must not mutate its input"
        );
    }
}
