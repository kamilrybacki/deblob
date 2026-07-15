//! `deblob-eval` binary — thin CLI stub (Task 6).
//!
//! Task 6 only builds the golden corpus format + loader; running this
//! binary today loads and validates the corpus and prints a summary. Task
//! 7 adds metric computation against a configured
//! `deblob_slm::SemanticInferencer` endpoint; Task 8 adds report emission
//! + CI wiring + a real-endpoint runbook.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

/// Deblob offline eval harness — scores a configured `SemanticInferencer`
/// endpoint against the golden corpus (spec
/// `docs/superpowers/plans/2026-07-14-deblob-p2ab.md` § Task 8). This
/// build (Task 6) only loads and validates the corpus; endpoint scoring
/// and report generation land in Task 7/8.
#[derive(Debug, Parser)]
#[command(name = "deblob-eval", version, about)]
struct Cli {
    /// Directory containing golden corpus `*.json` case files.
    #[arg(long, default_value = "crates/deblob-eval/corpus")]
    corpus_dir: PathBuf,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    match deblob_eval::load_corpus(&cli.corpus_dir) {
        Ok(cases) => {
            println!(
                "loaded {} golden corpus case(s) from {}",
                cases.len(),
                cli.corpus_dir.display()
            );
            println!(
                "NOTE: this build only loads/validates the corpus (Task 6); endpoint scoring \
                 and report generation land in Task 7/8."
            );
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("failed to load corpus: {err}");
            ExitCode::FAILURE
        }
    }
}
