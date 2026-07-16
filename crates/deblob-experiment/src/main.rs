//! Runs the Task-1 offline comparative experiment (synthetic corpus, mock
//! inferencer) with the default [`RunConfig`] and prints the Markdown
//! report to stdout. Real corpus/model wiring lands in later tasks — see
//! `deblob_experiment`'s crate docs.

use deblob_experiment::reporter::render_markdown;
use deblob_experiment::run::{run_experiment, RunConfig};

fn main() {
    let report = run_experiment(&RunConfig::default());
    println!("{}", render_markdown(&report));
}
