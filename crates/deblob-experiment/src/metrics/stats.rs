//! Shared statistical primitives used across the four metric layers (spec
//! §3, Layers 3 and 4): the rule-of-three false-merge upper bound,
//! McNemar's test, and a paired bootstrap confidence interval. None of
//! these depend on `deblob-eval`/`deblob` types — they operate on plain
//! counts/bools so they're independently testable and reusable by any
//! future layer.

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

/// 95% chi-square(1) critical value — McNemar's continuity-corrected
/// statistic exceeding this is the conventional significance threshold.
const CHI_SQUARE_1_DF_95: f64 = 3.841_458_82;

/// One-sided 95% upper confidence bound on a true event rate, given
/// `count` observed events over `n` trials.
///
/// - `count == 0`: the rule-of-three approximation (Hanley & Lippman-Hand
///   1983), `3/n` — spec §3 Layer 3: "false-merge count with N and upper
///   confidence bound (rule-of-three for zero-event)". This is the number
///   the acceptance test asserts exactly.
/// - `count > 0`: a Wilson score interval upper bound (a standard
///   extension beyond the zero-event case the spec names explicitly; kept
///   here so a nonzero false-merge run still gets an honest bound rather
///   than a `None`).
/// - `n == 0`: `None` — no trials, no bound to report.
pub fn upper_bound_95(count: usize, n: usize) -> Option<f64> {
    if n == 0 {
        return None;
    }
    if count == 0 {
        return Some(3.0 / n as f64);
    }
    Some(wilson_upper_bound(count, n, 1.959_963_98))
}

/// Wilson score interval upper bound at `z` standard deviations (1.96 ≈
/// 95%) for `count` successes over `n` trials.
fn wilson_upper_bound(count: usize, n: usize, z: f64) -> f64 {
    let n = n as f64;
    let p = count as f64 / n;
    let z2 = z * z;
    let denom = 1.0 + z2 / n;
    let center = p + z2 / (2.0 * n);
    let spread = z * ((p * (1.0 - p) / n) + z2 / (4.0 * n * n)).sqrt();
    ((center + spread) / denom).min(1.0)
}

/// The result of McNemar's test over paired binary (correct/incorrect)
/// outcomes from two arms scored on the SAME events.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
pub struct McNemarResult {
    /// Events where A was wrong and B was correct (B wins the discordant pair).
    pub n01: usize,
    /// Events where A was correct and B was wrong (A wins the discordant pair).
    pub n10: usize,
    /// Continuity-corrected McNemar chi-square statistic. `0.0` when there
    /// are no discordant pairs at all (`n01 == n10 == 0`) — no evidence of
    /// a difference either way.
    pub statistic: f64,
    /// `true` iff `statistic` exceeds the 95% chi-square(1) critical value.
    pub significant_at_95: bool,
}

/// Runs McNemar's test over `pairs`, one `(a_correct, b_correct)` bool pair
/// per event — A and B must have been scored on the identical event set
/// (spec §3 Layer 4: "A and B see identical events").
pub fn mcnemar(pairs: &[(bool, bool)]) -> McNemarResult {
    let mut n01 = 0usize; // a wrong, b correct
    let mut n10 = 0usize; // a correct, b wrong
    for (a_correct, b_correct) in pairs {
        match (a_correct, b_correct) {
            (false, true) => n01 += 1,
            (true, false) => n10 += 1,
            _ => {}
        }
    }
    let discordant = n01 + n10;
    let statistic = if discordant == 0 {
        0.0
    } else {
        let diff = (n01 as f64 - n10 as f64).abs() - 1.0;
        (diff.max(0.0)).powi(2) / discordant as f64
    };
    McNemarResult {
        n01,
        n10,
        statistic,
        significant_at_95: statistic > CHI_SQUARE_1_DF_95,
    }
}

/// A paired bootstrap confidence interval on `b_accuracy - a_accuracy`
/// (spec §3 Layer 4: "significance via McNemar / paired bootstrap CIs").
/// Resamples `pairs` with replacement `iterations` times using a
/// `ChaCha8Rng` seeded from `seed` — deterministic: the same `(pairs, seed,
/// iterations)` always produces the same result, no wall-clock/global RNG
/// state involved anywhere in this path.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
pub struct BootstrapCi {
    pub point_estimate: f64,
    pub ci_low_95: f64,
    pub ci_high_95: f64,
}

pub fn paired_bootstrap_ci(
    pairs: &[(bool, bool)],
    seed: u64,
    iterations: usize,
) -> Option<BootstrapCi> {
    if pairs.is_empty() || iterations == 0 {
        return None;
    }

    let point_estimate = delta_accuracy(pairs);

    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut deltas: Vec<f64> = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let resample: Vec<(bool, bool)> = (0..pairs.len())
            .map(|_| pairs[rng.gen_range(0..pairs.len())])
            .collect();
        deltas.push(delta_accuracy(&resample));
    }
    deltas.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let low_idx = ((deltas.len() as f64) * 0.025).floor() as usize;
    let high_idx = (((deltas.len() as f64) * 0.975).ceil() as usize).min(deltas.len() - 1);

    Some(BootstrapCi {
        point_estimate,
        ci_low_95: deltas[low_idx],
        ci_high_95: deltas[high_idx],
    })
}

fn delta_accuracy(pairs: &[(bool, bool)]) -> f64 {
    if pairs.is_empty() {
        return 0.0;
    }
    let a = pairs.iter().filter(|(a, _)| *a).count() as f64 / pairs.len() as f64;
    let b = pairs.iter().filter(|(_, b)| *b).count() as f64 / pairs.len() as f64;
    b - a
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rule_of_three_exact_bound_when_zero_events() {
        assert_eq!(upper_bound_95(0, 100), Some(0.03));
        assert_eq!(upper_bound_95(0, 300), Some(0.01));
    }

    #[test]
    fn upper_bound_is_none_with_zero_trials() {
        assert_eq!(upper_bound_95(0, 0), None);
    }

    #[test]
    fn nonzero_count_uses_wilson_bound_above_the_raw_rate() {
        let bound = upper_bound_95(2, 100).unwrap();
        assert!(
            bound > 0.02,
            "bound {bound} should exceed the raw rate 0.02"
        );
        assert!(bound < 1.0);
    }

    #[test]
    fn mcnemar_is_significant_when_b_strictly_dominates_a() {
        // B correct where A is wrong in 20 events; A never wins a
        // discordant pair; the rest agree.
        let mut pairs = vec![(false, true); 20];
        pairs.extend(vec![(true, true); 30]);
        let result = mcnemar(&pairs);
        assert_eq!(result.n01, 20);
        assert_eq!(result.n10, 0);
        assert!(result.significant_at_95, "statistic={}", result.statistic);
    }

    #[test]
    fn mcnemar_is_not_significant_on_a_tie() {
        let mut pairs = vec![(false, true); 5];
        pairs.extend(vec![(true, false); 5]);
        pairs.extend(vec![(true, true); 20]);
        let result = mcnemar(&pairs);
        assert_eq!(result.n01, 5);
        assert_eq!(result.n10, 5);
        assert!(!result.significant_at_95, "statistic={}", result.statistic);
    }

    #[test]
    fn mcnemar_statistic_is_zero_with_no_discordant_pairs() {
        let pairs = vec![(true, true), (false, false)];
        let result = mcnemar(&pairs);
        assert_eq!(result.statistic, 0.0);
        assert!(!result.significant_at_95);
    }

    #[test]
    fn bootstrap_ci_is_deterministic_given_the_same_seed() {
        let mut pairs = vec![(false, true); 15];
        pairs.extend(vec![(true, true); 15]);
        let first = paired_bootstrap_ci(&pairs, 42, 500).unwrap();
        let second = paired_bootstrap_ci(&pairs, 42, 500).unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn bootstrap_ci_reflects_b_dominance_with_a_positive_interval() {
        let mut pairs = vec![(false, true); 25];
        pairs.extend(vec![(true, true); 25]);
        let ci = paired_bootstrap_ci(&pairs, 7, 1000).unwrap();
        assert!(
            ci.point_estimate > 0.0,
            "point_estimate={}",
            ci.point_estimate
        );
        assert!(
            ci.ci_low_95 > 0.0,
            "expected the whole 95% interval above zero for a strict-dominance sample: {ci:?}"
        );
    }

    #[test]
    fn bootstrap_ci_is_none_for_empty_pairs() {
        assert_eq!(paired_bootstrap_ci(&[], 1, 100), None);
    }
}
