//! A thin `hdrhistogram` wrapper for end-to-end tag latency (spec §3.1:
//! "histograms → p50/p95/p99"). Nanosecond-resolution internally (matching
//! `crate::header`'s produce-timestamp unit), millisecond summary for the
//! report — operators read latency in ms, not ns.

use hdrhistogram::Histogram;

/// Highest latency this histogram can record: one hour, in nanoseconds.
/// Any single tag latency at or above this is almost certainly a clock
/// skew/bug rather than a real measurement, so `record_ns` clamps to it
/// rather than erroring — a benchmark run must never abort mid-stream over
/// one bad sample.
const MAX_NS: u64 = 3_600_000_000_000;

/// Significant value digits `hdrhistogram` preserves per bucket. `3` is the
/// library's own common default (0.1% relative error), plenty for a
/// benchmark report.
const SIGFIG: u8 = 3;

/// Millisecond latency summary (spec §3.1's p50/p95/p99 + max), the shape
/// that lands directly in the JSON report.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
pub struct LatencySummaryMs {
    pub count: u64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
}

/// Records nanosecond latencies and renders millisecond percentiles.
pub struct LatencyHistogram(Histogram<u64>);

impl LatencyHistogram {
    pub fn new() -> Self {
        Self(
            Histogram::new_with_bounds(1, MAX_NS, SIGFIG)
                .expect("1..=MAX_NS with 3 significant figures is always a valid histogram"),
        )
    }

    /// Records one latency sample, clamped to `1..=MAX_NS` (see `MAX_NS`'s
    /// docs — this never fails/panics on an out-of-range or zero value).
    pub fn record_ns(&mut self, ns: u64) {
        let clamped = ns.clamp(1, MAX_NS);
        // `Histogram::record` only errs when the value is outside the
        // configured range, which `clamp` above already rules out.
        let _ = self.0.record(clamped);
    }

    pub fn len(&self) -> u64 {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.len() == 0
    }

    /// `None` if no samples were recorded — an empty histogram has no
    /// meaningful percentiles, and the report must show that explicitly
    /// rather than a misleading all-zero summary.
    pub fn summary_ms(&self) -> Option<LatencySummaryMs> {
        if self.is_empty() {
            return None;
        }
        Some(LatencySummaryMs {
            count: self.0.len(),
            p50_ms: ns_to_ms(self.0.value_at_quantile(0.50)),
            p95_ms: ns_to_ms(self.0.value_at_quantile(0.95)),
            p99_ms: ns_to_ms(self.0.value_at_quantile(0.99)),
            max_ms: ns_to_ms(self.0.max()),
        })
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for LatencyHistogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LatencyHistogram")
            .field("len", &self.0.len())
            .finish()
    }
}

fn ns_to_ms(ns: u64) -> f64 {
    ns as f64 / 1_000_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_histogram_has_no_summary() {
        let h = LatencyHistogram::new();
        assert!(h.is_empty());
        assert_eq!(h.len(), 0);
        assert!(h.summary_ms().is_none());
    }

    #[test]
    fn percentiles_are_correct_within_hdrhistogram_precision_on_a_uniform_sample() {
        let mut h = LatencyHistogram::new();
        // 1..=1000 ms, in ns — a uniform distribution with well-known
        // exact percentiles to check hdrhistogram's approximation against.
        for ms in 1..=1000u64 {
            h.record_ns(ms * 1_000_000);
        }
        let summary = h.summary_ms().expect("non-empty histogram has a summary");

        assert_eq!(summary.count, 1000);
        // 3 significant figures ⇒ well under 1% relative error; allow a
        // generous 2% tolerance so this test isn't coupled to
        // hdrhistogram's exact bucket boundaries.
        assert_close(summary.p50_ms, 500.0, 0.02);
        assert_close(summary.p95_ms, 950.0, 0.02);
        assert_close(summary.p99_ms, 990.0, 0.02);
        assert_close(summary.max_ms, 1000.0, 0.02);
    }

    #[test]
    fn record_ns_clamps_rather_than_panics_on_extreme_values() {
        let mut h = LatencyHistogram::new();
        h.record_ns(0);
        h.record_ns(u64::MAX);
        assert_eq!(h.len(), 2);
        let summary = h.summary_ms().expect("clamped values still recorded");
        // 3-significant-figure bucketing near the very top of the
        // configured range can round the reported max slightly ABOVE the
        // true clamped value — allow 1% relative slack rather than
        // asserting exact equality.
        let max_bound_ms = (MAX_NS as f64) / 1_000_000.0 * 1.01;
        assert!(
            summary.max_ms <= max_bound_ms,
            "max_ms {} exceeded the clamp bound {max_bound_ms}",
            summary.max_ms
        );
    }

    #[test]
    fn single_sample_reports_it_as_every_percentile() {
        let mut h = LatencyHistogram::new();
        h.record_ns(42_000_000); // 42ms
        let summary = h.summary_ms().unwrap();
        assert_close(summary.p50_ms, 42.0, 0.05);
        assert_close(summary.p99_ms, 42.0, 0.05);
        assert_close(summary.max_ms, 42.0, 0.05);
    }

    fn assert_close(actual: f64, expected: f64, relative_tolerance: f64) {
        let tolerance = expected.abs() * relative_tolerance + 0.5;
        assert!(
            (actual - expected).abs() <= tolerance,
            "expected {actual} to be within {tolerance} of {expected}"
        );
    }
}
