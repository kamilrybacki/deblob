//! The management-API prober (spec §3.1/§4): times individual mgmt-API
//! calls (candidate listing, promotion, semantic annotation,
//! semantic-neighbor lookup) and scrapes+parses `/metrics` (spec §5).
//! Bearer-token auth — `token` is always supplied by the caller
//! (`crate::scenarios`/`main.rs`, sourced from `DEBLOB_API_TOKEN`), never
//! read from the environment by this module itself, so it stays
//! unit-testable and dependency-free of process-global state.
//!
//! [`MgmtProber`]'s methods need a LIVE management API and are exercised by
//! the controller's Docker-backed integration run. [`parse_prometheus_text`]
//! / [`extract_deblob_counters`] are pure text parsing and are unit-tested
//! below.

use std::collections::HashMap;
use std::time::Instant;

use reqwest::{Client, Response};
use serde::Serialize;

/// One timed mgmt-API call result.
#[derive(Debug, Clone, Serialize)]
pub struct ProbeSample {
    pub op: String,
    pub status: u16,
    pub latency_ms: f64,
}

#[derive(Debug, thiserror::Error)]
pub enum ProberError {
    #[error("http request failed: {0}")]
    Http(#[from] reqwest::Error),
}

/// Turns a completed (or failed) HTTP call into a [`ProbeSample`],
/// including the time spent draining the response body — a fair
/// comparison across ops whose response sizes differ (a `GET /candidates`
/// page vs. a single-object `POST .../promote` reply).
async fn record(
    op: &'static str,
    start: Instant,
    result: Result<Response, reqwest::Error>,
) -> Result<ProbeSample, ProberError> {
    let resp = result?;
    let status = resp.status().as_u16();
    let _ = resp.bytes().await;
    Ok(ProbeSample {
        op: op.to_string(),
        status,
        latency_ms: start.elapsed().as_secs_f64() * 1000.0,
    })
}

/// A bearer-authenticated client against one Deblob management API base
/// URL (spec §8's separate mgmt port).
pub struct MgmtProber {
    client: Client,
    base_url: String,
    token: String,
}

impl MgmtProber {
    pub fn new(
        base_url: impl Into<String>,
        token: impl Into<String>,
        timeout: std::time::Duration,
    ) -> Result<Self, ProberError> {
        let client = Client::builder().timeout(timeout).build()?;
        Ok(Self {
            client,
            base_url: base_url.into(),
            token: token.into(),
        })
    }

    /// Times `GET /api/v1/candidates?state=<state>` (`state` is
    /// `"provisional"` or `"staged"`, per the API contract).
    pub async fn list_candidates(&self, state: &str) -> Result<ProbeSample, ProberError> {
        let url = format!("{}/api/v1/candidates?state={state}", self.base_url);
        let start = Instant::now();
        let result = self.client.get(&url).bearer_auth(&self.token).send().await;
        record("list_candidates", start, result).await
    }

    /// Times `POST /api/v1/candidates/{cand_id}/promote`.
    pub async fn promote_candidate(
        &self,
        cand_id: &str,
        body: &serde_json::Value,
    ) -> Result<ProbeSample, ProberError> {
        let url = format!("{}/api/v1/candidates/{cand_id}/promote", self.base_url);
        let start = Instant::now();
        let result = self
            .client
            .post(&url)
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await;
        record("promote_candidate", start, result).await
    }

    /// Times `PUT /api/v1/schemas/{sch_id}/semantic`.
    pub async fn put_semantic(
        &self,
        sch_id: &str,
        body: &serde_json::Value,
    ) -> Result<ProbeSample, ProberError> {
        let url = format!("{}/api/v1/schemas/{sch_id}/semantic", self.base_url);
        let start = Instant::now();
        let result = self
            .client
            .put(&url)
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await;
        record("put_semantic", start, result).await
    }

    /// Times `GET /api/v1/schemas/{sch_id}/semantic-neighbors?k=<k>`.
    pub async fn semantic_neighbors(
        &self,
        sch_id: &str,
        k: usize,
    ) -> Result<ProbeSample, ProberError> {
        let url = format!(
            "{}/api/v1/schemas/{sch_id}/semantic-neighbors?k={k}",
            self.base_url
        );
        let start = Instant::now();
        let result = self.client.get(&url).bearer_auth(&self.token).send().await;
        record("semantic_neighbors", start, result).await
    }

    /// `GET /metrics` — unauthenticated (spec §11's scraper contract:
    /// orchestrators must not need a credential to scrape).
    pub async fn scrape_metrics(&self) -> Result<String, ProberError> {
        let url = format!("{}/metrics", self.base_url);
        let text = self.client.get(&url).send().await?.text().await?;
        Ok(text)
    }
}

// ---------------------------------------------------------------------
// Minimal Prometheus text-exposition-format (0.0.4) parsing — just enough
// to pull the handful of `deblob_*` counters spec §5 asks the prober to
// report (match/candidate/unresolved/quarantine rates, promotions) out of
// a `/metrics` scrape. Not a general Prometheus client: no HELP/TYPE
// interpretation, no exemplars, no bucket reconstruction beyond the plain
// `_count`/`_sum` suffixes `deblob_tag_latency_seconds` exposes.
// ---------------------------------------------------------------------

/// One parsed exposition-format sample line:
/// `metric_name{label="value",...} 1.23`.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricSample {
    pub name: String,
    pub labels: Vec<(String, String)>,
    pub value: f64,
}

/// Parses every non-comment, non-blank line of `text` as a
/// [`MetricSample`]. Lines that don't fit the `name{labels} value` or
/// `name value` shape are skipped rather than erroring — a scrape is
/// best-effort telemetry, not a hard API contract this harness must fail
/// over.
pub fn parse_prometheus_text(text: &str) -> Vec<MetricSample> {
    text.lines().filter_map(parse_line).collect()
}

fn parse_line(line: &str) -> Option<MetricSample> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let (head, value_str) = line.rsplit_once(' ')?;
    let value: f64 = value_str.trim().parse().ok()?;

    if let Some(brace_start) = head.find('{') {
        let name = head[..brace_start].to_string();
        let brace_end = head.rfind('}')?;
        let labels = parse_labels(&head[brace_start + 1..brace_end]);
        Some(MetricSample {
            name,
            labels,
            value,
        })
    } else {
        Some(MetricSample {
            name: head.to_string(),
            labels: Vec::new(),
            value,
        })
    }
}

fn parse_labels(labels_str: &str) -> Vec<(String, String)> {
    // Naive split on `,` — every label value on Deblob's exposition
    // surface is a short, controlled-vocabulary token ("fate"/"result"/
    // "reason"/"strength", per `deblob_match::metrics`) that never itself
    // contains a comma or an escaped quote, so a full escape-aware parser
    // is not needed here.
    labels_str
        .split(',')
        .filter_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            let v = v.trim().trim_matches('"');
            let k = k.trim();
            if k.is_empty() {
                None
            } else {
                Some((k.to_string(), v.to_string()))
            }
        })
        .collect()
}

fn label<'a>(labels: &'a [(String, String)], key: &str) -> Option<&'a str> {
    labels
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

/// The subset of `/metrics` spec §5 asks the prober to surface in the
/// report: totals by their bounded label, keyed by label value. Unknown
/// metric names are silently ignored by [`extract_deblob_counters`] — a
/// future Deblob metric added upstream must never break the bench.
#[derive(Debug, Clone, Default, Serialize)]
pub struct DeblobCounters {
    pub messages_by_fate: HashMap<String, f64>,
    pub quarantine_by_reason: HashMap<String, f64>,
    pub candidate_promotions_by_result: HashMap<String, f64>,
    pub candidates_active: Option<f64>,
    pub relay_records_total: Option<f64>,
    pub tag_latency_count: Option<f64>,
    pub tag_latency_sum_seconds: Option<f64>,
}

/// Buckets `samples` into the counters the bench report cares about,
/// matching the metric/label names `deblob_match::metrics::Metrics::new`
/// registers (`deblob_messages_total{fate}`,
/// `deblob_quarantine_records_total{reason}`,
/// `deblob_candidate_promotions_total{result}`, `deblob_candidates_active`,
/// `deblob_relay_records_total`, `deblob_tag_latency_seconds_{count,sum}`).
pub fn extract_deblob_counters(samples: &[MetricSample]) -> DeblobCounters {
    let mut out = DeblobCounters::default();
    for s in samples {
        match s.name.as_str() {
            "deblob_messages_total" => {
                if let Some(fate) = label(&s.labels, "fate") {
                    *out.messages_by_fate.entry(fate.to_string()).or_insert(0.0) += s.value;
                }
            }
            "deblob_quarantine_records_total" => {
                if let Some(reason) = label(&s.labels, "reason") {
                    *out.quarantine_by_reason
                        .entry(reason.to_string())
                        .or_insert(0.0) += s.value;
                }
            }
            "deblob_candidate_promotions_total" => {
                if let Some(result) = label(&s.labels, "result") {
                    *out.candidate_promotions_by_result
                        .entry(result.to_string())
                        .or_insert(0.0) += s.value;
                }
            }
            "deblob_candidates_active" => out.candidates_active = Some(s.value),
            "deblob_relay_records_total" => out.relay_records_total = Some(s.value),
            "deblob_tag_latency_seconds_count" => out.tag_latency_count = Some(s.value),
            "deblob_tag_latency_seconds_sum" => out.tag_latency_sum_seconds = Some(s.value),
            _ => {}
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_METRICS: &str = "\
# HELP deblob_messages_total Total messages classified on the hot path, by fate.
# TYPE deblob_messages_total counter
deblob_messages_total{fate=\"known\"} 120
deblob_messages_total{fate=\"provisional\"} 30
deblob_messages_total{fate=\"known\"} 5
deblob_quarantine_records_total{reason=\"duplicate_key\"} 2
deblob_candidate_promotions_total{result=\"success\"} 3
deblob_candidates_active 7
deblob_relay_records_total 155
deblob_tag_latency_seconds_count 155
deblob_tag_latency_seconds_sum 0.42
deblob_unrelated_metric_no_labels 999
";

    #[test]
    fn parses_labeled_and_unlabeled_lines_and_skips_comments() {
        let samples = parse_prometheus_text(SAMPLE_METRICS);
        // 10 data lines (2 `#`-prefixed HELP/TYPE lines skipped).
        assert_eq!(samples.len(), 10);

        let first = &samples[0];
        assert_eq!(first.name, "deblob_messages_total");
        assert_eq!(
            first.labels,
            vec![("fate".to_string(), "known".to_string())]
        );
        assert_eq!(first.value, 120.0);

        let unlabeled = samples
            .iter()
            .find(|s| s.name == "deblob_candidates_active")
            .expect("unlabeled sample present");
        assert!(unlabeled.labels.is_empty());
        assert_eq!(unlabeled.value, 7.0);
    }

    #[test]
    fn parse_prometheus_text_ignores_blank_lines() {
        let samples = parse_prometheus_text("\n\n  \nfoo 1\n\n");
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].name, "foo");
    }

    #[test]
    fn extract_deblob_counters_sums_duplicate_label_combinations() {
        let samples = parse_prometheus_text(SAMPLE_METRICS);
        let counters = extract_deblob_counters(&samples);

        // Two `fate="known"` lines in the fixture sum to 125 — proves this
        // buckets by label value rather than overwriting.
        assert_eq!(counters.messages_by_fate.get("known"), Some(&125.0));
        assert_eq!(counters.messages_by_fate.get("provisional"), Some(&30.0));
        assert_eq!(
            counters.quarantine_by_reason.get("duplicate_key"),
            Some(&2.0)
        );
        assert_eq!(
            counters.candidate_promotions_by_result.get("success"),
            Some(&3.0)
        );
        assert_eq!(counters.candidates_active, Some(7.0));
        assert_eq!(counters.relay_records_total, Some(155.0));
        assert_eq!(counters.tag_latency_count, Some(155.0));
        assert_eq!(counters.tag_latency_sum_seconds, Some(0.42));
    }

    #[test]
    fn extract_deblob_counters_ignores_unknown_metrics() {
        let samples = parse_prometheus_text(SAMPLE_METRICS);
        let counters = extract_deblob_counters(&samples);
        // `deblob_unrelated_metric_no_labels` must not appear anywhere —
        // proven by construction: `DeblobCounters` has no field it could
        // land in, so this test only needs to confirm parsing didn't
        // panic/error on it (already implied by the assertions above
        // succeeding) plus that every OTHER known field is still correct.
        assert_eq!(counters.candidates_active, Some(7.0));
    }
}
