//! Fail-closed capture of redacted troubleshooting samples (joint design
//! `dc-samples-dlp-1907`, Stage 2). Runs OFF the hot path (discovery consumer),
//! only when enabled for a TRUSTED source, and is best-effort for availability
//! but FAIL-CLOSED for confidentiality: any parse/DLP/size problem stores
//! nothing, never a raw payload.

use deblob_core::id::{CandidateId, SourceId};
use deblob_core::ports::SampleRecord;
use deblob_dlp::{redact, DlpConfig, DLP_VERSION};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Capture policy (`[samples]`).
#[derive(Debug, Clone)]
pub struct SampleCaptureCfg {
    pub enabled: bool,
    /// Trusted source strings authorized for capture (the relay-bound source
    /// identity, NEVER a producer-controlled header — source-spoof).
    pub capture_sources: Vec<String>,
    /// Reject a raw payload larger than this before parsing (bound the DLP).
    pub max_input_bytes: usize,
    /// Omit a sample whose REDACTED, serialized form exceeds this (never
    /// byte-truncate JSON — that can bisect a secret / emit invalid JSON).
    pub max_sample_bytes: usize,
    pub dlp: DlpConfig,
}

impl SampleCaptureCfg {
    pub fn source_allowed(&self, source: &str) -> bool {
        self.enabled && self.capture_sources.iter().any(|s| s == source)
    }
}

/// Deterministic, replay-idempotent sample id from the source coordinate — an
/// at-least-once consumer re-reading the same offset produces the SAME id, so
/// the store dedups it rather than spending the per-candidate budget on dupes.
fn sample_id(source_id: &SourceId, topic: &str, partition: i32, offset: i64) -> String {
    let mut h = Sha256::new();
    h.update(b"deblob-sample-v1\0");
    h.update(source_id.as_str().as_bytes());
    h.update([0]);
    h.update(topic.as_bytes());
    h.update([0]);
    h.update(partition.to_le_bytes());
    h.update(offset.to_le_bytes());
    let d: [u8; 32] = h.finalize().into();
    let mut s = String::with_capacity(4 + 32);
    s.push_str("smp_");
    for b in &d[..16] {
        s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((b & 0xf) as u32, 16).unwrap());
    }
    s
}

/// Build a redacted [`SampleRecord`], or `None` (store nothing) on any
/// confidentiality-relevant condition: source not authorized, oversize input,
/// non-JSON payload, DLP drop, or an over-budget redacted result.
///
/// `candidate_id` MUST be the RESOLVED id from `IngestOutcome::Ingested`, never
/// the pre-cluster `DiscoveryMsg.cand_id`.
#[allow(clippy::too_many_arguments)]
pub fn build_sample(
    cfg: &SampleCaptureCfg,
    source: &str,
    topic: &str,
    partition: i32,
    offset: i64,
    candidate_id: &CandidateId,
    raw_payload: &[u8],
    captured_at_ms: i64,
) -> Option<SampleRecord> {
    if !cfg.source_allowed(source) {
        return None;
    }
    if raw_payload.len() > cfg.max_input_bytes {
        return None; // fail-closed: don't even parse an oversize payload
    }
    // Parse to a JSON value (serde_json enforces a recursion limit, bounding
    // depth). A non-JSON / malformed payload stores nothing.
    let value: Value = serde_json::from_slice(raw_payload).ok()?;

    let outcome = redact(&value, &cfg.dlp);
    if outcome.dropped.is_some() {
        return None; // DLP dropped the whole sample -> store nothing
    }

    // Serialize the REDACTED document and enforce a size cap by OMISSION, never
    // by byte-truncating JSON (Hermes review §2).
    let doc_bytes = serde_json::to_vec(&outcome.document).ok()?;
    if doc_bytes.len() > cfg.max_sample_bytes {
        return None;
    }
    let redaction_counts = serde_json::to_value(&outcome.counts).ok()?;
    let source_id = SourceId::from_source(source);

    Some(SampleRecord {
        sample_id: sample_id(&source_id, topic, partition, offset),
        source_id: source_id.as_str().to_string(),
        candidate_id: candidate_id.clone(),
        captured_at_ms,
        dlp_version: DLP_VERSION.to_string(),
        redaction_counts,
        truncated: false,
        document: outcome.document,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(sources: &[&str]) -> SampleCaptureCfg {
        SampleCaptureCfg {
            enabled: true,
            capture_sources: sources.iter().map(|s| s.to_string()).collect(),
            max_input_bytes: 65536,
            max_sample_bytes: 8192,
            dlp: DlpConfig::default(),
        }
    }
    fn cand() -> CandidateId {
        CandidateId::from_digest(&[1u8; 32])
    }

    #[test]
    fn unauthorized_source_captures_nothing() {
        let s = build_sample(
            &cfg(&["events.grid"]),
            "events.secret",
            "t",
            0,
            0,
            &cand(),
            br#"{"a":1}"#,
            0,
        );
        assert!(s.is_none());
    }

    #[test]
    fn redacts_before_storing() {
        let s = build_sample(
            &cfg(&["events.grid"]),
            "events.grid",
            "t",
            0,
            5,
            &cand(),
            br#"{"price":42,"api_key":"REDACTED_AWS_CANARY"}"#,
            123,
        )
        .expect("captured");
        assert_eq!(s.document["price"], serde_json::json!(42));
        // Sensitive field name -> subtree replaced, never the raw key value.
        assert_ne!(s.document["api_key"], serde_json::json!("REDACTED_AWS_CANARY"));
        assert_eq!(s.captured_at_ms, 123);
        assert!(s.sample_id.starts_with("smp_"));
    }

    #[test]
    fn same_coordinate_yields_same_sample_id() {
        let a = build_sample(&cfg(&["s"]), "s", "t", 2, 9, &cand(), br#"{"a":1}"#, 1).unwrap();
        let b = build_sample(&cfg(&["s"]), "s", "t", 2, 9, &cand(), br#"{"a":1}"#, 999).unwrap();
        assert_eq!(a.sample_id, b.sample_id, "replay must be idempotent");
    }

    #[test]
    fn non_json_payload_captures_nothing() {
        assert!(build_sample(&cfg(&["s"]), "s", "t", 0, 0, &cand(), b"not json", 0).is_none());
    }

    #[test]
    fn oversize_input_captures_nothing() {
        let mut c = cfg(&["s"]);
        c.max_input_bytes = 4;
        assert!(build_sample(&c, "s", "t", 0, 0, &cand(), br#"{"a":1}"#, 0).is_none());
    }
}
