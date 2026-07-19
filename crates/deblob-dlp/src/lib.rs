//! `deblob-dlp` — the redact-before-store DLP layer for the troubleshooting
//! sample store (joint design `dc-samples-dlp-1907`, Stage 1).
//!
//! PURE, deterministic, no I/O. Operates ONLY on an already-parsed, bounded
//! `serde_json::Value` (never raw text — the caller uses the same bounded
//! parser the hot path uses). Redacts by FINDING TYPE, not uniformly:
//!
//!   * a **sensitive field NAME** → its entire value/subtree is replaced with a
//!     visible marker (never recursed into — nested names/lengths leak);
//!   * a **secret/PII in a scalar** → the whole scalar is replaced;
//!   * a **sensitive-looking dynamic KEY** (an email/token/high-entropy map
//!     key that is itself data) → the key is replaced with a PER-SAMPLE,
//!     non-linkable placeholder (never a stable hash);
//!   * **too many findings / unparseable** → the whole sample is DROPPED
//!     (`RedactionOutcome::dropped`), so the caller stores nothing.
//!
//! Detectors are LAYERED risk-reduction, never a completeness proof: a store
//! built from this output must still be treated as potentially sensitive
//! (Hermes review §4). Markers are visible strings, never type-preserving
//! substitutes (`0`/`false`) the console could mistake for real values.

use serde_json::Value;

mod detectors;
pub use detectors::{classify_scalar, is_sensitive_key_data, sensitive_field_name, ScalarClass};

/// Detector-set version, stored with every sample so a reader can re-run a
/// newer detector and know what produced the stored form.
pub const DLP_VERSION: &str = "deblob-dlp-v1";

/// Visible redaction markers (never `0`/`false`/`null` that could read as data).
pub const MARK_SENSITIVE_NAME: &str = "\u{2588} REDACTED (sensitive field name) \u{2588}";
pub const MARK_SECRET: &str = "\u{2588} REDACTED (secret) \u{2588}";
pub const MARK_PII: &str = "\u{2588} REDACTED (pii) \u{2588}";

#[derive(Debug, Clone, Copy)]
pub struct DlpConfig {
    /// Drop the whole sample once this many findings accrue (a pathological /
    /// secret-dense document is safer discarded than partially redacted).
    pub max_findings: u32,
    /// Drop if the tree is deeper than this (defense-in-depth beyond the
    /// bounded parser).
    pub max_depth: u32,
}

impl Default for DlpConfig {
    fn default() -> Self {
        Self { max_findings: 40, max_depth: 32 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DropReason {
    ExcessiveFindings,
    TooDeep,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct RedactionCounts {
    pub sensitive_key: u32,
    pub secret_pattern: u32,
    pub pii_pattern: u32,
    pub redacted_dynamic_key: u32,
}

impl RedactionCounts {
    fn total(&self) -> u32 {
        self.sensitive_key + self.secret_pattern + self.pii_pattern + self.redacted_dynamic_key
    }
}

/// The result of redacting one document. When `dropped` is `Some`, the caller
/// MUST store nothing (fail-closed for confidentiality).
#[derive(Debug, Clone)]
pub struct RedactionOutcome {
    pub document: Value,
    pub counts: RedactionCounts,
    pub dropped: Option<DropReason>,
}

struct Ctx<'a> {
    cfg: &'a DlpConfig,
    counts: RedactionCounts,
    dynamic_key_seq: u32,
    dropped: Option<DropReason>,
}

impl Ctx<'_> {
    fn next_key_placeholder(&mut self) -> String {
        self.dynamic_key_seq += 1;
        // Per-sample, non-linkable, non-hashed (a stable hash preserves
        // cross-sample linkability and is brute-forceable for low-entropy ids).
        format!("[REDACTED_KEY_{}]", self.dynamic_key_seq)
    }
    fn check_budget(&mut self) {
        if self.dropped.is_none() && self.counts.total() > self.cfg.max_findings {
            self.dropped = Some(DropReason::ExcessiveFindings);
        }
    }
}

/// Redact a bounded JSON document. Never panics; on any structural limit it
/// sets `dropped` and the caller stores nothing.
pub fn redact(value: &Value, cfg: &DlpConfig) -> RedactionOutcome {
    let mut ctx = Ctx { cfg, counts: RedactionCounts::default(), dynamic_key_seq: 0, dropped: None };
    let document = redact_value(value, &mut ctx, 0);
    RedactionOutcome { document, counts: ctx.counts, dropped: ctx.dropped }
}

fn redact_value(v: &Value, ctx: &mut Ctx, depth: u32) -> Value {
    if depth > ctx.cfg.max_depth {
        if ctx.dropped.is_none() {
            ctx.dropped = Some(DropReason::TooDeep);
        }
        return Value::Null;
    }
    match v {
        Value::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (k, val) in map {
                if sensitive_field_name(k) {
                    // Replace the ENTIRE subtree — never recurse into it.
                    ctx.counts.sensitive_key += 1;
                    ctx.check_budget();
                    out.insert(k.clone(), Value::String(MARK_SENSITIVE_NAME.to_string()));
                    continue;
                }
                // The KEY itself may be data (an email/token/high-entropy id).
                let out_key = if is_sensitive_key_data(k) {
                    ctx.counts.redacted_dynamic_key += 1;
                    ctx.check_budget();
                    ctx.next_key_placeholder()
                } else {
                    k.clone()
                };
                out.insert(out_key, redact_value(val, ctx, depth + 1));
            }
            Value::Object(out)
        }
        Value::Array(arr) => {
            Value::Array(arr.iter().map(|e| redact_value(e, ctx, depth + 1)).collect())
        }
        Value::String(s) => match classify_scalar(s) {
            ScalarClass::Secret => {
                ctx.counts.secret_pattern += 1;
                ctx.check_budget();
                Value::String(MARK_SECRET.to_string())
            }
            ScalarClass::Pii => {
                ctx.counts.pii_pattern += 1;
                ctx.check_budget();
                Value::String(MARK_PII.to_string())
            }
            ScalarClass::Clean => Value::String(s.clone()),
        },
        // Numbers / bools / null carry no string secret; kept as-is. (A card in
        // a JSON *number* has already lost formatting; deblob treats numeric
        // magnitude as non-sensitive — see the value-profile design.)
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn red(v: Value) -> RedactionOutcome {
        redact(&v, &DlpConfig::default())
    }

    #[test]
    fn sensitive_field_name_replaces_whole_subtree() {
        let out = red(json!({
            "user": "alice",
            "private_key": {"pem": "-----BEGIN...", "bits": 2048}
        }));
        assert_eq!(out.document["user"], json!("alice"));
        assert_eq!(out.document["private_key"], json!(MARK_SENSITIVE_NAME));
        // Never recursed: the nested keys must not appear.
        assert!(out.document["private_key"].get("bits").is_none());
        assert_eq!(out.counts.sensitive_key, 1);
        assert!(out.dropped.is_none());
    }

    #[test]
    fn secret_values_in_scalars_are_redacted() {
        let out = red(json!({
            "note": "deploy log line",
            "aws": "REDACTED_AWS_CANARY",
            "jwt": "REDACTED_JWT_CANARY",
            "gh": "REDACTED_GH_CANARY",
        }));
        assert_eq!(out.document["note"], json!("deploy log line"));
        assert_eq!(out.document["aws"], json!(MARK_SECRET));
        assert_eq!(out.document["jwt"], json!(MARK_SECRET));
        assert_eq!(out.document["gh"], json!(MARK_SECRET));
        assert_eq!(out.counts.secret_pattern, 3);
    }

    #[test]
    fn email_is_pii() {
        let out = red(json!({"contact": "alice@example.com", "city": "Gdansk"}));
        assert_eq!(out.document["contact"], json!(MARK_PII));
        assert_eq!(out.document["city"], json!("Gdansk"));
        assert_eq!(out.counts.pii_pattern, 1);
    }

    #[test]
    fn credit_card_luhn_is_secret_but_random_digits_are_not() {
        // 4111 1111 1111 1111 is a canonical Luhn-valid test card.
        let out = red(json!({"pan": "4111 1111 1111 1111", "qty": "1234567890123"}));
        assert_eq!(out.document["pan"], json!(MARK_SECRET));
        assert_eq!(out.document["qty"], json!("1234567890123")); // fails Luhn -> kept
    }

    #[test]
    fn dynamic_key_that_is_an_email_is_redacted() {
        let out = red(json!({"alice@example.com": {"status": "active"}}));
        // The email key is replaced with a per-sample placeholder; its value
        // subtree is still walked.
        let (k, v) = out.document.as_object().unwrap().iter().next().unwrap();
        assert_eq!(k, "[REDACTED_KEY_1]");
        assert_eq!(v["status"], json!("active"));
        assert_eq!(out.counts.redacted_dynamic_key, 1);
    }

    #[test]
    fn high_entropy_secret_but_low_entropy_word_kept() {
        let out = red(json!({
            "token_blob": "9f8e7d6c5b4a39281706f5e4d3c2b1a09f8e7d6c5b4a3928",
            "greeting": "hello world this is fine"
        }));
        assert_eq!(out.document["token_blob"], json!(MARK_SECRET)); // 48 hex chars
        assert_eq!(out.document["greeting"], json!("hello world this is fine"));
    }

    #[test]
    fn excessive_findings_drops_whole_sample() {
        let cfg = DlpConfig { max_findings: 2, max_depth: 32 };
        let out = redact(
            &json!({"a": "a@x.com", "b": "b@x.com", "c": "c@x.com", "d": "d@x.com"}),
            &cfg,
        );
        assert_eq!(out.dropped, Some(DropReason::ExcessiveFindings));
    }

    #[test]
    fn clean_document_passes_through_untouched() {
        let doc = json!({"region": "westeurope", "count": 42, "active": true, "tags": ["a", "b"]});
        let out = red(doc.clone());
        assert_eq!(out.document, doc);
        assert_eq!(out.counts, RedactionCounts::default());
        assert!(out.dropped.is_none());
    }

    // Canary corpus: every entry MUST be caught. Guards against detector
    // regressions (Hermes review: maintain a regression corpus).
    #[test]
    fn canary_corpus_all_caught() {
        let canaries = [
            "REDACTED_PEM_CANARY\nMIIEpAIBAAKCAQEA",
            "REDACTED_SSH_CANARY user@host",
            "Bearer abcdef0123456789abcdef",
            "REDACTED_SLACK_CANARY",
            "REDACTED_GOOGLE_CANARY",
            "REDACTED_STRIPE_CANARY",
            "REDACTED_DSN_CANARY",
            "REDACTED_JWT2_CANARY",
        ];
        for c in canaries {
            let out = red(json!({"field": c}));
            assert_eq!(
                out.document["field"],
                json!(MARK_SECRET),
                "canary NOT caught: {c}"
            );
        }
    }
}
