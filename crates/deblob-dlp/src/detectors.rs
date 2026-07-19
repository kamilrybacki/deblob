//! Deterministic name + value detectors (joint design Stage 1). Regexes are
//! from the `regex` crate (linear-time, no catastrophic backtracking — Hermes
//! review), compiled once. Every detector is layered risk-reduction, never a
//! completeness proof.

use std::sync::OnceLock;

use regex::Regex;
use unicode_normalization::UnicodeNormalization;

/// How a scalar string classifies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarClass {
    Secret,
    Pii,
    Clean,
}

/// Curated substrings that, once a field name is normalized (NFKC + lowercase +
/// alphanumerics only, which also folds camelCase / snake / kebab), mark the
/// field's VALUE as sensitive. Deliberately specific compounds — bare `token`/
/// `auth`/`key`/`session` would over-match `tokenize`/`author`/`keyboard`/
/// `sessionname`. Over-redaction is acceptable (safety outranks shape); gross
/// false positives on common fields are not.
const SENSITIVE_NAME_SUBSTRINGS: &[&str] = &[
    "password", "passwd", "passphrase", "apikey", "apitoken", "accesstoken",
    "refreshtoken", "idtoken", "clientsecret", "privatekey", "signingkey",
    "encryptionkey", "secretkey", "connectionstring", "webhooksecret",
    "sessionid", "sessiontoken", "setcookie", "cookie", "credential",
    "mnemonic", "recoverycode", "passport", "taxid", "bankaccount",
    "routingnumber", "sortcode", "cvv", "iban", "pesel", "ssn", "oauthtoken",
    "bearertoken", "authtoken", "clientsecret", "pwd", "secret",
];

/// Normalize a field name for matching: NFKC, lowercase, keep only
/// alphanumerics (folds `_`/`-`/`.`/space and splits camelCase into a single
/// comparable token stream).
fn normalize_name(name: &str) -> String {
    name.nfkc()
        .flat_map(|c| c.to_lowercase())
        .filter(|c| c.is_alphanumeric())
        .collect()
}

/// Whether a field NAME labels a sensitive value.
pub fn sensitive_field_name(name: &str) -> bool {
    let norm = normalize_name(name);
    if norm.is_empty() {
        return false;
    }
    SENSITIVE_NAME_SUBSTRINGS.iter().any(|s| norm.contains(s))
}

struct Patterns {
    secret: Vec<Regex>,
    email: Regex,
}

fn patterns() -> &'static Patterns {
    static P: OnceLock<Patterns> = OnceLock::new();
    P.get_or_init(|| {
        let s = |re: &str| Regex::new(re).expect("static regex");
        Patterns {
            secret: vec![
                // JWT: three base64url segments, `eyJ` header is a strong signal.
                s(r"\beyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}"),
                // PEM private-key block.
                s(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----"),
                // SSH public/authorized keys (presence implies key material context).
                s(r"\bssh-(rsa|ed25519|dss|ecdsa)\s+[A-Za-z0-9+/]{20,}"),
                // HTTP Authorization: Bearer/Basic <token>.
                s(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9+/._~=-]{8,}"),
                // AWS access key id.
                s(r"\bAKIA[0-9A-Z]{16}\b"),
                // GitHub tokens.
                s(r"\bgh[pousr]_[A-Za-z0-9]{30,}\b"),
                // Stripe secret/publishable/restricted keys.
                s(r"\b(sk|pk|rk)_(live|test)_[A-Za-z0-9]{16,}\b"),
                // Google API key.
                s(r"\bAIza[0-9A-Za-z_-]{35}\b"),
                // Slack tokens.
                s(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b"),
                // URL / connection string carrying inline credentials.
                s(r"[a-z][a-z0-9+.-]*://[^/\s:@]+:[^/\s:@]+@"),
            ],
            email: s(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
        }
    })
}

/// Shannon entropy (bits/byte) — used to gate the generic hex/base64 blob
/// detectors so a low-entropy run (`0000…`, `aaaa…`) is not flagged.
fn shannon(s: &str) -> f64 {
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return 0.0;
    }
    let mut freq = [0u32; 256];
    for &b in bytes {
        freq[b as usize] += 1;
    }
    let len = bytes.len() as f64;
    freq.iter()
        .filter(|&&c| c > 0)
        .map(|&c| {
            let p = c as f64 / len;
            -p * p.log2()
        })
        .sum()
}

fn luhn_ok(digits: &[u8]) -> bool {
    let mut sum = 0u32;
    let mut alt = false;
    for &d in digits.iter().rev() {
        let mut x = d as u32;
        if alt {
            x *= 2;
            if x > 9 {
                x -= 9;
            }
        }
        sum += x;
        alt = !alt;
    }
    sum % 10 == 0
}

/// A high-entropy contiguous hex (>=32) or base64 (>=40) run anywhere in `s`.
fn has_high_entropy_blob(s: &str) -> bool {
    // Scan maximal runs of the relevant char classes.
    let is_hex = |c: char| c.is_ascii_hexdigit();
    let is_b64 = |c: char| c.is_ascii_alphanumeric() || c == '+' || c == '/';
    for (pred, min, ent) in [
        (&is_hex as &dyn Fn(char) -> bool, 32usize, 2.5f64),
        (&is_b64 as &dyn Fn(char) -> bool, 40usize, 3.5f64),
    ] {
        let mut run = String::new();
        let push = |run: &mut String| -> bool {
            let hit = run.len() >= min && shannon(run) >= ent;
            run.clear();
            hit
        };
        for c in s.chars() {
            if pred(c) {
                run.push(c);
            } else if push(&mut run) {
                return true;
            }
        }
        if push(&mut run) {
            return true;
        }
    }
    false
}

/// A Luhn-valid 13–19 digit payment-card number anywhere in `s` (digits may be
/// separated by single spaces or dashes).
fn has_payment_card(s: &str) -> bool {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if !bytes[i].is_ascii_digit() {
            i += 1;
            continue;
        }
        // Consume a digit run allowing single space/dash separators.
        let mut digits = Vec::new();
        let mut j = i;
        while j < bytes.len() {
            let b = bytes[j];
            if b.is_ascii_digit() {
                digits.push(b - b'0');
                j += 1;
            } else if (b == b' ' || b == b'-')
                && j + 1 < bytes.len()
                && bytes[j + 1].is_ascii_digit()
            {
                j += 1;
            } else {
                break;
            }
        }
        if (13..=19).contains(&digits.len()) && luhn_ok(&digits) {
            return true;
        }
        i = j.max(i + 1);
    }
    false
}

/// Classify one scalar string. Search (not full-match) semantics: a secret or
/// PII appearing ANYWHERE in the value redacts the whole scalar.
pub fn classify_scalar(s: &str) -> ScalarClass {
    let p = patterns();
    if p.secret.iter().any(|re| re.is_match(s))
        || has_high_entropy_blob(s)
        || has_payment_card(s)
    {
        return ScalarClass::Secret;
    }
    if p.email.is_match(s) {
        return ScalarClass::Pii;
    }
    ScalarClass::Clean
}

/// Whether an object KEY is itself sensitive DATA (an email/token/high-entropy
/// dynamic-map key), as opposed to a label. Reuses the scalar classifier.
pub fn is_sensitive_key_data(key: &str) -> bool {
    classify_scalar(key) != ScalarClass::Clean
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sensitive_names_fold_case_and_separators() {
        for n in ["password", "apiKey", "api_key", "X-Api-Key", "clientSecret", "private_key", "PWD"] {
            assert!(sensitive_field_name(n), "should be sensitive: {n}");
        }
        for n in ["author", "sessionName", "keyboard", "tokenizer", "region", "count", "status"] {
            assert!(!sensitive_field_name(n), "should NOT be sensitive: {n}");
        }
    }

    #[test]
    fn luhn_validates_test_cards() {
        assert!(has_payment_card("4111 1111 1111 1111"));
        assert!(has_payment_card("pan=4111-1111-1111-1111 exp"));
        assert!(!has_payment_card("1234 5678 9012 3456")); // fails Luhn
    }

    #[test]
    fn entropy_gate_separates_blobs_from_words() {
        assert!(has_high_entropy_blob("9f8e7d6c5b4a39281706f5e4d3c2b1a0")); // 32 hex
        assert!(!has_high_entropy_blob("0000000000000000000000000000000000")); // low entropy
        assert!(!has_high_entropy_blob("this is just some normal english text"));
    }

    #[test]
    fn email_key_is_data() {
        assert!(is_sensitive_key_data("alice@example.com"));
        assert!(!is_sensitive_key_data("region"));
    }
}
