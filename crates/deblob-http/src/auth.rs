//! Constant-time bearer-token authentication for the HTTP ingest listener
//! (spec §4/§8): when configured, `POST /ingest` requires
//! `Authorization: Bearer <token>`, compared in constant time so a timing
//! side-channel can't be used to brute-force the token byte-by-byte.
//! Mirrors `deblob::api::auth::SecretToken` — the same pattern, applied to
//! the ingest path instead of the management API.

use std::fmt;
use std::sync::Arc;

use axum::http::header::AUTHORIZATION;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use subtle::ConstantTimeEq;

/// The ingest listener's shared bearer token, held as bytes so comparison
/// never has to re-derive them from a `String` on the hot path of every
/// request. `Debug` is hand-implemented to redact the value — this must
/// never be logged, even accidentally via a `{:?}` on a containing struct
/// (see [`crate::proxy::HttpProxyCfg`]'s own `#[derive(Debug)]`).
#[derive(Clone)]
pub struct IngestToken(Arc<[u8]>);

impl IngestToken {
    pub fn new(token: impl AsRef<str>) -> Self {
        Self(Arc::from(token.as_ref().as_bytes()))
    }

    /// Constant-time comparison against a presented token. Lengths are
    /// compared first (a cheap, non-secret-dependent check — token length
    /// isn't sensitive the way its contents are) so `subtle::ConstantTimeEq`
    /// is only ever asked to compare equal-length slices, which is the
    /// precondition its `ct_eq` requires.
    pub(crate) fn matches(&self, presented: &[u8]) -> bool {
        self.0.len() == presented.len() && bool::from(self.0.ct_eq(presented))
    }
}

impl fmt::Debug for IngestToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("IngestToken").field(&"<redacted>").finish()
    }
}

fn unauthorized() -> Box<Response> {
    Box::new((StatusCode::UNAUTHORIZED, "missing or invalid bearer token").into_response())
}

/// Enforces the ingest bearer-token requirement: a request without a valid
/// `Authorization: Bearer <token>` header (missing, malformed, or wrong
/// token) is rejected with 401 — a bounded error body that never echoes
/// the presented or expected token. Matches the `Result<(), Box<Response>>`
/// shape of [`crate::limits`]'s checks, so `proxy::ingest_handler` can
/// short-circuit on it the same way.
pub(crate) fn check_ingest_bearer(
    headers: &HeaderMap,
    token: &IngestToken,
) -> Result<(), Box<Response>> {
    let presented = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));

    match presented {
        Some(candidate) if token.matches(candidate.as_bytes()) => Ok(()),
        _ => Err(unauthorized()),
    }
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    #[test]
    fn debug_redacts_the_token_value() {
        let token = IngestToken::new("super-secret-value");
        let rendered = format!("{token:?}");
        assert!(!rendered.contains("super-secret-value"));
        assert!(rendered.contains("redacted"));
    }

    #[test]
    fn matches_accepts_only_the_exact_token() {
        let token = IngestToken::new("secret123");
        assert!(token.matches(b"secret123"));
        assert!(!token.matches(b"wrong"));
        assert!(!token.matches(b"secret1234"));
        assert!(!token.matches(b""));
    }

    #[test]
    fn check_ingest_bearer_accepts_valid_header() {
        let token = IngestToken::new("secret123");
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer secret123"));
        assert!(check_ingest_bearer(&headers, &token).is_ok());
    }

    #[test]
    fn check_ingest_bearer_rejects_missing_header() {
        let token = IngestToken::new("secret123");
        let headers = HeaderMap::new();
        let response = check_ingest_bearer(&headers, &token).unwrap_err();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn check_ingest_bearer_rejects_wrong_token() {
        let token = IngestToken::new("secret123");
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer wrong"));
        let response = check_ingest_bearer(&headers, &token).unwrap_err();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn check_ingest_bearer_rejects_malformed_header() {
        let token = IngestToken::new("secret123");
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("secret123"));
        let response = check_ingest_bearer(&headers, &token).unwrap_err();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
