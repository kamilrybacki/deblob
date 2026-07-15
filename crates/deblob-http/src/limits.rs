//! Framing/size hardening checks (spec §4/§6) — the request-smuggling and
//! size-limit guards enforced BEFORE the body is read and BEFORE
//! `HotMatcher::classify` ever sees a byte, so a lying/absent
//! `Content-Length`, an ambiguous `Content-Length`/`Transfer-Encoding`
//! combination, or an oversized header block can never reach the hot
//! path.
//!
//! Every check here returns `Err(Response)` rather than a bespoke error
//! enum: the caller (`proxy::ingest_handler`) short-circuits on the first
//! failing check and returns that response directly, and every check
//! already knows its own correct status code, so there's nothing left for
//! the caller to decide.

use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

const CONTENT_LENGTH: &str = "content-length";
const TRANSFER_ENCODING: &str = "transfer-encoding";

fn bad_request(msg: &'static str) -> Box<Response> {
    Box::new((StatusCode::BAD_REQUEST, msg).into_response())
}

/// The response `to_bytes`-style callers should return when the streamed
/// aggregate cap (or the `Content-Length` precheck) rejects a body.
/// Boxed per `clippy::result_large_err` — `Response` itself is ~128
/// bytes, too large to return unboxed from a `Result`'s `Err` arm.
pub fn payload_too_large() -> Box<Response> {
    Box::new(
        (
            StatusCode::PAYLOAD_TOO_LARGE,
            "request body exceeds the configured limit",
        )
            .into_response(),
    )
}

fn header_fields_too_large() -> Box<Response> {
    Box::new(
        (
            StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
            "request headers exceed the configured limit",
        )
            .into_response(),
    )
}

/// Request-smuggling guard (spec §4, §6): reject a request carrying BOTH
/// `Content-Length` and `Transfer-Encoding`, or more than one
/// `Content-Length` value — the two RFC 7230 §3.3.3 ambiguous-framing
/// shapes a hardened proxy must never forward, since either could let an
/// attacker smuggle a second, hidden request past us to the upstream.
/// Runs on the raw inbound `HeaderMap`, before a single body byte is read.
pub fn check_framing(headers: &HeaderMap) -> Result<(), Box<Response>> {
    let content_length_count = headers.get_all(CONTENT_LENGTH).iter().count();
    if content_length_count > 1 {
        return Err(bad_request("duplicate Content-Length headers"));
    }
    if content_length_count >= 1 && headers.contains_key(TRANSFER_ENCODING) {
        return Err(bad_request(
            "Content-Length and Transfer-Encoding are both present",
        ));
    }
    Ok(())
}

/// Body-size guard, precheck half (spec §4/§6): reject an obviously
/// oversized request before a single body byte is read, when the client
/// sent an honest `Content-Length`. A lying or absent `Content-Length`
/// (e.g. chunked transfer with no length hint) is NOT caught here — that's
/// [`crate::proxy`]'s job via a streamed `axum::body::to_bytes` cap, which
/// bounds the bytes actually read regardless of what any header claims.
pub fn check_content_length(
    headers: &HeaderMap,
    max_body_bytes: usize,
) -> Result<(), Box<Response>> {
    if let Some(value) = headers.get(CONTENT_LENGTH) {
        if let Ok(parsed) = value.to_str().unwrap_or_default().parse::<usize>() {
            if parsed > max_body_bytes {
                return Err(payload_too_large());
            }
        }
    }
    Ok(())
}

/// Header-size guard (spec §4): caps total header count and total header
/// byte weight (names + values). Hyper has already parsed the header
/// block into memory by the time this runs, so this doesn't bound the
/// server's own read buffer — it bounds what deblob is willing to accept
/// as a request shape, and is defense-in-depth alongside any
/// listener-level header limit.
pub fn check_header_limits(
    headers: &HeaderMap,
    max_header_bytes: usize,
    max_header_count: usize,
) -> Result<(), Box<Response>> {
    if headers.len() > max_header_count {
        return Err(header_fields_too_large());
    }
    let total_bytes: usize = headers
        .iter()
        .map(|(name, value)| name.as_str().len() + value.len())
        .sum();
    if total_bytes > max_header_bytes {
        return Err(header_fields_too_large());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    fn headers_from(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            headers.append(
                axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        headers
    }

    #[test]
    fn check_framing_allows_content_length_alone() {
        let headers = headers_from(&[("content-length", "10")]);
        assert!(check_framing(&headers).is_ok());
    }

    #[test]
    fn check_framing_allows_transfer_encoding_alone() {
        let headers = headers_from(&[("transfer-encoding", "chunked")]);
        assert!(check_framing(&headers).is_ok());
    }

    #[test]
    fn check_framing_rejects_content_length_and_transfer_encoding_together() {
        let headers = headers_from(&[("content-length", "10"), ("transfer-encoding", "chunked")]);
        let response = check_framing(&headers).unwrap_err();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn check_framing_rejects_duplicate_content_length() {
        let mut headers = HeaderMap::new();
        headers.append(CONTENT_LENGTH, HeaderValue::from_static("10"));
        headers.append(CONTENT_LENGTH, HeaderValue::from_static("20"));
        let response = check_framing(&headers).unwrap_err();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn check_content_length_rejects_oversize_declared_length() {
        let headers = headers_from(&[("content-length", "1000")]);
        let response = check_content_length(&headers, 100).unwrap_err();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[test]
    fn check_content_length_allows_missing_header() {
        assert!(check_content_length(&HeaderMap::new(), 100).is_ok());
    }

    #[test]
    fn check_header_limits_rejects_over_count() {
        let headers = headers_from(&[("a", "1"), ("b", "2"), ("c", "3")]);
        let response = check_header_limits(&headers, 10_000, 2).unwrap_err();
        assert_eq!(
            response.status(),
            StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE
        );
    }

    #[test]
    fn check_header_limits_rejects_over_byte_weight() {
        let headers = headers_from(&[("x", &"v".repeat(1000))]);
        let response = check_header_limits(&headers, 100, 100).unwrap_err();
        assert_eq!(
            response.status(),
            StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE
        );
    }

    #[test]
    fn check_header_limits_allows_within_bounds() {
        let headers = headers_from(&[("content-type", "application/json")]);
        assert!(check_header_limits(&headers, 10_000, 100).is_ok());
    }
}
