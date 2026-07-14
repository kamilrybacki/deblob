//! Bearer-token authentication for the management API (spec §8): every
//! `/api/v1/*` route requires `Authorization: Bearer <token>`, compared in
//! constant time so a timing side-channel can't be used to brute-force the
//! token byte-by-byte.

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::header::AUTHORIZATION;
use axum::middleware::Next;
use axum::response::Response;
use subtle::ConstantTimeEq;

use super::{ApiError, ApiState};

/// The shared management-API bearer token, held as bytes so comparison
/// never has to re-derive them from a `String` on the hot path of every
/// request.
#[derive(Clone)]
pub struct SecretToken(Arc<[u8]>);

impl SecretToken {
    pub fn new(token: impl AsRef<str>) -> Self {
        Self(Arc::from(token.as_ref().as_bytes()))
    }

    /// Constant-time comparison against a presented token. Lengths are
    /// compared first (a cheap, non-secret-dependent check — token length
    /// isn't sensitive the way its contents are) so `subtle::ConstantTimeEq`
    /// is only ever asked to compare equal-length slices, which is the
    /// precondition its `ct_eq` requires.
    fn matches(&self, presented: &[u8]) -> bool {
        self.0.len() == presented.len() && bool::from(self.0.ct_eq(presented))
    }
}

/// Axum middleware: rejects any `/api/v1/*` request that doesn't carry a
/// valid `Authorization: Bearer <token>` header with 401 + the standard
/// error envelope, before the request ever reaches a handler.
pub async fn require_bearer(
    State(state): State<ApiState>,
    req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let presented = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));

    match presented {
        Some(token) if state.token.matches(token.as_bytes()) => Ok(next.run(req).await),
        _ => Err(ApiError::unauthorized("missing or invalid bearer token")),
    }
}
