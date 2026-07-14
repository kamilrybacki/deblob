//! Authenticated management API (axum), spec §8.
//!
//! Listens on a **separate port** from the ingest hot path (wired up in
//! Task 18's `main.rs`) — never reachable from the producer network path.
//! Every `/api/v1/*` route requires `Authorization: Bearer <token>`
//! ([`auth::require_bearer`]); `/healthz`, `/readyz`, `/metrics` are
//! unauthenticated so orchestrators can probe them without a credential.

pub mod auth;
pub mod candidates;
pub mod schemas;

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{middleware, Json, Router};
use deblob_core::ports::{EvidenceStore, Registry};
use deblob_redis::health::HealthGate;
use serde::Serialize;

pub use auth::SecretToken;

use crate::promote::Promoter;

/// Shared state for every management-API handler.
#[derive(Clone)]
pub struct ApiState {
    pub registry: Arc<dyn Registry>,
    pub evidence: Arc<dyn EvidenceStore>,
    pub health: HealthGate,
    pub token: SecretToken,
    pub promoter: Arc<dyn Promoter>,
}

/// Standard error envelope, spec §8: `{"error":{"code","message","details"}}`.
#[derive(Debug, Serialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    code: &'static str,
    message: String,
    details: Vec<String>,
}

/// A handler-returnable error: carries the HTTP status alongside the
/// envelope fields so every failure path (auth, validation, not-found,
/// registry conflict) renders identically.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthorized", message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", message)
    }

    pub fn unprocessable(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "unprocessable_entity",
            message,
        )
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "conflict", message)
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(StatusCode::SERVICE_UNAVAILABLE, "unavailable", message)
    }

    pub fn not_implemented(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_IMPLEMENTED, "not_implemented", message)
    }

    /// Maps a core-layer error onto the HTTP/error-envelope contract in
    /// spec §8: `Conflict`/`ImmutabilityViolation` → 409, `NotFound` → 404,
    /// `PolicyRejected` → 422 (Task 14: a well-formed request against an
    /// existing candidate that hasn't crossed the promotion guards —
    /// distinct from `Conflict`'s state-machine/identity clashes), and
    /// everything else (a registry/evidence-store outage) → 503 rather
    /// than a bare 500, since it's a downstream-availability problem, not
    /// an API bug.
    pub fn from_core(err: deblob_core::error::CoreError) -> Self {
        use deblob_core::error::CoreError;
        match &err {
            CoreError::NotFound => Self::not_found(err.to_string()),
            CoreError::Conflict(_) | CoreError::ImmutabilityViolation(_) => {
                Self::conflict(err.to_string())
            }
            CoreError::PolicyRejected(_) => Self::unprocessable(err.to_string()),
            CoreError::RegistryUnavailable(_) => Self::unavailable(err.to_string()),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = ErrorEnvelope {
            error: ErrorBody {
                code: self.code,
                message: self.message,
                details: Vec::new(),
            },
        };
        (self.status, Json(body)).into_response()
    }
}

/// Generic `{"data": ...}` success envelope used by every non-list handler.
#[derive(Debug, Serialize)]
pub struct DataEnvelope<T: Serialize> {
    pub data: T,
}

/// Generic cursor-paginated list envelope: `{"data": [...], "next_cursor"}`.
#[derive(Debug, Serialize)]
pub struct ListResponse<T: Serialize> {
    pub data: Vec<T>,
    pub next_cursor: Option<String>,
}

/// Opaque cursor encoding: base64 of the registry/evidence-store's own
/// cursor string. The API contract only promises the cursor is opaque —
/// callers must not parse it — so base64 is sufficient; it also protects
/// against literal `Option<String>` cursors that happen to contain
/// characters awkward in a query string.
mod cursor {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine as _;

    use super::ApiError;

    pub fn encode(raw: &str) -> String {
        URL_SAFE_NO_PAD.encode(raw.as_bytes())
    }

    pub fn decode(encoded: &str) -> Result<String, ApiError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| ApiError::unprocessable("invalid cursor"))?;
        String::from_utf8(bytes).map_err(|_| ApiError::unprocessable("invalid cursor"))
    }
}

async fn healthz() -> StatusCode {
    // Always 200 if the process is alive to answer at all — no dependency
    // checks here, that's /readyz's job.
    StatusCode::OK
}

async fn readyz(State(state): State<ApiState>) -> StatusCode {
    if state.health.is_healthy() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// Placeholder body: Task 15 wires up the real `prometheus` registry and
/// instruments the matcher/coldlane/API. This just proves the endpoint
/// exists and answers 200 with a text/plain content type so scrapers don't
/// error out during P1 while metrics are still being built out.
async fn metrics() -> impl IntoResponse {
    (
        [("content-type", "text/plain; version=0.0.4")],
        "# deblob metrics placeholder — Task 15 wires up the real exposition\n",
    )
}

/// Builds the management API router. Callers (Task 18's `main.rs`) are
/// responsible for binding it to a **separate** listen address from the
/// ingest hot path (spec §8).
pub fn router(state: ApiState) -> Router {
    let authenticated = Router::new()
        .route("/schemas", get(schemas::list_schemas))
        .route("/schemas/{sch_id}", get(schemas::get_schema))
        .route("/families/{fam_id}", get(schemas::get_family))
        .route(
            "/families/{fam_id}/versions",
            get(schemas::get_family_versions),
        )
        .route("/candidates", get(candidates::list_candidates))
        .route("/candidates/{cand_id}/promote", post(candidates::promote))
        .route("/candidates/{cand_id}/reject", post(candidates::reject))
        .route("/quarantine", get(candidates::quarantine))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth::require_bearer,
        ));

    Router::new()
        .nest("/api/v1", authenticated)
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .with_state(state)
}
