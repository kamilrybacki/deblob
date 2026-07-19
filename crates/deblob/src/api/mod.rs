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
pub mod semantic;
pub mod sources;
pub mod stream;
pub mod umbrellas;

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{middleware, Json, Router};
use deblob_core::ports::{EvidenceStore, Registry, SourceRegistry, ValueProfileStore};
use deblob_redis::health::HealthGate;
use deblob_semantic::Registries;
use deblob_umbrella::store::UmbrellaStore;
use serde::Serialize;

pub use auth::SecretToken;

use crate::metrics::Metrics;
use crate::promote::Promoter;
use crate::semantic_store::SemanticStore;

/// Shared state for every management-API handler.
#[derive(Clone)]
pub struct ApiState {
    pub registry: Arc<dyn Registry>,
    pub evidence: Arc<dyn EvidenceStore>,
    pub health: HealthGate,
    pub token: SecretToken,
    pub promoter: Arc<dyn Promoter>,
    pub metrics: Arc<Metrics>,
    /// Append-only semantic-revision store (P2-D Task 5/6). `Arc`-wrapped
    /// like every other injected dependency here — see
    /// `crate::semantic_store::SemanticStore`.
    pub semantic: Arc<dyn SemanticStore>,
    /// Governance-registered `canonical_field_id`/`canonical_event_type_id`
    /// vocabularies (Task 2's `Registries`, deliberately empty by default —
    /// no registration endpoint exists yet; see `deblob_semantic::vocab`).
    /// `Arc`-wrapped so cloning `ApiState` per-request never deep-copies the
    /// underlying `BTreeSet`s.
    pub semantic_registries: Arc<Registries>,
    /// Gold-tier umbrella-schema governance store (read + reject surface
    /// only here — promotion happens via the controller/bundle path, not a
    /// raw endpoint). `Arc<dyn UmbrellaStore>`, same trait-object pattern
    /// as every other injected dependency on this struct.
    pub umbrellas: Arc<dyn UmbrellaStore>,
    /// Durable data-source registry (spec §9 lineage): every distinct source
    /// observed gets a stable `src_` id, surfaced via `GET /api/v1/sources`
    /// and populated off-path by `POST /api/v1/sources/reconcile`. Same
    /// `Arc<dyn …>` injection pattern as every other store here.
    pub sources: Arc<dyn SourceRegistry>,
    /// Durable value-profile sidecar store (joint design
    /// `dc-umbrella-signals-1907`, Stage 1). Read surface: `GET
    /// /api/v1/schemas/{id}/value-profile`. Populated at promotion by the
    /// `Promoter` (`with_value_profiles`), never on the hot path.
    pub value_profiles: Arc<dyn ValueProfileStore>,
    /// `[umbrella].enforce_value_guard` (joint design dc-umbrella-signals-1907,
    /// Stage 4): when `true`, `propose_umbrellas` SUPPRESSES an auto-proposal
    /// with any `CONTRADICTORY` field; when `false` (default) the guard runs in
    /// shadow (logged, never enforced).
    pub enforce_value_guard: bool,
    /// `[umbrella].min_value_support` — minimum numeric observations before a
    /// leaf's bucket mask may drive a `CONTRADICTORY` guard verdict.
    pub umbrella_min_support: u64,
    /// Live-stream tap (Stage L1, payload-free): `GET /api/v1/stream`
    /// subscribes a fresh `Receiver` from this per SSE connection
    /// (`Sender::subscribe`, cheap and `Clone`-free — the `Sender` itself
    /// is what's cloned onto `ApiState`). The SAME `Sender` `crate::serve::
    /// serve` also clones into `deblob_kafka::RelayCfg::stream_tx`, so
    /// every hot-path `StreamEvent` the relay broadcasts reaches every
    /// currently-subscribed SSE client.
    pub stream_tx: tokio::sync::broadcast::Sender<deblob_kafka::StreamEvent>,
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

    /// Task 6: a well-formed request that is rejected because required
    /// caller-supplied context is missing (e.g. `reason`/`reason_code` on a
    /// real semantic-assertion change) or malformed (e.g. an unparseable
    /// `If-Match` header) — distinct from `unprocessable` (`422`, an
    /// unknown/invalid controlled vocabulary token or path).
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "bad_request", message)
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

    /// Maps `deblob_core::revision::SemError` (Task 5's append-only
    /// semantic-revision store) onto the HTTP contract in the brief's §4:
    /// `MissingReason` → `400` (a REAL change attempted with no/empty
    /// `reason` — decided atomically by `SEM_APPEND_SCRIPT` itself, inside
    /// `api::semantic::put_semantic`'s single `append_revision` call, never
    /// by a separate Rust-side pre-check); `EtagConflict` → `409`
    /// (stale/missing `If-Match` on a real change); `StoreUnavailable`/
    /// `Corrupt` → `503`, a downstream-availability/data-integrity problem,
    /// never a caller mistake.
    pub fn from_sem(err: deblob_core::revision::SemError) -> Self {
        use deblob_core::revision::SemError;
        match &err {
            SemError::MissingReason => Self::bad_request(err.to_string()),
            SemError::EtagConflict { .. } => Self::conflict(err.to_string()),
            SemError::StoreUnavailable(_) | SemError::Corrupt(_) => {
                Self::unavailable(err.to_string())
            }
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

/// `GET /metrics` — Prometheus text exposition (version 0.0.4) of
/// `state.metrics`'s registry (spec §11). Unauthenticated, like `/healthz`/
/// `/readyz`, so scrapers don't need a credential.
async fn metrics(State(state): State<ApiState>) -> Response {
    match state.metrics.gather_text() {
        Ok(body) => (
            StatusCode::OK,
            [("content-type", "text/plain; version=0.0.4")],
            body,
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to encode metrics: {e}"),
        )
            .into_response(),
    }
}

/// Builds the management API router. Callers (Task 18's `main.rs`) are
/// responsible for binding it to a **separate** listen address from the
/// ingest hot path (spec §8).
pub fn router(state: ApiState) -> Router {
    let authenticated = Router::new()
        .route("/schemas", get(schemas::list_schemas))
        .route("/schemas/{sch_id}", get(schemas::get_schema))
        .route(
            "/schemas/{sch_id}/value-profile",
            get(schemas::get_schema_value_profile),
        )
        .route(
            "/schemas/{sch_id}/semantic",
            get(semantic::get_semantic).put(semantic::put_semantic),
        )
        .route(
            "/schemas/{sch_id}/semantic/revisions",
            get(semantic::get_semantic_revisions),
        )
        .route(
            "/schemas/{sch_id}/semantic-neighbors",
            get(semantic::get_semantic_neighbors),
        )
        .route("/semantic/{sem_id}", get(semantic::get_schemas_by_semantic))
        .route("/families/{fam_id}", get(schemas::get_family))
        .route(
            "/families/{fam_id}/versions",
            get(schemas::get_family_versions),
        )
        .route("/candidates", get(candidates::list_candidates))
        .route("/candidates/reindex", post(candidates::reindex))
        .route("/candidates/{cand_id}/promote", post(candidates::promote))
        .route("/candidates/{cand_id}/reject", post(candidates::reject))
        .route("/quarantine", get(candidates::quarantine))
        .route("/stream", get(stream::get_stream))
        .route("/umbrellas", get(umbrellas::list_umbrellas))
        .route("/umbrellas/propose", post(umbrellas::propose))
        .route("/umbrellas/{umbrella_id}", get(umbrellas::get_umbrella))
        .route(
            "/umbrellas/{umbrella_id}/transforms",
            get(umbrellas::list_transforms),
        )
        .route("/umbrellas/{umbrella_id}/approve", post(umbrellas::approve))
        .route("/umbrellas/{umbrella_id}/reject", post(umbrellas::reject))
        .route(
            "/umbrellas/{umbrella_id}/lineage",
            get(umbrellas::get_lineage),
        )
        .route(
            "/umbrellas/{umbrella_id}/lineage/fields",
            get(umbrellas::get_field_lineage),
        )
        .route("/sources", get(sources::list_sources))
        .route("/sources/reconcile", post(sources::reconcile))
        .route("/sources/{source_id}", get(sources::get_source))
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
