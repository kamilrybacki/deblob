//! `GET /api/v1/umbrellas`, `GET .../{umbrella_id}`,
//! `GET .../{umbrella_id}/transforms`, `POST .../{umbrella_id}/approve`,
//! `POST .../{umbrella_id}/reject` handlers — the governance surface for
//! gold-tier umbrella schemas (`deblob-umbrella`).
//!
//! Umbrella activation is HITL-only; the controller/SLM may only ever
//! create or update PROVISIONAL umbrellas — promotion to Active is
//! exclusively via the human-triggered `/approve` endpoint.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use deblob_umbrella::store::{StoreError, StoredUmbrella, UmbrellaState};
use deblob_umbrella::types::ChildTransform;
use serde::Deserialize;

use super::{ApiError, ApiState, DataEnvelope, ListResponse};

impl ApiError {
    /// Maps [`deblob_umbrella::store::StoreError`] onto the HTTP contract:
    /// `UmbrellaNotFound` → 404; `BundleMismatch`/`Backend` → 503, mirroring
    /// `ApiError::from_core`'s treatment of a downstream-store failure
    /// rather than a caller mistake (bundle promotion isn't exposed as an
    /// API surface here, so `BundleMismatch` should never actually surface
    /// through these handlers — still mapped defensively).
    fn from_umbrella_store(err: StoreError) -> Self {
        match &err {
            StoreError::UmbrellaNotFound(_) => Self::not_found(err.to_string()),
            StoreError::BundleMismatch { .. } | StoreError::Backend(_) => {
                Self::unavailable(err.to_string())
            }
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ListUmbrellasQuery {
    state: Option<String>,
}

fn parse_umbrella_state(raw: Option<&str>) -> Result<UmbrellaState, ApiError> {
    match raw {
        Some("provisional") => Ok(UmbrellaState::Provisional),
        Some("active") => Ok(UmbrellaState::Active),
        Some("rejected") => Ok(UmbrellaState::Rejected),
        Some(other) => Err(ApiError::unprocessable(format!(
            "invalid state {other:?}: expected \"provisional\", \"active\", or \"rejected\""
        ))),
        None => Err(ApiError::unprocessable(
            "state query parameter is required (provisional|active|rejected)",
        )),
    }
}

/// `GET /api/v1/umbrellas?state=provisional|active|rejected`.
pub async fn list_umbrellas(
    State(state): State<ApiState>,
    Query(q): Query<ListUmbrellasQuery>,
) -> Result<Json<ListResponse<StoredUmbrella>>, ApiError> {
    let umb_state = parse_umbrella_state(q.state.as_deref())?;

    let data = state
        .umbrellas
        .list_umbrellas(umb_state)
        .await
        .map_err(ApiError::from_umbrella_store)?;

    Ok(Json(ListResponse {
        data,
        next_cursor: None,
    }))
}

/// `GET /api/v1/umbrellas/{umbrella_id}` — the `StoredUmbrella` or 404.
pub async fn get_umbrella(
    State(state): State<ApiState>,
    Path(umbrella_id): Path<String>,
) -> Result<Json<DataEnvelope<StoredUmbrella>>, ApiError> {
    let umbrella = state
        .umbrellas
        .get_umbrella(&umbrella_id)
        .await
        .map_err(ApiError::from_umbrella_store)?
        .ok_or_else(|| ApiError::not_found("umbrella not found"))?;

    Ok(Json(DataEnvelope { data: umbrella }))
}

/// `GET /api/v1/umbrellas/{umbrella_id}/transforms`.
pub async fn list_transforms(
    State(state): State<ApiState>,
    Path(umbrella_id): Path<String>,
) -> Result<Json<ListResponse<ChildTransform>>, ApiError> {
    let data = state
        .umbrellas
        .list_transforms(&umbrella_id)
        .await
        .map_err(ApiError::from_umbrella_store)?;

    Ok(Json(ListResponse {
        data,
        next_cursor: None,
    }))
}

/// Request body for `POST /api/v1/umbrellas/{umbrella_id}/approve`. `reason`
/// is required (not optional, unlike `semantic::PutSemanticRequest`'s
/// conditionally-required `reason`) — HITL activation always needs a
/// human-supplied justification, no unconditional/idempotent path exists
/// the way `put_semantic` has one for non-REAL changes.
#[derive(Debug, Deserialize)]
pub struct ApproveRequest {
    pub reason: String,
}

/// `POST /api/v1/umbrellas/{umbrella_id}/approve` — the ONLY path in this
/// service that transitions an umbrella to `Active`. Human-triggered only:
/// requires a non-empty `reason` in the body, mirroring
/// `candidates::promote`'s audited-action style. 404 if the umbrella
/// doesn't exist, 400 if `reason` is empty.
///
/// TODO: re-run verify + promote_bundle atomically — this currently only
/// flips the umbrella's stored state via `UmbrellaStore::set_state`, not
/// the full trust-gate + atomic bundle promotion described in
/// `deblob_umbrella::store`'s docs. That wiring lands in a follow-up task;
/// until then this is a state-flip endpoint gated on human confirmation,
/// not the atomic `promote_bundle` path.
pub async fn approve(
    State(state): State<ApiState>,
    Path(umbrella_id): Path<String>,
    Json(req): Json<ApproveRequest>,
) -> Result<Json<DataEnvelope<StoredUmbrella>>, ApiError> {
    if req.reason.trim().is_empty() {
        return Err(ApiError::bad_request(
            "reason is required to approve an umbrella",
        ));
    }

    state
        .umbrellas
        .get_umbrella(&umbrella_id)
        .await
        .map_err(ApiError::from_umbrella_store)?
        .ok_or_else(|| ApiError::not_found("umbrella not found"))?;

    state
        .umbrellas
        .set_state(&umbrella_id, UmbrellaState::Active)
        .await
        .map_err(ApiError::from_umbrella_store)?;

    let umbrella = state
        .umbrellas
        .get_umbrella(&umbrella_id)
        .await
        .map_err(ApiError::from_umbrella_store)?
        .ok_or_else(|| ApiError::not_found("umbrella not found"))?;

    Ok(Json(DataEnvelope { data: umbrella }))
}

/// `POST /api/v1/umbrellas/{umbrella_id}/reject` — marks the umbrella
/// `Rejected` via `UmbrellaStore::set_state`; 404 if it doesn't exist, 204
/// on success.
pub async fn reject(
    State(state): State<ApiState>,
    Path(umbrella_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    state
        .umbrellas
        .get_umbrella(&umbrella_id)
        .await
        .map_err(ApiError::from_umbrella_store)?
        .ok_or_else(|| ApiError::not_found("umbrella not found"))?;

    state
        .umbrellas
        .set_state(&umbrella_id, UmbrellaState::Rejected)
        .await
        .map_err(ApiError::from_umbrella_store)?;

    Ok(StatusCode::NO_CONTENT)
}
