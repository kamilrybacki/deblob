//! `GET /api/v1/candidates`, `POST .../promote`, `POST .../reject`,
//! `GET /api/v1/quarantine` handlers (spec §8).

use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use deblob_core::id::CandidateId;
use deblob_core::ports::{CandidateRecord, CandidateState};
use serde::{Deserialize, Serialize};

use super::{cursor, ApiError, ApiState, DataEnvelope, ListResponse};
use crate::promote::PromoteRequest;

const DEFAULT_LIMIT: usize = 50;

/// Header used to record who's performing an administrative action, since
/// P1 ships a single shared bearer token rather than per-caller identities
/// (spec §8 only requires "Bearer/API-key auth from env"). Task 14's audit
/// trail still gets a real actor string this way instead of a hardcoded
/// placeholder; a later multi-token/identity task can replace this without
/// changing the `Promoter` contract.
const ACTOR_HEADER: &str = "x-deblob-actor";
const DEFAULT_ACTOR: &str = "api";

/// `pub(crate)`: reused by `super::semantic`'s `put_semantic` handler
/// (Task 6), which needs the exact same actor-from-header behavior
/// `promote` uses — mirrored, not reinvented, per the brief.
pub(crate) fn actor_from_headers(headers: &axum::http::HeaderMap) -> String {
    headers
        .get(ACTOR_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty())
        .unwrap_or(DEFAULT_ACTOR)
        .to_string()
}

#[derive(Debug, Deserialize)]
pub struct ListCandidatesQuery {
    state: Option<String>,
    cursor: Option<String>,
    limit: Option<usize>,
}

fn parse_candidate_state(raw: Option<&str>) -> Result<CandidateState, ApiError> {
    match raw {
        Some("provisional") => Ok(CandidateState::Provisional),
        Some("staged") => Ok(CandidateState::Staged),
        Some(other) => Err(ApiError::unprocessable(format!(
            "invalid state {other:?}: expected \"provisional\" or \"staged\""
        ))),
        None => Err(ApiError::unprocessable(
            "state query parameter is required (provisional|staged)",
        )),
    }
}

/// `GET /api/v1/candidates?state=provisional|staged&cursor=&limit=`.
pub async fn list_candidates(
    State(state): State<ApiState>,
    Query(q): Query<ListCandidatesQuery>,
) -> Result<Json<ListResponse<CandidateRecord>>, ApiError> {
    let cand_state = parse_candidate_state(q.state.as_deref())?;
    let cursor_in = q.cursor.as_deref().map(cursor::decode).transpose()?;
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT);

    let (data, next) = state
        .evidence
        .list_candidates(cand_state, cursor_in, limit)
        .await
        .map_err(ApiError::from_core)?;

    Ok(Json(ListResponse {
        data,
        next_cursor: next.map(|c| cursor::encode(&c)),
    }))
}

/// `POST /api/v1/candidates/{cand_id}/promote` — authenticated, audited
/// (spec §8). Delegates to `Promoter::promote` (Task 14's concrete
/// implementation; this crate only defines the seam, `promote.rs`) and maps
/// its result onto the HTTP contract: `Ok` → 201 + `Location:
/// /api/v1/schemas/{sch_id}` + `{"data": schema}`; `Conflict`/
/// `ImmutabilityViolation` → 409; `NotFound` → 404; anything else `ApiError
/// ::from_core` treats as validation/availability failure.
pub async fn promote(
    State(state): State<ApiState>,
    Path(cand_id): Path<String>,
    headers: axum::http::HeaderMap,
    Json(req): Json<PromoteRequest>,
) -> Result<Response, ApiError> {
    let id = CandidateId::parse(&cand_id).map_err(|e| ApiError::unprocessable(e.to_string()))?;
    let actor = actor_from_headers(&headers);

    let schema = state
        .promoter
        .promote(&id, req, &actor)
        .await
        .map_err(ApiError::from_core)?;

    let location = format!("/api/v1/schemas/{}", schema.schema_id.as_str());
    let mut response = (StatusCode::CREATED, Json(DataEnvelope { data: schema })).into_response();
    response.headers_mut().insert(
        axum::http::header::LOCATION,
        HeaderValue::from_str(&location).map_err(|_| {
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                "bad location",
            )
        })?,
    );
    Ok(response)
}

/// `POST /api/v1/candidates/{cand_id}/reject` — authenticated. Marks the
/// candidate `Rejected` via `EvidenceStore::set_state`; 404 if it doesn't
/// exist, 204 on success.
pub async fn reject(
    State(state): State<ApiState>,
    Path(cand_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let id = CandidateId::parse(&cand_id).map_err(|e| ApiError::unprocessable(e.to_string()))?;

    state
        .evidence
        .get_candidate(&id)
        .await
        .map_err(ApiError::from_core)?
        .ok_or_else(|| ApiError::not_found("candidate not found"))?;

    state
        .evidence
        .set_state(&id, CandidateState::Rejected)
        .await
        .map_err(ApiError::from_core)?;

    Ok(StatusCode::NO_CONTENT)
}

/// Response body for `POST /api/v1/candidates/reindex`.
#[derive(Debug, Serialize)]
pub struct ReindexResponse {
    /// The number of candidate records reindexed by
    /// `EvidenceStore::rebuild_candidate_index`.
    pub reindexed: u64,
}

/// `POST /api/v1/candidates/reindex` — authenticated backfill/repair
/// endpoint for the per-state candidate-listing index (spec §6's
/// `RedisEvidence::rebuild_candidate_index`, previously only reachable as
/// an inherent method with no API surface). Idempotent and always safe to
/// run again — see that method's own docs.
pub async fn reindex(
    State(state): State<ApiState>,
) -> Result<Json<DataEnvelope<ReindexResponse>>, ApiError> {
    let reindexed = state
        .evidence
        .rebuild_candidate_index()
        .await
        .map_err(ApiError::from_core)?;

    Ok(Json(DataEnvelope {
        data: ReindexResponse { reindexed },
    }))
}

#[derive(Debug, Serialize)]
pub struct QuarantineEntry {
    // Placeholder shape: the quarantine stream itself is Kafka-side (spec
    // §8 lists it under the management API, but the quarantine *topic* is
    // built in Task 16). No store exists yet for this endpoint to read
    // from, so it always returns an empty page rather than 501 — an empty
    // quarantine list is a legitimate (if currently permanent) answer, and
    // callers polling this endpoint don't need special-casing once Task 16
    // wires up a real backing store.
}

/// `GET /api/v1/quarantine?cursor=` — placeholder until Task 16 lands the
/// quarantine topic/store; always returns `{"data": [], "next_cursor":
/// null}`.
pub async fn quarantine() -> Json<ListResponse<QuarantineEntry>> {
    Json(ListResponse {
        data: Vec::new(),
        next_cursor: None,
    })
}
