//! `GET /api/v1/schemas`, `/schemas/{sch_id}`, `/families/*` handlers
//! (spec §8).

use axum::extract::{Path, Query, State};
use axum::Json;
use deblob_core::id::SchemaId;
use deblob_core::ports::SchemaRecord;
use serde::Deserialize;

use super::{cursor, ApiError, ApiState, DataEnvelope, ListResponse};

/// Default page size when the caller omits `limit`.
const DEFAULT_LIMIT: usize = 50;

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    cursor: Option<String>,
    limit: Option<usize>,
}

/// `GET /api/v1/schemas?cursor=&limit=` — cursor pagination over the
/// registry's own `list_schemas`, spec §8. The `cursor` query parameter is
/// opaque base64; `next_cursor` in the response is encoded the same way.
pub async fn list_schemas(
    State(state): State<ApiState>,
    Query(q): Query<ListQuery>,
) -> Result<Json<ListResponse<SchemaRecord>>, ApiError> {
    let cursor_in = q.cursor.as_deref().map(cursor::decode).transpose()?;
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT);

    let (data, next) = state
        .registry
        .list_schemas(cursor_in, limit)
        .await
        .map_err(ApiError::from_core)?;

    Ok(Json(ListResponse {
        data,
        next_cursor: next.map(|c| cursor::encode(&c)),
    }))
}

/// `GET /api/v1/schemas/{sch_id}` — 200 with the schema, or 404.
pub async fn get_schema(
    State(state): State<ApiState>,
    Path(sch_id): Path<String>,
) -> Result<Json<DataEnvelope<SchemaRecord>>, ApiError> {
    let id = SchemaId::parse(&sch_id).map_err(|e| ApiError::unprocessable(e.to_string()))?;

    let record = state
        .registry
        .get_schema(&id)
        .await
        .map_err(ApiError::from_core)?
        .ok_or_else(|| ApiError::not_found("schema not found"))?;

    Ok(Json(DataEnvelope { data: record }))
}

/// `GET /api/v1/families/{fam_id}` — the current `Registry` trait (Task 7)
/// exposes no family-lookup method (only `get_schema`, `resolve_structural`,
/// `publish`, `get_alias`, `list_schemas`); rather than invent one here,
/// this returns 501 until a later task adds family-indexed reads to the
/// trait.
pub async fn get_family(Path(_fam_id): Path<String>) -> ApiError {
    ApiError::not_implemented(
        "family lookup is not yet exposed by the Registry trait (see get_family_versions)",
    )
}

/// `GET /api/v1/families/{fam_id}/versions` — same gap as `get_family`.
pub async fn get_family_versions(Path(_fam_id): Path<String>) -> ApiError {
    ApiError::not_implemented("family version listing is not yet exposed by the Registry trait")
}
