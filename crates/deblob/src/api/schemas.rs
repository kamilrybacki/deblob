//! `GET /api/v1/schemas`, `/schemas/{sch_id}`, `/families/*` handlers
//! (spec §8).

use axum::extract::{Path, Query, State};
use axum::Json;
use deblob_core::id::{FamilyId, FamilyVersion, SchemaId};
use deblob_core::ports::{FamilyRecord, SchemaRecord};
use deblob_semantic::domain::{domain_of_source, Domain};
use serde::{Deserialize, Serialize};

use super::{cursor, ApiError, ApiState, DataEnvelope, ListResponse};

/// Default page size when the caller omits `limit`.
const DEFAULT_LIMIT: usize = 50;

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    cursor: Option<String>,
    limit: Option<usize>,
}

/// A schema record with its ingest source surfaced as FIRST-CLASS fields. The
/// source topic is stamped onto `provenance.source` at promote (b23), but was
/// only reachable by digging into the nested `provenance` blob — this view
/// flattens the full record and adds a top-level `source` (the ingest topic,
/// e.g. `events.compute.runpod`) plus its coarse `domain` (e.g. `compute`), so
/// every schema-read surface names where the schema came from. Additive: all
/// existing `SchemaRecord` fields (including `provenance`) still serialize.
#[derive(Debug, Serialize)]
pub struct SchemaView {
    #[serde(flatten)]
    record: SchemaRecord,
    /// Ingest source topic (`provenance.source`); `None` for a pre-b23 record.
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<String>,
    /// Coarse subject domain derived from `source`; `None` if source unknown.
    #[serde(skip_serializing_if = "Option::is_none")]
    domain: Option<&'static str>,
}

impl SchemaView {
    fn of(record: SchemaRecord) -> Self {
        let source =
            crate::api::semantic::provenance_source(&record.provenance).map(str::to_string);
        let domain = source
            .as_deref()
            .and_then(domain_of_source)
            .map(Domain::slug);
        SchemaView {
            record,
            source,
            domain,
        }
    }
}

/// `GET /api/v1/schemas?cursor=&limit=` — cursor pagination over the
/// registry's own `list_schemas`, spec §8. The `cursor` query parameter is
/// opaque base64; `next_cursor` in the response is encoded the same way.
pub async fn list_schemas(
    State(state): State<ApiState>,
    Query(q): Query<ListQuery>,
) -> Result<Json<ListResponse<SchemaView>>, ApiError> {
    let cursor_in = q.cursor.as_deref().map(cursor::decode).transpose()?;
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT);

    let (data, next) = state
        .registry
        .list_schemas(cursor_in, limit)
        .await
        .map_err(ApiError::from_core)?;

    Ok(Json(ListResponse {
        data: data.into_iter().map(SchemaView::of).collect(),
        next_cursor: next.map(|c| cursor::encode(&c)),
    }))
}

/// `GET /api/v1/schemas/{sch_id}` — 200 with the schema, or 404.
pub async fn get_schema(
    State(state): State<ApiState>,
    Path(sch_id): Path<String>,
) -> Result<Json<DataEnvelope<SchemaView>>, ApiError> {
    let id = SchemaId::parse(&sch_id).map_err(|e| ApiError::unprocessable(e.to_string()))?;

    let record = state
        .registry
        .get_schema(&id)
        .await
        .map_err(ApiError::from_core)?
        .ok_or_else(|| ApiError::not_found("schema not found"))?;

    Ok(Json(DataEnvelope {
        data: SchemaView::of(record),
    }))
}

/// `GET /api/v1/schemas/{sch_id}/value-profile` — the durable value-profile
/// snapshot captured for this schema at promotion (joint design
/// `dc-umbrella-signals-1907`, Stage 1). 404 if the schema doesn't exist OR
/// has no value profile (a legacy schema promoted before capture existed).
/// Returns coarse per-leaf evidence only (type counts + numeric-bucket mask),
/// never a raw observed value.
pub async fn get_schema_value_profile(
    State(state): State<ApiState>,
    Path(sch_id): Path<String>,
) -> Result<Json<DataEnvelope<deblob_core::ports::ValueProfileSnapshot>>, ApiError> {
    let id = SchemaId::parse(&sch_id).map_err(|e| ApiError::unprocessable(e.to_string()))?;
    let record = state
        .registry
        .get_schema(&id)
        .await
        .map_err(ApiError::from_core)?
        .ok_or_else(|| ApiError::not_found("schema not found"))?;
    let profile_id = record
        .value_profile_ref
        .ok_or_else(|| ApiError::not_found("schema has no value profile"))?;
    let snapshot = state
        .value_profiles
        .get_value_profile(&profile_id)
        .await
        .map_err(ApiError::from_core)?
        .ok_or_else(|| ApiError::not_found("value profile snapshot not found"))?;
    Ok(Json(DataEnvelope { data: snapshot }))
}

/// `GET /api/v1/families/{fam_id}` — 200 with the family record
/// (`Registry::get_family`, P2-D polish Task 2), or 404 if nothing has ever
/// been published to it.
pub async fn get_family(
    State(state): State<ApiState>,
    Path(fam_id): Path<String>,
) -> Result<Json<DataEnvelope<FamilyRecord>>, ApiError> {
    let id = FamilyId::parse(&fam_id).map_err(|e| ApiError::unprocessable(e.to_string()))?;

    let record = state
        .registry
        .get_family(&id)
        .await
        .map_err(ApiError::from_core)?
        .ok_or_else(|| ApiError::not_found("family not found"))?;

    Ok(Json(DataEnvelope { data: record }))
}

/// `GET /api/v1/families/{fam_id}/versions` — 200 with every version ever
/// published to the family (`Registry::list_family_versions`), or 404 if
/// the family itself doesn't exist. The existence check goes through
/// `get_family` first so an unknown family is a 404, not an empty-but-200
/// list (versions are never legitimately empty for a family that exists —
/// see `Registry::list_family_versions`'s contiguity invariant).
pub async fn get_family_versions(
    State(state): State<ApiState>,
    Path(fam_id): Path<String>,
) -> Result<Json<DataEnvelope<Vec<FamilyVersion>>>, ApiError> {
    let id = FamilyId::parse(&fam_id).map_err(|e| ApiError::unprocessable(e.to_string()))?;

    state
        .registry
        .get_family(&id)
        .await
        .map_err(ApiError::from_core)?
        .ok_or_else(|| ApiError::not_found("family not found"))?;

    let versions = state
        .registry
        .list_family_versions(&id)
        .await
        .map_err(ApiError::from_core)?;

    Ok(Json(DataEnvelope { data: versions }))
}
