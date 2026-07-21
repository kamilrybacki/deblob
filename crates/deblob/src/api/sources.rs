//! `GET /api/v1/sources`, `GET .../{source_id}`, `POST .../reconcile` — the
//! data-source registry surface (spec §9 lineage). Every distinct source the
//! service has observed gets a stable, content-addressed [`SourceId`] the UI
//! and lineage views can reference instead of a raw Kafka topic string.
//!
//! Registration is NEVER on the hot path: `reconcile` is an off-path backfill
//! that scans the candidate store's `source` provenance field and registers
//! each distinct source it finds — the same posture as the candidate-index
//! `reindex` endpoint.

use std::collections::HashMap;
use std::time::SystemTime;

use axum::extract::{Path, State};
use axum::Json;
use deblob_core::id::SourceId;
use deblob_core::ports::{CandidateState, SourceRecord};
use serde::Serialize;

use super::{ApiError, ApiState, DataEnvelope, ListResponse};

/// Every registered source, sorted by name for a stable UI/diff.
pub async fn list_sources(
    State(state): State<ApiState>,
) -> Result<Json<ListResponse<SourceRecord>>, ApiError> {
    let mut data = state
        .sources
        .list_sources()
        .await
        .map_err(ApiError::from_core)?;
    data.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Json(ListResponse {
        data,
        next_cursor: None,
    }))
}

/// `GET /api/v1/sources/{source_id}` — one `SourceRecord` or 404.
pub async fn get_source(
    State(state): State<ApiState>,
    Path(source_id): Path<String>,
) -> Result<Json<DataEnvelope<SourceRecord>>, ApiError> {
    let id = SourceId::parse(&source_id)
        .map_err(|_| ApiError::unprocessable("invalid source id (expected src_… )"))?;
    let record = state
        .sources
        .get_source(&id)
        .await
        .map_err(ApiError::from_core)?
        .ok_or_else(|| ApiError::not_found("source not found"))?;
    Ok(Json(DataEnvelope { data: record }))
}

/// Response body for `POST /api/v1/sources/reconcile`.
#[derive(Debug, Serialize)]
pub struct ReconcileResponse {
    /// Distinct sources registered/refreshed this run.
    pub registered: usize,
}

/// `POST /api/v1/sources/reconcile` — authenticated off-path backfill: scans
/// every candidate's `source` provenance field across all states and
/// idempotently registers each distinct source (advancing `last_seen_ms` to
/// the candidate's most recent sighting). Always safe to re-run.
pub async fn reconcile(
    State(state): State<ApiState>,
) -> Result<Json<DataEnvelope<ReconcileResponse>>, ApiError> {
    // Collect the most-recent sighting per distinct source across every
    // candidate state, paging each state's index fully.
    let mut latest: HashMap<String, i64> = HashMap::new();
    for cand_state in [
        CandidateState::Provisional,
        CandidateState::Staged,
        CandidateState::Rejected,
    ] {
        let mut cursor: Option<String> = None;
        loop {
            let (page, next) = state
                .evidence
                .list_candidates(cand_state, cursor.clone(), 500)
                .await
                .map_err(ApiError::from_core)?;
            for rec in page {
                if let Some(src) = rec.source {
                    let e = latest.entry(src).or_insert(rec.last_seen_ms);
                    if rec.last_seen_ms > *e {
                        *e = rec.last_seen_ms;
                    }
                }
            }
            match next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
    }

    let fallback_now = now_ms();
    let mut registered = 0usize;
    for (name, last_seen) in latest {
        // A candidate with an unset (0) last_seen falls back to wall-clock,
        // so a source is never registered with a meaningless zero timestamp.
        let observed_at = if last_seen > 0 {
            last_seen
        } else {
            fallback_now
        };
        state
            .sources
            .register_source(&name, observed_at)
            .await
            .map_err(ApiError::from_core)?;
        registered += 1;
    }

    Ok(Json(DataEnvelope {
        data: ReconcileResponse { registered },
    }))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
