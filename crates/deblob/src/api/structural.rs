//! `GET /api/v1/schemas/{sch_id}/structural-neighbors` — additive, read-only
//! near-duplicate family discovery via the structural SimHash sketch
//! (`deblob_monoid::structural_simhash`).
//!
//! ## Diagnostic-only, never an identity claim
//!
//! Mirrors `semantic_neighbors`'s posture exactly: every neighbor is a
//! *candidate* near-duplicate family, surfaced for a human or the
//! deterministic gate to adjudicate (governance: propose / decide / approve).
//! A small Hamming distance — even `0` — is NEVER "the same schema": the exact
//! `schema_id` fingerprint remains the sole authority on identity, and this
//! endpoint only reads. It exists because the exact structural bucket, by
//! design, gives every distinct structure its own family, so near-variants of
//! one logical source (validated on the live registry: `Flight/Transit/EvalPlus
//! … Observations` each split into two ~80%-identical families) are otherwise
//! unlinked. The sketch reconnects them as candidates without ever weakening
//! the exact identity.

use axum::extract::{Path, Query, State};
use axum::Json;
use deblob_core::id::SchemaId;
use deblob_core::ports::SchemaRecord;
use deblob_monoid::{structural_hamming, structural_simhash, NEAR_DUPLICATE_HAMMING_MAX};
use serde::{Deserialize, Serialize};

use super::{ApiError, ApiState, DataEnvelope};

/// Upper bound on schemas scanned in one call. The registry is small by design
/// (one schema per distinct structure), but this caps the read fan-out and is
/// surfaced via `scan_truncated` so a bounded result is never mistaken for an
/// exhaustive one (no silent caps).
const MAX_SCAN: usize = 5000;
/// Page size for the registry walk.
const SCAN_PAGE: usize = 200;

#[derive(Debug, Deserialize)]
pub struct NeighborsQuery {
    /// Maximum Hamming distance to include; clamped to `[0, 64]`. Defaults to
    /// `NEAR_DUPLICATE_HAMMING_MAX` (the precision-1.0 threshold validated on
    /// live data).
    max_hamming: Option<u32>,
    /// Maximum neighbors to return (after ranking). Defaults to 50.
    limit: Option<usize>,
}

/// One candidate near-duplicate. `hamming_distance` is over the 64-bit sketch;
/// `structural_simhash` is the candidate's own sketch as zero-padded hex.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StructuralNeighbor {
    pub schema_id: String,
    pub hamming_distance: u32,
    pub structural_simhash: String,
}

/// The full response: the ranked neighbors plus the query's own sketch and
/// whether the registry scan hit `MAX_SCAN` (a truncated, best-effort result).
#[derive(Debug, Serialize)]
pub struct StructuralNeighborsResponse {
    pub query_schema_id: String,
    pub query_structural_simhash: String,
    pub max_hamming: u32,
    pub neighbors: Vec<StructuralNeighbor>,
    pub scan_truncated: bool,
}

/// The sketch for a record: the stored `structural_simhash` when present (new
/// promotions carry it), else recomputed on the fly from the stored
/// `canonical` — deterministic and identical, so a not-yet-backfilled record is
/// fully comparable. `None` iff the canonical yields no structural tokens.
fn sketch_of(record: &SchemaRecord) -> Option<u64> {
    record
        .structural_simhash
        .or_else(|| structural_simhash(&record.canonical))
}

/// Pure ranking core (unit-tested without a registry): keep candidates within
/// `max_hamming` of `query_sketch`, exclude the query itself, rank by distance
/// ascending then `sch_` bytes ascending (a stable, reproducible tie-break),
/// and truncate to `limit`.
fn rank(
    query_id: &str,
    query_sketch: u64,
    candidates: &[(String, Option<u64>)],
    max_hamming: u32,
    limit: usize,
) -> Vec<StructuralNeighbor> {
    let mut out: Vec<StructuralNeighbor> = candidates
        .iter()
        .filter(|(id, _)| id != query_id)
        .filter_map(|(id, sketch)| {
            let s = (*sketch)?;
            let d = structural_hamming(query_sketch, s);
            (d <= max_hamming).then(|| StructuralNeighbor {
                schema_id: id.clone(),
                hamming_distance: d,
                structural_simhash: format!("{s:016x}"),
            })
        })
        .collect();
    out.sort_by(|a, b| {
        a.hamming_distance
            .cmp(&b.hamming_distance)
            .then_with(|| a.schema_id.cmp(&b.schema_id))
    });
    out.truncate(limit);
    out
}

pub async fn get_structural_neighbors(
    State(state): State<ApiState>,
    Path(sch_id): Path<String>,
    Query(q): Query<NeighborsQuery>,
) -> Result<Json<DataEnvelope<StructuralNeighborsResponse>>, ApiError> {
    let id = SchemaId::parse(&sch_id).map_err(|e| ApiError::unprocessable(e.to_string()))?;
    let max_hamming = q.max_hamming.unwrap_or(NEAR_DUPLICATE_HAMMING_MAX).min(64);
    let limit = q.limit.unwrap_or(50);

    let query_record = state
        .registry
        .get_schema(&id)
        .await
        .map_err(ApiError::from_core)?
        .ok_or_else(|| ApiError::not_found("schema not found"))?;

    // A schema whose canonical carries no structural tokens has no sketch and
    // thus no structural neighbors — a legitimate empty result, not an error.
    let query_sketch = sketch_of(&query_record);

    // Walk the whole (small) registry, gathering (id, sketch) pairs.
    let mut candidates: Vec<(String, Option<u64>)> = Vec::new();
    let mut cursor: Option<String> = None;
    let mut scan_truncated = false;
    loop {
        let (page, next) = state
            .registry
            .list_schemas(cursor, SCAN_PAGE)
            .await
            .map_err(ApiError::from_core)?;
        for record in page {
            candidates.push((record.schema_id.as_str().to_string(), sketch_of(&record)));
        }
        if candidates.len() >= MAX_SCAN {
            scan_truncated = true;
            break;
        }
        match next {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }

    let neighbors = match query_sketch {
        Some(qs) => rank(query_record.schema_id.as_str(), qs, &candidates, max_hamming, limit),
        None => Vec::new(),
    };

    Ok(Json(DataEnvelope {
        data: StructuralNeighborsResponse {
            query_schema_id: query_record.schema_id.as_str().to_string(),
            query_structural_simhash: query_sketch
                .map(|s| format!("{s:016x}"))
                .unwrap_or_default(),
            max_hamming,
            neighbors,
            scan_truncated,
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cands() -> Vec<(String, Option<u64>)> {
        vec![
            ("sch_self".into(), Some(0b0000)),
            ("sch_near_b".into(), Some(0b0011)), // 2 bits from self
            ("sch_near_a".into(), Some(0b0001)), // 1 bit from self
            ("sch_far".into(), Some(u64::MAX)),  // 64 bits
            ("sch_no_sketch".into(), None),      // no structural tokens
        ]
    }

    #[test]
    fn ranks_by_distance_then_excludes_self_and_far() {
        let out = rank("sch_self", 0b0000, &cands(), NEAR_DUPLICATE_HAMMING_MAX, 50);
        let ids: Vec<&str> = out.iter().map(|n| n.schema_id.as_str()).collect();
        // self excluded, far (>8) excluded, None skipped; nearest first.
        assert_eq!(ids, vec!["sch_near_a", "sch_near_b"]);
        assert_eq!(out[0].hamming_distance, 1);
        assert_eq!(out[1].hamming_distance, 2);
    }

    #[test]
    fn tie_break_is_smaller_sch_id() {
        // Two candidates at the SAME distance (1 bit) resolve by sch_ ascending.
        let c = vec![
            ("sch_zzz".into(), Some(0b0001)),
            ("sch_aaa".into(), Some(0b0010)),
        ];
        let out = rank("sch_q", 0b0000, &c, 8, 50);
        assert_eq!(out[0].schema_id, "sch_aaa");
        assert_eq!(out[1].schema_id, "sch_zzz");
    }

    #[test]
    fn max_hamming_zero_returns_only_exact_sketch_matches() {
        let out = rank("sch_self", 0b0000, &cands(), 0, 50);
        assert!(out.is_empty(), "no non-self candidate has an identical sketch");
    }

    #[test]
    fn limit_truncates_after_ranking() {
        let out = rank("sch_self", 0b0000, &cands(), NEAR_DUPLICATE_HAMMING_MAX, 1);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].schema_id, "sch_near_a"); // the closest survives
    }
}
