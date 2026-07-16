//! `GET /api/v1/schemas/{sch_id}/semantic`, `.../semantic/revisions`,
//! `PUT .../semantic`, `GET /api/v1/semantic/{sem_id}` handlers (P2-D Task
//! 6, `deblob-p2d-hermes-review.md` §4): the authenticated + audited
//! semantic-governance surface over Task 5's append-only revision store.
//!
//! Scope is deliberately narrow, per the brief: expose Tasks 1-5
//! (vocabulary validation, path validation, the byte-level digest, the
//! append-only revision store) on the management port. No drift/similarity
//! (Tasks 7/9), no new storage logic.

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use deblob_core::id::{FamilyVersion, RevisionId, SchemaId, SemanticId};
use deblob_core::revision::{Etag, ReasonCode, Revision};
use deblob_core::semantic::SemanticMetadata;
use deblob_semantic::signature::{Score, Strength};
use deblob_semantic::{
    canonical_field_paths, canonical_semantic_bytes, semantic_fingerprint, validate_metadata,
    validate_paths,
};
use serde::{Deserialize, Serialize};

use crate::semantic_drift;
use crate::semantic_neighbors::{self, NeighborOutcome};

use super::candidates::actor_from_headers;
use super::{ApiError, ApiState, DataEnvelope, ListResponse};

/// Request body for `PUT /api/v1/schemas/{sch_id}/semantic`. `reason_code`/
/// `reason` are optional at the wire level — `reason` is required ONLY when
/// the supplied `metadata` is a genuine change from the active revision (an
/// idempotent byte-identical replay needs neither, per the brief and
/// `deblob_redis::semantic`'s own `SEM_APPEND_SCRIPT` semantics, which
/// decides this atomically — see `put_semantic`'s docs). `reason_code`
/// defaults to [`ReasonCode::Correction`] when absent.
#[derive(Debug, Deserialize)]
pub struct PutSemanticRequest {
    pub metadata: SemanticMetadata,
    #[serde(default)]
    pub reason_code: Option<ReasonCode>,
    #[serde(default)]
    pub reason: Option<String>,
}

/// Response shape for a schema's active semantic assertion: the controlled
/// metadata plus its `sem_` identity. Un-annotated is never represented by
/// this type — see the module docs / brief §3: absence is `404` at the
/// endpoint level (`get_semantic`), not a sentinel value here.
#[derive(Debug, Serialize)]
pub struct SemanticView {
    pub semantic_fingerprint: SemanticId,
    pub metadata: SemanticMetadata,
}

fn now_epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Renders `etag` as a quoted HTTP `ETag` header value (`"3"`), inserted
/// onto `response` in place.
fn insert_etag(response: &mut Response, etag: Etag) -> Result<(), ApiError> {
    let value = HeaderValue::from_str(&format!("\"{}\"", etag.0))
        .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", "bad etag"))?;
    response
        .headers_mut()
        .insert(axum::http::header::ETAG, value);
    Ok(())
}

/// Parses the `If-Match` request header into `append_revision`'s
/// `expected_etag` argument: absent → `None` ("I believe this schema was
/// never annotated", matching etag `0`); present → `Some(Etag(n))`, tolerant
/// of an optionally-quoted value (`"3"` or `3`). A present-but-unparseable
/// header is a caller mistake — `400`, never silently treated as absent.
fn parse_if_match(headers: &HeaderMap) -> Result<Option<Etag>, ApiError> {
    let Some(raw) = headers.get(axum::http::header::IF_MATCH) else {
        return Ok(None);
    };
    let s = raw
        .to_str()
        .map_err(|_| ApiError::bad_request("invalid If-Match header"))?;
    let trimmed = s.trim().trim_matches('"');
    let value: u64 = trimmed
        .parse()
        .map_err(|_| ApiError::bad_request("invalid If-Match header"))?;
    Ok(Some(Etag(value)))
}

/// `GET /api/v1/schemas/{sch_id}/semantic` — the schema's active semantic
/// assertion + its `sem_` + an `ETag` header. `404` if the schema has never
/// been annotated (or doesn't exist at all) — un-annotated is a real
/// absence, never a sentinel value (brief §3).
pub async fn get_semantic(
    State(state): State<ApiState>,
    Path(sch_id): Path<String>,
) -> Result<Response, ApiError> {
    let id = SchemaId::parse(&sch_id).map_err(|e| ApiError::unprocessable(e.to_string()))?;

    let (metadata, sem_id, etag) = state
        .semantic
        .active_semantic(&id)
        .await
        .map_err(ApiError::from_sem)?
        .ok_or_else(|| ApiError::not_found("schema has no active semantic annotation"))?;

    let body = DataEnvelope {
        data: SemanticView {
            semantic_fingerprint: sem_id,
            metadata,
        },
    };
    let mut response = (StatusCode::OK, Json(body)).into_response();
    insert_etag(&mut response, etag)?;
    Ok(response)
}

/// `GET /api/v1/schemas/{sch_id}/semantic/revisions` — the schema's full
/// append-only revision history, oldest first. Empty (never `404`) for a
/// schema that has never been annotated — an empty history is a legitimate
/// answer, not an error.
pub async fn get_semantic_revisions(
    State(state): State<ApiState>,
    Path(sch_id): Path<String>,
) -> Result<Json<ListResponse<Revision>>, ApiError> {
    let id = SchemaId::parse(&sch_id).map_err(|e| ApiError::unprocessable(e.to_string()))?;

    let history = state
        .semantic
        .revisions(&id)
        .await
        .map_err(ApiError::from_sem)?;

    Ok(Json(ListResponse {
        data: history,
        next_cursor: None,
    }))
}

/// `GET /api/v1/semantic/{sem_id}` — every schema currently carrying
/// `sem_id` as its ACTIVE semantic assertion (the reverse-index diagnostic
/// lookup, Task 5/brief §5 — no same-`sem_`-different-`sch_` classification
/// here, that's Task 9).
pub async fn get_schemas_by_semantic(
    State(state): State<ApiState>,
    Path(sem_id): Path<String>,
) -> Result<Json<DataEnvelope<Vec<SchemaId>>>, ApiError> {
    let id = SemanticId::parse(&sem_id).map_err(|e| ApiError::unprocessable(e.to_string()))?;

    let schemas = state
        .semantic
        .schemas_by_semantic(&id)
        .await
        .map_err(ApiError::from_sem)?;

    Ok(Json(DataEnvelope { data: schemas }))
}

/// `PUT /api/v1/schemas/{sch_id}/semantic` — authenticated, audited (brief
/// §4). Flow: schema must exist → validate `metadata`'s controlled
/// vocabulary tokens (Task 2) → validate its field paths against the
/// schema's own structural canonical form (Task 4) → compute the canonical
/// bytes + `sem_` (Task 3) → append via the ONE atomic `SEM_APPEND_SCRIPT`
/// transition (Task 5), with `If-Match` threaded through as the
/// compare-and-swap token.
///
/// Deliberately does NOT pre-read the active revision to decide whether
/// this PUT is a genuine change (racy: a concurrent writer landing between
/// that read and the append below could flip the answer, turning a real
/// `409 EtagConflict` into a wrong `400 MissingReason`), and does NOT
/// re-read the active pointer afterward to learn the etag for the response
/// header (racy against a THIRD concurrent writer: the header could then
/// describe a different revision than the response body). Both concerns are
/// eliminated the same way: `append_revision`'s `AppendOutcome` already
/// carries the AUTHORITATIVE etag straight from `SEM_APPEND_SCRIPT`'s own
/// atomic reply, alongside the revision that is now (or still) active, and
/// the script itself — not this handler — decides idempotent-replay vs.
/// missing-reason vs. etag-conflict vs. genuine-append, all inside the same
/// atomic transition.
///
/// Status mapping: byte-identical to the active revision → `200`,
/// idempotent, no new revision (neither `reason` nor `reason_code` are even
/// inspected on this path — mirrors `SEM_APPEND_SCRIPT`'s own idempotency
/// check, which bypasses both); a genuine change with a missing/empty
/// `reason` → `400` (`SemError::MissingReason`); a genuine change whose
/// `If-Match` doesn't match the current active revision → `409`
/// (`SemError::EtagConflict`); a genuine change with a non-empty `reason`
/// and a correct `If-Match` → `201` with the new `sem_` + `ETag`. Unknown
/// vocabulary token / path not present on the schema → `422`, naming ONLY
/// the offending registered token/path (`VocabError`/`PathError`'s
/// `Display` never carries free-form user prose) — never `reason`, which is
/// free text and must never be echoed back in an error.
pub async fn put_semantic(
    State(state): State<ApiState>,
    Path(sch_id): Path<String>,
    headers: HeaderMap,
    Json(req): Json<PutSemanticRequest>,
) -> Result<Response, ApiError> {
    let id = SchemaId::parse(&sch_id).map_err(|e| ApiError::unprocessable(e.to_string()))?;

    let record = state
        .registry
        .get_schema(&id)
        .await
        .map_err(ApiError::from_core)?
        .ok_or_else(|| ApiError::not_found("schema not found"))?;

    // Task 2: controlled-vocabulary tokens. Names only the offending token.
    validate_metadata(&req.metadata, &state.semantic_registries)
        .map_err(|e| ApiError::unprocessable(e.to_string()))?;

    // Task 4: every annotated path must exist on the schema's own
    // structural canonical form. Names only the offending path.
    let valid_paths = canonical_field_paths(&record.canonical)
        .map_err(|e| ApiError::unprocessable(e.to_string()))?;
    validate_paths(&req.metadata, &valid_paths)
        .map_err(|e| ApiError::unprocessable(e.to_string()))?;

    // Task 3: byte-level canonical form + sem_ digest.
    let canonical_bytes = canonical_semantic_bytes(&req.metadata)
        .map_err(|e| ApiError::unprocessable(e.to_string()))?;
    let fingerprint = semantic_fingerprint(&req.metadata)
        .map_err(|e| ApiError::unprocessable(e.to_string()))?
        .ok_or_else(|| ApiError::unprocessable("no semantic assertions were provided"))?;

    let expected_etag = parse_if_match(&headers)?;
    let actor = actor_from_headers(&headers);
    let now_ms = now_epoch_ms();
    let reason_code = req.reason_code.unwrap_or(ReasonCode::Correction);
    let reason = req.reason.unwrap_or_default();

    // The ONE round trip: `SEM_APPEND_SCRIPT` atomically decides the
    // outcome and returns the authoritative etag alongside it (see the
    // doc comment above) — no pre-check read, no post-write re-read.
    let outcome = state
        .semantic
        .append_revision(
            &id,
            &req.metadata,
            &canonical_bytes,
            &fingerprint.0,
            &actor,
            reason_code,
            &reason,
            now_ms,
            now_ms,
            expected_etag,
        )
        .await
        .map_err(ApiError::from_sem)?;

    let was_appended = outcome.was_appended();
    let etag = outcome.etag();
    let revision = outcome.into_revision();

    // P2-D Task 8 follow-up (A2): fire the Task 7 diagnostics for real, but
    // ONLY on a genuine change (`was_appended`) — an idempotent replay
    // (`was_appended == false`) changed nothing about the active `sem_` or
    // the reverse index, so re-running either diagnostic would just
    // increment the counters again for no new information. Both calls are
    // READ-ONLY (see `crate::semantic_drift`'s module docs) and
    // deliberately best-effort: a diagnostic failing to compute must never
    // fail the annotation that already succeeded and was already returned
    // to the script's atomic reply — this handler logs and continues.
    if was_appended {
        // (b) same-sem_/different-sch_: scan the reverse index for the
        // sem_ this write just landed on.
        if let Err(e) = semantic_drift::scan_semantic_collisions(
            state.registry.as_ref(),
            state.semantic.as_ref(),
            &state.metrics,
            &revision.sem_id,
        )
        .await
        {
            tracing::warn!(
                error = %e,
                sem_id = %revision.sem_id.as_str(),
                "semantic-collision scan failed (diagnostic-only, annotation already succeeded)"
            );
        }

        // (a) semantic drift: only meaningful once the schema's family has
        // an ADJACENT (version - 1) member to compare against — version 1
        // has no prior version by definition.
        if record.version.0 > 1 {
            let prior_version = FamilyVersion(record.version.0 - 1);
            match state
                .registry
                .family_version_schema(&record.family_id, prior_version)
                .await
            {
                Ok(Some(prior_sch)) => {
                    if let Err(e) = semantic_drift::check_family_version_drift(
                        state.registry.as_ref(),
                        state.semantic.as_ref(),
                        &state.metrics,
                        record.family_id.clone(),
                        &prior_sch,
                        prior_version,
                        &id,
                        record.version,
                    )
                    .await
                    {
                        tracing::warn!(
                            error = %e,
                            family_id = %record.family_id.as_str(),
                            "semantic-drift check failed (diagnostic-only, annotation already succeeded)"
                        );
                    }
                }
                Ok(None) => {
                    // No adjacent version published (e.g. version 1 of a
                    // family that skipped a number, or a schema that was
                    // never actually published through a family at all) —
                    // nothing to compare against, not an error.
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        family_id = %record.family_id.as_str(),
                        "looking up the prior family version failed (diagnostic-only, annotation already succeeded)"
                    );
                }
            }
        }
    }

    let body = DataEnvelope {
        data: SemanticView {
            semantic_fingerprint: revision.sem_id,
            metadata: revision.metadata,
        },
    };
    let status = if was_appended {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    let mut response = (status, Json(body)).into_response();
    insert_etag(&mut response, etag)?;
    Ok(response)
}

// ---------------------------------------------------------------------
// P2-D Task 10: `GET .../semantic-neighbors` — diagnostic-only.
// ---------------------------------------------------------------------

/// Query parameters for `GET /api/v1/schemas/{sch_id}/semantic-neighbors`
/// (spec §4/§6).
#[derive(Debug, Deserialize)]
pub struct NeighborsQuery {
    #[serde(default)]
    pub k: Option<usize>,
    #[serde(default)]
    pub include_historical: Option<bool>,
}

/// Presentation-only string label for [`Strength`] — deliberately kept in
/// this API-response module rather than as a `serde` derive on the Task 9
/// type itself, matching this crate's existing pattern of explicit
/// string<->enum mapping at the storage/wire boundary (see
/// `deblob-redis::semantic::reason_code_str`).
fn strength_label(s: Strength) -> &'static str {
    match s {
        Strength::Insufficient => "insufficient",
        Strength::Weak => "weak",
        Strength::Medium => "medium",
        Strength::Strong => "strong",
    }
}

#[derive(Debug, Serialize)]
pub struct ScoreView {
    pub numerator: u64,
    pub denominator: u64,
    pub decimal: String,
}

impl From<Score> for ScoreView {
    fn from(score: Score) -> Self {
        ScoreView {
            numerator: score.numerator,
            denominator: score.denominator,
            // 6 fractional digits, matching the spec §6 example
            // (`"0.875000"`).
            decimal: score.decimal_string(6),
        }
    }
}

/// One ranked neighbor candidate — spec §6's `neighbors[]` entry. Labelled
/// a *candidate* everywhere it is rendered; this type has no field, and
/// this module has no code path, that could ever assert equivalence.
#[derive(Debug, Serialize)]
pub struct NeighborView {
    pub schema_id: SchemaId,
    pub semantic_revision_id: RevisionId,
    pub score: ScoreView,
    pub strength: &'static str,
    pub shared_anchor_count: usize,
    pub matched_feature_classes: Vec<&'static str>,
}

/// The full `GET .../semantic-neighbors` response envelope (spec §6).
/// `authority` is always the literal `"diagnostic_only"` — never
/// conditional, never omittable. `reason` is populated ONLY for the
/// no-anchor case (`neighbors` is then always empty).
#[derive(Debug, Serialize)]
pub struct SemanticNeighborsView {
    pub query_schema: SchemaId,
    pub signature_version: &'static str,
    pub weights_version: &'static str,
    pub neighbors: Vec<NeighborView>,
    pub authority: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strength: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<&'static str>,
}

/// `GET /api/v1/schemas/{sch_id}/semantic-neighbors?k=&include_historical=`
/// — the Task 10 diagnostic-only semantic-neighbor search (spec §4/§5/§6).
/// Authenticated like every other `/api/v1/*` route (`auth::require_bearer`,
/// wired in `api::router`); excludes the query schema; uses ACTIVE semantic
/// revisions only.
///
/// `k`: defaults to [`semantic_neighbors::DEFAULT_K`], CLAMPED (never
/// rejected with `422`) to [`semantic_neighbors::MAX_K`] — a deliberate
/// choice for a diagnostic best-effort endpoint: an over-large `k` is
/// caller carelessness, not a malformed request, and clamping always
/// returns something useful rather than forcing a retry.
///
/// `include_historical=true`: this API has exactly ONE authentication tier
/// (a single shared bearer token — `api::auth::require_bearer`; there is no
/// scope/role system anywhere in this codebase). The spec (§6) gates
/// `include_historical=true` to "auditors", which does not exist as a
/// concept here. Rather than invent a new auth mechanism to satisfy that
/// gate, this handler documents the gap explicitly: the query parameter is
/// accepted (so a future auditor-scope addition is a non-breaking wire
/// change) but is ALWAYS treated as `false` — every query runs
/// active-revisions-only, regardless of what the caller passed. Historical
/// revisions are consequently never queryable via this endpoint yet.
///
/// Never merges, aliases, promotes, or mutates any schema/`sem_`/family/
/// candidate state (spec §6) — every call this makes to
/// `SemanticStore`/`Registry` is a READ; see `crate::semantic_neighbors`'s
/// module docs and `crates/deblob/tests/semantic_neighbors_it.rs`'s
/// before/after state-snapshot test.
pub async fn get_semantic_neighbors(
    State(state): State<ApiState>,
    Path(sch_id): Path<String>,
    Query(params): Query<NeighborsQuery>,
) -> Result<Json<DataEnvelope<SemanticNeighborsView>>, ApiError> {
    let id = SchemaId::parse(&sch_id).map_err(|e| ApiError::unprocessable(e.to_string()))?;
    let k = params
        .k
        .unwrap_or(semantic_neighbors::DEFAULT_K)
        .min(semantic_neighbors::MAX_K);
    // See this function's doc comment: no auditor-scope infra exists, so
    // `include_historical` is accepted but never honored yet.
    let _ = params.include_historical;

    let outcome = semantic_neighbors::neighbors(state.semantic.as_ref(), &id, k)
        .await
        .map_err(ApiError::from_sem)?
        .ok_or_else(|| ApiError::not_found("schema has no active semantic annotation"))?;

    let view = match outcome {
        NeighborOutcome::Found(neighbors) => SemanticNeighborsView {
            query_schema: id,
            signature_version: deblob_semantic::signature::SIGNATURE_VERSION,
            weights_version: deblob_semantic::signature::WEIGHTS_VERSION,
            neighbors: neighbors
                .into_iter()
                .map(|n| NeighborView {
                    schema_id: n.schema_id,
                    semantic_revision_id: n.semantic_revision_id,
                    score: n.score.into(),
                    strength: strength_label(n.strength),
                    shared_anchor_count: n.shared_anchor_count,
                    matched_feature_classes: n.matched_feature_classes,
                })
                .collect(),
            authority: "diagnostic_only",
            strength: None,
            reason: None,
        },
        NeighborOutcome::NoAnchor => SemanticNeighborsView {
            query_schema: id,
            signature_version: deblob_semantic::signature::SIGNATURE_VERSION,
            weights_version: deblob_semantic::signature::WEIGHTS_VERSION,
            neighbors: Vec::new(),
            authority: "diagnostic_only",
            strength: Some(strength_label(Strength::Insufficient)),
            reason: Some("no_anchor_features"),
        },
        NeighborOutcome::TooBroad => {
            return Err(ApiError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "signature_too_broad",
                "the candidate union for this schema's signature exceeds the bounded limit; \
                 narrow the query rather than accept a silently truncated top-k result",
            ));
        }
    };

    Ok(Json(DataEnvelope { data: view }))
}
