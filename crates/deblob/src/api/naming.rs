//! `PUT /api/v1/schemas/{sch_id}/name` — SLM-proposed, human-editable schema
//! display names (`jr-schema-naming-211140`).
//!
//! Governance ("the model proposes, deterministic code + policy decides"): a
//! `human` name always wins. The precedence guard is enforced ATOMICALLY in
//! the registry ([`Registry::set_schema_name`] via `SET_NAME_SCRIPT`), so a
//! human edit landing between an automatic namer's read and its write can
//! never be clobbered — this handler is thin glue over that guarantee plus a
//! server-side sanity gate on the name string.
//!
//! The name is DISPLAY metadata: it never touches the schema's identity digest
//! (`schema_id` is content-addressed over shape) or its version. It is
//! overlaid onto `provenance.label` on read, which is exactly what the console
//! already renders — so no read-side wiring changes.

use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::Json;
use deblob_core::id::SchemaId;
use deblob_core::ports::NameWriteOutcome;
use serde::{Deserialize, Serialize};

use super::candidates::actor_from_headers;
use super::{ApiError, ApiState, DataEnvelope};

/// The three legitimate name sources, in precedence order (highest wins).
/// `human` is an operator edit and is never overwritten by an automatic run;
/// `slm` is an accepted model proposal; `heuristic` is the deterministic
/// baseline the namer falls back to when the SLM output fails validation.
const VALID_SOURCES: [&str; 3] = ["human", "slm", "heuristic"];

/// Max display-name length (characters). Names are 2-4 words in practice; this
/// is a generous hard cap, not the semantic constraint (the namer controller
/// enforces the 2-4-word grounded shape — the server only sanity-gates).
const MAX_NAME_CHARS: usize = 60;

#[derive(Debug, Deserialize)]
pub struct PutNameRequest {
    /// The proposed/edited display name.
    pub name: String,
    /// `human` | `slm` | `heuristic`.
    pub source: String,
    /// Optional audit/idempotency metadata (prompt_version, model_digest,
    /// field_set_hash, confidence, …). Stored verbatim under
    /// `provenance.name_meta`; never interpreted by the server.
    #[serde(default)]
    pub meta: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct NameWriteResult {
    /// `true` when the name was written; `false` when a human override was
    /// protected (a benign no-op — the automatic namer must NOT treat this as
    /// a failure).
    pub applied: bool,
    pub name: String,
    pub source: String,
    /// Present only when `applied == false`, explaining why.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Server-side sanity gate on the display name. Deliberately permissive on
/// character set (Title Case words, digits, spaces, and a few joiners like
/// `& - / . ( )`) — the semantic grounding (2-4 words, every token licensed by
/// a field) is the namer controller's job, not the API's. Rejects empty /
/// whitespace-only / control-character / over-long / no-alphanumeric input.
/// Returns the trimmed name on success.
pub fn validate_name(raw: &str) -> Result<String, String> {
    let name = raw.trim();
    if name.is_empty() {
        return Err("name must not be empty".to_string());
    }
    if name.chars().count() > MAX_NAME_CHARS {
        return Err(format!("name must be at most {MAX_NAME_CHARS} characters"));
    }
    if name.chars().any(|c| c.is_control()) {
        return Err("name must not contain control characters".to_string());
    }
    if !name.chars().any(|c| c.is_alphanumeric()) {
        return Err("name must contain at least one letter or digit".to_string());
    }
    Ok(name.to_string())
}

/// Validates the `source` field against the closed set of legitimate sources.
pub fn resolve_source(raw: &str) -> Result<String, String> {
    if VALID_SOURCES.contains(&raw) {
        Ok(raw.to_string())
    } else {
        Err(format!(
            "source must be one of {} (got {raw:?})",
            VALID_SOURCES.join(", ")
        ))
    }
}

/// `PUT /api/v1/schemas/{sch_id}/name` — set the governed display name.
///
/// - `200 {applied:true}`  — name written.
/// - `200 {applied:false, reason}` — a human override was protected (the
///   incoming `slm`/`heuristic` write was atomically refused). This is a
///   success at the HTTP layer so the namer controller's run does not fail.
/// - `404` — no such schema.
/// - `422` — malformed name or unknown source.
pub async fn put_name(
    State(state): State<ApiState>,
    Path(sch_id): Path<String>,
    headers: HeaderMap,
    Json(req): Json<PutNameRequest>,
) -> Result<Json<DataEnvelope<NameWriteResult>>, ApiError> {
    let id = SchemaId::parse(&sch_id).map_err(|e| ApiError::unprocessable(e.to_string()))?;
    let source = resolve_source(&req.source).map_err(ApiError::unprocessable)?;
    let name = validate_name(&req.name).map_err(ApiError::unprocessable)?;

    // Actor is captured for the audit trail (who set/edited the name).
    let _actor = actor_from_headers(&headers);

    let outcome = state
        .registry
        .set_schema_name(&id, &name, &source, req.meta)
        .await
        .map_err(ApiError::from_core)?;

    match outcome {
        NameWriteOutcome::Applied => Ok(Json(DataEnvelope {
            data: NameWriteResult {
                applied: true,
                name,
                source,
                reason: None,
            },
        })),
        NameWriteOutcome::SkippedHumanProtected => Ok(Json(DataEnvelope {
            data: NameWriteResult {
                applied: false,
                name,
                source,
                reason: Some("a human-set name is protected and was not overwritten".to_string()),
            },
        })),
        NameWriteOutcome::NotFound => Err(ApiError::not_found("schema not found")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_name_accepts_titlecase_multiword() {
        assert_eq!(validate_name("Scholarly Works").unwrap(), "Scholarly Works");
        assert_eq!(
            validate_name("Knowledge Graph Entities").unwrap(),
            "Knowledge Graph Entities"
        );
        // trims surrounding whitespace
        assert_eq!(
            validate_name("  Location Observations  ").unwrap(),
            "Location Observations"
        );
        // a few joiners are allowed
        assert!(validate_name("Repository Push/Issue Events").is_ok());
    }

    #[test]
    fn validate_name_rejects_garbage() {
        assert!(validate_name("").is_err());
        assert!(validate_name("   ").is_err());
        assert!(validate_name("\u{0007}\u{0007}").is_err());
        assert!(validate_name("---").is_err()); // no alphanumeric
        let too_long: String = "A".repeat(MAX_NAME_CHARS + 1);
        assert!(validate_name(&too_long).is_err());
        // control character embedded mid-string
        assert!(validate_name("Good\nName").is_err());
    }

    #[test]
    fn resolve_source_is_a_closed_set() {
        assert_eq!(resolve_source("human").unwrap(), "human");
        assert_eq!(resolve_source("slm").unwrap(), "slm");
        assert_eq!(resolve_source("heuristic").unwrap(), "heuristic");
        assert!(resolve_source("robot").is_err());
        assert!(resolve_source("HUMAN").is_err()); // case-sensitive
        assert!(resolve_source("").is_err());
    }
}
