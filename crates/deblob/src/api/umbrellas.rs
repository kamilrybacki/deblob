//! `GET /api/v1/umbrellas`, `GET .../{umbrella_id}`,
//! `GET .../{umbrella_id}/transforms`, `POST .../{umbrella_id}/approve`,
//! `POST .../{umbrella_id}/reject` handlers — the governance surface for
//! gold-tier umbrella schemas (`deblob-umbrella`).
//!
//! Umbrella activation is HITL-only; the controller/SLM may only ever
//! create or update PROVISIONAL umbrellas — promotion to Active is
//! exclusively via the human-triggered `/approve` endpoint.

use std::collections::BTreeMap;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use deblob_core::id::SchemaId;
use deblob_core::ports::SchemaRecord;
use deblob_core::semantic::{FieldSemantics, PathSegment, SemanticMetadata};
use deblob_umbrella::store::{
    LineageAssertion, LineageMember, StoreError, StoredUmbrella, UmbrellaBundle, UmbrellaState,
};
use deblob_umbrella::types::{ChildTransform, JsonPath, ScalarType};
use deblob_umbrella::verify::{self, ChildField};
use serde::{Deserialize, Serialize};

use super::{ApiError, ApiState, DataEnvelope, ListResponse};

impl ApiError {
    /// Maps [`deblob_umbrella::store::StoreError`] onto the HTTP contract:
    /// `UmbrellaNotFound` → 404; `BundleMismatch`/`Backend` → 503, mirroring
    /// `ApiError::from_core`'s treatment of a downstream-store failure
    /// rather than a caller mistake (bundle promotion isn't exposed as an
    /// API surface here, so `BundleMismatch` should never actually surface
    /// through these handlers — still mapped defensively).
    pub(crate) fn from_umbrella_store(err: StoreError) -> Self {
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

/// `GET /api/v1/umbrellas/{umbrella_id}/lineage` — the immutable
/// governance-lineage assertion written by `approve` at promotion time, or
/// 404 if the umbrella was never approved (including if it doesn't exist at
/// all).
pub async fn get_lineage(
    State(state): State<ApiState>,
    Path(umbrella_id): Path<String>,
) -> Result<Json<DataEnvelope<LineageAssertion>>, ApiError> {
    let assertion = state
        .umbrellas
        .get_lineage_assertion(&umbrella_id)
        .await
        .map_err(ApiError::from_umbrella_store)?
        .ok_or_else(|| ApiError::not_found("lineage assertion not found"))?;

    Ok(Json(DataEnvelope { data: assertion }))
}

/// One child schema's contribution to a single umbrella field: which child
/// field path feeds it, under which pinned child revision, and how (op-chain
/// length + missing-handling). Derived purely from the persisted
/// `ChildTransform` bindings — no separate storage.
#[derive(Debug, Serialize)]
pub struct FieldContributor {
    pub child_schema_id: String,
    pub child_revision: String,
    pub source_path: String,
    pub op_count: usize,
    pub on_missing: String,
    /// Leaf field NAME (last path segment) as observed on this child.
    pub source_name: String,
    /// Unit code from the child's semantic annotation, if any.
    pub unit: Option<String>,
    /// Coarse value-bucket summary for this child's leaf (human-readable bucket
    /// names, e.g. `["small","large"]`) — never a raw value. Empty when no
    /// value profile exists or the leaf carried no numbers.
    pub value_buckets: Vec<&'static str>,
    /// Numeric observation count backing `value_buckets` (`0` if unknown).
    pub numeric_count: u64,
    /// Whether this child had a durable value profile at all.
    pub has_value_profile: bool,
}

/// Field-level lineage for one umbrella field: the gold target, every child
/// field bound to it, and the SHADOW name/value corroboration evidence
/// (joint design `dc-umbrella-signals-1907`, Stage 2 — recorded/surfaced,
/// not enforced).
#[derive(Debug, Serialize)]
pub struct FieldLineage {
    pub umbrella_path: String,
    pub canonical_field_id: String,
    pub contributors: Vec<FieldContributor>,
    pub guard: crate::umbrella_guard::FieldGuard,
}

/// Per-child-schema evidence gathered once and reused across every umbrella
/// field that child contributes to: value-profile leaves (path → mask +
/// numeric count) and semantic units (path → unit code).
struct ChildInfo {
    has_value_profile: bool,
    leaves: std::collections::HashMap<String, (u8, u64)>,
    units: std::collections::HashMap<String, Option<String>>,
}

/// Human-readable coarse bucket names for a mask — for display only, never
/// reversible to a value.
fn bucket_names(mask: u8) -> Vec<&'static str> {
    use deblob_core::ports::value_bucket as vb;
    let mut out = Vec::new();
    if mask & vb::NEGATIVE != 0 {
        out.push("negative");
    }
    if mask & vb::ZERO != 0 {
        out.push("zero");
    }
    if mask & vb::SMALL_POSITIVE != 0 {
        out.push("small");
    }
    if mask & vb::MEDIUM_POSITIVE != 0 {
        out.push("medium");
    }
    if mask & vb::LARGE_POSITIVE != 0 {
        out.push("large");
    }
    out
}

/// `$.a.b` → `a.b` (strip the JsonPath root prefix so it joins to a
/// value-profile leaf path); `$.x` → `x`.
fn leaf_path_of(json_path: &str) -> String {
    json_path.strip_prefix("$.").unwrap_or(json_path).to_string()
}

fn leaf_name_of(leaf_path: &str) -> String {
    leaf_path.rsplit('.').next().unwrap_or(leaf_path).to_string()
}

/// Gathers one child schema's value + unit evidence (best-effort: any missing
/// piece degrades to "no profile"/unit `None`, never an error).
async fn child_info(state: &ApiState, child_schema_id: &str) -> ChildInfo {
    let mut info = ChildInfo {
        has_value_profile: false,
        leaves: std::collections::HashMap::new(),
        units: std::collections::HashMap::new(),
    };
    let Ok(sch_id) = SchemaId::parse(child_schema_id) else {
        return info;
    };
    let Ok(Some(rec)) = state.registry.get_schema(&sch_id).await else {
        return info;
    };
    // Value profile leaves.
    if let Some(vp_ref) = &rec.value_profile_ref {
        if let Ok(Some(snap)) = state.value_profiles.get_value_profile(vp_ref).await {
            info.has_value_profile = true;
            for leaf in snap.leaves {
                info.leaves
                    .insert(leaf.path, (leaf.numeric_bucket_mask, leaf.type_counts.number));
            }
        }
    }
    // Semantic units (via the same child-field derivation the controller uses).
    let semantic = match state.semantic.active_semantic(&sch_id).await {
        Ok(Some((m, _, _))) => Some(m),
        _ => None,
    };
    for cf in child_fields_from_schema(&rec, semantic.as_ref()) {
        let p = leaf_path_of(&String::from(cf.path.clone()));
        info.units.insert(p, cf.unit.map(|u| u.code));
    }
    info
}

/// `GET /api/v1/umbrellas/{umbrella_id}/lineage/fields` — FIELD-level lineage:
/// for every field of the umbrella, which child schemas + child field paths
/// feed it (from the persisted `ChildTransform` bindings). Complements the
/// schema-level `/lineage` assertion. 404 if the umbrella doesn't exist.
///
/// A pure read-model over what `approve`/the controller already persisted —
/// no new storage, no migration: `list_transforms` + the umbrella's own field
/// set are joined here, in the handler, on `binding.target == field.path`.
pub async fn get_field_lineage(
    State(state): State<ApiState>,
    Path(umbrella_id): Path<String>,
) -> Result<Json<ListResponse<FieldLineage>>, ApiError> {
    let umbrella = state
        .umbrellas
        .get_umbrella(&umbrella_id)
        .await
        .map_err(ApiError::from_umbrella_store)?
        .ok_or_else(|| ApiError::not_found("umbrella not found"))?
        .schema;
    let transforms = state
        .umbrellas
        .list_transforms(&umbrella_id)
        .await
        .map_err(ApiError::from_umbrella_store)?;

    // Gather each distinct child schema's value + unit evidence once.
    let mut child_ids: Vec<String> = transforms.iter().map(|t| t.child_schema_id.clone()).collect();
    child_ids.sort();
    child_ids.dedup();
    let mut cache: std::collections::HashMap<String, ChildInfo> = std::collections::HashMap::new();
    for cid in &child_ids {
        cache.insert(cid.clone(), child_info(&state, cid).await);
    }

    let mut fields = Vec::new();
    for uf in &umbrella.fields {
        let mut contributors = Vec::new();
        let mut evidence = Vec::new();
        for t in &transforms {
            for b in &t.bindings {
                if b.target != uf.path {
                    continue;
                }
                let source_path = String::from(b.source.clone());
                let leaf_path = leaf_path_of(&source_path);
                let name = leaf_name_of(&leaf_path);
                let info = cache.get(&t.child_schema_id);
                let (mask, numeric_count) = info
                    .and_then(|i| i.leaves.get(&leaf_path).copied())
                    .unwrap_or((0, 0));
                let unit = info.and_then(|i| i.units.get(&leaf_path).cloned()).flatten();
                let has_value_profile = info.map(|i| i.has_value_profile).unwrap_or(false);

                evidence.push(crate::umbrella_guard::MemberEvidence {
                    name: name.clone(),
                    unit: unit.clone(),
                    mask,
                    numeric_count,
                    has_profile: has_value_profile,
                });
                contributors.push(FieldContributor {
                    child_schema_id: t.child_schema_id.clone(),
                    child_revision: t.child_revision.clone(),
                    source_path,
                    op_count: b.ops.len(),
                    on_missing: format!("{:?}", b.on_missing),
                    source_name: name,
                    unit,
                    value_buckets: bucket_names(mask),
                    numeric_count,
                    has_value_profile,
                });
            }
        }
        // Deterministic order for a stable UI/diff.
        contributors.sort_by(|a, b| a.child_schema_id.cmp(&b.child_schema_id));
        let guard = crate::umbrella_guard::evaluate_field(&evidence);
        fields.push(FieldLineage {
            umbrella_path: String::from(uf.path.clone()),
            canonical_field_id: uf.canonical_field_id.as_str().to_string(),
            contributors,
            guard,
        });
    }

    Ok(Json(ListResponse {
        data: fields,
        next_cursor: None,
    }))
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

/// Response body for a successful `POST .../approve`.
#[derive(Debug, Serialize)]
pub struct ApproveResponse {
    pub umbrella_id: String,
    /// Always `UmbrellaState::Active` on success — spelled out as a typed
    /// field (rather than a bare literal) so its JSON rendering
    /// (`"active"`) stays pinned to `UmbrellaState`'s own `Serialize` impl.
    pub state: UmbrellaState,
    pub verified_transforms: usize,
}

/// `POST /api/v1/umbrellas/{umbrella_id}/approve` — the ONLY path in this
/// service that transitions an umbrella to `Active`. Human-triggered only:
/// requires a non-empty `reason` in the body, mirroring
/// `candidates::promote`'s audited-action style. 404 if the umbrella
/// doesn't exist, 400 if `reason` is empty, 409 if it isn't `Provisional`,
/// 422 if static verification of any of its transforms fails.
///
/// This is the real trust gate, not a bare state flip: every transform is
/// re-verified against its child schema's CURRENT registry shape via
/// [`verify::verify_static`], and the umbrella is only promoted — via
/// [`deblob_umbrella::store::UmbrellaStore::promote_bundle`]'s atomic
/// umbrella-plus-transforms write — if every transform passes. Held-out
/// replay ([`verify::replay`]) is NOT re-run here: the proposal pipeline
/// that creates a provisional umbrella is expected to have replayed it
/// against held-out samples already, and those samples are never persisted
/// (spec §9's "profiles hold no raw values" posture) so there is nothing
/// left to replay against by the time a human reaches this endpoint. This
/// re-runs the deterministic, data-free half of the gate (`verify_static`)
/// and performs the atomic promotion; it does not re-derive the
/// data-dependent half.
pub async fn approve(
    State(state): State<ApiState>,
    Path(umbrella_id): Path<String>,
    Json(req): Json<ApproveRequest>,
) -> Result<Json<DataEnvelope<ApproveResponse>>, ApiError> {
    if req.reason.trim().is_empty() {
        return Err(ApiError::bad_request(
            "reason is required to approve an umbrella",
        ));
    }

    let stored = state
        .umbrellas
        .get_umbrella(&umbrella_id)
        .await
        .map_err(ApiError::from_umbrella_store)?
        .ok_or_else(|| ApiError::not_found("umbrella not found"))?;

    if stored.state != UmbrellaState::Provisional {
        return Err(ApiError::conflict(format!(
            "umbrella {umbrella_id} is not provisional (current state: {:?})",
            stored.state
        )));
    }

    let transforms = state
        .umbrellas
        .list_transforms(&umbrella_id)
        .await
        .map_err(ApiError::from_umbrella_store)?;

    let mut issues: Vec<String> = Vec::new();
    for transform in &transforms {
        let child_id = SchemaId::parse(&transform.child_schema_id).map_err(|e| {
            ApiError::unprocessable(format!(
                "transform child_schema_id {:?} is invalid: {e}",
                transform.child_schema_id
            ))
        })?;

        let child_record = state
            .registry
            .get_schema(&child_id)
            .await
            .map_err(ApiError::from_core)?
            .ok_or_else(|| {
                ApiError::unprocessable(format!(
                    "child schema {} referenced by a transform no longer exists",
                    transform.child_schema_id
                ))
            })?;

        // The registry's own `SchemaRecord::semantic` is always `None` in
        // practice (semantic annotations live in the append-only
        // `SemanticStore`, spec P2-D Task 5/6) — fetch the schema's CURRENT
        // active annotation from there instead. Best-effort: a store error
        // or "no active revision yet" both fall back to `None`
        // (unannotated), matching `child_fields_from_schema`'s own posture
        // that a missing annotation is a legitimate, common case, not a
        // reason to fail verification outright.
        let active_semantic = state.semantic.active_semantic(&child_id).await;
        let semantic_metadata = match active_semantic {
            Ok(Some((metadata, _, _))) => Some(metadata),
            Ok(None) => None,
            Err(_) => None,
        };

        let child_fields = child_fields_from_schema(&child_record, semantic_metadata.as_ref());
        for issue in verify::verify_static(transform, &stored.schema, &child_fields) {
            issues.push(format!("{}: {issue}", transform.child_schema_id));
        }
    }

    if !issues.is_empty() {
        return Err(ApiError::unprocessable(format!(
            "umbrella {umbrella_id} failed static verification ({} issue(s)): {}",
            issues.len(),
            issues.join("; ")
        )));
    }

    let verified_transforms = transforms.len();
    let bundle = UmbrellaBundle {
        umbrella: stored.schema,
        transforms,
    };
    state
        .umbrellas
        .promote_bundle(&bundle)
        .await
        .map_err(ApiError::from_umbrella_store)?;

    // Governed lineage assertion (payload-free, immutable): captures
    // exactly what was consolidated into this umbrella at the moment a
    // human approved it. Written right after `promote_bundle` succeeds —
    // see `LineageAssertion`'s own docs for why a separate call (rather
    // than folding it into the same atomic write) is sufficient here.
    let lineage = LineageAssertion {
        umbrella_id: bundle.umbrella.umbrella_id.clone(),
        umbrella_version: bundle.umbrella.version,
        members: bundle
            .transforms
            .iter()
            .map(|t| LineageMember {
                child_schema_id: t.child_schema_id.clone(),
                child_revision: t.child_revision.clone(),
                transform_present: true,
            })
            .collect(),
        approved_reason: req.reason.clone(),
    };
    state
        .umbrellas
        .put_lineage_assertion(&lineage)
        .await
        .map_err(ApiError::from_umbrella_store)?;

    Ok(Json(DataEnvelope {
        data: ApproveResponse {
            umbrella_id,
            state: UmbrellaState::Active,
            verified_transforms,
        },
    }))
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

/// Response body for `POST /api/v1/umbrellas/propose`.
#[derive(Debug, Serialize)]
pub struct ProposeResponse {
    /// `umbrella_id`s of every umbrella created by this run.
    pub proposed: Vec<String>,
}

/// `POST /api/v1/umbrellas/propose` — manual trigger for the umbrella
/// proposal controller (`crate::umbrella_controller::propose_umbrellas`).
/// Bearer-authenticated like every other route on this router (spec §8); no
/// request body. The controller itself only ever creates PROVISIONAL
/// umbrellas — this endpoint is not a promotion path, `approve` is (see its
/// doc comment).
pub async fn propose(
    State(state): State<ApiState>,
) -> Result<Json<DataEnvelope<ProposeResponse>>, ApiError> {
    let proposed = crate::umbrella_controller::propose_umbrellas(&state).await?;
    Ok(Json(DataEnvelope {
        data: ProposeResponse { proposed },
    }))
}

/// Builds the leaf [`ChildField`]s of a child schema from its stored
/// canonical shape (`SchemaRecord::canonical`) plus its semantic
/// annotations (`SchemaRecord::semantic`), for `approve`'s call into
/// [`verify::verify_static`].
///
/// Only understands the `deblob-monoid-v1` generalized-field-body shape
/// (`{"optional":...,"types":[...],"children":{...},"elem":{...}}`) — the
/// only canonicalizer `Registry::publish` is ever called with (see
/// `crate::policy`, which always sets `canonical:
/// profile.generalized_canonical_json()`). A schema stored under any other
/// `canonicalizer` yields no fields, so every transform binding sourced
/// from it fails `verify_static`'s `SourceMissing` check rather than this
/// function misreading an unrelated JSON shape as monoid fields.
///
/// Mirrors the console's `canonToFields` (`web/console.html`): walks
/// `root.children`, a node with no non-empty object `children` is a leaf,
/// its scalar type is derived from the first non-`"null"` entry of its
/// `types` array, and `is_array` is set whenever `types` contains
/// `"array"`. A leaf whose leading type is `"object"`/`"null"`/unrecognized
/// (an always-empty object, or a field observed only as explicit null) has
/// no [`ScalarType`] representation and is skipped, for the same
/// "structurally absent rather than guessed" reason.
///
/// A field with no matching entry in `SemanticMetadata::fields` (no
/// semantic annotation ever recorded for it) gets `canonical_field_id:
/// None` / `unit: None` — `verify::verify_static` doesn't require either to
/// check structural/type/unit soundness, so this is a legitimate, common
/// case, not an error.
///
/// `semantic` is the schema's CURRENT active semantic annotation, fetched
/// separately by the caller via `SemanticStore::active_semantic` —
/// `SchemaRecord::semantic` itself is always `None` in practice (semantic
/// annotations live in the append-only `SemanticStore`, spec P2-D Task 5/6,
/// never on the registry record), so reading `rec.semantic` here would
/// silently see zero annotations for every schema. `None` means either "no
/// active revision" or "caller couldn't/didn't fetch one" — both are
/// legitimate, common cases (a schema with no semantic annotations yet),
/// not an error.
pub(crate) fn child_fields_from_schema(
    rec: &SchemaRecord,
    semantic: Option<&SemanticMetadata>,
) -> Vec<ChildField> {
    if rec.canonicalizer != deblob_monoid::GENERALIZER {
        return Vec::new();
    }

    let Ok(root) = serde_json::from_str::<serde_json::Value>(&rec.canonical) else {
        return Vec::new();
    };

    let semantic_by_path: BTreeMap<Vec<String>, &FieldSemantics> = semantic
        .map(|sem| {
            sem.fields
                .iter()
                .filter_map(|fe| {
                    let mut segs = Vec::with_capacity(fe.path.len());
                    for seg in &fe.path {
                        match seg {
                            PathSegment::Key(k) => segs.push(k.clone()),
                            // JsonPath is object keys only — an array
                            // wildcard has no leaf path this walk can ever
                            // produce, so such an entry can never match.
                            PathSegment::Wildcard => return None,
                        }
                    }
                    Some((segs, &fe.semantics))
                })
                .collect()
        })
        .unwrap_or_default();

    let mut out = Vec::new();
    walk_canon_node(&root, Vec::new(), &semantic_by_path, &mut out);
    out
}

/// First non-`"null"` entry of a monoid-canonical `"types"` array, falling
/// back to the first entry (even if `"null"`) — matches the console's
/// `canonToFields`'s `leafType`: `ts.find(t=>t!=='null')||ts[0]`.
fn leading_type<'a>(types: &[&'a str]) -> Option<&'a str> {
    types
        .iter()
        .find(|t| **t != "null")
        .copied()
        .or_else(|| types.first().copied())
}

/// Maps one monoid-canonical type tag to a [`ScalarType`]. `"object"` and
/// `"null"` (and anything unrecognized) have no scalar representation.
fn scalar_type_of(tag: &str) -> Option<ScalarType> {
    match tag {
        "bool" => Some(ScalarType::Bool),
        // The monoid canonical shape only ever records the generic
        // "number" tag — never a separate integer/decimal distinction, see
        // `deblob_monoid::profile::write_generalized_field` — so a numeric
        // leaf is conservatively typed `Decimal`: `Integer` widens
        // losslessly to `Decimal` but not the reverse (`ScalarType::
        // widens_losslessly_to`), so this can never cause `verify_static`
        // to silently accept a cast that would actually be lossy.
        "number" => Some(ScalarType::Decimal),
        "string" => Some(ScalarType::String),
        _ => None,
    }
}

/// Recursive walk of one monoid-canonical node, appending every leaf
/// [`ChildField`] reachable under it (dotted `path`, relative to the
/// document root) to `out`. See [`child_fields_from_schema`]'s doc for the
/// exact semantics.
fn walk_canon_node(
    node: &serde_json::Value,
    path: Vec<String>,
    semantic_by_path: &BTreeMap<Vec<String>, &FieldSemantics>,
    out: &mut Vec<ChildField>,
) {
    let Some(obj) = node.as_object() else {
        return;
    };
    let types: Vec<&str> = obj
        .get("types")
        .and_then(serde_json::Value::as_array)
        .map(|a| a.iter().filter_map(serde_json::Value::as_str).collect())
        .unwrap_or_default();
    let children = obj.get("children").and_then(serde_json::Value::as_object);
    let has_object_children = types.contains(&"object") && children.is_some_and(|c| !c.is_empty());

    if has_object_children {
        let children = children.expect("has_object_children implies Some");
        let mut keys: Vec<&String> = children.keys().collect();
        keys.sort();
        for k in keys {
            let mut child_path = path.clone();
            child_path.push(k.clone());
            walk_canon_node(&children[k], child_path, semantic_by_path, out);
        }
        return;
    }

    if path.is_empty() {
        // The root itself carried no object children: no fields at all.
        return;
    }

    let is_array = types.contains(&"array");
    let ty = if is_array {
        // Element scalar type is best-effort — array element typing is
        // itself deferred by `verify::verify_static` for V1 (both sides
        // just need to agree they're arrays) — defaulting to `String` when
        // it can't be determined (e.g. an always-empty array).
        obj.get("elem")
            .and_then(serde_json::Value::as_object)
            .and_then(|e| e.get("types"))
            .and_then(serde_json::Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(serde_json::Value::as_str)
                    .collect::<Vec<_>>()
            })
            .and_then(|elem_types| leading_type(&elem_types))
            .and_then(scalar_type_of)
            .unwrap_or(ScalarType::String)
    } else {
        match leading_type(&types).and_then(scalar_type_of) {
            Some(t) => t,
            None => return, // object/null/unrecognized leaf: not representable
        }
    };

    let Ok(json_path) = JsonPath::parse(&format!("$.{}", path.join("."))) else {
        return;
    };

    let (canonical_field_id, unit) = semantic_by_path
        .get(&path)
        .map(|fs| (fs.canonical_field_id.clone(), fs.unit.clone()))
        .unwrap_or((None, None));

    out.push(ChildField {
        path: json_path,
        ty,
        unit,
        is_array,
        canonical_field_id,
    });
}

#[cfg(test)]
mod child_fields_tests {
    use super::*;
    use deblob_core::id::{FamilyId, FamilyVersion};
    use deblob_core::semantic::{CanonicalFieldId, FieldEntry, SemanticMetadata, Unit, UnitSystem};

    fn schema_record(canonical: &str, semantic: Option<SemanticMetadata>) -> SchemaRecord {
        SchemaRecord {
            schema_id: SchemaId::from_digest(&[1u8; 32]),
            family_id: FamilyId::new_v7(),
            version: FamilyVersion(1),
            canonical: canonical.to_string(),
            canonicalizer: deblob_monoid::GENERALIZER.to_string(),
            provenance: serde_json::json!({}),
            semantic,
            semantic_fingerprint: None,
            privacy_class: None,
            value_profile_ref: None,
            value_profile_summary: None,
        }
    }

    #[test]
    fn walks_nested_leaves_and_applies_semantic_annotations() {
        // {"main":{"temp":<number>},"dt":<number>,"tags":<array of string>}
        let canonical = serde_json::json!({
            "optional": false,
            "types": ["object"],
            "children": {
                "main": {
                    "optional": false,
                    "types": ["object"],
                    "children": {
                        "temp": {"optional": false, "types": ["number"]}
                    }
                },
                "dt": {"optional": false, "types": ["number"]},
                "tags": {
                    "optional": true,
                    "types": ["array"],
                    "elem": {"optional": false, "types": ["string"]}
                }
            }
        })
        .to_string();

        let semantic = SemanticMetadata {
            event_type: None,
            fields: vec![FieldEntry {
                path: vec![
                    PathSegment::Key("main".into()),
                    PathSegment::Key("temp".into()),
                ],
                semantics: FieldSemantics {
                    canonical_field_id: Some(CanonicalFieldId::new("temperature.ambient")),
                    identifier_namespace: None,
                    unit: Some(Unit {
                        system: UnitSystem::Ucum,
                        code: "Cel".into(),
                    }),
                    numeric_scale: None,
                    temporal: None,
                    enum_semantics: None,
                },
            }],
        };

        let rec = schema_record(&canonical, Some(semantic.clone()));
        let mut fields = child_fields_from_schema(&rec, Some(&semantic));
        fields.sort_by(|a, b| String::from(a.path.clone()).cmp(&String::from(b.path.clone())));

        assert_eq!(fields.len(), 3);

        let dt = &fields[0];
        assert_eq!(String::from(dt.path.clone()), "$.dt");
        assert_eq!(dt.ty, ScalarType::Decimal);
        assert!(!dt.is_array);
        assert_eq!(dt.canonical_field_id, None);

        let temp = &fields[1];
        assert_eq!(String::from(temp.path.clone()), "$.main.temp");
        assert_eq!(temp.ty, ScalarType::Decimal);
        assert_eq!(
            temp.canonical_field_id.as_ref().map(|c| c.as_str()),
            Some("temperature.ambient")
        );
        assert_eq!(
            temp.unit.as_ref().map(|u| u.code.clone()),
            Some("Cel".to_string())
        );

        let tags = &fields[2];
        assert_eq!(String::from(tags.path.clone()), "$.tags");
        assert!(tags.is_array);
        assert_eq!(tags.ty, ScalarType::String);
    }

    #[test]
    fn non_monoid_canonicalizer_yields_no_fields() {
        let mut rec = schema_record(
            r#"{"optional":false,"types":["object"],"children":{}}"#,
            None,
        );
        rec.canonicalizer = "deblob-canon-v1".to_string();
        assert!(child_fields_from_schema(&rec, None).is_empty());
    }

    #[test]
    fn malformed_canonical_yields_no_fields() {
        let rec = schema_record("not json", None);
        assert!(child_fields_from_schema(&rec, None).is_empty());
    }
}
