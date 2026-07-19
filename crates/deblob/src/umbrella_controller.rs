//! Umbrella proposal controller — the pipeline that turns silver-annotated
//! schemas into PROVISIONAL gold [`deblob_umbrella::types::UmbrellaSchema`]s
//! plus their [`deblob_umbrella::types::ChildTransform`]s, ready for human
//! review via `POST /api/v1/umbrellas/{umbrella_id}/approve`
//! (`crate::api::umbrellas::approve`).
//!
//! This module only ever creates PROVISIONAL umbrellas — promotion to
//! `Active` is exclusively the human-triggered `approve` gate's job (spec:
//! umbrella activation is HITL-only). Currently a documented stub: `POST
//! /api/v1/umbrellas/propose` (`crate::api::umbrellas::propose`) calls
//! [`propose_umbrellas`] so the manual trigger has a concrete target to
//! call, but the actual pipeline is not implemented here yet.

use crate::api::{ApiError, ApiState};

/// Proposes new PROVISIONAL umbrellas from the current registry state.
/// Returns the `umbrella_id`s of every umbrella created by this run (empty
/// if nothing new was found to propose).
///
/// TODO(main): enumerate silver-annotated schemas -> adjudicate -> assemble
/// -> verify_static -> create provisional umbrellas + transforms.
pub async fn propose_umbrellas(_state: &ApiState) -> Result<Vec<String>, ApiError> {
    Ok(vec![])
}
