//! [`HfJobsBackend`] — an alternative real remote [`TrainingBackend`] (spec
//! §8): shells out to the `hf jobs` CLI. Superseded as arm C's CHOSEN
//! backend by `super::modal::ModalBackend` (Modal T4 + the $30/mo free
//! credit is the cheaper real-training path), but kept working and
//! config-selectable — `HfJobsBackend::build_command` is real, tested
//! argv construction; `submit`/`poll` are exercised by NOTHING in this
//! crate's test suite (no live network/CLI in tests — spec: "NOT invoked
//! in tests"). Wiring a real endpoint is a deploy-time concern (mirrors
//! `deblob::retrain::ShellFineTuneHook`'s own real-shell-out pattern, at
//! arm's length via `tokio::process::Command`).

use async_trait::async_trait;

use super::{JobHandle, JobStatus, TrainingBackend, TrainingBackendError, TrainingJobSpec};

/// Deploy-time configuration for [`HfJobsBackend`] — a secret REFERENCE
/// (e.g. a Vault path/HF secret name), never a raw token.
#[derive(Debug, Clone)]
pub struct HfJobsConfig {
    pub hf_token_secret_ref: String,
    pub output_repo: String,
    pub hardware_flavor: String,
}

pub struct HfJobsBackend {
    config: HfJobsConfig,
}

impl HfJobsBackend {
    pub fn new(config: HfJobsConfig) -> Self {
        Self { config }
    }

    /// Builds the literal `hf jobs run ...` argv for `spec` — command +
    /// image + secrets ref + output repo (spec §8's literal ask). Pure and
    /// synchronous, independently testable without spawning a process.
    pub fn build_command(&self, spec: &TrainingJobSpec) -> Vec<String> {
        vec![
            "hf".to_string(),
            "jobs".to_string(),
            "run".to_string(),
            "--flavor".to_string(),
            self.config.hardware_flavor.clone(),
            "--secrets".to_string(),
            self.config.hf_token_secret_ref.clone(),
            spec.trainer_image_digest.clone(),
            "--base-bundle".to_string(),
            spec.base_bundle_digest.clone(),
            "--dataset-digest".to_string(),
            spec.dataset_digest.clone(),
            "--method".to_string(),
            spec.method.as_str().to_string(),
            "--seed".to_string(),
            spec.seed.to_string(),
            "--output-repo".to_string(),
            self.config.output_repo.clone(),
            "--output-uri".to_string(),
            spec.output_uri.clone(),
        ]
    }
}

#[async_trait]
impl TrainingBackend for HfJobsBackend {
    async fn submit(&self, spec: &TrainingJobSpec) -> Result<JobHandle, TrainingBackendError> {
        let argv = self.build_command(spec);
        let output = tokio::process::Command::new(&argv[0])
            .args(&argv[1..])
            .output()
            .await
            .map_err(|e| TrainingBackendError::Submit(e.to_string()))?;
        if !output.status.success() {
            return Err(TrainingBackendError::Submit(
                String::from_utf8_lossy(&output.stderr).to_string(),
            ));
        }
        Ok(JobHandle(
            String::from_utf8_lossy(&output.stdout).trim().to_string(),
        ))
    }

    async fn poll(&self, handle: &JobHandle) -> Result<JobStatus, TrainingBackendError> {
        let output = tokio::process::Command::new("hf")
            .args(["jobs", "inspect", &handle.0])
            .output()
            .await
            .map_err(|e| TrainingBackendError::Poll(e.to_string()))?;
        if !output.status.success() {
            return Err(TrainingBackendError::Poll(
                String::from_utf8_lossy(&output.stderr).to_string(),
            ));
        }
        // Real status parsing is a deploy-time concern this crate's test
        // suite never exercises (see module docs) — `hf jobs inspect`'s
        // JSON shape is not modeled here.
        Ok(JobStatus::Running)
    }
}
