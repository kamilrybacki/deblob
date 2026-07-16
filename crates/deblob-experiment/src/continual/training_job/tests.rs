use std::sync::Arc;

use deblob::retrain::{FineTuneError, FineTuneHook, ReplaySet};
use deblob_slm::runtime::ModelFamily;

use super::*;

fn fixed() -> FixedJobParams {
    FixedJobParams {
        trainer_image_digest: "sha256:trainer-v1".to_string(),
        method: TrainingMethod::LoraSft,
        lora: LoraParams::default(),
        seed: 7,
        requested_budget: Budget {
            max_usd: 5.0,
            max_runtime_minutes: 30,
        },
        output_uri: "s3://bucket/out".to_string(),
    }
}

fn replay() -> ReplaySet {
    ReplaySet::default()
}

#[test]
fn needle_family_maps_to_needle_custom_not_lora_sft() {
    assert_eq!(
        TrainingMethod::from_model_family(ModelFamily::NeedleContinualUpdate),
        TrainingMethod::NeedleCustom
    );
    assert_eq!(TrainingMethod::NeedleCustom.as_str(), "needle-custom");
    assert_ne!(TrainingMethod::NeedleCustom, TrainingMethod::LoraSft);
}

#[test]
fn standard_forward_pass_maps_to_lora_sft() {
    assert_eq!(
        TrainingMethod::from_model_family(ModelFamily::StandardForwardPass),
        TrainingMethod::LoraSft
    );
}

#[test]
fn validate_budget_rejects_a_spec_over_the_ceiling() {
    let policy = BudgetPolicy {
        max_usd_ceiling: 10.0,
    };
    let mut spec_ok = TrainingJobSpec {
        base_bundle_digest: "d".to_string(),
        dataset_digest: "d".to_string(),
        feedback_cutoff: 0,
        trainer_image_digest: "d".to_string(),
        method: TrainingMethod::LoraSft,
        lora: LoraParams::default(),
        replay_manifest_digest: "d".to_string(),
        seed: 1,
        budget: Budget {
            max_usd: 9.99,
            max_runtime_minutes: 10,
        },
        output_uri: "u".to_string(),
    };
    assert!(validate_budget(&spec_ok, &policy).is_ok());
    spec_ok.budget.max_usd = 10.01;
    let err = validate_budget(&spec_ok, &policy).unwrap_err();
    assert!(matches!(err, TrainingBackendError::OverBudget { .. }));
}

#[tokio::test]
async fn an_over_budget_spec_never_reaches_submit() {
    let backend = Arc::new(FakeBackend::new());
    let mut over_budget = fixed();
    over_budget.requested_budget.max_usd = 1_000_000.0;
    let hook = TrainingBackendFineTuneHook::new(
        Arc::clone(&backend),
        BudgetPolicy {
            max_usd_ceiling: 100.0,
        },
        over_budget,
    );
    let err = hook.train("base-v0", &replay()).await.unwrap_err();
    assert!(matches!(err, FineTuneError::Process(_)));
    assert_eq!(
        backend.submit_calls(),
        0,
        "submit must never be reached for an over-budget spec"
    );
    assert_eq!(hook.submit_attempts(), 0);
}

#[tokio::test]
async fn fake_backend_pipeline_produces_a_model_artifact_deterministically() {
    let backend = Arc::new(FakeBackend::new());
    let hook = TrainingBackendFineTuneHook::new(
        Arc::clone(&backend),
        BudgetPolicy {
            max_usd_ceiling: 100.0,
        },
        fixed(),
    );
    let artifact_a = hook.train("base-v0", &replay()).await.unwrap();
    let artifact_b = hook.train("base-v0", &replay()).await.unwrap();
    assert_eq!(
        artifact_a, artifact_b,
        "the same base_snapshot + replay_set must deterministically produce the same artifact"
    );
    assert!(artifact_a.model_id.starts_with("lora-sft-"));
    assert_eq!(backend.submit_calls(), 2);
    assert_eq!(backend.poll_calls(), 2, "FakeBackend completes on poll #1");
}

#[tokio::test]
async fn needle_custom_method_is_stamped_into_the_returned_model_id() {
    let backend = Arc::new(FakeBackend::new());
    let mut needle_fixed = fixed();
    needle_fixed.method = TrainingMethod::NeedleCustom;
    let hook = TrainingBackendFineTuneHook::new(
        backend,
        BudgetPolicy {
            max_usd_ceiling: 100.0,
        },
        needle_fixed,
    );
    let artifact = hook.train("base-v0", &replay()).await.unwrap();
    assert!(
        artifact.model_id.starts_with("needle-custom-"),
        "model_id={}",
        artifact.model_id
    );
}

#[test]
fn hf_jobs_backend_builds_a_real_argv_never_invoked_in_tests() {
    let backend = HfJobsBackend::new(HfJobsConfig {
        hf_token_secret_ref: "vault:homelab/hf-token".to_string(),
        output_repo: "kamil/needle-continual".to_string(),
        hardware_flavor: "cpu-basic".to_string(),
    });
    let spec = TrainingJobSpec {
        base_bundle_digest: "sha256:base".to_string(),
        dataset_digest: "sha256:data".to_string(),
        feedback_cutoff: 1000,
        trainer_image_digest: "sha256:trainer".to_string(),
        method: TrainingMethod::NeedleCustom,
        lora: LoraParams::default(),
        replay_manifest_digest: "sha256:replay".to_string(),
        seed: 42,
        budget: Budget {
            max_usd: 1.0,
            max_runtime_minutes: 5,
        },
        output_uri: "s3://out".to_string(),
    };
    let argv = backend.build_command(&spec);
    assert_eq!(argv[0], "hf");
    assert!(argv.contains(&"needle-custom".to_string()));
    assert!(argv.contains(&"vault:homelab/hf-token".to_string()));
    assert!(argv.contains(&"kamil/needle-continual".to_string()));
    assert!(argv.contains(&"sha256:trainer".to_string()));
}
