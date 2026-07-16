use std::sync::Mutex;

use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::*;
use crate::continual::training_job::{Budget, JobStatus, TrainingBackendError, TrainingMethod};

/// Guards the two `MODAL_TOKEN_ID`/`MODAL_TOKEN_SECRET` env-mutating tests
/// below from each other — `std::env::set_var`/`remove_var` touch global
/// process state, and Rust runs tests in the same binary concurrently by
/// default.
static ENV_LOCK: Mutex<()> = Mutex::new(());

fn spec(method: TrainingMethod, max_usd: f64) -> TrainingJobSpec {
    TrainingJobSpec {
        base_bundle_digest: "sha256:base".to_string(),
        dataset_digest: "sha256:data".to_string(),
        feedback_cutoff: 1_000,
        trainer_image_digest: "sha256:trainer".to_string(),
        method,
        lora: LoraParams::default(),
        replay_manifest_digest: "sha256:replay".to_string(),
        seed: 42,
        budget: Budget {
            max_usd,
            max_runtime_minutes: 30,
        },
        output_uri: "s3://bucket/out".to_string(),
    }
}

fn config(endpoint_base: String) -> ModalConfig {
    ModalConfig {
        endpoint_base,
        app_name: "deblob-trainer".to_string(),
        cached_image_tag: "sha256:trainer-image-v1".to_string(),
        cached_volume_name: "deblob-base-models".to_string(),
        budget_policy: BudgetPolicy {
            max_usd_ceiling: 5.0,
        },
    }
}

fn credentials() -> ModalCredentials {
    ModalCredentials {
        token_id: "id-123".to_string(),
        token_secret: "secret-abc".to_string(),
    }
}

// ---------------------------------------------------------------------
// ModalBackend has no path to `promote` (separation of duties, spec §8) —
// proved at COMPILE TIME: this fails to build if `ModalBackend` ever
// implements `deblob::model_registry::ModelRegistry` (the ONLY trait
// `promote` lives on).
// ---------------------------------------------------------------------
static_assertions::assert_not_impl_any!(ModalBackend: deblob::model_registry::ModelRegistry);

#[test]
fn credentials_from_env_reads_the_token_pair_not_a_hardcoded_value() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var(MODAL_TOKEN_ID_ENV, "env-id");
    std::env::set_var(MODAL_TOKEN_SECRET_ENV, "env-secret");
    let creds = ModalCredentials::from_env().unwrap();
    assert_eq!(creds.token_id, "env-id");
    assert_eq!(creds.token_secret, "env-secret");
    std::env::remove_var(MODAL_TOKEN_ID_ENV);
    std::env::remove_var(MODAL_TOKEN_SECRET_ENV);
}

#[test]
fn credentials_from_env_errors_without_a_panic_when_unset() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::remove_var(MODAL_TOKEN_ID_ENV);
    std::env::remove_var(MODAL_TOKEN_SECRET_ENV);
    let err = ModalCredentials::from_env().unwrap_err();
    assert!(matches!(err, TrainingBackendError::Submit(_)));
}

#[test]
fn credentials_debug_never_prints_the_real_token_values() {
    let creds = credentials();
    let rendered = format!("{creds:?}");
    assert!(!rendered.contains("id-123"));
    assert!(!rendered.contains("secret-abc"));
}

#[test]
fn build_submit_request_carries_the_spec_and_the_modal_token_headers() {
    let backend = ModalBackend::new(config("http://example.invalid".to_string()), credentials());
    let req = backend.build_submit_request(&spec(TrainingMethod::LoraSft, 1.0));

    assert_eq!(req.url, "http://example.invalid/submit");
    assert!(req
        .headers
        .contains(&("Modal-Key".to_string(), "id-123".to_string())));
    assert!(req
        .headers
        .contains(&("Modal-Secret".to_string(), "secret-abc".to_string())));
    assert_eq!(req.body["base_bundle_digest"], "sha256:base");
    assert_eq!(req.body["dataset_digest"], "sha256:data");
    assert_eq!(req.body["seed"], 42);
    assert_eq!(req.body["method"], "lora-sft");
    assert_eq!(req.body["cached_image_tag"], "sha256:trainer-image-v1");
    assert_eq!(req.body["cached_volume_name"], "deblob-base-models");
}

#[test]
fn needle_custom_method_is_sent_as_needle_custom_never_lora_sft() {
    let backend = ModalBackend::new(config("http://example.invalid".to_string()), credentials());
    let req = backend.build_submit_request(&spec(TrainingMethod::NeedleCustom, 1.0));
    assert_eq!(req.body["method"], "needle-custom");
    assert_ne!(req.body["method"], "lora-sft");
}

#[test]
fn build_status_request_targets_the_handles_own_status_path() {
    let backend = ModalBackend::new(config("http://example.invalid".to_string()), credentials());
    let req = backend.build_status_request(&JobHandle("job-xyz".to_string()));
    assert_eq!(req.url, "http://example.invalid/status/job-xyz");
    assert!(req
        .headers
        .contains(&("Modal-Key".to_string(), "id-123".to_string())));
}

#[tokio::test]
async fn an_over_budget_spec_is_rejected_before_any_network_call() {
    // No mock server at all: if `submit` reached the network, dialing a
    // non-routable address would surface as `TrainingBackendError::Submit`,
    // not `OverBudget` — asserting the exact variant AND `submit_calls()
    // == 0` together prove the HTTP path was never reached.
    let backend = ModalBackend::new(config("http://example.invalid".to_string()), credentials());
    let over_budget = spec(TrainingMethod::LoraSft, 1_000_000.0);
    let err = backend.submit(&over_budget).await.unwrap_err();
    assert!(matches!(err, TrainingBackendError::OverBudget { .. }));
    assert_eq!(backend.submit_calls(), 0);
}

#[tokio::test]
async fn submit_sends_the_translated_spec_and_returns_the_job_id() {
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/submit"))
        .and(header("Modal-Key", "id-123"))
        .and(header("Modal-Secret", "secret-abc"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"job_id": "job-xyz"})),
        )
        .mount(&mock_server)
        .await;

    let backend = ModalBackend::new(config(mock_server.uri()), credentials());
    let handle = backend
        .submit(&spec(TrainingMethod::LoraSft, 1.0))
        .await
        .unwrap();
    assert_eq!(handle.0, "job-xyz");
    assert_eq!(backend.submit_calls(), 1);
}

#[tokio::test]
async fn submit_maps_a_non_success_http_status_to_a_submit_error_not_a_panic() {
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/submit"))
        .respond_with(ResponseTemplate::new(500).set_body_string("internal error"))
        .mount(&mock_server)
        .await;

    let backend = ModalBackend::new(config(mock_server.uri()), credentials());
    let err = backend
        .submit(&spec(TrainingMethod::LoraSft, 1.0))
        .await
        .unwrap_err();
    assert!(matches!(err, TrainingBackendError::Submit(_)));
}

#[tokio::test]
async fn poll_maps_running_done_and_failed_statuses() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/status/job-running"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"status": "running"})),
        )
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/status/job-done"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "done",
            "artifact_digests": {
                "training_checkpoint": "sha256:ckpt",
                "quantized_weights": "sha256:quant",
            },
        })))
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/status/job-failed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "failed",
            "reason": "OOM on the T4",
        })))
        .mount(&mock_server)
        .await;

    let backend = ModalBackend::new(config(mock_server.uri()), credentials());

    let running = backend
        .poll(&JobHandle("job-running".to_string()))
        .await
        .unwrap();
    assert_eq!(running, JobStatus::Running);

    let done = backend
        .poll(&JobHandle("job-done".to_string()))
        .await
        .unwrap();
    match done {
        JobStatus::Done { artifact_digests } => {
            assert_eq!(
                artifact_digests.get("training_checkpoint"),
                Some(&"sha256:ckpt".to_string())
            );
            assert_eq!(
                artifact_digests.get("quantized_weights"),
                Some(&"sha256:quant".to_string())
            );
        }
        other => panic!("expected Done, got {other:?}"),
    }

    let failed = backend
        .poll(&JobHandle("job-failed".to_string()))
        .await
        .unwrap();
    assert_eq!(
        failed,
        JobStatus::Failed {
            reason: "OOM on the T4".to_string()
        }
    );
    assert_eq!(backend.poll_calls(), 3);
}

#[test]
fn parse_poll_response_maps_a_malformed_body_to_a_safe_failed_never_a_panic() {
    let status = parse_poll_response("not json at all");
    match status {
        JobStatus::Failed { reason } => assert!(reason.contains("malformed")),
        other => panic!("expected Failed, got {other:?}"),
    }

    // Valid JSON, but missing the required `status` tag entirely.
    let status2 = parse_poll_response(r#"{"foo": "bar"}"#);
    assert!(matches!(status2, JobStatus::Failed { .. }));
}

#[tokio::test]
async fn poll_maps_a_non_success_http_status_to_a_poll_error_not_a_panic() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/status/job-500"))
        .respond_with(ResponseTemplate::new(503).set_body_string("unavailable"))
        .mount(&mock_server)
        .await;

    let backend = ModalBackend::new(config(mock_server.uri()), credentials());
    let err = backend
        .poll(&JobHandle("job-500".to_string()))
        .await
        .unwrap_err();
    assert!(matches!(err, TrainingBackendError::Poll(_)));
}

#[tokio::test]
async fn from_env_builds_a_backend_that_reads_the_token_pair_from_the_process_environment() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var(MODAL_TOKEN_ID_ENV, "env-id-2");
    std::env::set_var(MODAL_TOKEN_SECRET_ENV, "env-secret-2");
    let backend = ModalBackend::from_env(config("http://example.invalid".to_string())).unwrap();
    let req = backend.build_submit_request(&spec(TrainingMethod::LoraSft, 1.0));
    assert!(req
        .headers
        .contains(&("Modal-Key".to_string(), "env-id-2".to_string())));
    assert!(req
        .headers
        .contains(&("Modal-Secret".to_string(), "env-secret-2".to_string())));
    std::env::remove_var(MODAL_TOKEN_ID_ENV);
    std::env::remove_var(MODAL_TOKEN_SECRET_ENV);
}
