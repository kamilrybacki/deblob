//! Arm C — the prequential continual-learning loop (spec §7) + the
//! provider-neutral remote fine-tune hook (spec §8).
//!
//! - [`datasets`] — the three strictly-separated prequential datasets
//!   (round stream / development set / sealed final audit set).
//! - [`prequential`] — [`prequential::PrequentialRunner`], which
//!   ORCHESTRATES (never reimplements/weakens) `deblob::feedback` +
//!   `deblob::retrain::RetrainPlan` + `deblob::model_registry`, and
//!   [`prequential::FrozenTrajectory`], the sealed-audit-set gateway.
//! - [`training_job`] — [`training_job::TrainingJobSpec`] +
//!   [`training_job::TrainingBackend`] (submit/poll), the provider-neutral
//!   remote fine-tune hook `deblob::retrain::FineTuneHook` plugs into via
//!   [`training_job::TrainingBackendFineTuneHook`].
//! - [`metrics`] — adaptation-gain / retention-loss primitives, reusing
//!   Task 1's external-label scoring convention.

pub mod datasets;
pub mod metrics;
pub mod prequential;
pub mod training_job;

pub use datasets::PrequentialConfig;
pub use prequential::{FrozenTrajectory, PrequentialError, PrequentialRunner, RoundRecord};
pub use training_job::{
    Budget, BudgetPolicy, FakeBackend, FixedJobParams, HfJobsBackend, HfJobsConfig, JobHandle,
    JobStatus, LoraParams, TrainingBackend, TrainingBackendError, TrainingBackendFineTuneHook,
    TrainingJobSpec, TrainingMethod,
};
