//! Error types. Spec §3, §7.

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuarantineReason {
    DuplicateKey,
    NonFiniteNumber,
    DepthExceeded,
    SizeExceeded,
    FieldCountExceeded,
    KeyLengthExceeded,
    ParseError,
    Utf8Error,
}

#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    #[error("registry unavailable: {0}")]
    RegistryUnavailable(String),
    #[error("immutability violation: {0}")]
    ImmutabilityViolation(String),
    #[error("not found")]
    NotFound,
    #[error("conflict: {0}")]
    Conflict(String),
    /// A request was well-formed and its target exists, but a business
    /// policy guard rejected it (spec §5/§6/§8) — e.g. promoting a
    /// candidate before it has crossed the minimum sample-count/age bar.
    /// Deliberately distinct from `Conflict` (a state-machine/identity
    /// clash the caller can't fix by waiting) so the API layer can map
    /// this to `422 Unprocessable Entity` rather than `409 Conflict`.
    #[error("policy rejected: {0}")]
    PolicyRejected(String),
}
