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
}
