//! Error type for the thin-mode SODA layer.

use crate::Error;

/// Errors raised by the SODA domain layer before (or instead of) hitting the
/// database. Driver/server errors are surfaced through the wrapped
/// [`crate::Error`] in the `Driver` variant.
#[derive(Debug)]
#[non_exhaustive]
pub enum SodaError {
    /// The collection metadata JSON could not be parsed into a usable shape.
    InvalidMetadata(String),
    /// A query-by-example filter could not be translated to SQL/JSON.
    Qbe(String),
    /// The requested operation or operator is not supported by thin-mode SODA.
    NotSupported(String),
    /// An underlying driver/server error.
    Driver(Error),
}

impl std::fmt::Display for SodaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SodaError::InvalidMetadata(m) => write!(f, "invalid SODA metadata: {m}"),
            SodaError::Qbe(m) => write!(f, "invalid SODA QBE filter: {m}"),
            SodaError::NotSupported(m) => write!(f, "SODA feature not supported in thin mode: {m}"),
            SodaError::Driver(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for SodaError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SodaError::Driver(e) => Some(e),
            _ => None,
        }
    }
}

impl From<Error> for SodaError {
    fn from(e: Error) -> Self {
        SodaError::Driver(e)
    }
}

/// Result alias for SODA operations.
pub type Result<T> = std::result::Result<T, SodaError>;
