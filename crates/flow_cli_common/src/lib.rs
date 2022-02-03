//! Contains helpers and things that are used by all Flow rust executables.

mod logging;

pub use logging::{init_logging, LogArgs, LogFormat};

/// Helper trait for exiting the application early if there's an error.
pub trait OrBail<T> {
    fn or_bail(self, message: &str) -> T;
}

impl<T, E> OrBail<T> for Result<T, E>
where
    E: std::fmt::Display + std::fmt::Debug,
{
    fn or_bail(self, message: &str) -> T {
        match self {
            Ok(t) => t,
            Err(e) => {
                tracing::debug!(error_details = ?e, message);
                tracing::error!(error = %e, message);
                std::process::exit(1);
            }
        }
    }
}
