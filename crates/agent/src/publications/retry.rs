use super::{JobStatus, PublicationResult};

pub const MAX_OPTIMISTIC_LOCKING_RETRIES: u32 = 10;

/// Inspect a failed commit and return a boolean indicating whether it should be retried.
pub trait RetryPolicy {
    fn retry(&self, result: &PublicationResult) -> bool;
}
pub struct DoNotRetry;
impl RetryPolicy for DoNotRetry {
    fn retry(&self, _result: &PublicationResult) -> bool {
        false
    }
}

pub struct DefaultRetryPolicy;
impl RetryPolicy for DefaultRetryPolicy {
    fn retry(&self, result: &PublicationResult) -> bool {
        if result.retry_count >= MAX_OPTIMISTIC_LOCKING_RETRIES {
            tracing::error!(
                retry_count = result.retry_count,
                status = ?result.status,
                "giving up after maximum number of retries"
            );
            return false;
        }
        // Has there been an optimistic locking failure?
        match &result.status {
            JobStatus::BuildIdLockFailure { failures } => {
                tracing::info!(
                    ?failures,
                    retry_count = result.retry_count,
                    "will retry due to optimistic locking failure"
                );
                true
            }
            _ => false,
        }
    }
}
