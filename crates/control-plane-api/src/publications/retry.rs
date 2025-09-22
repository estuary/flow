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
            JobStatus::BuildFailed { .. } if has_only_build_errors(result) => {
                let retry = result.built.errors.iter().all(|err| {
                    match err.error.downcast_ref::<validation::Error>() {
                        Some(validation::Error::BuildSuperseded { .. }) => return true,
                        Some(validation::Error::PublicationSuperseded { .. }) => return true,
                        _ => false,
                    }
                });
                if retry {
                    tracing::info!(
                        retry_count = result.retry_count,
                        "will retry due to publication/build superseded error"
                    )
                }
                retry
            }
            _ => false,
        }
    }
}

fn has_only_build_errors(result: &PublicationResult) -> bool {
    !result.built.errors.is_empty()
        && result.draft.errors.is_empty()
        && result.live.errors.is_empty()
        && result.test_errors.is_empty()
}
