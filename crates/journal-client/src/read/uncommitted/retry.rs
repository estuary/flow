use std::fmt::Debug;
use std::io;
use std::time::Duration;

/// Re-exports the type from the `exponential-backoff` crate, so that users of the library don't
/// need to add their own dependency in order to configure it.
pub use exponential_backoff::Backoff;

/// Determines which errors to retry and how long to wait before doing so.
pub trait Retry: Debug + Clone + Unpin {
    /// Reset the internal state of the `Retry`. This function is called whenever any read
    /// operation was successful, so that dynamic backoffs can go back to their minimum values.
    fn reset(&mut self);

    /// Determines whether the given `error` should be retried, and what the backoff should be.
    /// If the returned backoff is `None`, it will abort the read and the error will be surfaced to
    /// the caller of the `Reader`. If `Some`, then the operation will be retried after the given
    /// duration elapses.
    fn next_backoff(&mut self, error: &io::Error) -> Option<Duration>;
}

/// A `Retry` that doesn't. Using a `retry::Reader<NoRetry>`
#[derive(Debug, Clone)]
pub struct NoRetry;
impl Retry for NoRetry {
    fn reset(&mut self) { /* no-op */
    }

    fn next_backoff(&mut self, _error: &io::Error) -> Option<Duration> {
        None
    }
}

#[derive(Debug, Clone)]
pub struct ExponentialBackoff {
    error_count: u32,
    backoff: Backoff,
}

impl ExponentialBackoff {
    pub fn new(max_retries: impl Into<Option<u32>>) -> ExponentialBackoff {
        let backoff = Backoff::new(
            max_retries.into().unwrap_or(u32::MAX),
            Duration::from_millis(100),
            Some(Duration::from_secs(300)),
        );
        ExponentialBackoff {
            error_count: 0,
            backoff,
        }
    }

    pub fn with_min(mut self, min: Duration) -> Self {
        self.backoff.set_min(min);
        self
    }

    pub fn with_max(mut self, max: Duration) -> Self {
        self.backoff.set_max(Some(max));
        self
    }

    pub fn with_jitter(mut self, jitter: f32) -> Self {
        self.backoff.set_jitter(jitter);
        self
    }

    pub fn with_factor(mut self, factor: u32) -> Self {
        self.backoff.set_factor(factor);
        self
    }
}

impl Retry for ExponentialBackoff {
    fn reset(&mut self) {
        self.error_count = 0;
    }

    fn next_backoff(&mut self, _error: &io::Error) -> Option<Duration> {
        self.error_count += 1;
        self.backoff.next(self.error_count)
    }
}

impl From<Backoff> for ExponentialBackoff {
    fn from(backoff: Backoff) -> Self {
        ExponentialBackoff {
            error_count: 0,
            backoff,
        }
    }
}
impl Default for ExponentialBackoff {
    fn default() -> Self {
        Self {
            error_count: 0,
            backoff: Backoff::new(
                u32::MAX,
                Duration::from_millis(100),
                Some(Duration::from_secs(300)),
            ),
        }
    }
}
