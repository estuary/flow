use std::sync::Arc;

pub mod jwt;
mod rest;
mod stream;
mod watch;

// Re-export CancellationToken and friends from tokio-util.
// These are foundational for signal handling in this crate and its users.
pub use tokio_util::sync::{
    CancellationToken, WaitForCancellationFuture, WaitForCancellationFutureOwned,
};

// Re-export of chrono::DateTime<Utc>, as the foundational datetime type.
pub type DateTime = chrono::DateTime<chrono::Utc>;

// Re-export of chrono::TimeDelta, as the foundational duration type.
pub use chrono::TimeDelta;

pub use rest::RestSource;
pub use stream::StreamSource;
pub use watch::{fixed, manual, map, watch};

/// Source is a trait for producing an associated Token type on demand.
pub trait Source: Send + Sized + 'static {
    type Token: Send + Sync + 'static;

    /// Future type that, when it resolves, signals early token revocation.
    /// Use `std::future::Pending<()>` for sources that never revoke early.
    type Revoke: std::future::Future<Output = ()> + Send;

    /// Refresh a Token from the Source.
    ///
    /// `started` is when the overall refresh operation began,
    /// and is held constant across retries.
    ///
    /// Refresh returns a future that resolves to:
    /// - Ok(Ok((Token, valid_for, revoke))) if the refresh was successful,
    ///   where `valid_for` is the remaining lifetime of the yielded Token,
    ///   and `revoke` is a future that, when it resolves, signals that the
    ///   Token should be refreshed immediately rather than waiting for
    ///   `valid_for` to elapse. Sources that don't need early revocation
    ///   should use `std::future::pending()`.
    /// - Ok(Err(retry_after)) if the refresh result was indeterminate
    ///   and should be retried after `retry_after`.
    /// - Err(err) if a client-facing error occurred.
    fn refresh(
        &mut self,
        started: DateTime,
    ) -> impl std::future::Future<
        Output = tonic::Result<Result<(Self::Token, TimeDelta, Self::Revoke), TimeDelta>>,
    > + Send;
}

/// Refresh represents the result of a Token refresh operation.
pub struct Refresh<Token> {
    result: tonic::Result<Token>,
    version: u64,
    expired: CancellationToken,
}

impl<Token> Refresh<Token> {
    /// Result of this token refresh.
    #[inline]
    pub fn result(&self) -> tonic::Result<&Token> {
        self.result.as_ref().map_err(Clone::clone)
    }
    /// Returns true if this Refresh has expired.
    pub fn is_expired(&self) -> bool {
        self.expired.is_cancelled()
    }
    /// Future that resolves when this Refresh has expired.
    #[inline]
    pub fn expired<'a>(&'a self) -> WaitForCancellationFuture<'a> {
        self.expired.cancelled()
    }
    /// Owned Future that resolves when this Refresh has expired.
    #[inline]
    pub fn expired_owned(&self) -> WaitForCancellationFutureOwned {
        self.expired.clone().cancelled_owned()
    }
    /// Version of this Token refresh.
    #[inline]
    pub fn version(&self) -> u64 {
        self.version
    }
}

/// Watch provides access to a Token that is periodically refreshed.
pub trait Watch<Token>: Send + Sync {
    /// Get the current Refresh of the Token.
    fn token(&self) -> Arc<Refresh<Token>>;
    /// Get the current version of the Token.
    /// This is more efficient than calling token().version.
    fn version(&self) -> u64;
}

/// PendingWatch wraps a dyn Watch which may not yet be ready for use.
pub struct PendingWatch<Token> {
    inner: Arc<dyn Watch<Token>>,
    signal: CancellationToken,
}

// Manual Clone impl to avoid requiring Token: Clone
impl<Token> Clone for PendingWatch<Token> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            signal: self.signal.clone(),
        }
    }
}

impl<Token> PendingWatch<Token> {
    /// Return a future that resolves when the PendingWatch becomes ready,
    /// yielding a reference to the dyn Watch.
    pub fn ready<'a>(
        &'a self,
    ) -> impl std::future::Future<Output = &'a Arc<dyn Watch<Token>>> + 'a {
        let Self { inner, signal } = self;

        async move {
            signal.cancelled().await;
            inner
        }
    }

    /// Return a future that resolves when the PendingWatch becomes ready,
    /// yielding an owned dyn Watch.
    pub fn ready_owned(self) -> impl std::future::Future<Output = Arc<dyn Watch<Token>>> {
        let Self { inner, signal } = self;

        async move {
            signal.cancelled().await;
            inner
        }
    }

    /// Return a future that resolves when the PendingWatch becomes ready.
    pub fn ready_signal(&self) -> WaitForCancellationFutureOwned {
        self.signal.clone().cancelled_owned()
    }
}

/// Return the current DateTime.
///
/// This routine is intended for Tokens and JWTs which generally use Unix
/// timestamps for validity periods that are communicated between systems.
///
/// The timestamps returned by this routine are aware of tokio test time,
/// and will return coherent values in paused testing contexts where
/// tokio::time::advance() or auto-advance is used.
///
/// Use this routine over SystemTime or jsonwebtoken::get_current_timestamp().
pub fn now() -> DateTime {
    // In debug builds, use a fixed point to map between Instant and DateTime,
    // allowing tokio test time to influence the result.
    #[cfg(debug_assertions)]
    {
        // TIME_POINT is a (Instant, DateTime) pair captured at the same time.
        // It allows for mapping between Instant and DateTime.
        //
        // Beware! There's potential for clock drift over time due to NTP steps
        // or because of subtle drifts between the two clocks. This is acceptable
        // in tests (only).
        static TIME_POINT: std::sync::LazyLock<(std::time::Instant, DateTime)> =
            std::sync::LazyLock::new(|| (std::time::Instant::now(), chrono::Utc::now()));

        let (start_instant, start_unix) = *TIME_POINT;
        let elapsed = tokio::time::Instant::now()
            .duration_since(tokio::time::Instant::from_std(start_instant));

        start_unix + elapsed
    }

    // In release builds, just use chrono (system time) directly.
    #[cfg(not(debug_assertions))]
    {
        chrono::Utc::now()
    }
}
