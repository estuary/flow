use crate::{
    CancellationToken, PendingWatch, Refresh, Source, TimeDelta, WaitForCancellationFutureOwned,
    Watch,
};
use futures::future::OptionFuture;
use std::sync::Arc;

/// Cell is a Watch implemented as a Mutex-protected Arc of a Refresh Token.
struct Cell<Token>(std::sync::Mutex<Arc<Refresh<Token>>>);

impl<Token> Cell<Token> {
    /// Replace the current Token with a new result, notifying waiters.
    /// Returns None if all clones have been dropped, or Some otherwise.
    /// The caller may use the returned CancellationToken to detect when the Cell
    /// has been dropped (no recipients remain).
    fn replace(
        this: &std::sync::Weak<Self>,
        result: tonic::Result<Token>,
    ) -> Option<WaitForCancellationFutureOwned> {
        let Some(this) = this.upgrade() else {
            return None; // All clones dropped.
        };
        let mut cell = this.0.lock().unwrap();
        cell.expired.cancel(); // Notify current waiters of refresh.

        let expired = CancellationToken::new();
        *cell = Arc::new(Refresh {
            result,
            version: cell.version + 1,
            expired: expired.clone(),
        });
        Some(expired.cancelled_owned())
    }
}

impl<Token> Drop for Cell<Token> {
    fn drop(&mut self) {
        // Cancel the current Refresh signal to notify any Refresh bearers
        // that the owning Watch has been dropped.
        let cell = self.0.get_mut().unwrap();
        cell.expired.cancel();
    }
}

impl<Token> Watch<Token> for Cell<Token>
where
    Token: Send + Sync,
{
    fn token(&self) -> Arc<Refresh<Token>> {
        let guard = self.0.lock().unwrap();
        Arc::clone(&guard)
    }

    fn version(&self) -> u64 {
        let guard = self.0.lock().unwrap();
        guard.version
    }
}

/// Build a PendingWatch that is refreshed through the returned closure.
/// It becomes ready upon the first invocation of the closure.
///
/// If clones of the returned Watch remain, then the replacement closure will
/// return Some(WaitForCancellationFutureOwned). Callers may want to await this
/// Future to detect that the final Watch clone has been dropped.
///
/// If all clones of the Watch have already been dropped, then the replacement
/// closure returns None.
pub fn manual<Token>() -> (
    PendingWatch<Token>,
    impl Fn(tonic::Result<Token>) -> Option<WaitForCancellationFutureOwned>,
)
where
    Token: Send + Sync + 'static,
{
    let ready = CancellationToken::new();
    let cell = Arc::new(Cell(std::sync::Mutex::new(Arc::new(Refresh {
        expired: ready.clone(),
        result: Err(tonic::Status::unavailable("placeholder")),
        version: 0,
    }))));
    let cell_weak = Arc::downgrade(&cell);

    let pending = PendingWatch {
        inner: cell,
        signal: ready.clone(),
    };

    let replace = move |result: tonic::Result<Token>| -> Option<WaitForCancellationFutureOwned> {
        Cell::replace(&cell_weak, result)
    };
    (pending, replace)
}

/// Build a PendingWatch that always returns the same fixed result.
/// It becomes ready immediately.
pub fn fixed<Token>(result: tonic::Result<Token>) -> PendingWatch<Token>
where
    Token: Send + Sync + 'static,
{
    let (pending, replace) = manual();
    _ = replace(result);
    pending
}

/// Watch a Source via a spawned tokio task, periodically refreshing ahead of
/// Token expiry. Returns a PendingWatch that's ready after its first refresh.
/// The spawned task stops when all clones of the returned Watch are dropped.
pub fn watch<S>(mut source: S) -> PendingWatch<S::Token>
where
    S: Source,
{
    let (pending, replace) = manual();

    // Signaled when the Watch has been dropped, to terminate the background task.
    let mut dropped = Box::pin(pending.ready_signal());

    tokio::spawn(async move {
        let mut backoff = TimeDelta::zero();
        let mut maybe_started = None;
        let mut maybe_revoke: Option<S::Revoke> = None;

        loop {
            let revoke = OptionFuture::from(maybe_revoke.take());
            tokio::pin!(revoke);

            tokio::select! {
                _ = dropped.as_mut() => {
                    return; // All clones dropped.
                }
                Some(()) = revoke => {
                    // Source requested early refresh.
                }
                () = tokio::time::sleep(backoff.to_std().unwrap_or_default()) => {}
            }

            let started = if let Some(started) = maybe_started {
                started
            } else {
                let now = crate::now();
                maybe_started = Some(now);
                now
            };

            let result = match source.refresh(started).await {
                Ok(Ok((token, valid_for, new_revoke))) => {
                    backoff = (valid_for - TimeDelta::minutes(2)).max(TimeDelta::minutes(1));
                    maybe_started = None;
                    maybe_revoke = Some(new_revoke);

                    Ok(token)
                }
                Ok(Err(retry_after)) => {
                    backoff = retry_after;
                    continue;
                }
                Err(err) => {
                    // Re-attempt after a random backoff centered around 1 minute.
                    backoff = TimeDelta::milliseconds(rand::random_range(45_000..75_000));
                    maybe_started = None;

                    Err(err)
                }
            };

            if let Some(next_dropped) = replace(result) {
                dropped = Box::pin(next_dropped);
            }
        }
    });

    pending
}

/// Map a Watch of Input into Output via a closure, producing a new Watch.
///
/// The closure takes the latest Input value, and (if available) the prior
/// Ok(Input) and Ok(Output). Implementors may want to optimize a current
/// mapping by re-using parts of the prior Output.
///
/// The closure is called just once for each version of the parent Watch,
/// and its Output is cached until the parent Watch version changes again.
pub fn map<Input, F, Output>(parent: Arc<dyn Watch<Input>>, f: F) -> Arc<dyn Watch<Output>>
where
    F: for<'a> Fn(&'a Input, Option<(&'a Input, &'a Output)>) -> tonic::Result<Output>
        + Send
        + Sync
        + 'static,
    Output: Send + Sync + 'static,
    Input: Send + Sync + 'static,
{
    Arc::new(MappedCell {
        cell: std::sync::Mutex::new((
            parent.token(),
            Arc::new(Refresh {
                expired: CancellationToken::new(),
                result: Err(tonic::Status::unavailable("placeholder")),
                version: u64::MAX, // Sentinel != parent.version, to force re-evaluation.
            }),
        )),
        f,
        parent,
    })
}

impl<Token> PendingWatch<Token> {
    /// Map a PendingWatch of Input into Output via a closure, producing a new PendingWatch.
    /// See `map` for details.
    pub fn map<F, Output>(self, f: F) -> PendingWatch<Output>
    where
        F: for<'a> Fn(&'a Token, Option<(&'a Token, &'a Output)>) -> tonic::Result<Output>
            + Send
            + Sync
            + 'static,
        Output: Send + Sync + 'static,
        Token: Send + Sync + 'static,
    {
        let Self { inner, signal } = self;
        let inner = map(inner, f);
        PendingWatch { inner, signal }
    }
}

// MappedCell is a Watch that maps Input from a parent Watch into Output via a closure.
struct MappedCell<Input, F, Output>
where
    F: for<'a> Fn(&'a Input, Option<(&'a Input, &'a Output)>) -> tonic::Result<Output>,
{
    cell: std::sync::Mutex<(Arc<Refresh<Input>>, Arc<Refresh<Output>>)>,
    f: F,
    parent: Arc<dyn Watch<Input>>,
}

impl<Input, F, Output> Watch<Output> for MappedCell<Input, F, Output>
where
    F: for<'a> Fn(&'a Input, Option<(&'a Input, &'a Output)>) -> tonic::Result<Output>
        + Send
        + Sync
        + 'static,
    Output: Send + Sync + 'static,
    Input: Send + Sync + 'static,
{
    fn token(&self) -> Arc<Refresh<Output>> {
        let mut cell = self.cell.lock().unwrap();
        let (prev_input, output) = &mut *cell;

        if output.version == self.parent.version() {
            // Fast path: parent version is unchanged from our own.
            return Arc::clone(output);
        }

        let next_input = self.parent.token();

        let transition =
            if let (Ok(prev_input), Ok(prev_output)) = (prev_input.result(), output.result()) {
                Some((prev_input, prev_output))
            } else {
                None
            };

        let result = next_input
            .result
            .as_ref()
            .map_err(Clone::clone)
            .and_then(|next_input| (self.f)(next_input, transition));

        *output = Arc::new(Refresh {
            expired: next_input.expired.clone(),
            result,
            version: next_input.version,
        });
        *prev_input = next_input;

        Arc::clone(output)
    }

    fn version(&self) -> u64 {
        self.parent.version()
    }
}

// An Arc<dyn Watch<Token>> is a Watch<Token>.
impl<Token> Watch<Token> for Arc<dyn Watch<Token>> {
    #[inline]
    fn version(&self) -> u64 {
        (**self).version()
    }
    #[inline]
    fn token(&self) -> Arc<Refresh<Token>> {
        (**self).token()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_fixed() {
        // Success case.
        let watch = fixed(Ok("my-token".to_string())).ready_owned().await;

        let refresh = watch.token();
        assert_eq!(refresh.result.as_ref().unwrap(), "my-token");
        assert_eq!(refresh.version, 1);
        assert_eq!(watch.version(), 1);

        // Error case.
        let watch = fixed::<String>(Err(tonic::Status::unauthenticated("bad")))
            .ready_owned()
            .await;

        assert_eq!(
            watch.token().result.as_ref().unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
    }

    #[tokio::test]
    async fn test_manual_lifecycle() {
        let (pending, replace) = manual::<String>();

        // Initial state: version 0, placeholder error, not ready.
        assert!(!pending.signal.is_cancelled());

        // First update makes it ready with version 1.
        assert!(replace(Ok("first".to_string())).is_some());
        assert!(pending.signal.is_cancelled());

        let watch = pending.ready_owned().await;
        assert_eq!(watch.version(), 1);
        let first = watch.token();
        assert_eq!(first.result.as_ref().unwrap(), "first");
        assert!(!first.expired.is_cancelled());

        // Update increments version and signals replacement.
        assert!(replace(Ok("second".to_string())).is_some());
        assert_eq!(watch.version(), 2);
        assert_eq!(watch.token().result.as_ref().unwrap(), "second");
        assert!(first.expired.is_cancelled());

        // Update to error.
        replace(Err(tonic::Status::internal("failed")));
        assert_eq!(watch.version(), 3);
        assert!(watch.token().result.is_err());

        // After drop, replace returns None.
        drop(watch);
        assert!(replace(Ok("ignored".to_string())).is_none());
    }

    #[tokio::test]
    async fn test_map() {
        // Basic transformation.
        let (pending, replace) = manual::<i32>();
        replace(Ok(10));

        let mapped = pending.map(|n, _prior| Ok(n * 2));
        let mapped = mapped.ready_owned().await;
        assert_eq!(*mapped.token().result.as_ref().unwrap(), 20);

        // Recomputes on version change.
        replace(Ok(5));
        assert_eq!(mapped.version(), 2);
        assert_eq!(*mapped.token().result.as_ref().unwrap(), 10);
    }

    #[tokio::test]
    async fn test_map_caching() {
        let call_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = Arc::clone(&call_count);

        let (pending, replace) = manual::<i32>();
        let mapped = pending.map(move |n, _prior| {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(*n)
        });

        replace(Ok(42));
        let mapped = mapped.ready_owned().await;

        let _ = mapped.token();
        let _ = mapped.token();
        let _ = mapped.token();
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_map_error_propagation() {
        // Parent error propagates.
        let (pending, replace) = manual::<i32>();
        replace(Err(tonic::Status::unavailable("down")));
        let parent = pending.ready_owned().await;
        let mapped = map(parent, |n, _prior| Ok(n * 2));
        assert_eq!(
            mapped.token().result.as_ref().unwrap_err().code(),
            tonic::Code::Unavailable
        );

        // Mapping error propagates.
        let (pending, replace) = manual::<i32>();
        replace(Ok(42));
        let parent = pending.ready_owned().await;
        let mapped: Arc<dyn Watch<i32>> = map(parent, |_, _prior| {
            Err(tonic::Status::invalid_argument("bad"))
        });
        assert_eq!(
            mapped.token().result.as_ref().unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[tokio::test]
    async fn test_map_transition() {
        let (pending, replace) = manual::<i32>();
        replace(Ok(1));

        // Output = input + prior_input + prior_output, or just input if no prior.
        // Negative input triggers a mapping error.
        let mapped = pending.map(|input, prior| {
            if *input < 0 {
                Err(tonic::Status::internal("fail"))
            } else {
                Ok(prior.map(|(pi, po)| input + pi + po).unwrap_or(*input))
            }
        });
        let mapped = mapped.ready_owned().await;

        // First call has no prior.
        assert_eq!(*mapped.token().result.as_ref().unwrap(), 1);

        // Subsequent calls incorporate prior input and output.
        replace(Ok(2));
        assert_eq!(*mapped.token().result.as_ref().unwrap(), 4); // 2 + 1 + 1
        replace(Ok(3));
        assert_eq!(*mapped.token().result.as_ref().unwrap(), 9); // 3 + 2 + 4

        // Parent error: closure not called. After recovery, no prior available.
        replace(Err(tonic::Status::unavailable("down")));
        assert!(mapped.token().result.is_err());
        replace(Ok(10));
        assert_eq!(*mapped.token().result.as_ref().unwrap(), 10); // No prior.

        // Mapping error (negative input): next call also has no prior.
        replace(Ok(-1));
        assert!(mapped.token().result.is_err());
        replace(Ok(5));
        assert_eq!(*mapped.token().result.as_ref().unwrap(), 5); // No prior.
    }

    struct MockSource(Vec<(Result<String, tonic::Status>, TimeDelta, CancellationToken)>);

    impl Source for MockSource {
        type Token = String;
        type Revoke = crate::WaitForCancellationFutureOwned;

        async fn refresh(
            &mut self,
            _started: crate::DateTime,
        ) -> tonic::Result<Result<(Self::Token, TimeDelta, Self::Revoke), TimeDelta>> {
            let (result, dur, revoke) = if self.0.is_empty() {
                (
                    Ok("default".into()),
                    TimeDelta::hours(1),
                    CancellationToken::new(),
                )
            } else {
                self.0.remove(0)
            };
            match result {
                Ok(token) => Ok(Ok((token, dur, revoke.cancelled_owned()))),
                Err(e) if e.code() == tonic::Code::Aborted => Ok(Err(dur)), // Retry signal.
                Err(e) => Err(e),
            }
        }
    }

    #[tokio::test]
    async fn test_watch_initial_and_retry() {
        // Retries internally, then succeeds.
        let source = MockSource(vec![
            (
                Err(tonic::Status::aborted("")),
                TimeDelta::milliseconds(1),
                Default::default(),
            ),
            (
                Ok("after-retry".into()),
                TimeDelta::hours(1),
                Default::default(),
            ),
        ]);
        let watch = watch(source).ready_owned().await;
        assert_eq!(watch.token().result.as_ref().unwrap(), "after-retry");
        assert_eq!(watch.version(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn test_watch_error_recovery_refresh_and_revoke() {
        // Token that will be used to trigger early revocation.
        let revoke = CancellationToken::new();

        let source = MockSource(vec![
            (
                Err(tonic::Status::unavailable("down")),
                TimeDelta::zero(),
                Default::default(),
            ),
            (
                Ok("recovered".into()),
                TimeDelta::minutes(1),
                Default::default(),
            ),
            (
                Ok("with-revoke".into()),
                TimeDelta::hours(1), // Long validity, but we'll revoke early.
                revoke.clone(),
            ),
            (
                Ok("after-revoke".into()),
                TimeDelta::hours(1),
                Default::default(),
            ),
        ]);

        let watch = watch(source).ready_owned().await;

        // First refresh was an error.
        let first = watch.token();
        assert_eq!(
            first.result.as_ref().unwrap_err().code(),
            tonic::Code::Unavailable
        );

        // Auto-advance past error backoff (45-75s), expect recovery.
        first.expired().await;

        let second = watch.token();
        assert_eq!(second.result.as_ref().unwrap(), "recovered");
        assert_eq!(second.version, 2);

        // Auto-advance to next refresh (1 min), expect next token.
        second.expired().await;

        let third = watch.token();
        assert_eq!(third.result.as_ref().unwrap(), "with-revoke");
        assert_eq!(third.version, 3);

        // Revoke the token early, without awaiting it. Time is not advanced.
        revoke.cancel();
        tokio::task::yield_now().await; // Allow task to process revoke.
        assert!(third.is_expired());

        let fourth = watch.token();
        assert_eq!(fourth.result.as_ref().unwrap(), "after-revoke");
        assert_eq!(fourth.version, 4);

        // Dropping `watch` cancels the latest token.
        assert!(!fourth.expired.is_cancelled());
        drop(watch);
        assert!(fourth.expired.is_cancelled());
    }
}
