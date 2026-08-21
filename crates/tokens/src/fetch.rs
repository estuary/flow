use crate::Source;

/// Fetch a Token from a Source exactly once, retrying as the Source directs.
///
/// This is the counterpart of [`crate::watch`] for an operation which needs a
/// Token once and must not retain it. A Watch is the wrong tool for such an
/// operation: it holds its Token and periodically re-fetches, which for (say)
/// a decrypted secret is exactly the caching that must not happen.
///
/// `fetch_once` holds `started` constant across attempts, as `watch()` does --
/// which is what lets a server decide that a provisional denial has become
/// terminal -- and retries indeterminate results for as long as the Source
/// asks. Bound it with a combinator such as [`tokio::time::timeout`], or
/// cancel it by dropping the returned Future.
///
/// The Token's `valid_for` and revocation signal are discarded: there is
/// nothing to refresh ahead of, and nothing to revoke.
pub async fn fetch_once<S>(mut source: S) -> tonic::Result<S::Token>
where
    S: Source,
{
    let started = crate::now();

    loop {
        let retry_after = match source.refresh(started).await? {
            Ok((token, _valid_for, _revoke)) => return Ok(token),
            Err(retry_after) => retry_after,
        };
        tokio::time::sleep(retry_after.to_std().unwrap_or_default()).await;
    }
}

#[cfg(test)]
mod tests {
    use super::fetch_once;
    use crate::{DateTime, Source, TimeDelta};

    /// Source yielding scripted outcomes, and recording the `started` of each
    /// attempt so that a test may assert it's held across retries.
    struct MockSource {
        outcomes: Vec<tonic::Result<Result<&'static str, TimeDelta>>>,
        starts: std::sync::Arc<std::sync::Mutex<Vec<DateTime>>>,
    }

    impl Source for MockSource {
        type Token = &'static str;
        type Revoke = std::future::Pending<()>;

        async fn refresh(
            &mut self,
            started: DateTime,
        ) -> tonic::Result<Result<(Self::Token, TimeDelta, Self::Revoke), TimeDelta>> {
            self.starts.lock().unwrap().push(started);

            match self.outcomes.remove(0) {
                Ok(Ok(token)) => Ok(Ok((token, TimeDelta::hours(1), std::future::pending()))),
                Ok(Err(retry_after)) => Ok(Err(retry_after)),
                Err(status) => Err(status),
            }
        }
    }

    fn source(
        outcomes: Vec<tonic::Result<Result<&'static str, TimeDelta>>>,
    ) -> (MockSource, std::sync::Arc<std::sync::Mutex<Vec<DateTime>>>) {
        let starts = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        (
            MockSource {
                outcomes,
                starts: starts.clone(),
            },
            starts,
        )
    }

    #[tokio::test(start_paused = true)]
    async fn test_immediate_success() {
        let (source, starts) = source(vec![Ok(Ok("token"))]);

        assert_eq!(fetch_once(source).await.unwrap(), "token");
        assert_eq!(starts.lock().unwrap().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn test_retries_hold_started_constant() {
        let (source, starts) = source(vec![
            Ok(Err(TimeDelta::milliseconds(20))),
            Ok(Err(TimeDelta::milliseconds(20))),
            Ok(Ok("token")),
        ]);
        let entered = tokio::time::Instant::now();

        assert_eq!(fetch_once(source).await.unwrap(), "token");

        // Time advanced across the retry sleeps, but `started` did not.
        assert_eq!(
            tokio::time::Instant::now() - entered,
            std::time::Duration::from_millis(40)
        );

        let starts = starts.lock().unwrap();
        assert_eq!(starts.len(), 3);
        assert!(starts.iter().all(|start| *start == starts[0]));
    }

    #[tokio::test(start_paused = true)]
    async fn test_caller_bounds_retries() {
        // The third attempt's retry outlives the caller's timeout, and the
        // fourth attempt is never made.
        let (source, starts) = source(vec![
            Ok(Err(TimeDelta::milliseconds(20))),
            Ok(Err(TimeDelta::milliseconds(20))),
            Ok(Err(TimeDelta::seconds(60))),
            Ok(Ok("never reached")),
        ]);

        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(5), fetch_once(source))
                .await
                .is_err()
        );
        assert_eq!(starts.lock().unwrap().len(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn test_error_is_terminal() {
        // An error is client-facing and final: the retry which follows it in
        // the script is never reached.
        let (source, starts) = source(vec![
            Err(tonic::Status::permission_denied("nope")),
            Ok(Ok("never reached")),
        ]);

        let status = fetch_once(source).await.unwrap_err();
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        assert_eq!(starts.lock().unwrap().len(), 1);
    }
}
