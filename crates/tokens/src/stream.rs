use crate::{DateTime, TimeDelta};
use futures::{Stream, StreamExt};
use std::future::Ready;
use std::pin::Pin;

/// A Source adapter that wraps a futures::Stream<Item = tonic::Result<Token>>.
///
/// On each refresh, it:
/// 1. Awaits the next item from the stream (blocking)
/// 2. Drains all immediately-ready items, keeping only the latest (Ok or Err)
/// 3. Returns with infinite validity and immediate revocation
///
/// The immediate revocation causes watch() to call refresh() again,
/// which provides the opportunity to poll the stream for more items.
pub struct StreamSource<S> {
    stream: Pin<Box<S>>,
}

impl<S, Token> StreamSource<S>
where
    S: Stream<Item = tonic::Result<Token>> + Send + 'static,
    Token: Send + Sync + 'static,
{
    pub fn new(stream: S) -> Self {
        Self {
            stream: Box::pin(stream),
        }
    }
}

impl<S, Token> crate::Source for StreamSource<S>
where
    S: Stream<Item = tonic::Result<Token>> + Send + 'static,
    Token: Send + Sync + 'static,
{
    type Token = Token;
    type Revoke = Ready<()>;

    async fn refresh(
        &mut self,
        _started: DateTime,
    ) -> tonic::Result<Result<(Self::Token, TimeDelta, Self::Revoke), TimeDelta>> {
        // Wait for at least one item
        let mut latest = match self.stream.next().await {
            Some(result) => result,
            None => {
                // Stream ended
                return Err(tonic::Status::resource_exhausted("stream ended"));
            }
        };

        // Coalesce: drain all immediately-ready items, keeping only the latest
        // (regardless of whether it's Ok or Err)
        loop {
            match futures::poll!(self.stream.next()) {
                std::task::Poll::Ready(Some(result)) => {
                    latest = result;
                }
                std::task::Poll::Ready(None) | std::task::Poll::Pending => {
                    break;
                }
            }
        }

        // Return the latest result (Ok or Err) with infinite validity and an
        // immediate revocation, which causes watch() to call refresh() again,
        // driving the stream forward.
        Ok(Ok((latest?, TimeDelta::MAX, std::future::ready(()))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Source;
    use futures::stream;

    const IGNORED: DateTime = DateTime::UNIX_EPOCH;

    #[tokio::test]
    async fn test_refresh_single_item() {
        let mut source = StreamSource::new(stream::iter([Ok(42i32)]));
        let (token, dur, _revoke) = source.refresh(IGNORED).await.unwrap().unwrap();
        assert_eq!(token, 42);
        assert_eq!(dur, TimeDelta::MAX);
    }

    #[tokio::test]
    async fn test_refresh_coalesces_ready_items() {
        // All items are immediately ready; should coalesce to the last one.
        let mut source = StreamSource::new(stream::iter([Ok(1i32), Ok(2), Ok(3)]));
        assert_eq!(source.refresh(IGNORED).await.unwrap().unwrap().0, 3);

        // Stream is now exhausted.
        assert_eq!(
            source.refresh(IGNORED).await.unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
    }

    #[tokio::test]
    async fn test_refresh_coalesces_to_latest_error() {
        let mut source = StreamSource::new(stream::iter([
            Ok(1i32),
            Err(tonic::Status::internal("mid")),
            Err(tonic::Status::unavailable("last")),
        ]));
        assert_eq!(
            source.refresh(IGNORED).await.unwrap_err().code(),
            tonic::Code::Unavailable
        );
    }

    #[tokio::test]
    async fn test_refresh_empty_stream() {
        let mut source = StreamSource::new(stream::empty::<tonic::Result<i32>>());
        assert_eq!(
            source.refresh(IGNORED).await.unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
    }

    #[tokio::test]
    async fn test_refresh_blocks_then_coalesces() {
        use futures::channel::mpsc;

        let (mut tx, rx) = mpsc::channel::<tonic::Result<i32>>(10);
        let mut source = StreamSource::new(rx);

        // Spawn refresh which will block waiting for first item.
        let handle = tokio::spawn(async move { source.refresh(IGNORED).await });

        // Send multiple items before refresh can poll again.
        tx.try_send(Ok(10)).unwrap();
        tx.try_send(Ok(20)).unwrap();
        tx.try_send(Ok(30)).unwrap();
        drop(tx);

        // Should get the last coalesced value.
        assert_eq!(handle.await.unwrap().unwrap().unwrap().0, 30);
    }

    #[tokio::test]
    async fn test_watch_integration() {
        use futures::channel::mpsc;

        let (mut tx, rx) = mpsc::channel::<tonic::Result<String>>(10);
        let source = StreamSource::new(rx);

        let pending = crate::watch(source);
        tx.try_send(Ok("first".into())).unwrap();

        let watch = pending.ready_owned().await;
        assert_eq!(watch.token().result().unwrap().as_str(), "first");
        let v1 = watch.version();

        // Immediate revocation means watch should already be polling for next.
        // Send next item and wait for it to propagate.
        tx.try_send(Ok("second".into())).unwrap();
        watch.token().expired().await;
        assert_eq!(watch.token().result().unwrap().as_str(), "second");
        assert!(watch.version() > v1);

        // Stream end causes resource exhausted error.
        drop(tx);
        watch.token().expired().await;
        assert_eq!(
            watch.token().result().unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
    }
}
