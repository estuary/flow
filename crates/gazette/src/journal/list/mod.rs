use super::{Client, check_ok};
use crate::{Error, router};
use futures::TryStreamExt;
use proto_gazette::broker;
use std::future::Future;

mod subscriber;
pub use subscriber::{Subscriber, SubscriberFold};

/// Fold processes streamed listing snapshot chunks into an aggregated Output.
///
/// A listing snapshot may arrive as multiple `ListResponse` chunks from the broker.
/// Fold implementations accumulate these chunks and produce a final output when complete.
///
/// The lifecycle for each snapshot is:
/// 1. `begin()` - Initialize state for a new snapshot
/// 2. `chunk()` - Called one or more times with response chunks
/// 3. `finish()` - Produce the final output
///
/// On error (from the stream, `chunk()`, or `finish()`), the fold is reset via `begin()`
/// on retry without a prior `finish()` call. Implementations should handle this by
/// discarding any partial state in `begin()`.
pub trait Fold {
    type Output;

    /// Initialize or reset state for a new listing snapshot.
    fn begin<'s>(&'s mut self) -> impl Future<Output = ()> + Send + 's;

    /// Process a chunk of the listing snapshot.
    fn chunk<'s>(
        &'s mut self,
        resp: broker::ListResponse,
    ) -> impl Future<Output = crate::Result<()>> + Send + 's;

    /// Complete the snapshot and return the aggregated output.
    fn finish<'s>(&'s mut self) -> impl Future<Output = crate::Result<Self::Output>> + Send + 's;
}

impl Client {
    /// List journals that match the ListRequest.
    pub async fn list(&self, req: broker::ListRequest) -> crate::Result<broker::ListResponse> {
        self.list_with(req, FoldViaExtend(Default::default())).await
    }

    /// List journals that match the ListRequest, using a custom Fold.
    pub async fn list_with<F: Fold>(
        &self,
        mut req: broker::ListRequest,
        mut fold: F,
    ) -> crate::Result<F::Output> {
        assert!(
            !req.watch,
            "list_with() requires ListRequest.watch is not set"
        );
        let mut stream = self.start_list(&req).await?;
        recv_snapshot_with_fold(&mut req, &mut stream, &mut fold).await
    }

    /// Watch journals that match the ListRequest, returning a Stream which yields
    /// on every updated listing snapshot pushed by the Gazette broker.
    pub fn list_watch(
        self,
        req: broker::ListRequest,
    ) -> impl futures::Stream<Item = crate::RetryResult<broker::ListResponse>> + 'static {
        self.list_watch_with(req, FoldViaExtend(Default::default()))
    }

    /// Watch journals that match the ListRequest with a custom Fold,
    /// returning a Stream which yields on every updated listing snapshot.
    pub fn list_watch_with<F: Fold + 'static>(
        self,
        mut req: broker::ListRequest,
        mut fold: F,
    ) -> impl futures::Stream<Item = crate::RetryResult<F::Output>> + 'static {
        assert!(
            req.watch,
            "list_watch_with() requires ListRequest.watch is set"
        );

        coroutines::coroutine(move |mut co| async move {
            let mut attempt = 0;
            let mut maybe_stream = None;

            loop {
                let err = match maybe_stream.take() {
                    Some(mut stream) => {
                        match recv_snapshot_with_fold(&mut req, &mut stream, &mut fold).await {
                            Ok(output) => {
                                () = co.yield_(Ok(output)).await;
                                attempt = 0;
                                maybe_stream = Some(stream);
                                continue;
                            }
                            Err(err) => err,
                        }
                    }
                    None => match self.start_list(&req).await {
                        Ok(stream) => {
                            maybe_stream = Some(stream);
                            continue;
                        }
                        Err(err) => err,
                    },
                };

                if matches!(err, Error::UnexpectedEof if req.watch_resume.is_some()) {
                    // Server stopped an ongoing watch. Expected and not an error.
                    continue;
                }

                // Surface error to the caller, who can either drop to cancel or poll to retry.
                () = co.yield_(Err(err.with_attempt(attempt))).await;
                () = tokio::time::sleep(crate::backoff(attempt)).await;
                attempt += 1;
            }
        })
    }

    async fn start_list(
        &self,
        req: &broker::ListRequest,
    ) -> crate::Result<tonic::Streaming<broker::ListResponse>> {
        let mut client = self
            .subclient(
                None, // No route header (any member can answer).
                router::Mode::Default,
            )
            .await?;

        Ok(client.list(req.clone()).await?.into_inner())
    }
}

async fn recv_snapshot_with_fold<F: Fold>(
    req: &mut broker::ListRequest,
    stream: &mut tonic::Streaming<broker::ListResponse>,
    fold: &mut F,
) -> crate::Result<F::Output> {
    let mut started = false;

    loop {
        let next = stream.try_next().await?;

        match (started, next) {
            // Completion of listing snapshot in a unary !watch request.
            (true, None) if !req.watch => {
                return fold.finish().await;
            }
            // Unexpected EOF of a watch request.
            (true, None) => {
                return Err(Error::UnexpectedEof);
            }
            // First response of listing snapshot.
            (false, Some(next)) => {
                let next = check_ok(next.status(), next)?;
                req.watch_resume = next.header.clone();
                fold.begin().await;
                fold.chunk(next).await?;
                started = true;
            }
            // Continued response of a listing snapshot.
            (true, Some(next)) if !next.journals.is_empty() => {
                fold.chunk(next).await?;
            }
            // Completion of listing snapshot in an ongoing watch request.
            (true, Some(_next)) if req.watch => {
                return fold.finish().await;
            }
            // !watch responses after the first should never be empty.
            (true, Some(_next)) => {
                return Err(Error::Protocol(
                    "unexpected empty ListResponse continuation in a !watch request",
                ));
            }
            (false, None) => return Err(Error::UnexpectedEof),
        }
    }
}

struct FoldViaExtend(broker::ListResponse);

impl Fold for FoldViaExtend {
    type Output = broker::ListResponse;

    async fn begin(&mut self) {
        self.0 = Default::default();
    }

    async fn chunk(&mut self, resp: broker::ListResponse) -> crate::Result<()> {
        self.0.journals.extend(resp.journals.into_iter());
        Ok(())
    }

    async fn finish(&mut self) -> crate::Result<Self::Output> {
        Ok(std::mem::take(&mut self.0))
    }
}
