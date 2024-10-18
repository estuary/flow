use super::{check_ok, Client};
use crate::Error;
use futures::TryStreamExt;
use proto_gazette::broker;

impl Client {
    /// List journals that match the ListRequest.
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn list(&self, mut req: broker::ListRequest) -> crate::Result<broker::ListResponse> {
        assert!(!req.watch, "list() requires ListRequest.watch is not set");
        tracing::trace!("Starting list request");
        let mut stream = self.start_list(&self.router, &req).await?;
        recv_snapshot(&mut req, &mut stream).await
    }

    /// Watch journals that match the ListRequest, returning a Stream which yields
    /// on every updated listing snapshot pushed by the Gazette broker.
    pub fn list_watch(
        self,
        mut req: broker::ListRequest,
    ) -> impl futures::Stream<Item = crate::Result<broker::ListResponse>> + 'static {
        assert!(req.watch, "list_watch() requires ListRequest.watch is set");

        coroutines::coroutine(move |mut co| async move {
            loop {
                let mut stream = match self.start_list(&self.router, &req).await {
                    Ok(stream) => stream,
                    Err(err) => {
                        () = co.yield_(Err(err)).await;
                        continue;
                    }
                };

                loop {
                    match recv_snapshot(&mut req, &mut stream).await {
                        Ok(resp) => co.yield_(Ok(resp)).await,
                        Err(err) => {
                            if matches!(err, Error::UnexpectedEof if req.watch_resume.is_some()) {
                                // Server stopped an ongoing watch. Expected and not an error.
                            } else {
                                co.yield_(Err(err)).await;
                            }
                            break; // Start new stream on next poll.
                        }
                    }
                }
            }
        })
    }

    async fn start_list(
        &self,
        router: &crate::Router,
        req: &broker::ListRequest,
    ) -> crate::Result<tonic::Streaming<broker::ListResponse>> {
        let mut client = self.into_sub(router.route(None, false, &self.default).await?);
        Ok(client.list(req.clone()).await?.into_inner())
    }
}

#[tracing::instrument(level = "trace", skip_all)]
async fn recv_snapshot(
    req: &mut broker::ListRequest,
    stream: &mut tonic::Streaming<broker::ListResponse>,
) -> crate::Result<broker::ListResponse> {
    let mut maybe_resp: Option<broker::ListResponse> = None;

    loop {
        tracing::trace!("Polling for next ListResponse");
        let next = stream.try_next().await?;
        tracing::trace!(?next, "Got next ListResponse");

        match (maybe_resp.take(), next) {
            // Completion of listing snapshot in a unary !watch request.
            (Some(resp), None) if !req.watch => {
                return Ok(resp);
            }
            // Unexpected EOF of a watch request.
            (Some(_resp), None) => {
                return Err(Error::UnexpectedEof);
            }
            // First response of listing snapshot.
            (None, Some(next)) => {
                let next = check_ok(next.status(), next)?;
                req.watch_resume = next.header.clone();
                maybe_resp = Some(next);
            }
            // Continued response of a listing snapshot.
            (Some(mut resp), Some(next)) if !next.journals.is_empty() => {
                resp.journals.extend(next.journals.into_iter());
                maybe_resp = Some(resp);
            }
            // Completion of listing snapshot in an ongoing watch request.
            (Some(resp), Some(_next)) if req.watch => {
                return Ok(resp);
            }
            // !watch responses after the first should never be empty.
            (Some(_resp), Some(_next)) => {
                return Err(Error::Protocol(
                    "unexpected empty ListResponse continutation in a !watch request",
                ));
            }
            (None, None) => return Err(Error::UnexpectedEof),
        }
    }
}
