use super::Client;
use crate::{router, Error};
use futures::{FutureExt, Stream, StreamExt};
use proto_gazette::broker::{self, AppendResponse};

impl Client {
    /// Append the contents of a byte stream to the specified journal.
    /// Returns a Stream of results which will yield either:
    /// - An AppendResponse after all data is successfully appended
    /// - Errors for any failures encountered.
    /// If polled after an error, regenerates the input stream and
    /// retries the request from the beginning.
    pub fn append<'a, S>(
        &'a self,
        mut req: broker::AppendRequest,
        source: impl Fn() -> S + Send + Sync + 'a,
    ) -> impl Stream<Item = crate::RetryResult<broker::AppendResponse>> + '_
    where
        S: Stream<Item = std::io::Result<bytes::Bytes>> + Send + 'static,
    {
        coroutines::coroutine(move |mut co| async move {
            let mut attempt = 0;

            loop {
                let err = match self.try_append(&mut req, source()).await {
                    Ok(resp) => {
                        () = co.yield_(Ok(resp)).await;
                        return;
                    }
                    Err(err) => err,
                };

                if matches!(err, Error::BrokerStatus(broker::Status::NotJournalPrimaryBroker) if req.do_not_proxy)
                {
                    // This is an expected error which drives dynamic route discovery.
                    // Route topology in `req.header` has been updated, and we restart the request.
                    continue;
                }

                // Surface error to the caller, who can either drop to cancel or poll to retry.
                () = co.yield_(Err(err.with_attempt(attempt))).await;
                () = tokio::time::sleep(crate::backoff(attempt)).await;
                attempt += 1;

                // Restart route discovery.
                req.header = None;
            }
        })
    }

    async fn try_append<S>(
        &self,
        req: &mut broker::AppendRequest,
        source: S,
    ) -> crate::Result<AppendResponse>
    where
        S: Stream<Item = std::io::Result<bytes::Bytes>> + Send + 'static,
    {
        let mut client = self.into_sub(self.router.route(
            req.header.as_mut(),
            if req.do_not_proxy {
                router::Mode::Primary
            } else {
                router::Mode::Default
            },
            &self.default,
        )?);
        let req_clone = req.clone();

        let (source_err_tx, source_err_rx) = tokio::sync::oneshot::channel();

        // `JournalClient::append()` wants a stream of `AppendRequest`s, so let's compose one starting with
        // the initial metadata request containing the journal name and any other request metadata, then
        // "data" requests that contain chunks of data to write, then the final EOF indicating completion.
        let source = futures::stream::once(async move { Ok(req_clone) })
            .chain(source.filter_map(|input| {
                futures::future::ready(match input {
                    // It's technically possible to get an empty set of bytes when reading
                    // from the input stream. Filter these out as otherwise they would look
                    // like EOFs to the append RPC and cause confusion.
                    Ok(content) if content.len() == 0 => None,
                    Ok(content) => Some(Ok(broker::AppendRequest {
                        content,
                        ..Default::default()
                    })),
                    Err(err) => Some(Err(err)),
                })
            }))
            // Final empty chunk signals the broker to commit (rather than rollback).
            .chain(futures::stream::once(async {
                Ok(broker::AppendRequest {
                    ..Default::default()
                })
            }))
            // Since it's possible to error when reading input data, we handle an error by stopping
            // the stream and storing the error. Later, we first check if we have hit an input error
            // and if so we bubble it up, otherwise proceeding with handling the output of the RPC
            .scan(Some(source_err_tx), |err_tx, result| {
                futures::future::ready(match result {
                    Ok(request) => Some(request),
                    Err(err) => {
                        err_tx
                            .take()
                            .expect("we should reach this point at most once")
                            .send(err)
                            .expect("we should reach this point at most once");
                        None
                    }
                })
            });
        let result = client.append(source).await;

        // An error reading `source` has precedence,
        // as it's likely causal if the broker *also* errored.
        if let Ok(err) = source_err_rx.now_or_never().expect("tx has been dropped") {
            return Err(Error::AppendRead(err));
        }
        let mut resp = result?.into_inner();

        if resp.status() == broker::Status::Ok {
            Ok(resp)
        } else {
            req.header = resp.header.take();
            Err(Error::BrokerStatus(resp.status()))
        }
    }
}
