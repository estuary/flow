use super::Client;
use crate::{journal::check_ok, Error};
use futures::{FutureExt, Stream, StreamExt};
use proto_gazette::broker::{self, AppendResponse};
use std::sync::Arc;

impl Client {
    /// Append the contents of a byte stream to the specified journal.
    /// Returns a Stream of results which will yield either:
    /// - An AppendResponse after all data is successfully appended
    /// - Errors for any failures encountered.
    /// If polled after an error, regenerates the input stream and
    /// retries the request from the beginning.
    pub fn append<'a, S>(
        &'a self,
        request: broker::AppendRequest,
        source: impl Fn() -> S + Send + Sync + 'a,
    ) -> impl Stream<Item = crate::Result<broker::AppendResponse>> + '_
    where
        S: Stream<Item = std::io::Result<bytes::Bytes>> + Send + 'static,
    {
        coroutines::coroutine(move |mut co| async move {
            loop {
                let input_stream = source();

                match self.try_append(request.clone(), input_stream).await {
                    Ok(resp) => {
                        () = co.yield_(Ok(resp)).await;
                        return;
                    }
                    Err(err) => {
                        () = co.yield_(Err(err)).await;
                        // Polling after an error indicates the caller would like to retry,
                        // so continue the loop to re-generate the input stream and try again.
                    }
                }
            }
        })
    }

    async fn try_append<S>(
        &self,
        request: broker::AppendRequest,
        source: S,
    ) -> crate::Result<AppendResponse>
    where
        S: Stream<Item = std::io::Result<bytes::Bytes>> + Send + 'static,
    {
        let (input_err_tx, input_err_rx) = tokio::sync::oneshot::channel();

        // `JournalClient::append()` wants a stream of `AppendRequest`s, so let's compose one starting with
        // the initial metadata request containing the journal name and any other request metadata, then
        // "data" requests that contain chunks of data to write, then the final EOF indicating completion.
        let request_stream = futures::stream::once(async move { Ok(request) })
            .chain(source.filter_map(|input| {
                futures::future::ready(match input {
                    // It's technically possible to get an empty set of bytes when reading
                    // from the input stream. Filter these out as otherwise they would look
                    // like EOFs to the append RPC and cause confusion.
                    Ok(input_bytes) if input_bytes.len() == 0 => None,
                    Ok(input_bytes) => Some(Ok(broker::AppendRequest {
                        content: input_bytes.to_vec(),
                        ..Default::default()
                    })),
                    Err(err) => Some(Err(err)),
                })
            }))
            // Final empty chunk / EOF to signal we're done
            .chain(futures::stream::once(async {
                Ok(broker::AppendRequest {
                    ..Default::default()
                })
            }))
            // Since it's possible to error when reading input data, we handle an error by stopping
            // the stream and storing the error. Later, we first check if we have hit an input error
            // and if so we bubble it up, otherwise proceeding with handling the output of the RPC
            .scan(Some(input_err_tx), |input_err_tx, input_res| {
                futures::future::ready(match input_res {
                    Ok(input) => Some(input),
                    Err(err) => {
                        input_err_tx
                            .take()
                            .expect("we should reach this point at most once")
                            .send(err)
                            .expect("we should reach this point at most once");
                        None
                    }
                })
            });

        let mut client = self.into_sub(self.router.route(None, false, &self.default)?);

        let resp = client.append(request_stream).await;

        if let Some(Ok(input_err)) = input_err_rx.now_or_never() {
            return Err(Error::AppendRead(input_err));
        } else {
            match resp {
                Ok(resp) => {
                    let resp = resp.into_inner();
                    check_ok(resp.status(), resp)
                }
                Err(err) => Err(err.into()),
            }
        }
    }
}
