use super::Client;
use crate::{Error, router};
use futures::{FutureExt, Stream, StreamExt};
use proto_gazette::broker::{self, AppendResponse};

impl Client {
    /// Append the contents of a byte stream to the specified journal.
    /// Returns a Stream of results which will yield either:
    /// - Errors for any failures encountered.
    /// - An AppendResponse after all data is successfully appended, followed by EOF.
    /// If polled after an error, it retries the request from the beginning.
    pub fn append<'a, S>(
        &'a self,
        mut req: broker::AppendRequest,
        source: impl Fn() -> S + Send + Sync + 'a,
    ) -> impl Stream<Item = crate::RetryResult<broker::AppendResponse>> + 'a
    where
        S: Stream<Item = std::io::Result<bytes::Bytes>> + Send + 'static,
    {
        coroutines::coroutine(move |mut co| async move {
            let mut attempt = 0;
            let metrics = Metrics::new(&req.journal);

            loop {
                let err = match self.try_append(&metrics, &mut req, source()).await {
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
        metrics: &Metrics,
        req: &mut broker::AppendRequest,
        source: S,
    ) -> crate::Result<AppendResponse>
    where
        S: Stream<Item = std::io::Result<bytes::Bytes>> + Send + 'static,
    {
        let mut client = self
            .subclient(&mut req.header, router::Mode::Primary)
            .await?;

        let req_clone = req.clone();

        // Bytes read from `source` by this attempt.
        let attempt_bytes = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let attempt_bytes_inner = attempt_bytes.clone();

        let (source_err_tx, source_err_rx) = tokio::sync::oneshot::channel();

        // `JournalClient::append()` wants a stream of `AppendRequest`s, so let's compose one starting with
        // the initial metadata request containing the journal name and any other request metadata, then
        // "data" requests that contain chunks of data to write, then the final EOF indicating completion.
        let source = futures::stream::once(async move { Ok(req_clone) })
            .chain(source.filter_map(move |input| {
                futures::future::ready(match input {
                    // It's technically possible to get an empty set of bytes when reading
                    // from the input stream. Filter these out as otherwise they would look
                    // like EOFs to the append RPC and cause confusion.
                    Ok(content) if content.len() == 0 => None,
                    Ok(content) => {
                        _ = attempt_bytes_inner
                            .fetch_add(content.len() as u64, std::sync::atomic::Ordering::Relaxed);
                        Some(Ok(broker::AppendRequest {
                            content,
                            ..Default::default()
                        }))
                    }
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

        // An error reading `source` has precedence as it's likely causal if the
        // broker *also* errored. It's possible that the broker response arrives
        // before we've sent off the complete request.
        if let Some(Ok(err)) = source_err_rx.now_or_never() {
            return Err(Error::AppendRead(err));
        }
        let mut resp = result?.into_inner();

        if resp.status() == broker::Status::Ok {
            let bytes = attempt_bytes.load(std::sync::atomic::Ordering::Relaxed);
            metrics.append.increment(bytes);

            // The broker reports a count of delayed chunks, not which ones, so
            // delayed bytes are prorated (exact when chunks are uniform).
            if resp.delayed_chunks > 0 && resp.total_chunks > 0 {
                let delayed =
                    bytes as u128 * resp.delayed_chunks as u128 / resp.total_chunks as u128;
                metrics.append_delayed.increment(delayed as u64);
            }
            return Ok(resp);
        }
        req.header = resp.header.take();

        // A refused append carries the failing store's diagnostic in
        // `store_health_error`; surface it rather than the bare status.
        if resp.status() == broker::Status::FragmentStoreUnhealthy {
            return Err(Error::FragmentStoreUnhealthy(std::mem::take(
                &mut resp.store_health_error,
            )));
        }
        Err(Error::BrokerStatus(resp.status()))
    }
}

struct Metrics {
    append: metrics::Counter,
    append_delayed: metrics::Counter,
}

impl Metrics {
    fn new(journal: &str) -> Self {
        static DESCRIBE: std::sync::Once = std::sync::Once::new();
        DESCRIBE.call_once(|| {
            metrics::describe_counter!(
                "gazette_append",
                metrics::Unit::Bytes,
                "number of bytes appended to a journal by successful (200 OK) appends",
            );
            metrics::describe_counter!(
                "gazette_append_delayed",
                metrics::Unit::Bytes,
                "number of appended bytes delayed by journal flow control, prorated from the broker's count of delayed chunks",
            );
        });
        let append = metrics::counter!("gazette_append", "journal" => journal.to_string());
        let append_delayed =
            metrics::counter!("gazette_append_delayed", "journal" => journal.to_string());

        Self {
            append,
            append_delayed,
        }
    }
}
