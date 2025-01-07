use super::Client;
use crate::{journal::check_ok, Error};
use futures::{Stream, StreamExt};
use proto_gazette::broker;
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncSeek, AsyncSeekExt, BufReader},
    pin,
};

// TODO: Tune this?
const CHUNK_SIZE: usize = 1 << 14;

impl Client {
    /// Helper function to appends the contents of `source` to the specified journal.
    /// This drives a single append RPC cycle and propagates any errors. If you need
    /// streaming input or retries, use `[append_stream]` instead.
    pub async fn append_once(
        &self,
        journal: String,
        source: Vec<u8>,
    ) -> crate::Result<broker::AppendResponse> {
        let mapped_source = std::io::Cursor::new(source);

        let appender = self.append_stream(journal, mapped_source);
        tokio::pin!(appender);

        match appender.next().await {
            Some(Ok(resp)) => {
                if let None = appender.next().await {
                    Ok(resp)
                } else {
                    Err(Error::Append("Didn't get EOF after Ok".to_string()))
                }
            }
            Some(err) => err,
            None => Err(Error::UnexpectedEof),
        }
    }

    /// Append the contents of an `AsyncRead + AsyncSeek` to the specified journal.
    /// Returns a Stream of results which will yield either:
    /// - An AppendResponse after all data is successfully appended
    /// - Errors for any failures encountered.
    /// If polled after an error, restarts the request from the beginning.
    pub fn append_stream<R>(
        &self,
        journal: String,
        source: R,
    ) -> impl Stream<Item = crate::Result<broker::AppendResponse>> + '_
    where
        R: AsyncRead + AsyncSeek + Send + Unpin + 'static,
    {
        coroutines::coroutine(move |mut co| async move {
            let mut reader = BufReader::with_capacity(CHUNK_SIZE, source);
            loop {
                match self.append_all(&journal, &mut reader).await {
                    Ok(resp) => {
                        () = co.yield_(Ok(resp)).await;
                        return;
                    }
                    Err(err) => {
                        () = co.yield_(Err(err)).await;
                        // Polling after an error indicates the caller would like to retry,
                        // so seek back to the beginning and restart.
                        // Seeking shouldn't error unless there's a bug
                        reader.seek(std::io::SeekFrom::Start(0)).await.unwrap();
                    }
                }
            }
        })
    }

    async fn append_all<R>(
        &self,
        journal: &str,
        source: &mut R,
    ) -> crate::Result<broker::AppendResponse>
    where
        R: AsyncBufReadExt + Send + Unpin,
    {
        // Transforms `source` into a stream of `Result<AppendRequest, gazette::Error>`. This deals with
        // the append RPC's semantics that require an initial "metadata" request, followed by a stream of
        // "chunk" requests, followed by an empty request to indicate we're done. Potential errors ultimately
        // originate from reading the input AsyncRead.
        let request_generator = coroutines::coroutine(move |mut co| async move {
            // Send initial request
            () = co
                .yield_(Ok(broker::AppendRequest {
                    journal: journal.to_string(),
                    ..Default::default()
                }))
                .await;

            loop {
                // Process chunks until EOF
                let bytes_read = match source.fill_buf().await {
                    // An empty buffer indicates EOF, as otherwise fill_buf() will wait until data is available
                    Ok(chunk) if chunk.len() == 0 => break,
                    Ok(chunk) => {
                        () = co
                            .yield_(Ok(broker::AppendRequest {
                                content: chunk.to_vec(),
                                ..Default::default()
                            }))
                            .await;
                        chunk.len()
                    }
                    Err(e) => {
                        () = co.yield_(Err(Error::Append(e.to_string()))).await;
                        return;
                    }
                };

                source.consume(bytes_read);
            }
            // Send final empty chunk
            () = co
                .yield_(Ok(broker::AppendRequest {
                    ..Default::default()
                }))
                .await;
        });

        // Since reading from `source` can error, we need this whole song and dance to
        // handle those errors. We could just `.collect()` all of the requests and catch
        // any errors there, but since this is supposed to handle significant volumes of data
        // over an undefined period of time, that won't work. So instead we need to pass
        // `JournalClient::append()` a stream of _just_ the `AppendRequest`s that come out
        // of the above `request_generator`, while also promptly returning any errors if they
        // crop up, and cancelling the append request.

        let (req_tx, req_rx) = tokio::sync::mpsc::channel(100);

        let mut client = self.into_sub(self.router.route(None, false, &self.default)?);

        // Run `JournalClient::append` in a separate Tokio task, and feed it a steady diet of `AppendRequest`s
        // while also giving us a convenient handle to `.abort()` if we encounter an error.
        let mut append_handle = tokio::spawn(async move {
            let resp = client
                .append(tokio_stream::wrappers::ReceiverStream::new(req_rx))
                .await
                .map_err(crate::Error::Grpc)?
                .into_inner();

            check_ok(resp.status(), resp)
        });

        pin!(request_generator);

        loop {
            tokio::select! {
                maybe_item = request_generator.next() => {
                    match maybe_item {
                        Some(Ok(req)) => {
                            req_tx.send(req).await.map_err(|e|Error::Append(e.to_string()))?;
                        },
                        Some(Err(e)) => {
                            // If `request_generator` errors, i.e we failed to read incoming data,
                            // cancel the `append` RPC and propagate the error
                            drop(req_tx);
                            append_handle.abort();
                            return Err(e);
                        },
                        None => {
                            // We hit EOF, drop the request channel sender which will close the
                            // `ReceiverStream` and signal `JournalClient::append` to finish up.
                            drop(req_tx);
                            break;
                        },
                    }
                },
                res = &mut append_handle => {
                    // Handle `JournalClient::append` finishing first. This will probably only happen
                    // if there's an error, as EOF breaks out and relies on the final `.await` to
                    // get the `AppendResponse` out.
                    return res.map_err(|e|Error::Append(e.to_string()))?;
                },
            }
        }

        // We hit EOF and now have to wait for `JournalClient::append` to finish
        append_handle
            .await
            .map_err(|e| Error::Append(e.to_string()))?
    }
}
