use super::Client;
use crate::{Error, router};
use futures::TryStreamExt;
use proto_gazette::broker;

mod lines;
pub use lines::{LinesBatch, ReadLines};

// TODO(johnny): Replace usages of ReadJsonLines with ReadLines.
mod json_lines;
pub use json_lines::{ReadJsonLine, ReadJsonLines};

impl Client {
    /// Invoke the Gazette journal Read API.
    /// This routine directly fetches journal fragments from cloud storage where possible,
    /// rather than reading through the broker.
    pub fn read(
        self,
        mut req: broker::ReadRequest,
    ) -> impl futures::Stream<Item = crate::RetryResult<broker::ReadResponse>> + Send + 'static
    {
        coroutines::coroutine(move |mut co| async move {
            let mut attempt = 0;
            let mut write_head = i64::MAX;
            let metrics = Metrics::new(&req.journal);

            loop {
                // Have we read through requested `end_offset`? Use `>=`, not
                // `==`: a fragment hole spanning `end_offset` can fast-forward
                // `req.offset` *past* `end_offset`.
                if req.end_offset != 0 && req.offset >= req.end_offset {
                    return;
                }
                // Have we read through the `write_head` and our request is non-blocking?
                if !req.block && req.offset == write_head {
                    return;
                }

                let err = match self
                    .read_some(&mut co, metrics.clone(), &mut req, &mut write_head)
                    .await
                {
                    Ok(()) => {
                        attempt = 0;

                        // Yield now rather than looping immediately back into
                        // `read_some`, which issues a request for metadata &
                        // maybe a pre-signed fragment URL. Callers may not
                        // immediately consume the resulting stream, and the
                        // pre-signed URL might expire in that case. Wait until
                        // actively polled again to mint any pre-signed URLs.
                        () = tokio::task::yield_now().await;
                        continue;
                    }
                    Err(err) => err,
                };

                if matches!(err, Error::BrokerStatus(broker::Status::NotJournalBroker) if req.do_not_proxy)
                {
                    // This is an expected error which drives dynamic route discovery.
                    // `req.header` has updated route topology and we restart the request.
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

    async fn read_some(
        &self,
        co: &mut coroutines::Suspend<crate::RetryResult<broker::ReadResponse>, ()>,
        metrics: Metrics,
        req: &mut broker::ReadRequest,
        write_head: &mut i64,
    ) -> crate::Result<()> {
        let mut client = self
            .subclient(&mut req.header, router::Mode::Replica)
            .await?;

        // Fetch metadata first before we start the actual read.
        req.metadata_only = true;

        let mut stream = client.read(req.clone()).await?.into_inner();
        let mut metadata = stream.try_next().await?.ok_or(Error::UnexpectedEof)?;
        let _eof = stream.try_next().await?; // Broker sends EOF.
        std::mem::drop(stream);

        tracing::trace!(req=?ops::DebugJson(&req), meta=?ops::DebugJson(&metadata), "fetched read metadata");

        // Use routing topology from the metadata response for subsequent
        // requests, dispatching to a broker that serves this journal rather
        // than the default broker we initially dialed for the metadata request.
        req.header = metadata.header.take();

        // OFFSET_NOT_YET_AVAILABLE means there's no content at our requested
        // offset. The broker reports, via `metadata.offset`, the offset it
        // resolved to: our request (resolved from -1 to the write head), or a
        // *fast-forwarded* offset when fragments were skipped because they fall
        // before `begin_mod_time` or sit beyond a hole in the offset space. When
        // that resolved offset equals the write head, the read is definitively
        // caught up: there's no content between it and the head. Yield the
        // metadata so the caller observes `offset` and `write_head`, then return.
        // Setting both `*write_head` and `req.offset` to the write head causes
        // the outer read() loop to exit for non-blocking reads.
        if metadata.status() == broker::Status::OffsetNotYetAvailable
            && metadata.offset == metadata.write_head
        {
            *write_head = metadata.write_head;
            req.offset = metadata.write_head;

            () = co.yield_(Ok(metadata)).await;
            metrics.tick(req.offset, *write_head);

            return Ok(());
        } else if metadata.status() != broker::Status::Ok {
            // Note: we used to fall through and retry below on !Ok. That was
            // subtly wrong, because we may have a transient error here that
            // resolves before the Read RPC below, where that RPC then fails
            // with an OffsetNotYetAvailable not having our above handling.
            return Err(Error::BrokerStatus(metadata.status()));
        }

        // Can we directly read the fragment from cloud storage?
        if let (broker::Status::Ok, false, Some(fragment)) = (
            metadata.status(),
            metadata.fragment_url.is_empty() || metadata.fragment_url.starts_with("file://"),
            &metadata.fragment,
        ) {
            if req.offset != metadata.offset {
                tracing::info!(req.journal, req.offset, metadata.offset, "offset jump");
                req.offset = metadata.offset;
            }
            *write_head = metadata.write_head;

            // A fragment hole may have fast-forwarded `req.offset` to or past a
            // bounded read's `end_offset`. The requested range is then fully
            // covered; yield the metadata (so the caller observes the resolved
            // offset) and terminate.
            if req.end_offset != 0 && req.offset >= req.end_offset {
                () = co.yield_(Ok(metadata)).await;
                metrics.tick(req.offset, *write_head);
                return Ok(());
            }

            let (fragment, fragment_url) = (fragment.clone(), metadata.fragment_url.clone());
            () = co.yield_(Ok(metadata)).await;
            metrics.tick(req.offset, *write_head);

            return read_fragment_url(
                co,
                metrics,
                fragment,
                &self.fragment_client,
                fragment_url,
                req,
                *write_head,
            )
            .await;
        }

        // We skipped the direct-fragment path. If the broker returned a
        // `file://` URL, the fragment is persisted but lives on the broker's
        // local filesystem — we have no transport to read it ourselves, so
        // we must ask the broker to proxy. With `do_not_proxy=true` and no
        // open spool file, `serveRead` short-circuits after sending only the
        // fragment metadata, EOFs the stream, and the outer loop spins.
        if metadata.fragment_url.starts_with("file://") {
            req.do_not_proxy = false;
        }

        tracing::trace!(req.offset, write_head, "started direct journal read");

        // Restart as a regular (non-metadata) read, re-picking a routed subclient.
        req.metadata_only = false;

        let mut client = self
            .subclient(&mut req.header, router::Mode::Replica)
            .await?;
        let mut stream = client.read(req.clone()).await?.into_inner();

        while let Some(resp) = stream.try_next().await? {
            if resp.header.is_some() {
                req.header = resp.header.clone();
            }
            match (resp.status(), &resp.fragment, resp.content.is_empty()) {
                // Metadata response telling us of a new fragment being read.
                (broker::Status::Ok, Some(fragment), true) => {
                    tracing::trace!(fragment=?ops::DebugJson(fragment), "read fragment metadata");

                    // Offset jumps happen if content is removed from the middle of a journal,
                    // or when reading from the journal head (offset -1).
                    if req.offset != resp.offset {
                        tracing::info!(req.journal, req.offset, resp.offset, "offset jump");
                        req.offset = resp.offset;
                    }
                    *write_head = resp.write_head;

                    () = co.yield_(Ok(resp)).await;
                    metrics.tick(req.offset, *write_head);
                }
                // Content response.
                (broker::Status::Ok, None, false) => {
                    req.offset += resp.content.len() as i64;

                    () = co.yield_(Ok(resp)).await;
                    metrics.tick(req.offset, *write_head);
                }
                // All other statuses end the stream, and are handled by the caller.
                (status, _, _) => return Err(Error::BrokerStatus(status)),
            }
        }

        Ok(())
    }
}

async fn read_fragment_url(
    co: &mut coroutines::Suspend<crate::RetryResult<broker::ReadResponse>, ()>,
    metrics: Metrics,
    fragment: broker::Fragment,
    fragment_client: &reqwest::Client,
    fragment_url: String,
    req: &mut broker::ReadRequest,
    write_head: i64,
) -> crate::Result<()> {
    let mut get = fragment_client.get(fragment_url);

    match fragment.compression_codec() {
        broker::CompressionCodec::GzipOffloadDecompression => {
            get = get.header("Accept-Encoding", "identity");
        }
        broker::CompressionCodec::Gzip => {
            get = get.header("Accept-Encoding", "gzip");
        }
        _ => {}
    }

    let response = get
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(Error::FetchFragment)?;

    tracing::trace!(fragment=?ops::DebugJson(&fragment), "started direct fragment read");

    let raw_reader = response
        // Map into a Stream<Item = Result<Bytes, _>>.
        .bytes_stream()
        // Wrap reqwest::Error as an io::Error for compatibility with AsyncBufRead.
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))
        // Adapt into an AsyncBufRead.
        .into_async_read();

    match fragment.compression_codec() {
        broker::CompressionCodec::None | broker::CompressionCodec::GzipOffloadDecompression => {
            read_fragment_url_body(co, metrics, fragment, raw_reader, req, write_head).await
        }
        broker::CompressionCodec::Gzip => {
            let mut decoder = async_compression::futures::bufread::GzipDecoder::new(raw_reader);
            decoder.multiple_members(true);
            read_fragment_url_body(co, metrics, fragment, decoder, req, write_head).await
        }
        broker::CompressionCodec::Zstandard => {
            let decoder = async_compression::futures::bufread::ZstdDecoder::new(raw_reader);
            read_fragment_url_body(co, metrics, fragment, decoder, req, write_head).await
        }
        broker::CompressionCodec::Snappy => Err(Error::Protocol(
            "snappy compression is not yet implemented by this client",
        )),
        broker::CompressionCodec::Invalid => {
            Err(Error::Protocol("invalid compression codec for fragment"))
        }
    }
}

async fn read_fragment_url_body(
    co: &mut coroutines::Suspend<crate::RetryResult<broker::ReadResponse>, ()>,
    metrics: Metrics,
    fragment: broker::Fragment,
    r: impl futures::io::AsyncRead,
    req: &mut broker::ReadRequest,
    write_head: i64,
) -> crate::Result<()> {
    use bytes::Buf;
    use tokio_util::compat::FuturesAsyncReadCompatExt;

    let r = tokio_util::io::ReaderStream::with_capacity(r.compat(), 1 << 17 /* 131KB */);
    let mut r = std::pin::pin!(r);

    // We may need to discard a leading portion of fragment content through the requested offset.
    let mut discard = req.offset.max(0) - fragment.begin;
    tracing::trace!(
        fragment=?ops::DebugJson(fragment),
        req.offset,
        discard,
        "started direct fragment read"
    );

    let mut remaining = if req.end_offset != 0 {
        req.end_offset - req.offset
    } else {
        i64::MAX
    };

    while let Some(mut content) = r.try_next().await.map_err(Error::ReadFragment)? {
        let mut content_len = content.len() as i64;

        if discard >= content_len {
            discard -= content_len;
            continue;
        } else if discard > 0 {
            content.advance(discard as usize);
            content_len -= discard;
            discard = 0;
        }

        if content_len > remaining {
            content.truncate(remaining as usize);
            content_len = remaining;
        }
        remaining -= content_len;

        () = co
            .yield_(Ok(broker::ReadResponse {
                content,
                offset: req.offset,
                ..Default::default()
            }))
            .await;

        req.offset += content_len;
        metrics.tick(req.offset, write_head);
        metrics.fragment.increment(content_len as u64);

        // If a bounded read that has reached its `end_offset` is complete,
        // return now rather than reading the fragment tail into empty batches.
        if remaining == 0 {
            return Ok(());
        }
    }

    Ok(())
}

#[derive(Clone)]
struct Metrics {
    offset: metrics::Gauge,
    remainder: metrics::Gauge,
    fragment: metrics::Counter,
}

impl Metrics {
    fn new(journal: &str) -> Self {
        static DESCRIBE: std::sync::Once = std::sync::Once::new();
        DESCRIBE.call_once(|| {
            metrics::describe_gauge!(
                "gazette_read_offset",
                metrics::Unit::Bytes,
                "current read offset for a journal",
            );
            metrics::describe_gauge!(
                "gazette_read_remainder",
                metrics::Unit::Bytes,
                "distance from current read offset to write head for a journal",
            );
            metrics::describe_counter!(
                "gazette_read_fragment",
                metrics::Unit::Bytes,
                "number of bytes directly read from journal fragment files",
            );
        });
        let offset = metrics::gauge!("gazette_read_offset", "journal" => journal.to_string());
        let remainder = metrics::gauge!("gazette_read_remainder", "journal" => journal.to_string());
        let fragment = metrics::counter!("gazette_read_fragment", "journal" => journal.to_string());

        Self {
            offset,
            remainder,
            fragment,
        }
    }

    fn tick(&self, offset: i64, write_head: i64) {
        self.offset.set(offset as f64);
        self.remainder.set((write_head - offset) as f64);
    }
}
