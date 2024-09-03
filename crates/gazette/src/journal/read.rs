use super::Client;
use crate::Error;
use futures::TryStreamExt;
use proto_gazette::broker;

impl Client {
    pub fn read(
        self,
        mut req: broker::ReadRequest,
    ) -> impl futures::Stream<Item = crate::Result<broker::ReadResponse>> + 'static {
        coroutines::coroutine(move |mut co| async move {
            let mut write_head = i64::MAX;

            loop {
                // Have we read through requested `end_offset`?
                if req.end_offset != 0 && req.offset == req.end_offset {
                    return;
                }
                // Have we read through the `write_head` and our request is non-blocking?
                if !req.block && req.offset == write_head {
                    return;
                }

                match self.read_some(&mut co, &mut req, &mut write_head).await {
                    Ok(()) => (),
                    Err(Error::BrokerStatus(broker::Status::NotJournalBroker))
                        if req.do_not_proxy =>
                    {
                        // Expected error which drives dynamic route discovery.
                        // `req.header` has updated route topology and we restart the request.
                    }
                    Err(err) => {
                        // Surface error to the caller, which can either drop us
                        // or poll us again to retry.
                        () = co.yield_(Err(err)).await;
                    }
                }
            }
        })
    }

    async fn read_some(
        &self,
        co: &mut coroutines::Suspend<crate::Result<broker::ReadResponse>, ()>,
        req: &mut broker::ReadRequest,
        write_head: &mut i64,
    ) -> crate::Result<()> {
        let route = req.header.as_ref().and_then(|hdr| hdr.route.as_ref());
        let mut client = self.into_sub(self.router.route(route, false).await?);

        // Fetch metadata first before we start the actual read.
        req.metadata_only = true;

        let mut stream = client.read(req.clone()).await?.into_inner();
        let metadata = stream.try_next().await?.ok_or(Error::UnexpectedEof)?;
        let _eof = stream.try_next().await?; // Broker sends EOF.
        std::mem::drop(stream);

        tracing::trace!(req=?ops::DebugJson(&req), meta=?ops::DebugJson(&metadata), "fetched read metadata");

        // Can we directly read the fragment from cloud storage?
        if let (broker::Status::Ok, false, Some(fragment)) = (
            metadata.status(),
            metadata.fragment_url.is_empty(),
            &metadata.fragment,
        ) {
            if req.offset != metadata.offset {
                tracing::info!(req.journal, req.offset, metadata.offset, "offset jump");
                req.offset = metadata.offset;
            }
            *write_head = metadata.write_head;
            let (fragment, fragment_url) = (fragment.clone(), metadata.fragment_url.clone());
            () = co.yield_(Ok(metadata)).await;
            return read_fragment_url(co, fragment, fragment_url, &self.http, req).await;
        }

        tracing::trace!(req.offset, write_head, "started direct journal read");

        // Restart as a regular (non-metadata) read.
        req.metadata_only = false;
        let mut stream = client.read(req.clone()).await?.into_inner();

        while let Some(resp) = stream.try_next().await? {
            if resp.header.is_some() {
                req.header = resp.header.clone();
            }
            match (resp.status(), &resp.fragment, resp.content.is_empty()) {
                // Metadata response telling us of a new fragment being read.
                (broker::Status::Ok, Some(_fragment), true) => {
                    // Offset jumps happen if content is removed from the middle of a journal,
                    // or when reading from the journal head (offset -1).
                    if req.offset != resp.offset {
                        tracing::info!(req.journal, req.offset, resp.offset, "offset jump");
                        req.offset = resp.offset;
                    }
                    *write_head = resp.write_head;
                    () = co.yield_(Ok(resp)).await;
                }
                // Content response.
                (broker::Status::Ok, None, false) => {
                    req.offset += resp.content.len() as i64;
                    () = co.yield_(Ok(resp)).await;
                }
                // All other statuses end the stream, and are handled by the caller.
                (status, _, _) => return Err(Error::BrokerStatus(status)),
            }
        }

        Ok(())
    }
}

async fn read_fragment_url(
    co: &mut coroutines::Suspend<crate::Result<broker::ReadResponse>, ()>,
    fragment: broker::Fragment,
    fragment_url: String,
    http: &reqwest::Client,
    req: &mut broker::ReadRequest,
) -> crate::Result<()> {
    let mut get = http.get(fragment_url);

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

    let raw_reader = response
        // Map into a Stream<Item = Result<Bytes, _>>.
        .bytes_stream()
        // Wrap reqwest::Error as an io::Error for compatibility with AsyncBufRead.
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))
        // Adapt into an AsyncBufRead.
        .into_async_read();

    match fragment.compression_codec() {
        broker::CompressionCodec::None | broker::CompressionCodec::GzipOffloadDecompression => {
            read_fragment_url_body(co, fragment, raw_reader, req).await
        }
        broker::CompressionCodec::Gzip => {
            let decoder = async_compression::futures::bufread::GzipDecoder::new(raw_reader);
            read_fragment_url_body(co, fragment, decoder, req).await
        }
        broker::CompressionCodec::Zstandard => {
            let decoder = async_compression::futures::bufread::ZstdDecoder::new(raw_reader);
            read_fragment_url_body(co, fragment, decoder, req).await
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
    co: &mut coroutines::Suspend<crate::Result<broker::ReadResponse>, ()>,
    fragment: broker::Fragment,
    r: impl futures::io::AsyncRead,
    req: &mut broker::ReadRequest,
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
    }

    Ok(())
}
