use crate::read::Error;
use futures::future::BoxFuture;
use futures::{io::AsyncRead, ready};
use proto_gazette::broker;
use std::{
    fmt::{self, Debug},
    io,
    marker::Unpin,
    pin::Pin,
    task::Poll,
};

lazy_static::lazy_static! {
    /// http client used to fetch fragment files from cloud storage.
    /// TODO: consider using `builder` instead, since `new` may panic
    static ref HTTP_CLIENT: ::reqwest::Client = ::reqwest::Client::new();
}

/// Reads a single fragment file from cloud storage, using a pre-signed URL. Performs decompression
/// as necessary.
#[derive(Debug)]
pub struct FragmentReader {
    fragment: broker::Fragment,
    state: FragmentReadState,
}

impl FragmentReader {
    // Returns a new `FragmentReader` for the given url, which must correspond with the metadata in
    // `fragment`. The fragment file is fetched lazily, and the content is streamed directly from
    // the HTTP response.
    pub fn new(signed_url: String, fragment: broker::Fragment) -> FragmentReader {
        tracing::debug!(?fragment, "fetching fragment");
        let mut get = HTTP_CLIENT.get(signed_url);

        match fragment.compression_codec() {
            broker::CompressionCodec::GzipOffloadDecompression => {
                get = get.header("Accept-Encoding", "identity");
            }
            broker::CompressionCodec::Gzip => {
                get = get.header("Accept-Encoding", "gzip");
            }
            _ => {}
        }

        FragmentReader {
            fragment,
            state: FragmentReadState::PendingResponse(Box::pin(get.send())),
        }
    }
}

impl AsyncRead for FragmentReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match &mut self.state {
            FragmentReadState::Done => Poll::Ready(Ok(0)),
            FragmentReadState::Reading(read) => Pin::new(read.as_mut()).poll_read(cx, buf),

            FragmentReadState::PendingResponse(fut) => {
                let result = ready!(fut.as_mut().poll(cx))
                    .and_then(|resp| resp.error_for_status())
                    .map_err(Error::FragmentRequestFailed)
                    .and_then(|resp| {
                        new_fragment_response_reader(self.fragment.compression_codec(), resp)
                    });
                match result {
                    Ok(mut reader) => {
                        let result = Pin::new(reader.as_mut()).poll_read(cx, buf);
                        self.state = FragmentReadState::Reading(reader);
                        result
                    }
                    Err(err) => {
                        self.state = FragmentReadState::Done;
                        Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, err)))
                    }
                }
            }
        }
    }
}

fn new_fragment_response_reader(
    compression: broker::CompressionCodec,
    resp: reqwest::Response,
) -> Result<Box<dyn AsyncRead + Unpin + Send>, Error> {
    use async_compression::futures::bufread::{GzipDecoder, ZstdDecoder};
    use broker::CompressionCodec;
    use futures::{
        io::{self, BufReader, ErrorKind},
        prelude::*,
    };
    // TODO: this introduces an extra copy because the decompressors require an `AsyncBufRead`, but
    // the response is only turned into an `AsyncRead`. AFAIK there's no reason we couldn't
    // implement `AsyncBufRead` for the response directly, to avoid the copy. I'm just considering
    // that an optimization to be left for the future.
    let reader = resp
        .bytes_stream()
        .map_err(|e| io::Error::new(ErrorKind::Other, e))
        .into_async_read();
    match compression {
        // If GzipOffload, then we've already instructed the storage provider to decompress it on
        // our behalf.
        CompressionCodec::None | CompressionCodec::GzipOffloadDecompression => Ok(Box::new(reader)),
        CompressionCodec::Gzip => Ok(Box::new(GzipDecoder::new(BufReader::new(reader)))),
        CompressionCodec::Zstandard => Ok(Box::new(ZstdDecoder::new(BufReader::new(reader)))),
        CompressionCodec::Snappy => Err(Error::ProtocolError(
            "snappy compression is not yet implemented by this client".into(),
        )),
        CompressionCodec::Invalid => Err(Error::ProtocolError(
            "invalid compression codec for fragment".into(),
        )),
    }
}

enum FragmentReadState {
    PendingResponse(BoxFuture<'static, reqwest::Result<reqwest::Response>>),
    Reading(Box<dyn AsyncRead + Unpin + Send>),
    Done,
}

impl Debug for FragmentReadState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PendingResponse(_) => f.write_str("PendingResponse"),
            Self::Reading(_) => f.write_str("Reading"),
            Self::Done => f.write_str("Done"),
        }
    }
}
