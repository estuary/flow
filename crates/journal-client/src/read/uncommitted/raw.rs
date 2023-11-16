use crate::read::uncommitted::fragment::FragmentReader;
use crate::read::{async_try, io_err, Error};
use crate::Client;
use futures::{io::AsyncRead, ready, Stream};
use proto_gazette::broker;
use std::io::Read;
use std::io::{self, Cursor};
use std::pin::Pin;
use std::task::Poll;
use tonic::codec::Streaming;

pub async fn start_read(client: &mut Client, req: broker::ReadRequest) -> Result<Reader, Error> {
    let offset = req.offset;
    let journal = req.journal.clone();
    tracing::debug!(?req, "starting new read of journal");
    let response = client.read(req).await?;
    // TODO: see if there's anything in the response we should check or log before proceeding to read
    Ok(Reader::new(journal, offset, response.into_inner()))
}

/// A basic reader that doesn't do any sort of reties, but implements `futures::io::AsyncRead`.
/// It handles reading content directly from streaming responses, or reading fragment files
/// directly from cloud storage. In the case where fragment files are read directly from cloud
/// storage, the `Reader` will return EOF (`Ok(0)`) upon reaching the end of the first fragment
/// file. A new `Reader` will need to be created for each subsequent fragment file. This is
/// considered an implementation detail of `journal-client::read::uncommitted::Reader`.
pub struct Reader {
    journal: String,
    write_head: i64,
    current_offset: i64,
    response_stream: Option<Streaming<broker::ReadResponse>>,
    current_content: Option<Content>,
    current_fragment_metadata: Option<broker::Fragment>,
}

impl Reader {
    pub(crate) fn new(
        journal: String,
        start_offset: i64,
        stream: Streaming<broker::ReadResponse>,
    ) -> Reader {
        Reader {
            journal,
            write_head: 0,
            current_offset: start_offset,
            response_stream: Some(stream),
            current_content: None,
            current_fragment_metadata: None,
        }
    }

    /// Returns metadata on the fragment that's currently being read by the reader.
    pub fn current_fragment(&self) -> Option<&broker::Fragment> {
        self.current_fragment_metadata.as_ref()
    }

    /// Returns the current write head of the journal, or 0 if unknown.
    pub fn write_head(&self) -> i64 {
        self.write_head
    }

    /// Returns the current read offset within the journal, accounting for the bytes that have been read so far.
    /// Note that this offset may jump forward if there are gaps in journal content caused by deleted fragment files.
    pub fn current_offset(&self) -> i64 {
        self.current_offset
    }

    /// When reading directly from fragment files, we expect a single fragment url before the response stream closes.
    /// This function just reads the remaining messages from the stream. This should normally just observe the end of
    // the stream and then move on.
    fn poll_response_stream_drained(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(s) = self.response_stream.as_mut() {
            if let Some(result) = ready!(Pin::new(s).poll_next(cx)) {
                let msg = async_try!(result.map_err(io_err));
                check_status(&msg).map_err(io_err)?;
                tracing::warn!(ignored = ?msg, "ignoring response because response stream is being drained pending direct fragment read");
                cx.waker().wake_by_ref();
                return Poll::Pending;
            } else {
                tracing::debug!(journal = %self.journal, "end of read response stream");
            }
        }
        self.response_stream.take();
        return Poll::Ready(Ok(()));
    }
}

impl AsyncRead for Reader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        // If we're going to read a fragment directly, then we need to first
        // read the response stream to completion. This is so that we don't hold open an
        // idle grpc streaming response while reading the fragment, since it could take
        // a while.
        if self
            .current_content
            .as_ref()
            .map(Content::is_fragment)
            .unwrap_or_default()
            && self.response_stream.is_some()
        {
            ready!(self.poll_response_stream_drained(cx))?;
        }

        let res = self
            .current_content
            .as_mut()
            .map(|c| Pin::new(c).poll_read(cx, buf)); //.unwrap_or(Poll::Ready(Ok(0)));
        match res {
            None => { /* no-op */ }
            Some(Poll::Ready(Ok(0))) => {
                // Don't return Ok(0) until we've reached the end of the stream, since there may be
                // more content forthcoming.
                self.current_content.take();
            }
            Some(Poll::Ready(Ok(n))) => {
                tracing::trace!(current_offset = %self.current_offset, n = %n, "read bytes from raw reader");
                self.current_offset += n as i64;
                return Poll::Ready(Ok(n));
            }
            // Either we got an error, or need polled again
            Some(other) => return other,
        };

        // We need to read another response
        let next_result = if let Some(stream) = self.response_stream.as_mut() {
            ready!(Pin::new(stream).poll_next(cx))
        } else {
            None
        };
        if let Some(result) = next_result {
            let mut resp = async_try!(result.map_err(io_err));
            async_try!(check_status(&resp).map_err(io_err));
            if resp.write_head > self.write_head {
                tracing::trace!(
                    prev_write_head = self.write_head,
                    resp_write_head = resp.write_head,
                    "advanced write head"
                );
                self.write_head = resp.write_head;
            }

            // Save the most recent fragment metadata. This will be used below when fetching
            // fragment files, and is also made accessible by the public `current_fragment`
            // function.
            if let Some(fragment) = resp.fragment.take() {
                tracing::debug!(?fragment, "got metadata response");
                self.current_fragment_metadata = Some(fragment);
            }

            // The offset may skip ahead in cases where fragment files have been deleted (which
            // is perfectly acceptable). So we'll update our current_offset to the one from the
            // response. It's worth logger, though, because it ought to be very rare.
            if resp.offset > self.current_offset {
                // The Go client returns an error in this case, and expects the caller to restart
                // a new read with the corrected offset. While that behavior may or may not be important
                // for Go use cases, it's not important for the Rust client at this time, so we're just
                // logging and moving on.
                tracing::info!(prev_offset = self.current_offset, next_offset = resp.offset, journal = %self.journal, "gap in journal offset");
                self.current_offset = resp.offset;
            }

            // The repsonse will contain either a chunk of journal content directly, or else a
            // signed cloud storage URL that can be fetched in order to read the content.
            if !resp.fragment_url.is_empty() {
                if let Some(fragment) = self.current_fragment_metadata.as_ref() {
                    // Do we need to skip some bytes within this fragment
                    let discard_bytes =
                        (self.current_offset.max(0) - fragment.begin).max(0) as usize;
                    if discard_bytes > 0 {
                        tracing::debug!(discard = %discard_bytes, "will need to discard some bytes at the start of the fragment in order to begin reading at the requested offset");
                    } else {
                        tracing::debug!(current_offset=%self.current_offset, ?fragment, "why no discarding");
                    }
                    let content = Content::Fragment {
                        discard_bytes,
                        fragment: FragmentReader::new(resp.fragment_url, fragment.clone()),
                    };
                    self.current_content = Some(content);
                } else {
                    return Poll::Ready(Err(io_err(Error::ProtocolError(
                        "response contains fragment_url but no fragment metadata".into(),
                    ))));
                }
            } else if !resp.content.is_empty() {
                tracing::trace!(nbytes = resp.content.len(), "got content for journal");
                self.current_content = Some(Content::Resp(Cursor::new(resp.content)));
            } else {
                // This was a metadata-only message
                self.current_content.take();
            }
        } else {
            tracing::debug!(journal = %self.journal, "end of read response stream");
            return Poll::Ready(Ok(0));
        }

        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

fn check_status(resp: &broker::ReadResponse) -> Result<(), Error> {
    match resp.status() {
        broker::Status::Ok => Ok(()),
        other => Err(Error::NotOk(other)),
    }
}

/// Used by `Reader` to adapt streaming reads and direct reads from fragment files to a single
/// interface. Also handles discarding unneeded bytes at the beginning of fragment files in cases
/// where the requested offset falls in between the begin and end offsets of the fragment.
enum Content {
    Resp(Cursor<Vec<u8>>),
    Fragment {
        /// The number of bytes to be discarded from the beginning of the fragment.
        // This value is mutated towards 0 as bytes are discarded.
        discard_bytes: usize,
        fragment: FragmentReader,
    },
}

impl Content {
    fn is_fragment(&self) -> bool {
        match self {
            Content::Fragment { .. } => true,
            _ => false,
        }
    }
}

impl futures::io::AsyncRead for Content {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<io::Result<usize>> {
        match self.get_mut() {
            Content::Resp(cursor) => Poll::Ready(cursor.read(buf)),
            Content::Fragment {
                ref mut discard_bytes,
                ref mut fragment,
            } => {
                while *discard_bytes > 0 {
                    let n = async_try!(ready!(poll_discard_unused_fragment_prefix(
                        *discard_bytes,
                        cx,
                        fragment
                    )));
                    *discard_bytes -= n;
                    tracing::trace!(discarded_bytes = %n, remaining_to_discard = %*discard_bytes, "discarded unneeded bytes from fragment file");
                }
                Pin::new(fragment).poll_read(cx, buf)
            }
        }
    }
}

fn poll_discard_unused_fragment_prefix(
    discard_bytes: usize,
    cx: &mut std::task::Context<'_>,
    frag: &mut FragmentReader,
) -> Poll<io::Result<usize>> {
    const MAX_DISCARD_BUF_SIZE: usize = 32 * 1024;
    let mut raw_buf = [0; MAX_DISCARD_BUF_SIZE];
    let buf = &mut raw_buf[..MAX_DISCARD_BUF_SIZE.min(discard_bytes)];
    Pin::new(frag).poll_read(cx, buf)
}
