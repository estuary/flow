/// Types for reading fragment files directly, using pre-signed urls.
pub mod fragment;
/// Implements retry behavior on top of a `raw` reader.
pub mod retry;

/// A plain reader that adapts a grpc stream of `ReadResponse`s to the `futures::io::AsyncRead`
/// trait.
mod raw;

use crate::read::{async_try, io_err, Error};
use crate::Client;
use futures::future::BoxFuture;
use futures::io::AsyncRead;
use proto_gazette::broker;
use std::{fmt::Debug, future::Future, io, pin::Pin, task::Poll};

pub use retry::{ExponentialBackoff, NoRetry, Retry};

/// Determines where in the journal to begin reading.
#[derive(Debug, Clone, Copy)]
pub enum ReadStart {
    /// Start reading at the current end of the journal. In other words, only read new data that's
    /// been written after the start of the read. This is typically only useful in combination with
    /// `ReadUntil::Forever` in order to read indefinitely.
    WriteHead,

    /// Start reading at the given (inclusive) byte offset. Reads will return an error if this
    /// offset is greater than the current write head of the journal unless this is a blocking read.
    Offset(u64),
}

impl ReadStart {
    pub(crate) fn to_broker_offset(&self) -> i64 {
        match self {
            ReadStart::WriteHead => -1,
            ReadStart::Offset(o) => i64::try_from(*o).expect("read start offset > i64::MAX"),
        }
    }
}

/// Determines where to stop reading.
#[derive(Debug, Clone, Copy)]
pub enum ReadUntil {
    /// Read up until the current write head. The write head offset is taken from the first
    /// successful read response, and content will be read only until that point. This prevents a
    /// reader from continuing indefinitely in cases where journal content is appended faster than
    /// it's being read.
    WriteHead,
    /// Keep going forever, or until the retry limit is exhausted.
    Forever,
    /// Read up until a specified (exclusive) offset, stopping at the current write head if that
    /// offset is not yet avaialable.
    Offset(u64),
    /// Read up until the given (exclusive) offset, blocking if necessary until the content is
    /// avaialable.
    OffsetBlocking(u64),
}

impl ReadUntil {
    pub(crate) fn to_broker_offset(&self) -> i64 {
        match self {
            ReadUntil::WriteHead => 0,
            ReadUntil::Forever => 0,
            ReadUntil::Offset(o) | ReadUntil::OffsetBlocking(o) => {
                i64::try_from(*o).expect("read end offset > i64::MAX")
            }
        }
    }

    pub(crate) fn is_blocking(&self) -> bool {
        match self {
            ReadUntil::Forever | ReadUntil::OffsetBlocking(_) => true,
            _ => false,
        }
    }
}

/// Represents a desired read of a single journal, which may involve multiple Read RPCs.
#[derive(Debug, Clone)]
pub struct JournalRead {
    journal: String,
    offset: i64,
    end_offset: i64,
    fetch_fragments: bool,
    block: bool,
    begin_mod_time: i64,
}

impl JournalRead {
    pub fn new(journal: String) -> JournalRead {
        JournalRead {
            journal,
            offset: 0,
            end_offset: 0,
            fetch_fragments: true,
            block: false,
            begin_mod_time: 0,
        }
    }

    pub fn starting_at(mut self, start: ReadStart) -> Self {
        self.offset = start.to_broker_offset();
        self
    }

    pub fn begin_mod_time(mut self, seconds: i64) -> Self {
        self.begin_mod_time = seconds;
        self
    }

    pub fn read_until(mut self, until: ReadUntil) -> Self {
        self.end_offset = until.to_broker_offset();
        self.block = until.is_blocking();
        self
    }

    pub fn allow_direct_fragment_reads(mut self, enabled: bool) -> Self {
        self.fetch_fragments = enabled;
        self
    }

    fn to_read_request(&self, needs_direct_read: bool) -> broker::ReadRequest {
        broker::ReadRequest {
            header: None,
            journal: self.journal.clone(),
            offset: self.offset,
            end_offset: self.end_offset,
            block: self.block,
            do_not_proxy: false,
            metadata_only: self.fetch_fragments && !needs_direct_read,
            begin_mod_time: self.begin_mod_time,
        }
    }
}

pub struct Reader<R: Retry> {
    client: Client,
    read: JournalRead,
    inner: State,
    retry: R,
    /// The first observed write_head, which is where we'll stop if the caller requested to read
    /// until `ReadUntil::WriteHead`. This prevents us from continuing to read indefinitely if a
    /// the write head of the journal advances as fast as the reader is reading it.
    target_write_head: i64,
    needs_direct_read: bool,
}

impl<R: Retry> Reader<R> {
    pub fn start_read(client: Client, req: JournalRead, retry: R) -> Reader<R> {
        let start = start_new_read(client.clone(), req.to_read_request(false));
        Reader {
            client,
            read: req,
            retry,
            inner: State::StartReq(start),
            target_write_head: 0,
            needs_direct_read: false,
        }
    }

    /// Returns the current offset of the reader
    pub fn current_offset(&self) -> i64 {
        self.read.offset
    }

    fn restart_after_eof(&mut self) -> Result<(), io::Error> {
        tracing::debug!(offset = ?self.read.offset, end_offset = ?self.read.end_offset, journal = %self.read.journal, "continuing after EOF");

        // Should we switch over to reading directly from brokers instead of opening
        // fragment files? This will be the case if we've reached the end of the persisted
        // fragments, as indicated by the last fragment having no `backing_store`.
        let switch_to_direct = self
            .unwrap_raw_reader()
            .current_fragment()
            .map(|f| f.backing_store.is_empty())
            .ok_or_else(|| {
                io_err(Error::ProtocolError(
                    "reader returned EOF without having read any fragment metadata".into(),
                ))
            })?;
        if switch_to_direct && !self.needs_direct_read {
            tracing::debug!(offset = %self.read.offset, journal = %self.read.journal, "switching to direct reads from broker");
            self.needs_direct_read = true;
        }

        self.retry.reset();
        let fut = start_new_read(self.client.clone(), self.read_req());
        self.inner = State::StartReq(fut);
        Ok(())
    }

    fn update_offsets_from_reader(&mut self) {
        let (offset, write_head) = {
            let reader = self.unwrap_raw_reader();
            (reader.current_offset(), reader.write_head())
        };
        self.read.offset = offset;
        if self.target_write_head == 0 {
            self.target_write_head = write_head;
        }
    }

    fn unwrap_raw_reader(&self) -> &raw::Reader {
        match &self.inner {
            State::Reading(ref r) => r,
            other => panic!(
                "expected reader to be in reading state, but was: {:?}",
                other
            ),
        }
    }

    fn read_req(&self) -> broker::ReadRequest {
        self.read.to_read_request(self.needs_direct_read)
    }

    fn continue_after_eof(&self) -> bool {
        if self.read.end_offset > 0 {
            self.read.offset < self.read.end_offset
                || (self.read.offset >= self.target_write_head && !self.read.block)
        } else {
            self.read.offset < self.target_write_head
        }
    }

    /// Tries to progress the state machine, returning the first error that occurs.
    fn try_poll_read(
        &mut self,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let Reader {
            inner,
            client,
            read: req,
            needs_direct_read,
            ..
        } = &mut *self;
        loop {
            let next_state = match inner {
                State::StartReq(ref mut fut) => {
                    let result = futures::ready!(fut.as_mut().poll(cx));
                    let response = async_try!(result.map_err(io_err));
                    State::Reading(response)
                }
                State::Reading(ref mut read) => {
                    return Pin::new(read).poll_read(cx, buf);
                }
                State::Backoff(ref mut wait) => {
                    futures::ready!(wait.as_mut().poll(cx));
                    State::StartReq(start_new_read(
                        client.clone(),
                        req.to_read_request(*needs_direct_read),
                    ))
                }
            };
            *inner = next_state;
        }
    }
}

impl<R: Retry> futures::io::AsyncRead for Reader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let result = futures::ready!(self.as_mut().try_poll_read(cx, buf));
        match result {
            // If we're reading fragments directly, then it's possible to read EOF prior to
            // reaching the desired `end_offset`. This happens in cases where the requested offsets
            // span multiple fragment files and `do_not_proxy` is `true`, because each
            // `FragmentReader` will return `Ok(0)` at the end. We need to restart a read of the
            // next fragment in this case.
            Ok(0) => {
                // This just ensures that we know about the `write_head` of the journal in case
                // we hadn't updated that before.
                self.update_offsets_from_reader();
                if self.continue_after_eof() {
                    async_try!(self.restart_after_eof());
                    // Arrange to be polled again.
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    tracing::debug!(offset = %self.read.offset, wirte_head = %self.target_write_head, ndr = %self.needs_direct_read, req = ?self.read_req(), "EOF");
                    Poll::Ready(Ok(0))
                }
            }
            Ok(n) => {
                self.update_offsets_from_reader();
                self.retry.reset();
                Poll::Ready(Ok(n))
            }
            Err(err) => {
                if !self.read.block
                    && Some(broker::Status::OffsetNotYetAvailable) == error_status(&err)
                {
                    tracing::debug!(current_offset = %self.read.offset, "EOF due to OffsetNotYetAvailable and !block");
                    return Poll::Ready(Ok(0));
                }

                let next_backoff = self.retry.next_backoff(&err);
                tracing::warn!(error = ?err, backoff = ?next_backoff, will_retry = %next_backoff.is_some(), journal = %self.read.journal, "reader returned error");
                if let Some(backoff) = next_backoff {
                    // If backoff is zero, then don't both asking the runtime to sleep
                    if backoff.is_zero() {
                        let fut = start_new_read(self.client.clone(), self.read_req());
                        self.inner = State::StartReq(fut);
                    } else {
                        self.inner = State::Backoff(Box::pin(tokio::time::sleep(backoff)));
                    }
                    cx.waker().wake_by_ref(); // Arrange to be polled again
                    Poll::Pending
                } else {
                    Poll::Ready(Err(err))
                }
            }
        }
    }
}

/// Models the state of this simple state machine. Either we are opening a new
/// `raw::Reader`, reading data from said reader, or waiting on a backoff to try again.
enum State {
    StartReq(BoxFuture<'static, Result<raw::Reader, Error>>),
    Reading(raw::Reader),
    Backoff(Pin<Box<tokio::time::Sleep>>),
}

impl Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StartReq(_) => f.write_str("StartReq"),
            Self::Reading(_) => f.write_str("Reading"),
            Self::Backoff(_) => f.write_str("Backoff"),
        }
    }
}

fn error_status(err: &io::Error) -> Option<broker::Status> {
    read_err(err).and_then(|e| match e {
        Error::NotOk(status) => Some(*status),
        _ => None,
    })
}

fn read_err(err: &io::Error) -> Option<&Error> {
    err.get_ref().and_then(|e| e.downcast_ref::<Error>())
}

/// This exists because it helps us avoid a lifetime (wink) of suffering. The async function on a
/// `Client` have `&mut self` receivers, so the returned opaque `Future` will have same lifetime as
/// the `&mut self`. This lifetime is not `'static` and so it cannot be turned into a `Pin<Box<dyn
/// Future<...>>>`. We _really_ want this to be a `Pin<Box<dyn Future<...>>>`, because _otherwise_
/// we'd end up with a self-referential struct containing a `Future` field that's bound to the
/// lifetime of the `client` field, which requires all sorts of arcane trickery. The trick here is
/// to `move` the client into a closure, and box the closure, thus allowing the returned `Future`
/// to be `'static`.
fn start_new_read(
    client: Client,
    req: broker::ReadRequest,
) -> BoxFuture<'static, Result<raw::Reader, Error>> {
    Box::pin(async move {
        let mut c = client;
        raw::start_read(&mut c, req).await
    })
}
