//! Chunked transport of protocol messages too large for one gRPC frame.
//!
//! Each top-level message of the shuffle, runtime, and connector protocols has
//! `bytes chunk = 2046` and `uint32 remaining = 2047` fields. A sender whose
//! message encodes to more than `MAX_FRAME` bytes sends it instead as a
//! sequence of chunk messages of the same type. Each chunk sets only `chunk`,
//! a non-empty slice of the encoding, and `remaining`, the bytes of the
//! encoding which follow it. The last chunk has `remaining` of zero, and the
//! receiver decodes the concatenated chunks as the original message.
//!
//! The first chunk gives the exact total, which the receiver checks against
//! `MAX_REASSEMBLED` before allocating it in full. Each later chunk must
//! account for exactly the bytes its predecessor said remained.
//!
//! A message larger than `MAX_REASSEMBLED` is never encoded. The sender
//! instead sends two one-byte sentinel chunks: the first with `remaining` of
//! the message's encoded length, and the second with `remaining` of zero.
//! Then its stream ends. A receiver fails the first chunk if its own limit is
//! lower, and the second chunk if its limit is higher, because the second
//! doesn't account for the remaining bytes. Either way, the RPC fails with a
//! receiver-side error which the sender reads from its own response stream.
//!
//! Chunking applies only where messages are serialized: `join` and the
//! `split_*` adapters wrap the streams of gRPC servers and clients.
//! In-process paths exchange messages without them.

use futures::StreamExt;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Largest encoded message which a sender sends as a single frame.
/// `usize::MAX` never chunks: every receiver must be able to reassemble
/// chunks before any sender produces them. Once every deployed receiver can,
/// this will be lowered to 4MB, the default gRPC message limit.
pub const MAX_FRAME: usize = usize::MAX;

/// Largest encoded message which may be sent or received in chunks.
pub const MAX_REASSEMBLED: usize = 1 << 30;

/// Upper bound of the bytes which a chunk message adds around its payload:
/// a two-byte key and a length varint of at most five bytes for `chunk`,
/// and a two-byte key and a varint of at most five bytes for `remaining`.
pub const CHUNK_OVERHEAD: usize = 16;

/// Chunked is a top-level protocol message having `chunk` and `remaining`.
pub trait Chunked: prost::Message + Default + Unpin + Send + 'static {
    fn chunk_mut(&mut self) -> (&mut bytes::Bytes, &mut u32);
}

macro_rules! impl_chunked {
    ($($message:ty),* $(,)?) => {
        $(
            impl Chunked for $message {
                fn chunk_mut(&mut self) -> (&mut bytes::Bytes, &mut u32) {
                    (&mut self.chunk, &mut self.remaining)
                }
            }
        )*
    };
}

impl_chunked!(
    proto_flow::connector::Request,
    proto_flow::connector::Response,
    proto_flow::runtime::Capture,
    proto_flow::runtime::Derive,
    proto_flow::runtime::Materialize,
    proto_flow::shuffle::LogRequest,
    proto_flow::shuffle::LogResponse,
    proto_flow::shuffle::SessionRequest,
    proto_flow::shuffle::SessionResponse,
    proto_flow::shuffle::SliceRequest,
    proto_flow::shuffle::SliceResponse,
);

/// Reassemble chunks of a received gRPC stream into whole messages.
///
/// An error of `stream` or of reassembly is yielded once, and ends the stream.
pub fn join<M, S>(stream: S) -> futures::stream::BoxStream<'static, tonic::Result<M>>
where
    M: Chunked,
    S: futures::Stream<Item = tonic::Result<M>> + Send + 'static,
{
    futures::stream::unfold(
        Some((Box::pin(stream), Reassembly::default())),
        |state| async move {
            let (mut stream, mut reassembly) = state?;
            loop {
                let result = match stream.next().await {
                    Some(Ok(message)) => reassembly.push(message, MAX_REASSEMBLED),
                    Some(Err(status)) => Err(status),
                    None => return reassembly.finish::<M>().err().map(|err| (Err(err), None)),
                };
                match result {
                    Ok(Some(message)) => return Some((Ok(message), Some((stream, reassembly)))),
                    Ok(None) => continue,
                    Err(status) => return Some((Err(status), None)),
                }
            }
        },
    )
    .boxed()
}

/// Split oversized messages of a gRPC server's response stream into chunks.
pub fn split_responses<M, S>(stream: S) -> futures::stream::BoxStream<'static, tonic::Result<M>>
where
    M: Chunked,
    S: futures::Stream<Item = tonic::Result<M>> + Send + 'static,
{
    Split {
        inner: Some(Box::pin(stream)),
        frames: Frames::Done,
        max_frame: MAX_FRAME,
        max_reassembled: MAX_REASSEMBLED,
    }
    .boxed()
}

/// Split oversized messages of a gRPC client's request stream into chunks.
/// The returned stream is `Sync`, as tonic requires of a request stream.
pub fn split_requests<M, S>(stream: S) -> impl futures::Stream<Item = M> + Send + Sync + 'static
where
    M: Chunked + Sync,
    S: futures::Stream<Item = M> + Send + Sync + 'static,
{
    Split {
        inner: Some(Box::pin(stream.map(Ok))),
        frames: Frames::Done,
        max_frame: MAX_FRAME,
        max_reassembled: MAX_REASSEMBLED,
    }
    .map(|result| result.expect("Split yields only errors of its inner stream"))
}

struct Split<M, S> {
    // Inner stream, or None if it ended or a message exceeded `max_reassembled`.
    // Dropping it promptly fails the sends of its producer.
    inner: Option<Pin<Box<S>>>,
    frames: Frames<M>,
    max_frame: usize,
    max_reassembled: usize,
}

impl<M, S> futures::Stream for Split<M, S>
where
    M: Chunked,
    S: futures::Stream<Item = tonic::Result<M>>,
{
    type Item = tonic::Result<M>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(frame) = this.frames.next() {
                return Poll::Ready(Some(Ok(frame)));
            }
            let Some(inner) = this.inner.as_mut() else {
                return Poll::Ready(None);
            };
            match std::task::ready!(inner.as_mut().poll_next(cx)) {
                Some(Ok(message)) => {
                    this.frames = Frames::new(message, this.max_frame, this.max_reassembled);
                    if let Frames::Sentinel(_) = &this.frames {
                        this.inner = None;
                    }
                }
                Some(Err(status)) => return Poll::Ready(Some(Err(status))),
                None => {
                    this.inner = None;
                    return Poll::Ready(None);
                }
            }
        }
    }
}

/// Frames is the sequence of frames which send one message.
enum Frames<M> {
    Done,
    // Message which fits in one frame.
    One(M),
    // Encoding of a message, sent as chunks of `payload` bytes from `offset`.
    Chunks {
        encoded: bytes::Bytes,
        offset: usize,
        payload: usize,
    },
    // Sentinel chunk to send with this `remaining`, of a message which exceeds
    // the reassembly limit.
    Sentinel(u32),
}

impl<M: Chunked> Frames<M> {
    fn new(message: M, max_frame: usize, max_reassembled: usize) -> Self {
        let encoded_len = message.encoded_len();

        if encoded_len <= max_frame {
            return Self::One(message);
        } else if encoded_len > max_reassembled {
            return Self::Sentinel(encoded_len.min(u32::MAX as usize) as u32);
        }
        let mut encoded = bytes::BytesMut::with_capacity(encoded_len);
        message
            .encode(&mut encoded)
            .expect("BytesMut has capacity of encoded_len");

        Self::Chunks {
            encoded: encoded.freeze(),
            offset: 0,
            payload: max_frame - CHUNK_OVERHEAD,
        }
    }

    fn next(&mut self) -> Option<M> {
        let (chunk, remaining) = match self {
            Self::Done => return None,
            Self::One(_) => {
                let Self::One(message) = std::mem::replace(self, Self::Done) else {
                    unreachable!()
                };
                return Some(message);
            }
            Self::Chunks {
                encoded,
                offset,
                payload,
            } => {
                let end = encoded.len().min(*offset + *payload);
                let chunk = encoded.slice(*offset..end);
                let remaining = encoded.len() - end;
                *offset = end;

                if remaining == 0 {
                    *self = Self::Done;
                }
                (chunk, remaining as u32)
            }
            Self::Sentinel(remaining) => {
                let remaining = *remaining;
                *self = if remaining == 0 {
                    Self::Done
                } else {
                    Self::Sentinel(0)
                };
                (bytes::Bytes::from_static(&[0]), remaining)
            }
        };

        let mut message = M::default();
        *message.chunk_mut().0 = chunk;
        *message.chunk_mut().1 = remaining;
        Some(message)
    }
}

/// Reassembly is the receive-side state of a chunked message.
#[derive(Default)]
struct Reassembly {
    // Bytes of the message received so far, if a chunk sequence is underway.
    buf: Option<bytes::BytesMut>,
    // Bytes which the last chunk said remain.
    remaining: u32,
}

impl Reassembly {
    /// Push a received message, returning a whole message once one is ready.
    fn push<M: Chunked>(
        &mut self,
        mut message: M,
        max_reassembled: usize,
    ) -> tonic::Result<Option<M>> {
        let name = std::any::type_name::<M>();
        let (chunk, remaining) = message.chunk_mut();
        let (chunk, remaining) = (std::mem::take(chunk), std::mem::take(remaining));

        if chunk.is_empty() {
            if remaining != 0 {
                return Err(protocol_error(format!(
                    "{name} sets `remaining` ({remaining}) without a `chunk`"
                )));
            } else if self.buf.is_some() {
                return Err(protocol_error(format!(
                    "received a whole {name} while {} bytes of a chunked {name} remain",
                    self.remaining
                )));
            }
            return Ok(Some(message));
        }

        if message.encoded_len() != 0 {
            return Err(protocol_error(format!(
                "chunk of {name} sets fields other than `chunk` and `remaining`"
            )));
        }

        let mut buf = match self.buf.take() {
            None => {
                let total = chunk.len() + remaining as usize;
                if total > max_reassembled {
                    return Err(tonic::Status::resource_exhausted(format!(
                        "received chunked {name} of {total} bytes, which exceeds the {max_reassembled}-byte limit of chunked messages"
                    )));
                }
                bytes::BytesMut::with_capacity(total)
            }
            Some(buf) => {
                if (self.remaining as usize).checked_sub(chunk.len()) != Some(remaining as usize) {
                    return Err(protocol_error(format!(
                        "chunk of {name} having {} bytes and {remaining} remaining doesn't follow its predecessor's {} remaining (the peer may have exceeded its own limit of chunked messages)",
                        chunk.len(),
                        self.remaining,
                    )));
                }
                buf
            }
        };
        buf.extend_from_slice(&chunk);

        if remaining != 0 {
            self.buf = Some(buf);
            self.remaining = remaining;
            return Ok(None);
        }

        let mut message = M::decode(buf.freeze())
            .map_err(|err| protocol_error(format!("failed to decode reassembled {name}: {err}")))?;

        let (chunk, remaining) = message.chunk_mut();
        if !chunk.is_empty() || *remaining != 0 {
            return Err(protocol_error(format!(
                "reassembled {name} is itself a chunk"
            )));
        }
        Ok(Some(message))
    }

    /// Finish at the end of the received stream.
    fn finish<M>(&self) -> tonic::Result<()> {
        if self.buf.is_none() {
            return Ok(());
        }
        Err(protocol_error(format!(
            "stream ended while {} bytes of a chunked {} remain",
            self.remaining,
            std::any::type_name::<M>(),
        )))
    }
}

fn protocol_error(message: String) -> tonic::Status {
    tonic::Status::internal(format!("chunk protocol error: {message}"))
}

#[cfg(test)]
mod test {
    use super::*;
    use prost::Message;
    use proto_flow::shuffle::{LogRequest, log_request};

    fn fixture(docs: usize) -> LogRequest {
        LogRequest {
            appends: (0..docs)
                .map(|i| log_request::Append {
                    journal_name_suffix: format!("journal/{i}"),
                    binding: i as u32,
                    doc_archived: bytes::Bytes::from(vec![i as u8; 20]),
                    ..Default::default()
                })
                .collect(),
            barrier: Some(log_request::Barrier {
                quota: 1234,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn frames(message: LogRequest, max_frame: usize, max_reassembled: usize) -> Vec<LogRequest> {
        let mut frames = Frames::new(message, max_frame, max_reassembled);
        std::iter::from_fn(|| frames.next()).collect()
    }

    // Each frame as (chunk length, remaining, encoded length), or "whole".
    fn describe(frames: &[LogRequest]) -> Vec<String> {
        frames
            .iter()
            .map(|frame| {
                if frame.chunk.is_empty() {
                    format!("whole message of {} bytes", frame.encoded_len())
                } else {
                    format!(
                        "chunk: {} bytes, remaining: {}, frame: {} bytes",
                        frame.chunk.len(),
                        frame.remaining,
                        frame.encoded_len()
                    )
                }
            })
            .collect()
    }

    fn reassemble(
        frames: Vec<LogRequest>,
        max_reassembled: usize,
    ) -> Result<Vec<LogRequest>, String> {
        let mut reassembly = Reassembly::default();
        let mut out = Vec::new();

        for frame in frames {
            match reassembly.push(frame, max_reassembled) {
                Ok(Some(message)) => out.push(message),
                Ok(None) => (),
                Err(status) => return Err(format!("{:?}: {}", status.code(), status.message())),
            }
        }
        reassembly
            .finish::<LogRequest>()
            .map_err(|status| format!("{:?}: {}", status.code(), status.message()))?;

        Ok(out)
    }

    #[test]
    fn test_split_and_join_round_trip() {
        let message = fixture(4);
        let len = message.encoded_len();

        // Under, exactly at, and over the frame limit.
        for (max_frame, expect_whole) in [(len + 1, true), (len, true), (64, false)] {
            let frames = frames(message.clone(), max_frame, MAX_REASSEMBLED);
            assert_eq!(
                frames.len() == 1 && frames[0].chunk.is_empty(),
                expect_whole
            );

            for frame in &frames {
                assert!(frame.encoded_len() <= max_frame);
            }
            assert_eq!(
                reassemble(frames, MAX_REASSEMBLED).unwrap(),
                vec![message.clone()]
            );
        }

        insta::assert_debug_snapshot!(describe(&frames(message, 64, MAX_REASSEMBLED)), @r#"
        [
            "chunk: 48 bytes, remaining: 103, frame: 54 bytes",
            "chunk: 48 bytes, remaining: 55, frame: 54 bytes",
            "chunk: 48 bytes, remaining: 7, frame: 54 bytes",
            "chunk: 7 bytes, remaining: 0, frame: 10 bytes",
        ]
        "#);
    }

    #[test]
    fn test_messages_interleave_with_chunked_messages() {
        let (small, large) = (fixture(1), fixture(8));

        let mut sent = Vec::new();
        for message in [small.clone(), large.clone(), small.clone()] {
            sent.extend(frames(message, 128, MAX_REASSEMBLED));
        }
        assert_eq!(
            reassemble(sent, MAX_REASSEMBLED).unwrap(),
            vec![small.clone(), large, small]
        );
    }

    #[test]
    fn test_oversize_sentinels() {
        let message = fixture(4);
        let len = message.encoded_len();

        let sentinels = frames(message, 64, len - 1);
        insta::assert_debug_snapshot!(describe(&sentinels), @r#"
        [
            "chunk: 1 bytes, remaining: 151, frame: 8 bytes",
            "chunk: 1 bytes, remaining: 0, frame: 4 bytes",
        ]
        "#);

        // A receiver with the sender's limit, or a lower one, fails the first.
        insta::assert_snapshot!(reassemble(sentinels.clone(), len - 1).unwrap_err(), @"ResourceExhausted: received chunked proto_flow::shuffle::LogRequest of 152 bytes, which exceeds the 150-byte limit of chunked messages");
        // A receiver with a higher limit fails the second.
        insta::assert_snapshot!(reassemble(sentinels, MAX_REASSEMBLED).unwrap_err(), @"Internal: chunk protocol error: chunk of proto_flow::shuffle::LogRequest having 1 bytes and 0 remaining doesn't follow its predecessor's 151 remaining (the peer may have exceeded its own limit of chunked messages)");
    }

    #[test]
    fn test_protocol_errors() {
        let chunks = frames(fixture(4), 64, MAX_REASSEMBLED);

        // A chunk which sets other fields.
        let mut other_fields = chunks.clone();
        other_fields[1].barrier = Some(Default::default());
        insta::assert_snapshot!(reassemble(other_fields, MAX_REASSEMBLED).unwrap_err(), @"Internal: chunk protocol error: chunk of proto_flow::shuffle::LogRequest sets fields other than `chunk` and `remaining`");

        // A whole message in the midst of a chunk sequence.
        let mut interleaved = chunks.clone();
        interleaved.insert(2, fixture(1));
        insta::assert_snapshot!(reassemble(interleaved, MAX_REASSEMBLED).unwrap_err(), @"Internal: chunk protocol error: received a whole proto_flow::shuffle::LogRequest while 55 bytes of a chunked proto_flow::shuffle::LogRequest remain");

        // The stream ends in the midst of a chunk sequence.
        let truncated = chunks[..3].to_vec();
        insta::assert_snapshot!(reassemble(truncated, MAX_REASSEMBLED).unwrap_err(), @"Internal: chunk protocol error: stream ended while 7 bytes of a chunked proto_flow::shuffle::LogRequest remain");

        // A dropped chunk.
        let mut dropped = chunks.clone();
        dropped.remove(2);
        insta::assert_snapshot!(reassemble(dropped, MAX_REASSEMBLED).unwrap_err(), @"Internal: chunk protocol error: chunk of proto_flow::shuffle::LogRequest having 7 bytes and 0 remaining doesn't follow its predecessor's 55 remaining (the peer may have exceeded its own limit of chunked messages)");

        // `remaining` without a `chunk`.
        let stray = vec![LogRequest {
            remaining: 7,
            ..Default::default()
        }];
        insta::assert_snapshot!(reassemble(stray, MAX_REASSEMBLED).unwrap_err(), @"Internal: chunk protocol error: proto_flow::shuffle::LogRequest sets `remaining` (7) without a `chunk`");

        // A reassembled message which is itself a chunk.
        let nested = frames(chunks[0].clone(), 32, MAX_REASSEMBLED);
        insta::assert_snapshot!(reassemble(nested, MAX_REASSEMBLED).unwrap_err(), @"Internal: chunk protocol error: reassembled proto_flow::shuffle::LogRequest is itself a chunk");
    }

    #[tokio::test]
    async fn test_stream_adapters() {
        let (small, large) = (fixture(1), fixture(4));

        let split = |max_reassembled| Split {
            inner: Some(Box::pin(
                futures::stream::iter([small.clone(), large.clone(), small.clone()]).map(Ok),
            )),
            frames: Frames::Done,
            max_frame: 64,
            max_reassembled,
        };

        // `Split` chunks only the message which exceeds its frame,
        // and `join` reassembles it.
        let sent: Vec<LogRequest> = split(MAX_REASSEMBLED).map(Result::unwrap).collect().await;
        insta::assert_debug_snapshot!(describe(&sent), @r#"
        [
            "whole message of 40 bytes",
            "chunk: 48 bytes, remaining: 103, frame: 54 bytes",
            "chunk: 48 bytes, remaining: 55, frame: 54 bytes",
            "chunk: 48 bytes, remaining: 7, frame: 54 bytes",
            "chunk: 7 bytes, remaining: 0, frame: 10 bytes",
            "whole message of 40 bytes",
        ]
        "#);

        let joined: Vec<tonic::Result<LogRequest>> =
            join(futures::stream::iter(sent.into_iter().map(Ok)))
                .collect()
                .await;
        let joined: Vec<LogRequest> = joined.into_iter().map(Result::unwrap).collect();
        assert_eq!(joined, vec![small.clone(), large.clone(), small.clone()]);

        // A message beyond `max_reassembled` is sent as sentinels, and the
        // stream then ends without sending the messages which follow it.
        let sent: Vec<LogRequest> = split(large.encoded_len() - 1)
            .map(Result::unwrap)
            .collect()
            .await;
        insta::assert_debug_snapshot!(describe(&sent), @r#"
        [
            "whole message of 40 bytes",
            "chunk: 1 bytes, remaining: 151, frame: 8 bytes",
            "chunk: 1 bytes, remaining: 0, frame: 4 bytes",
        ]
        "#);

        // `join` yields an inner error once, and ends.
        let errored: Vec<tonic::Result<LogRequest>> = join(futures::stream::iter([
            Ok(fixture(1)),
            Err(tonic::Status::cancelled("whoops")),
            Ok(fixture(1)),
        ]))
        .collect()
        .await;
        assert_eq!(errored.len(), 2);
        assert_eq!(
            errored[1].as_ref().unwrap_err().code(),
            tonic::Code::Cancelled
        );
    }
}
