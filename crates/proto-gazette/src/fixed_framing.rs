//! Gazette's fixed framing of protobuf records. A frame is a magic word, a
//! little-endian u32 payload length, and the payload.
//!
//! The magic word lets a reader which starts mid-frame recover. A reader may begin
//! at an arbitrary journal offset. Without a recognizable boundary it would
//! interpret whatever bytes it landed on as a record.
//!
//! The reference implementation is `message/fixed_framing.go`. This module lives
//! beside `uuid` as Go keeps `fixed_framing.go` beside its UUID sequencing in
//! package `message`. Neither implementation bounds the length a frame states.
//!
//! [`unpack`] consumes a frame from its buffer, as Go's `UnpackFixedFrame` consumes
//! one from its reader. It splits the frame off before decoding it, so that the
//! message's bytes fields reference the frame rather than copies of it.

/// Word which precedes every fixed frame.
pub const MAGIC: [u8; 4] = [0x66, 0x33, 0x93, 0x36];

/// Length of a frame's magic word and payload length.
pub const HEADER_LEN: usize = 8;

/// Append a fixed frame of `message` to `buf`.
pub fn encode<M: prost::Message>(message: &M, buf: &mut bytes::BytesMut) {
    let len = u32::try_from(message.encoded_len()).expect("a record is smaller than 4 GiB");

    buf.reserve(HEADER_LEN + len as usize);
    buf.extend_from_slice(&MAGIC);
    buf.extend_from_slice(&len.to_le_bytes());
    message.encode_raw(buf);
}

/// What the head of a byte stream holds, as its header states it.
#[derive(Debug, PartialEq)]
pub enum Header {
    /// A frame whose payload is `payload` bytes follows. The payload may not have
    /// been read yet.
    Frame { payload: usize },
    /// The input does not begin on a frame boundary. Its first `skipped` bytes
    /// belong to no frame this reader can interpret.
    Desync { skipped: usize },
    /// The input holds less than a header.
    Incomplete,
}

/// Read the header at the head of `input`.
pub fn header(input: &[u8]) -> Header {
    let Some(header) = input.get(..HEADER_LEN) else {
        return Header::Incomplete;
    };
    if header[..MAGIC.len()] != MAGIC {
        return Header::Desync {
            skipped: desync_span(input),
        };
    }
    let payload = u32::from_le_bytes(header[MAGIC.len()..].try_into().unwrap()) as usize;

    Header::Frame { payload }
}

/// Outcome of unpacking the head of a buffer.
#[derive(Debug)]
pub enum Frame<M> {
    /// `message`, decoded from `framed`: the whole frame, split off the buffer.
    Record { message: M, framed: bytes::Bytes },
    /// The buffer does not begin on a frame boundary, and `skipped` were split off
    /// it: they belong to no frame this reader can interpret. A caller which tracks
    /// a journal offset reports them against that offset.
    Desync { skipped: bytes::Bytes },
    /// The buffer holds a partial header or a partial frame, and nothing was split
    /// off it. The caller extends it and unpacks again.
    Incomplete,
}

/// Unpack the frame at the head of `buf`, splitting it off.
///
/// A payload which is framed but does not decode is an error, and not another
/// desync: a reader must not skip a record it cannot interpret. That frame has been
/// split off `buf` all the same.
pub fn unpack<M: prost::Message + Default>(
    buf: &mut bytes::BytesMut,
) -> Result<Frame<M>, prost::DecodeError> {
    match header(buf) {
        Header::Incomplete => Ok(Frame::Incomplete),
        Header::Desync { skipped } => Ok(Frame::Desync {
            skipped: buf.split_to(skipped).freeze(),
        }),
        Header::Frame { payload } if buf.len() < HEADER_LEN + payload => Ok(Frame::Incomplete),
        Header::Frame { payload } => {
            let framed = buf.split_to(HEADER_LEN + payload).freeze();
            let message = M::decode(framed.slice(HEADER_LEN..))?;

            Ok(Frame::Record { message, framed })
        }
    }
}

/// Count of leading bytes to discard from a desynchronized `input` to reach the next
/// magic word.
///
/// When no magic word follows, the final three bytes are kept. A magic word may
/// straddle the end of the input, and discarding those bytes would destroy the
/// boundary recovery depends on.
fn desync_span(input: &[u8]) -> usize {
    match input[1..].windows(MAGIC.len()).position(|w| w == MAGIC) {
        Some(index) => 1 + index,
        None => input.len() - (MAGIC.len() - 1),
    }
}

#[cfg(test)]
mod test {
    use super::{Frame, HEADER_LEN, Header, MAGIC, encode, header, unpack};
    use crate::broker;

    fn label(name: &str) -> broker::Label {
        broker::Label {
            name: name.to_string(),
            value: "a value".to_string(),
            prefix: false,
        }
    }

    fn framed(names: &[&str]) -> bytes::BytesMut {
        let mut buf = bytes::BytesMut::new();
        for name in names {
            encode(&label(name), &mut buf);
        }
        buf
    }

    #[test]
    fn test_round_trip() {
        let mut buf = framed(&["one"]);
        let len = buf.len();

        assert_eq!(&buf[..MAGIC.len()], &MAGIC);
        assert_eq!(
            header(&buf),
            Header::Frame {
                payload: len - HEADER_LEN
            }
        );

        let Ok(Frame::Record { message, framed }) = unpack::<broker::Label>(&mut buf) else {
            panic!("expected a record");
        };
        assert_eq!(message, label("one"));
        assert_eq!(framed.len(), len);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_concatenated_stream() {
        let mut buf = framed(&["one", "two", "three"]);
        let mut decoded = Vec::new();

        while !buf.is_empty() {
            let Ok(Frame::Record { message, .. }) = unpack::<broker::Label>(&mut buf) else {
                panic!("expected a record");
            };
            decoded.push(message.name);
        }
        assert_eq!(decoded, vec!["one", "two", "three"]);
    }

    #[test]
    fn test_bytes_fields_reference_the_frame() {
        let mut buf = bytes::BytesMut::new();
        // A message with a `bytes` field: an AppendRequest's content.
        let request = broker::AppendRequest {
            content: bytes::Bytes::from_static(&[0xab; 64]),
            ..Default::default()
        };
        encode(&request, &mut buf);

        let Ok(Frame::Record { message, framed }) = unpack::<broker::AppendRequest>(&mut buf)
        else {
            panic!("expected a record");
        };
        let framed = framed.as_ptr_range();
        let content = message.content.as_ptr_range();
        assert!(framed.start <= content.start && content.end <= framed.end);
    }

    #[test]
    fn test_desync_resynchronizes_on_the_next_frame() {
        let mut buf = bytes::BytesMut::from(&b"leading garbage"[..]);
        buf.extend_from_slice(&framed(&["after"]));

        let Ok(Frame::Desync { skipped }) = unpack::<broker::Label>(&mut buf) else {
            panic!("expected a desync");
        };
        assert_eq!(&skipped[..], b"leading garbage");

        let Ok(Frame::Record { message, .. }) = unpack::<broker::Label>(&mut buf) else {
            panic!("expected a record");
        };
        assert_eq!(message, label("after"));
    }

    #[test]
    fn test_desync_without_a_following_frame_retains_a_partial_magic_word() {
        // The final three bytes could begin a magic word which continues in input
        // the caller has not read yet.
        let mut buf = bytes::BytesMut::from(&[&b"garbage"[..], &MAGIC[..3]].concat()[..]);

        let Ok(Frame::Desync { skipped }) = unpack::<broker::Label>(&mut buf) else {
            panic!("expected a desync");
        };
        assert_eq!(&skipped[..], b"garbage");
        assert_eq!(&buf[..], &MAGIC[..3]);
    }

    #[test]
    fn test_truncated_input_is_incomplete() {
        let whole = framed(&["one"]);

        for len in [0, 1, HEADER_LEN - 1, HEADER_LEN, whole.len() - 1] {
            let mut buf = bytes::BytesMut::from(&whole[..len]);
            assert!(
                matches!(unpack::<broker::Label>(&mut buf), Ok(Frame::Incomplete)),
                "expected {len} bytes to be incomplete",
            );
            assert_eq!(buf.len(), len, "an incomplete frame consumes nothing");
        }
    }

    #[test]
    fn test_a_framed_payload_which_does_not_decode_is_an_error() {
        let mut buf = bytes::BytesMut::new();
        buf.extend_from_slice(&MAGIC);
        buf.extend_from_slice(&2u32.to_le_bytes());
        buf.extend_from_slice(&[0xff, 0xff]); // Field zero is invalid.

        assert!(unpack::<broker::Label>(&mut buf).is_err());
    }
}
