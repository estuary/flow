//! Gazette's fixed framing of protobuf records. A frame is a magic word, a
//! little-endian u32 payload length, and the payload.
//!
//! The magic word lets a reader which starts mid-frame recover. A reader may begin
//! at an arbitrary journal offset. Without a recognizable boundary it would
//! interpret whatever bytes it landed on as a record.
//!
//! The reference implementation is `message/fixed_framing.go`.

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

/// Outcome of decoding the head of a byte stream.
#[derive(Debug)]
pub enum Frame<M> {
    /// `message` was framed by the first `consumed` bytes of the input.
    Record { message: M, consumed: usize },
    /// The input does not begin on a frame boundary. Its first `skipped` bytes
    /// belong to no frame this reader can interpret, so they are discarded. A caller
    /// which tracks a journal offset reports the skipped span against that offset.
    Desync { skipped: usize },
    /// The input holds a partial header or a partial frame, and nothing is consumed.
    /// The caller extends its input and decodes again.
    Incomplete,
}

/// Decode the frame at the head of `input`.
///
/// A payload which is framed but does not decode is an error, and not another
/// desync. A reader must not skip a record it cannot interpret.
pub fn decode<M: prost::Message + Default>(input: &[u8]) -> crate::Result<Frame<M>> {
    let Some(header) = input.get(..HEADER_LEN) else {
        return Ok(Frame::Incomplete);
    };
    if header[..MAGIC.len()] != MAGIC {
        return Ok(Frame::Desync {
            skipped: desync_span(input),
        });
    }
    let len = u32::from_le_bytes(header[MAGIC.len()..].try_into().unwrap()) as usize;

    let Some(payload) = input.get(HEADER_LEN..HEADER_LEN + len) else {
        return Ok(Frame::Incomplete);
    };

    Ok(Frame::Record {
        message: M::decode(payload)?,
        consumed: HEADER_LEN + len,
    })
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
    use super::{Frame, HEADER_LEN, MAGIC, decode, encode};
    use proto_gazette::broker;

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
        let buf = framed(&["one"]);

        assert_eq!(&buf[..MAGIC.len()], &MAGIC);
        assert_eq!(
            u32::from_le_bytes(buf[MAGIC.len()..HEADER_LEN].try_into().unwrap()) as usize,
            buf.len() - HEADER_LEN,
        );

        let Ok(Frame::Record { message, consumed }) = decode::<broker::Label>(&buf) else {
            panic!("expected a record");
        };
        assert_eq!(message, label("one"));
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn test_concatenated_stream() {
        let buf = framed(&["one", "two", "three"]);
        let mut rest = &buf[..];
        let mut decoded = Vec::new();

        while !rest.is_empty() {
            let Ok(Frame::Record { message, consumed }) = decode::<broker::Label>(rest) else {
                panic!("expected a record");
            };
            decoded.push(message.name);
            rest = &rest[consumed..];
        }
        assert_eq!(decoded, vec!["one", "two", "three"]);
    }

    #[test]
    fn test_desync_resynchronizes_on_the_next_frame() {
        let mut buf = bytes::BytesMut::from(&b"leading garbage"[..]);
        buf.extend_from_slice(&framed(&["after"]));

        let Ok(Frame::Desync { skipped }) = decode::<broker::Label>(&buf) else {
            panic!("expected a desync");
        };
        assert_eq!(skipped, b"leading garbage".len());

        let Ok(Frame::Record { message, .. }) = decode::<broker::Label>(&buf[skipped..]) else {
            panic!("expected a record");
        };
        assert_eq!(message, label("after"));
    }

    #[test]
    fn test_desync_without_a_following_frame_retains_a_partial_magic_word() {
        // The final three bytes could begin a magic word which continues in input
        // the caller has not read yet.
        let buf = [&b"garbage"[..], &MAGIC[..3]].concat();

        let Ok(Frame::Desync { skipped }) = decode::<broker::Label>(&buf) else {
            panic!("expected a desync");
        };
        assert_eq!(skipped, buf.len() - 3);
    }

    #[test]
    fn test_truncated_input_is_incomplete() {
        let buf = framed(&["one"]);

        for len in [0, 1, HEADER_LEN - 1, HEADER_LEN, buf.len() - 1] {
            assert!(
                matches!(decode::<broker::Label>(&buf[..len]), Ok(Frame::Incomplete)),
                "expected {len} bytes to be incomplete",
            );
        }
    }

    #[test]
    fn test_a_framed_payload_which_does_not_decode_is_an_error() {
        let mut buf = bytes::BytesMut::new();
        buf.extend_from_slice(&MAGIC);
        buf.extend_from_slice(&2u32.to_le_bytes());
        buf.extend_from_slice(&[0xff, 0xff]); // Field zero is invalid.

        assert!(decode::<broker::Label>(&buf).is_err());
    }
}
