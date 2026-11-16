//! Gazette protobuf framing: a magic word, a little-endian u32 payload length,
//! and the payload. The magic word allows recovery from arbitrary read offsets.
//!
//! A frame's declared payload length is unbounded. [`unpack`] splits frames from
//! `bytes::BytesMut` before decoding, so message bytes can reference the frame
//! without copying. Callers track journal offsets and decide whether to tolerate
//! desynchronization.

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
    /// A frame follows, whose `payload` bytes may not be read yet.
    Frame { payload: usize },
    /// Off a frame boundary: the first `skipped` bytes belong to no frame.
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
    /// Off a frame boundary: `skipped`, split off the buffer, belong to no frame.
    Desync { skipped: bytes::Bytes },
    /// A partial frame, left in the buffer for the caller to extend and unpack again.
    Incomplete,
}

/// Unpack the frame at the head of `buf`, splitting it off.
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

    /// `names`, each framed as a label.
    fn framed(names: &[&str]) -> Vec<u8> {
        let mut buf = bytes::BytesMut::new();
        for name in names {
            let label = broker::Label {
                name: name.to_string(),
                value: "a value".to_string(),
                prefix: false,
            };
            encode(&label, &mut buf);
        }
        buf.to_vec()
    }

    #[test]
    fn test_encode() {
        // The magic word, then the payload's length in little-endian. The label's
        // payload is 14 bytes.
        let frame = framed(&["one"]);
        assert_eq!(frame[..HEADER_LEN], [0x66, 0x33, 0x93, 0x36, 14, 0, 0, 0]);
        assert_eq!(frame.len(), HEADER_LEN + 14);
    }

    #[test]
    fn test_unpack() {
        let one = framed(&["one"]);
        let after = framed(&["after"]);

        // (name, reads, what unpacking yields after each read until the buffer is empty
        // or incomplete, or a frame fails to decode)
        let cases = [
            ("a frame", vec![one.clone()], vec!["one"]),
            (
                "a stream",
                vec![framed(&["one", "two", "three"])],
                vec!["one", "two", "three"],
            ),
            // The next magic word ends a desync.
            (
                "garbage first",
                vec![[b"leading garbage".as_slice(), &after].concat()],
                vec!["desync: leading garbage", "after"],
            ),
            // Where no magic word follows, a desync keeps the last three bytes, which may
            // begin one that the next read completes.
            (
                "magic across reads",
                vec![
                    [b"garbage".as_slice(), &after[..3]].concat(),
                    after[3..].to_vec(),
                ],
                vec!["desync: garbage", "incomplete", "after"],
            ),
            (
                "partial header",
                vec![one[..HEADER_LEN - 1].to_vec()],
                vec!["incomplete"],
            ),
            (
                "partial payload",
                vec![one[..one.len() - 1].to_vec()],
                vec!["incomplete"],
            ),
            // A declared length is unbounded: a frame is incomplete until all of it is
            // read.
            (
                "huge payload",
                vec![[MAGIC.as_slice(), &u32::MAX.to_le_bytes()].concat()],
                vec!["incomplete"],
            ),
            // A frame which doesn't decode is an error rather than another desync, and
            // is split off all the same.
            (
                "undecodable",
                // Field zero is invalid.
                vec![[MAGIC.as_slice(), &2u32.to_le_bytes(), &[0xff, 0xff]].concat()],
                vec!["error"],
            ),
        ];

        for (name, reads, expect) in cases {
            let mut buf = bytes::BytesMut::new();
            let mut outcomes = Vec::new();

            for read in reads {
                buf.extend_from_slice(&read);

                while !buf.is_empty() {
                    let len = buf.len();

                    match unpack::<broker::Label>(&mut buf) {
                        Ok(Frame::Record { message, framed }) => {
                            assert_eq!(
                                header(&framed),
                                Header::Frame {
                                    payload: framed.len() - HEADER_LEN
                                },
                                "{name}: a record carries its whole frame"
                            );
                            outcomes.push(message.name);
                        }
                        Ok(Frame::Desync { skipped }) => {
                            outcomes.push(format!("desync: {}", String::from_utf8_lossy(&skipped)))
                        }
                        Ok(Frame::Incomplete) => {
                            assert_eq!(
                                buf.len(),
                                len,
                                "{name}: an incomplete frame consumes nothing"
                            );
                            outcomes.push("incomplete".to_string());
                            break;
                        }
                        Err(_) => {
                            outcomes.push("error".to_string());
                            break;
                        }
                    }
                }
            }
            assert_eq!(outcomes, expect, "{name}");

            let incomplete = outcomes.last().is_some_and(|last| last == "incomplete");
            assert_eq!(buf.is_empty(), !incomplete, "{name}: bytes left");
        }
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
}
