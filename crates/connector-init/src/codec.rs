use anyhow::Context;
use tokio::io::{AsyncRead, AsyncReadExt};

#[derive(Debug, Copy, Clone)]
pub enum Codec {
    Proto,
    Json,
}

impl Codec {
    pub fn encode<M>(self, m: &M, buf: &mut Vec<u8>)
    where
        M: prost::Message + serde::Serialize,
    {
        match self {
            Self::Proto => {
                // The protobuf encoding is prefixed with a fix four-byte little endian length header.
                let length = m.encoded_len();
                buf.reserve(4 + length);
                buf.extend_from_slice(&(length as u32).to_le_bytes());
                m.encode(buf).expect("buf has pre-allocated capacity");
            }
            Self::Json => {
                // Encode as newline-delimited JSON.
                serde_json::to_writer(&mut *buf, m).unwrap();
                buf.push(b'\n');
            }
        }
    }

    // Decode all complete messages contained within `buf`.
    // Returns the number of bytes consumed, and the decoded messages.
    // The unconsumed remainder is either empty, or contains a partial message
    // which has not yet been fully read.
    pub fn decode<M>(self, buffer: &mut Vec<u8>) -> anyhow::Result<Vec<M>>
    where
        M: prost::Message + for<'de> serde::Deserialize<'de> + Default,
    {
        let mut buf = buffer.as_slice();
        let mut consumed = 0;
        let mut out = Vec::new();

        match self {
            Self::Proto => loop {
                if buf.len() < 4 {
                    break;
                }
                let bound = 4 + u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;

                if buf.len() < bound {
                    break;
                }

                out.push(M::decode(&buf[4..bound]).context("decoding protobuf message")?);
                consumed += bound;

                buf = &buf[bound..];
            },
            Self::Json => loop {
                let Some(bound) = buf.iter().position(|b| *b == b'\n') else {
                    break;
                };
                let bound = bound + 1; // Byte index after '\n'.

                out.push(serde_json::from_slice::<M>(&buf[..bound]).with_context(|| {
                    format!(
                        "could not parse {:?} into JSON response",
                        String::from_utf8_lossy(&buf[..bound])
                    )
                })?);
                consumed += bound;

                buf = &buf[bound..];
            },
        }

        // Remove consumed portion of `buffer`.
        let len = buffer.len();
        if consumed != 0 && consumed != len {
            buffer.copy_within(consumed..len, 0); // Shift remainder to front.
        }
        buffer.truncate(len - consumed);

        Ok(out)
    }
}

// Maps an AsyncRead into a Stream of decoded messages.
pub fn reader_to_message_stream<M, R>(
    codec: Codec,
    reader: R,
    min_capacity: usize,
) -> impl futures::Stream<Item = anyhow::Result<M>>
where
    M: prost::Message + for<'de> serde::Deserialize<'de> + Default,
    R: AsyncRead + Unpin,
{
    let buffer = Vec::with_capacity(min_capacity);

    futures::stream::try_unfold(
        (Vec::new().into_iter(), buffer, reader),
        move |(mut it, mut buffer, mut reader)| async move {
            loop {
                if let Some(next) = it.next() {
                    return Ok(Some((next, (it, buffer, reader))));
                }

                // Read next chunk of bytes into unused `buffer`, first growing
                // if needed. We don't bound the maximum size because connector-init
                // runs inside of the container context and can consume only its
                // allotted memory.
                if buffer.len() == buffer.capacity() {
                    buffer.reserve(1); // This uses quadratic resize.
                }
                let n = reader.read_buf(&mut buffer).await?;

                if n == 0 && buffer.len() == 0 {
                    tracing::debug!("finished reading connector output");
                    return Ok(None); // Graceful EOF.
                } else if n == 0 {
                    anyhow::bail!("connector wrote a partial message and then closed its output");
                }

                let decoded = codec.decode::<M>(&mut buffer)?;
                it = decoded.into_iter();
            }
        },
    )
}

#[cfg(test)]
mod test {
    use super::{Codec, reader_to_message_stream};
    use futures::{StreamExt, TryStreamExt};
    use proto_flow::flow::TestSpec;

    #[test]
    fn test_generic_encode_and_decode() {
        for codec in [Codec::Proto, Codec::Json] {
            let mut buf = Vec::new();
            codec.encode(
                &TestSpec {
                    name: "hello world".to_string(),
                    ..Default::default()
                },
                &mut buf,
            );

            let mut r = buf.repeat(2);

            // Expect we decode our fixture.
            assert_eq!(
                codec.decode::<TestSpec>(&mut r).unwrap(),
                vec![
                    TestSpec {
                        name: "hello world".to_string(),
                        ..Default::default()
                    },
                    TestSpec {
                        name: "hello world".to_string(),
                        ..Default::default()
                    }
                ]
            );

            // Expect the input is fully consumed.
            assert!(r.is_empty());
            assert_eq!(codec.decode::<TestSpec>(&mut r).unwrap(), vec![]);

            let mut r = buf.repeat(3);
            r.pop(); // Drop final byte.

            assert_eq!(codec.decode::<TestSpec>(&mut r).unwrap().len(), 2);

            // Remainder was shifted down to Vector start.
            assert_eq!(r.as_slice(), &buf[0..buf.len() - 1]);
            assert_eq!(codec.decode::<TestSpec>(&mut r).unwrap(), vec![]);
        }
    }

    #[tokio::test]
    async fn test_reader_to_stream() {
        for codec in [Codec::Proto, Codec::Json] {
            let mut buf = Vec::new();
            codec.encode(
                &TestSpec {
                    name: "hello world".to_string(),
                    ..Default::default()
                },
                &mut buf,
            );

            // We can collect multiple encoded items as a stream, and then read a clean EOF.
            let ten = buf.repeat(10);
            let stream = reader_to_message_stream(codec, ten.as_slice(), 16);
            let ten: Vec<TestSpec> = stream.try_collect().await.unwrap();
            assert_eq!(ten.len(), 10);

            // If the stream bytes have extra content, we read a message and then an unexpected EOF.
            buf.extend_from_slice(&[0xaa, 0xbb, 0xcc, 0xdd, 0xee]);
            let stream = reader_to_message_stream::<TestSpec, _>(codec, buf.as_slice(), 16);
            tokio::pin!(stream);

            let output = stream.next().await;
            insta::allow_duplicates! {
            insta::assert_debug_snapshot!(output, @r###"
            Some(
                Ok(
                    TestSpec {
                        name: "hello world",
                        steps: [],
                    },
                ),
            )
            "###);
            }
            let output = stream.next().await;
            insta::allow_duplicates! {
            insta::assert_debug_snapshot!(output, @r###"
            Some(
                Err(
                    "connector wrote a partial message and then closed its output",
                ),
            )
            "###);
            }
        }
    }
}
