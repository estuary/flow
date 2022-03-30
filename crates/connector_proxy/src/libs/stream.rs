use crate::apis::InterceptorStream;
use crate::libs::airbyte_catalog::Message;

use crate::errors::raise_custom_error;
use async_stream::try_stream;
use bytes::{Buf, Bytes, BytesMut};
use futures::stream;
use futures_core::TryStream;
use futures_util::{StreamExt, TryStreamExt};
use serde_json::{Deserializer, Value};
use tokio::io::{AsyncRead, AsyncReadExt};
use validator::Validate;

pub fn stream_all_bytes<R: 'static + AsyncRead + std::marker::Unpin>(
    mut reader: R,
) -> impl TryStream<Item = std::io::Result<Bytes>> {
    stream::try_unfold(reader, |mut r| async {
        // consistent with the default capacity of ReaderStream.
        // https://github.com/tokio-rs/tokio/blob/master/tokio-util/src/io/reader_stream.rs#L8
        let mut buf = BytesMut::with_capacity(4096);
        match r.read_buf(&mut buf).await {
            Ok(0) => Ok(None),
            Ok(_) => Ok(Some((Bytes::from(buf), r))),
            Err(e) => {
                Err(raise_custom_error(&format!("error during streaming {:?}.", e)).unwrap_err())
            }
        }
    })
}

pub fn stream_all_airbyte_messages(
    mut in_stream: InterceptorStream,
) -> impl TryStream<Item = std::io::Result<Message>> {
    stream::try_unfold(in_stream, |mut istream| async {
        let mut buf = BytesMut::new();

        let items = istream.flat_map(|bytes| {
            match bytes {
                Ok(b) => {
                    buf.extend_from_slice(b.chunk());
                }
                Err(e) => {
                    raise_custom_error(&format!("error in reading next in_stream: {:?}", e))?;
                }
            }

            let chunk = buf.chunk();
            let deserializer = Deserializer::from_slice(&chunk);

            // Deserialize to Value first, instead of Message, to avoid missing 'is_eof' signals in error.
            let mut value_stream = deserializer.into_iter::<Value>();
            let values = value_stream
                .map(|value| match value {
                    Ok(v) => {
                        let message: Message = serde_json::from_value(v).unwrap();
                        if let Err(e) = message.validate() {
                            raise_custom_error(&format!(
                                "error in validating message: {:?}, {:?}",
                                e,
                                std::str::from_utf8(&chunk[value_stream.byte_offset()..])
                            ))?;
                        };
                        tracing::debug!("read message:: {:?}", &message);
                        return Ok(Some(message));
                    }
                    Err(e) => {
                        if e.is_eof() {
                            return Ok(None);
                        }

                        Err(raise_custom_error(&format!(
                            "error in decoding message: {:?}, {:?}",
                            e,
                            std::str::from_utf8(&chunk[value_stream.byte_offset()..])
                        ))
                        .unwrap_err())
                    }
                })
                .try_fold(Vec::new(), |vec, item| match item {
                    Ok(None) => Ok(vec),
                    Ok(Some(v)) => {
                        vec.push(v);
                        Ok(vec)
                    }
                    Err(e) => Err(e),
                });

            let byte_offset = value_stream.byte_offset();
            drop(buf.split_to(byte_offset));

            values
        });

        if buf.len() > 0 {
            raise_custom_error("unconsumed content in stream found.")?;
        }

        tracing::info!("done reading all in_stream.");

        Ok(Some((items, istream)))
    })
}
