use crate::apis::InterceptorStream;
use crate::libs::airbyte_catalog::Message;

use crate::errors::raise_custom_error;
use bytes::{Buf, Bytes, BytesMut};
use futures::{stream, StreamExt, TryStream, TryStreamExt};
use serde_json::{Deserializer, Value};
use tokio::io::{AsyncRead, AsyncReadExt};
use validator::Validate;

pub fn stream_all_bytes<R: 'static + AsyncRead + std::marker::Unpin>(
    reader: R,
) -> impl TryStream<Item = std::io::Result<Bytes>> {
    stream::try_unfold(reader, |mut r| async {
        // consistent with the default capacity of ReaderStream.
        // https://github.com/tokio-rs/tokio/blob/master/tokio-util/src/io/reader_stream.rs#L8
        let mut buf = BytesMut::with_capacity(4096);
        match r.read_buf(&mut buf).await {
            Ok(0) => Ok(None),
            Ok(_) => Ok(Some((Bytes::from(buf), r))),
            Err(e) => raise_custom_error(&format!("error during streaming {:?}.", e)),
        }
    })
}

pub fn stream_all_airbyte_messages(
    in_stream: InterceptorStream,
) -> impl TryStream<Item = std::io::Result<Message>> {
    stream::once(async {
        let mut buf = BytesMut::new();
        let items = in_stream
            .map(move |bytes| {
                // Can someone explain to me why do we need this buf, instead of just using `chunk = b.chunk()`?
                let b = bytes?;
                buf.extend_from_slice(b.chunk());
                let chunk = buf.chunk();
                let deserializer = Deserializer::from_slice(chunk);

                // Deserialize to Value first, instead of Message, to avoid missing 'is_eof' signals in error.
                let value_stream = deserializer.into_iter::<Value>();
                //let values = value_stream.try_fold(Vec::new(), |vec, value| match value {
                let values: Vec<Result<Message, std::io::Error>> = value_stream
                    .map(|value| match value {
                        Ok(v) => {
                            let message: Message = serde_json::from_value(v).unwrap();
                            if let Err(e) = message.validate() {
                                raise_custom_error(&format!(
                                    "error in validating message: {:?}",
                                    e
                                ))?;
                            }
                            tracing::debug!("read message:: {:?}", &message);
                            Ok(Some(message))
                        }
                        Err(e) => {
                            if e.is_eof() {
                                return Ok(None);
                            }

                            raise_custom_error(&format!(
                                "error in decoding message: {:?}, {:?}",
                                e,
                                std::str::from_utf8(chunk)
                            ))
                        }
                    })
                    .filter_map(|value| match value {
                        Ok(Some(v)) => Some(Ok(v)),
                        Ok(None) => None,
                        Err(e) => Some(Err(e)),
                    })
                    .collect();

                // Stream<Result<Message>>
                Ok::<_, std::io::Error>(stream::iter(values))
            })
            // Stream<Result<Stream<Result<Message>>>
            .try_flatten();

        tracing::info!("done reading all in_stream.");

        // We need to set explicit error type, see https://github.com/rust-lang/rust/issues/63502
        Ok::<_, std::io::Error>(items)
    })
    .try_flatten()
}
