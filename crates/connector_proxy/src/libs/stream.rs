use crate::libs::airbyte_catalog::Message;
use crate::{apis::InterceptorStream, errors::create_custom_error};

use crate::errors::raise_err;
use bytes::{Buf, Bytes, BytesMut};
use futures::{stream, StreamExt, TryFuture, TryStream, TryStreamExt};
use serde_json::{Deserializer, Value};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::io::StreamReader;
use validator::Validate;

use super::protobuf::decode_message;

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
            Err(e) => raise_err(&format!("error during streaming {:?}.", e)),
        }
    })
}

// Given a stream of bytes, try to deserialize them into Airbyte Messages, validate them
// and handling Log messages
pub fn stream_all_airbyte_messages(
    in_stream: InterceptorStream,
) -> impl TryStream<Item = std::io::Result<Message>, Ok = Message, Error = std::io::Error> {
    stream::once(async {
        let mut buf = BytesMut::new();
        let items = in_stream
            .map(move |bytes| {
                // TODO: Can someone explain to me why do we need this buf, instead of just using `chunk = b.chunk()`?
                let b = bytes?;
                buf.extend_from_slice(b.chunk());
                let chunk = buf.chunk();

                // Deserialize to Value first, instead of Message, to avoid missing 'is_eof' signals in error.
                let deserializer = Deserializer::from_slice(chunk);
                let value_stream = deserializer.into_iter::<Value>();

                // Turn Values into Messages and validate them
                let values: Vec<Result<Message, std::io::Error>> = value_stream
                    .map(|value| match value {
                        Ok(v) => {
                            let message: Message = serde_json::from_value(v).unwrap();
                            if let Err(e) = message.validate() {
                                raise_err(&format!("error in validating message: {:?}", e))?;
                            }
                            tracing::debug!("read message:: {:?}", &message);
                            Ok(Some(message))
                        }
                        Err(e) => {
                            if e.is_eof() {
                                return Ok(None);
                            }

                            raise_err(&format!(
                                "error in decoding message: {:?}, {:?}",
                                e,
                                std::str::from_utf8(chunk)
                            ))
                        }
                    })
                    // Flipping the Option and Result to filter out the None values
                    .filter_map(|value| match value {
                        Ok(Some(v)) => Some(Ok(v)),
                        Ok(None) => None,
                        Err(e) => Some(Err(e)),
                    })
                    .collect();

                Ok::<_, std::io::Error>(stream::iter(values))
            })
            .try_flatten();

        // We need to set explicit error type, see https://github.com/rust-lang/rust/issues/63502
        Ok::<_, std::io::Error>(items)
    })
    .try_flatten()
    // Handle logs here so we don't have to worry about them everywhere else
    .try_filter_map(|message| async {
        if let Some(log) = message.log {
            log.log();
            Ok(None)
        } else {
            Ok(Some(message))
        }
    })
}

/// Read the given stream and try to find a message that matches the predicate
/// This allows consumers to work with a single message, simplifying the code
pub fn get_airbyte_message<F: 'static>(
    in_stream: InterceptorStream,
    predicate: F,
) -> impl futures::Future<Output = std::io::Result<Message>>
where
    F: Fn(&Message) -> bool,
{
    async move {
        let stream_head = Box::pin(stream_all_airbyte_messages(in_stream))
            .next()
            .await;

        let message = match stream_head {
            Some(m) => m,
            None => return raise_err("Could not find message in stream"),
        }?;

        if predicate(&message) {
            Ok(message)
        } else {
            raise_err("Could not find message matching condition")
        }
    }
}

/// Read the given stream of bytes and try to decode it to type <T>
pub fn get_decoded_message<T>(
    in_stream: InterceptorStream,
) -> impl futures::Future<Output = std::io::Result<T>>
where
    T: prost::Message + std::default::Default,
{
    async move {
        let mut reader = StreamReader::new(in_stream);
        decode_message::<T, _>(&mut reader)
            .await?
            .ok_or(create_custom_error("missing request"))
    }
}
