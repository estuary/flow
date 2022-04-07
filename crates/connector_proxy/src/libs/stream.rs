use crate::apis::InterceptorStream;
use crate::libs::airbyte_catalog::Message;

use crate::errors::raise_custom_error;
use async_stream::try_stream;
use bytes::{Buf, Bytes, BytesMut};
use futures::{stream, Stream};
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
            Err(e) => raise_custom_error(&format!("error during streaming {:?}.", e)),
        }
    })
}

pub fn stream_all_airbyte_messages(
    mut in_stream: InterceptorStream,
) -> impl TryStream<Item = std::io::Result<Message>> {
    stream::once(async {
        let items = in_stream
            .map(|bytes| {
                let chunk = bytes?.clone.chunk();
                let deserializer = Deserializer::from_slice(chunk);

                // Deserialize to Value first, instead of Message, to avoid missing 'is_eof' signals in error.
                let value_stream = deserializer.into_iter::<Value>();
                //let values = value_stream.try_fold(Vec::new(), |vec, value| match value {
                let values = value_stream
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
                            //vec.push(message);
                            Ok(Some(message))
                        }
                        Err(e) => {
                            if e.is_eof() {
                                return Ok(None);
                            }

                            raise_custom_error(&format!("error in decoding message: {:?}", e))
                        }
                    })
                    .filter_map(|value| match value {
                        Ok(Some(v)) => Some(Ok(v)),
                        Ok(None) => None,
                        Err(e) => Some(Err(e)),
                    });

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
