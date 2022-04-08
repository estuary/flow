use crate::libs::airbyte_catalog::Message;
use crate::{apis::InterceptorStream, errors::create_custom_error};

use crate::errors::raise_err;
use bytes::{Buf, Bytes, BytesMut};
use futures::{stream, StreamExt, TryStream, TryStreamExt};
use serde_json::{Deserializer, Value};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::io::StreamReader;
use validator::Validate;

use super::protobuf::decode_message;

pub fn stream_all_bytes<R: 'static + AsyncRead + std::marker::Unpin>(
    reader: R,
) -> impl TryStream<Item = std::io::Result<Bytes>, Error = std::io::Error, Ok = bytes::Bytes> {
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

/// Given a stream of bytes, try to deserialize them into Airbyte Messages.
/// This can be used when reading responses from the Airbyte connector, and will
/// handle validation of messages as well as handling of AirbyteLogMessages.
/// Will ignore* messages that cannot be parsed to an AirbyteMessage.
/// * See https://docs.airbyte.com/understanding-airbyte/airbyte-specification#the-airbyte-protocol
pub fn stream_airbyte_responses(
    in_stream: InterceptorStream,
) -> impl TryStream<Item = std::io::Result<Message>, Ok = Message, Error = std::io::Error> {
    stream::once(async {
        let mut buf = BytesMut::new();
        let items = in_stream
            .map(move |bytes| {
                let b = bytes?;
                buf.extend_from_slice(b.chunk());
                let chunk = buf.chunk();

                // Deserialize to Value first, instead of Message, to avoid missing 'is_eof' signals in error.
                let deserializer = Deserializer::from_slice(chunk);
                let mut value_stream = deserializer.into_iter::<Value>();

                // Turn Values into Messages and validate them
                let values: Vec<Result<Message, std::io::Error>> = value_stream
                    .by_ref()
                    .map_while(|value| match value {
                        Ok(v) => Some(Ok(v)),
                        Err(e) => {
                            // we must stop as soon as we hit EOF to avoid
                            // progressing value_stream.byte_offset() so that we can
                            // safely drop the buffer up to byte_offset() and pick up the leftovers
                            // when working with the next bytes
                            if e.is_eof() {
                                return None;
                            }

                            Some(raise_err(&format!(
                                "error in decoding JSON: {:?}, {:?}",
                                e,
                                std::str::from_utf8(chunk)
                            )))
                        }
                    })
                    .map(|value| match value {
                        Ok(v) => {
                            let message: Message = match serde_json::from_value(v) {
                                Ok(m) => m,
                                // We ignore JSONs that are not Airbyte Messages according
                                // to the specification:
                                // https://docs.airbyte.com/understanding-airbyte/airbyte-specification#the-airbyte-protocol
                                Err(_) => return Ok(None),
                            };

                            message.validate().map_err(|e| {
                                create_custom_error(&format!("error in validating message {:?}", e))
                            })?;

                            tracing::debug!("read message:: {:?}", &message);
                            Ok(Some(message))
                        }
                        Err(e) => Err(e),
                    })
                    // Flipping the Option and Result to filter out the None values
                    .filter_map(|value| match value {
                        Ok(Some(v)) => Some(Ok(v)),
                        Ok(None) => None,
                        Err(e) => Some(Err(e)),
                    })
                    .collect();

                let byte_offset = value_stream.byte_offset();
                drop(buf.split_to(byte_offset));

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

/// Read the given stream and try to find an Airbyte message that matches the predicate
/// ignoring* other message kinds. This can be used to work with Airbyte connector responses.
/// * See https://docs.airbyte.com/understanding-airbyte/airbyte-specification#the-airbyte-protocol
pub fn get_airbyte_response<F: 'static>(
    in_stream: InterceptorStream,
    predicate: F,
) -> impl futures::Future<Output = std::io::Result<Message>>
where
    F: Fn(&Message) -> bool,
{
    async move {
        let stream_head = Box::pin(stream_airbyte_responses(in_stream)).next().await;

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

#[cfg(test)]
mod test {
    use futures::future;

    use crate::libs::airbyte_catalog::{ConnectionStatus, MessageType, Status};

    use super::*;

    #[tokio::test]
    async fn test_stream_all_bytes() {
        let input = "{\"test\": \"hello\"}".as_bytes();
        let stream = stream::once(future::ready(Ok::<_, std::io::Error>(input)));
        let reader = StreamReader::new(stream);
        let mut all_bytes = Box::pin(stream_all_bytes(reader));

        let result = all_bytes.next().await.unwrap().unwrap();
        assert_eq!(result.chunk(), input);
    }

    #[tokio::test]
    async fn test_stream_airbyte_responses_eof_split_json() {
        let input_message = Message {
            message_type: MessageType::ConnectionStatus,
            log: None,
            state: None,
            record: None,
            spec: None,
            catalog: None,
            connection_status: Some(ConnectionStatus {
                status: Status::Succeeded,
                message: Some("test".to_string()),
            }),
        };
        let input = vec![
            Ok::<_, std::io::Error>(
                "{\"type\": \"CONNECTION_STATUS\", \"connectionStatus\": {".as_bytes(),
            ),
            Ok::<_, std::io::Error>("\"status\": \"SUCCEEDED\",\"message\":\"test\"}}".as_bytes()),
        ];
        let stream = stream::iter(input);
        let reader = StreamReader::new(stream);

        let byte_stream = Box::pin(stream_all_bytes(reader));
        let mut messages = Box::pin(stream_airbyte_responses(byte_stream));

        let result = messages.next().await.unwrap().unwrap();
        assert_eq!(
            result.connection_status.unwrap(),
            input_message.connection_status.unwrap()
        );
    }

    #[tokio::test]
    async fn test_stream_airbyte_responses_eof_split_json_partial() {
        let input_message = Message {
            message_type: MessageType::ConnectionStatus,
            log: None,
            state: None,
            record: None,
            spec: None,
            catalog: None,
            connection_status: Some(ConnectionStatus {
                status: Status::Succeeded,
                message: Some("test".to_string()),
            }),
        };
        let input = vec![
            Ok::<_, std::io::Error>(
                "{}\n{\"type\": \"CONNECTION_STATUS\", \"connectionStatus\": {".as_bytes(),
            ),
            Ok::<_, std::io::Error>("\"status\": \"SUCCEEDED\",\"message\":\"test\"}}".as_bytes()),
        ];
        let stream = stream::iter(input);
        let reader = StreamReader::new(stream);

        let byte_stream = Box::pin(stream_all_bytes(reader));
        let mut messages = Box::pin(stream_airbyte_responses(byte_stream));

        let result = messages.next().await.unwrap().unwrap();
        assert_eq!(
            result.connection_status.unwrap(),
            input_message.connection_status.unwrap()
        );
    }
}
