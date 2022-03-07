use std::io::Read;

use crate::apis::InterceptorStream;
use crate::libs::airbyte_catalog::Message;

use async_stream::stream;
use bytes::{Buf, Bytes, BytesMut};
use futures_core::Stream;
use futures_util::StreamExt;
use serde_json::{Deserializer, Value};
use tokio::io::{AsyncRead, AsyncReadExt};

pub fn stream_all_bytes<R: 'static + AsyncRead + std::marker::Unpin>(
    mut reader: R,
) -> impl Stream<Item = std::io::Result<Bytes>> {
    stream! {
        loop {
            // consistent with the default capacity of ReaderStream.
            // https://github.com/tokio-rs/tokio/blob/master/tokio-util/src/io/reader_stream.rs#L8
            let mut buf = BytesMut::with_capacity(4096);
            match reader.read_buf(&mut buf).await {
                Ok(0) => break,
                Ok(_) => {
                    yield Ok(buf.into());
                }
                Err(e) => {
                    panic!("error during streaming {:?}.", e);
                }
            }
        }
    }
}

pub fn stream_all_airbyte_messages(
    mut in_stream: InterceptorStream,
) -> impl Stream<Item = std::io::Result<Message>> {
    stream! {
        let mut buf = BytesMut::new();
        while let Some(bytes) = in_stream.next().await {
            match bytes {
                Ok(b) => {
                    buf.extend_from_slice(b.chunk());
                }
                Err(e) => {
                    panic!("error during streaming {:?}.", e);
                }
            }
            let buf_split = buf.split();
            let chunk = buf_split.chunk();

            let deserializer = Deserializer::from_slice(&chunk);
            let mut message_stream = deserializer.into_iter::<Message>();
            while let Some(message) = message_stream.next() {
                match message {
                    Ok(m) => yield Ok(m),
                    Err(e) => {
                        panic!("error during deserializing airbyte json message {:?}", e);
                    }
                }
            }

            // TODO(Jixiang): Improve efficiency here.
            // There are unnecessary copying activities in and our from the buf, especially for large messages that spans multiple
            // bytes messages in the stream. Ideally, we could both write and read from the same buf. However, both reading and writing
            // from the same buf is not recommended, which yields warning of https://github.com/rust-lang/rust/issues/59159.
            let remaining = &chunk[message_stream.byte_offset()..];
            buf.extend_from_slice(remaining);
        }
    }
}
