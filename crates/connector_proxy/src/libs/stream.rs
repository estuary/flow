use async_stream::stream;
use bytes::{Bytes, BytesMut};
use futures_core::Stream;
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
