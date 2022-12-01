/// Largely copied from https://github.com/mxinden/asynchronous-codec/blob/master/src/codec/json.rs
use std::marker::PhantomData;

use bytes::{Buf, BytesMut};
use serde::Deserialize;
use serde_json::Value;
use tokio_util::codec::Decoder;

#[derive(Default)]
pub struct JsonCodec<Dec = Value> {
    /// Number of bytes this decoder has successfully decoded into documents
    bytes: usize,
    /// Rate at which we grow the buffer. Optimally, this will be the smallest
    /// value that keps the buffer from getting emptied before we get a partial read
    /// and grow it again.
    buffer_grow_rate: usize,
    _dec: PhantomData<Dec>,
}

impl<Dec> JsonCodec<Dec>
where
    for<'de> Dec: Deserialize<'de> + 'static,
{
    /// Creates a new `JsonCodec` with the associated types
    pub fn new() -> JsonCodec<Dec> {
        JsonCodec {
            bytes: 0,
            buffer_grow_rate: 1_000_000,
            _dec: PhantomData,
        }
    }

    pub fn bytes_read(&self) -> usize {
        self.bytes
    }

    fn increase_buffer_grow_rate(&mut self) {
        // We don't want this number to grow unbounded,
        // otherwise we could end up reserving tons of memory
        self.buffer_grow_rate = std::cmp::min(128_000_000, self.buffer_grow_rate * 2);
    }
}

impl<Dec> Clone for JsonCodec<Dec>
where
    for<'de> Dec: Deserialize<'de> + 'static,
{
    /// Clone creates a new instance of the `JsonCodec`
    fn clone(&self) -> JsonCodec<Dec> {
        JsonCodec::new()
    }
}

/// JSON Codec error enumeration
#[derive(Debug, thiserror::Error)]
pub enum JsonCodecError {
    #[error("IO error: {:?}", .0)]
    Io(#[from] std::io::Error),
    #[error("JSON error: {:?}", .0)]
    Json(#[from] serde_json::Error),
}

/// Decoder impl parses json objects from bytes
impl<Dec> Decoder for JsonCodec<Dec>
where
    for<'de> Dec: Deserialize<'de> + 'static,
{
    type Item = Dec;
    type Error = JsonCodecError;

    #[tracing::instrument(skip_all)]
    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Build streaming JSON iterator over data
        let de = serde_json::Deserializer::from_slice(&buf);
        let mut iter = de.into_iter::<Dec>();

        // Attempt to fetch an item and generate response
        match iter.next() {
            // We successfully decoded something
            // Let's move up the left-hand-side of the buffer to the end of the parsed document
            // and return that document, then come back around for another iteration
            Some(Ok(v)) => {
                // How many bytes to "throw away", since they represented the document we just parsed
                let offset = iter.byte_offset();
                self.bytes += offset;
                buf.advance(offset);
                // tracing::trace!(bytes_advance = offset, "Successfully read document");

                Ok(Some(v))
            }
            // We errored while parsing a document
            Some(Err(e)) if !e.is_eof() => {
                tracing::trace!("Error reading document: {e}");
                return Err(e.into());
            }
            // The buffer is empty or entirely whitespace (None)
            None => {
                assert!(
                    buf.iter().all(u8::is_ascii_whitespace),
                    "Got None from streaming JSON deserializer, but buffer contained non-whitespace characters!"
                );
                tracing::trace!(
                    whitespace_bytes = buf.len(),
                    "Consuming irrelevant whitespace"
                );

                // Now that we know that the buffer contains nothing or only whitespace
                // We need to actually consume that whitespace, otherwise `decode_eof` will
                // complain that we returned Ok(None) with bytes still in the buffer
                buf.advance(buf.len());
                Ok(None)
            }
            // It only contains a partial document (premature EOF). In this case,
            // let's indicate to the Framed instance that it needs to read some more bytes before calling this method again.
            Some(Err(_)) => {
                // Increase the buffer grow rate until we settle on a value that leaves us with some room in the buffer.
                // So long as the buffer's capacity never reaches zero, we should be continuously streaming data from the network.
                // note: it appears that BytesMut doesn't ever like to have actually 0 capacity, hence the lower bound of 1kb
                if buf.capacity() < 1_000 {
                    self.increase_buffer_grow_rate();
                }

                // Try and make sure the buffer has self.buffer_grow_rate of capacity.
                let bytes_addl = std::cmp::min(
                    std::cmp::max(0, self.buffer_grow_rate as isize - buf.capacity() as isize),
                    self.buffer_grow_rate as isize,
                ) as usize;

                tracing::trace!(
                    bytes_addl = format!("{:.1} MB", bytes_addl as f32 / 1_000_000f32),
                    bytes_read = format!("{:.1} MB", self.bytes as f32 / 1_000_000f32),
                    buf_capacity_remaining =
                        format!("{:.1} MB", buf.capacity() as f32 / 1_000_000f32),
                    buf_grow_rate =
                        format!("{:.1} MB", self.buffer_grow_rate as f32 / 1_000_000f32),
                    "Partial read, reserving additional bytes"
                );
                buf.reserve(bytes_addl);
                Ok(None)
            }
        }
    }
}
