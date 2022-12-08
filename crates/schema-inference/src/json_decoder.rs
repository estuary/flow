/// Largely copied from https://github.com/mxinden/asynchronous-codec/blob/master/src/codec/json.rs
use std::marker::PhantomData;

use bytes::{Buf, BytesMut};
use bytesize::ByteSize;
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
    buffer_target_capacity: usize,
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
            buffer_target_capacity: 1_000_000,
            _dec: PhantomData,
        }
    }

    pub fn bytes_read(&self) -> usize {
        self.bytes
    }

    fn increase_buffer_target_capacity(&mut self) {
        // We don't want this number to grow unbounded,
        // otherwise we could end up reserving tons of memory.
        // A single Flow document is limited to fitting within the max message size,
        // which is 16 MiB. Since we're parsing messages here (which contain documents),
        // a reasonable max buffer size is at least one message's worth, or 16 MiB.
        self.buffer_target_capacity = std::cmp::min(16_777_216, self.buffer_target_capacity * 2);
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
                    self.increase_buffer_target_capacity();
                }

                // Try and make sure the buffer has self.buffer_target_capacity of capacity.
                let bytes_addl = match buf.capacity().cmp(&self.buffer_target_capacity) {
                    // Buffer capacity is below target capacity, we need to grow by the difference
                    std::cmp::Ordering::Less => self.buffer_target_capacity - buf.capacity(),
                    // Buffer capacity is already at or above the target capacity.
                    // This likely happened because we recently successfully parsed a document
                    // greater than buffer_target_capacity in size, resulting in a large amount of
                    // space freeing up in the buffer. In this case, we have no need to grow the buffer's capacity
                    // and can safely just return Ok(None) to indicate that we need more data before we can try parsing again
                    std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => 0,
                };

                if bytes_addl > 0 {
                    tracing::trace!(
                        bytes_addl = display(ByteSize(bytes_addl.try_into().unwrap())),
                        bytes_read = display(ByteSize(self.bytes.try_into().unwrap())),
                        buf_capacity_remaining =
                            display(ByteSize(buf.capacity().try_into().unwrap())),
                        buf_grow_rate =
                            display(ByteSize(self.buffer_target_capacity.try_into().unwrap())),
                        "Partial read, reserving additional bytes"
                    );
                    buf.reserve(bytes_addl);
                }
                Ok(None)
            }
        }
    }
}
