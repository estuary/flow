/// Largely copied from https://github.com/mxinden/asynchronous-codec/blob/master/src/codec/json.rs
use std::marker::PhantomData;

use bytes::{Buf, BytesMut};
use serde::Deserialize;
use serde_json::Value;
use tokio_util::codec::Decoder;

#[derive(Default)]
pub struct JsonCodec<Dec = Value> {
    _dec: PhantomData<Dec>,
}

impl<Dec> JsonCodec<Dec>
where
    for<'de> Dec: Deserialize<'de> + 'static,
{
    /// Creates a new `JsonCodec` with the associated types
    pub fn new() -> JsonCodec<Dec> {
        JsonCodec { _dec: PhantomData }
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
    #[error("IO error")]
    Io(#[from] std::io::Error),
    #[error("JSON error")]
    Json(#[from] serde_json::Error),
}

/// Decoder impl parses json objects from bytes
impl<Dec> Decoder for JsonCodec<Dec>
where
    for<'de> Dec: Deserialize<'de> + 'static,
{
    type Item = Dec;
    type Error = JsonCodecError;

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
                buf.advance(offset);

                Ok(Some(v))
            }
            // We errored while parsing a document
            Some(Err(e)) if !e.is_eof() => return Err(e.into()),
            // Either the buffer is empty or entirely whitespace (None),
            // or it only contains a partial document (premature EOF). Either way,
            // let's indicate to the Framed instance that it needs to read some more bytes before calling this method again.
            None | Some(Err(_)) => {
                // Theoretically we could grow the amount of additional bytes we ask for
                // each time we fail to deserialize a record binary-search style
                // but 1mb feels like a reasonable upper bound, and also not an unreasonable size for a buffer to grow by
                // so let's go with this for now
                buf.reserve(1_000_000);
                Ok(None)
            }
        }
    }
}
