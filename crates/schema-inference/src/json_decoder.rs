/// Largely copied from https://github.com/mxinden/asynchronous-codec/blob/master/src/codec/json.rs

use std::marker::PhantomData;

use bytes::{BytesMut, Buf};
use serde::Deserialize;
use serde_json::Value;
use tokio_util::codec::Decoder;

#[derive(Default)]
pub struct JsonCodec<Dec = Value> {
    dec: PhantomData<Dec>
}

impl<Dec> JsonCodec<Dec>
where
    for<'de> Dec: Deserialize<'de> + 'static,
{
    /// Creates a new `JsonCodec` with the associated types
    pub fn new() -> JsonCodec<Dec> {
        JsonCodec {
            dec: PhantomData,
        }
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
        let res = match iter.next() {
            Some(Ok(v)) => Ok(Some(v)),
            Some(Err(ref e)) if e.is_eof() => Ok(None),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        };

        // Update offset from iterator
        let offset = iter.byte_offset();

        // Advance buffer
        buf.advance(offset);

        res
    }
}