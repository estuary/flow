use super::Error;
use bytes::{Bytes, BytesMut};
use std::iter::FromIterator;

/// RecordBatch is a Bytes which holds a batch of zero or more "application/json-seq"
/// media-type encodings. Each non-empty RecordBatch has a first byte 0x1E (ASCII
/// record separator), and a last byte 0x0A ('\n') as per RFC 7464.
#[derive(Clone, Debug)]
pub struct RecordBatch(bytes::Bytes);

impl RecordBatch {
    pub fn new(b: Bytes) -> RecordBatch {
        RecordBatch(b)
    }
    pub fn to_bytes(self) -> Bytes {
        self.0
    }
}

impl From<RecordBatch> for Bytes {
    fn from(rb: RecordBatch) -> Self {
        rb.0
    }
}

pub fn parse_record_batch(
    rem: &mut Bytes,
    data_or_eof: Option<Bytes>,
) -> Result<Option<RecordBatch>, Error> {
    let mut data = match data_or_eof {
        None if !rem.is_empty() => return Err(Error::InvalidJsonSeq),
        None => return Ok(None),
        Some(data) if rem.is_empty() => data,
        Some(data) => BytesMut::from_iter(rem.iter().copied().chain(data.into_iter())).freeze(),
    };

    // Select |pivot| as one-past the last newline of |data|. Usually,
    // |pivot| will equal chunk.len().
    let pivot = data.iter().rev().position(|v| *v == b'\x0A');

    match pivot {
        Some(pivot) => {
            // Return through |pivot|, and keep the tail as remainder.
            *rem = data.split_off(data.len() - pivot);
            Ok(Some(RecordBatch(data)))
        }
        None => {
            // No records in |data|. Keep all as remainder.
            *rem = data;
            Ok(None)
        }
    }
}
