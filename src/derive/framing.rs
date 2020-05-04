use super::Error;
use bytes::{Bytes, BytesMut};
use futures::{future, StreamExt, TryStream, TryStreamExt};
use std::iter::FromIterator;

/// RecordBatch is a Bytes which holds a batch of zero or more "application/json-seq"
/// media-type encodings. Each non-empty RecordBatch has a first byte 0x1E (ASCII
/// record separator), and a last byte 0x0A ('\n') as per RFC 7464.
pub struct RecordBatch(bytes::Bytes);

impl RecordBatch {
    fn bytes(&self) -> &Bytes {
        &self.0
    }
}

impl From<RecordBatch> for Bytes {
    fn from(rb: RecordBatch) -> Self {
        rb.0
    }
}

pub fn parse_record_batches<E>(
    input: impl TryStream<Ok = Bytes, Error = E>,
) -> impl TryStream<Ok = RecordBatch, Error = Error>
where
    E: Into<Error>,
{
    let mut remainder = Bytes::new();

    input
        .err_into::<Error>()
        // Map |input| items into Some(data), and chain a final None to mark EOF.
        .map_ok(Option::Some)
        .chain(futures::stream::once(future::ok(None)))
        // Parse to RecordBatch boundaries, keeping a (moved) Bytes |remainder|
        // in between closure invocations.
        .and_then(move |data_or_eof| future::ready(parse_record_batch(&mut remainder, data_or_eof)))
        // Filter out empty RecordBatches, which are produced if one record spans
        // multiple |input| items, and at stream end.
        .try_filter(|rb| future::ready(!rb.0.is_empty()))
}

fn parse_record_batch(rem: &mut Bytes, data_or_eof: Option<Bytes>) -> Result<RecordBatch, Error> {
    let mut data = match data_or_eof {
        None if !rem.is_empty() => return Err(Error::InvalidJsonSeq),
        None => return Ok(RecordBatch(Bytes::new())),
        Some(data) if rem.is_empty() => data,
        Some(data) => BytesMut::from_iter(rem.iter().copied().chain(data.into_iter())).freeze(),
    };

    if !data.first().map(|v| *v == b'\x1E').unwrap_or(false) {
        return Err(Error::InvalidJsonSeq);
    }

    // Find the last newline of the |chunk|. Select |pivot| to split at one beyond the
    // newline (or zero if there is no newline), keeping the tail as remainder. Usually,
    // |pivot| will equal chunk.len() and there _is_ no remainder, making this efficient.
    let pivot = data
        .iter()
        .rev()
        .position(|v| *v == b'\x0A')
        .map(|i| i + 1)
        .unwrap_or(0);

    *rem = data.split_off(pivot);
    Ok(RecordBatch(data))
}
