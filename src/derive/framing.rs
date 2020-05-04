use super::Error;
use bytes::{Buf, Bytes, BytesMut};
use futures::{StreamExt, Stream, TryStreamExt};
use std::iter::FromIterator;

/// RecordBatch is a Bytes which holds a batch of zero or more "application/json-seq"
/// media-type encodings. Each non-empty RecordBatch has a first byte 0x1E (ASCII
/// record separator), and a last byte 0x0A ('\n') as per RFC 7464.
pub struct RecordBatch(bytes::Bytes);

impl RecordBatch {
    pub fn bytes(&self) -> &Bytes {
        &self.0
    }
}

impl From<RecordBatch> for Bytes {
    fn from(rb: RecordBatch) -> Self {
        rb.0
    }
}

pub fn data_into_record_batches<E>(
    input: impl Stream<Item=Result<impl Buf, E>>,
) -> impl Stream<Item=Result<RecordBatch, Error>>
where
    E: Into<Error>,
{
    let input = input.err_into::<Error>();

    async_stream::try_stream! {
        let mut remainder = Bytes::new();
        pin_utils::pin_mut!(input);

        while let Some(data) = input.next().await {
            // impl bytes::Buf => bytes::Bytes. If the impl is already Bytes
            // (as is the case with hyper::Body), this is zero-cost.
            let data = data?.to_bytes();

            match parse_record_batch(&mut remainder, Some(data))? {
                Some(batch) => yield batch,
                None => {}, // Wait for more data.
            }
        }
        parse_record_batch(&mut remainder, None)?;
    }

    /*
        // Map |input| items into Some(data), and chain a final None to mark EOF.
        .map_ok(Option::Some)
        .chain(futures::stream::once(future::ok(None)))
        // Parse to RecordBatch boundaries, keeping a (moved) Bytes |remainder|
        // in between closure invocations.
        .and_then(move |data_or_eof| future::ready(parse_record_batch(&mut remainder, data_or_eof)))
        // Filter out empty RecordBatches, which are produced if one record spans
        // multiple |input| items, and at stream end.
        .try_filter(|rb: &RecordBatch| future::ready(!rb.bytes().is_empty()))
     */
}

pub fn parse_record_batch(rem: &mut Bytes, data_or_eof: Option<Bytes>) -> Result<Option<RecordBatch>, Error> {
    let mut data = match data_or_eof {
        None if !rem.is_empty() => return Err(Error::InvalidJsonSeq),
        None => return Ok(None),
        Some(data) if rem.is_empty() => data,
        Some(data) => BytesMut::from_iter(rem.iter().copied().chain(data.into_iter())).freeze(),
    };

    if !data.first().map(|v| *v == b'\x1E').unwrap_or(false) {
        return Err(Error::InvalidJsonSeq);
    }

    // Select |pivot| as one-past the last newline of |data|. Usually,
    // |pivot| will equal chunk.len().
    let pivot = data
        .iter()
        .rev()
        .position(|v| *v == b'\x0A');

    match pivot {
        Some(pivot) => {
            // Return through |pivot|, and keep the tail as remainder.
            *rem = data.split_off(pivot + 1);
            Ok(Some(RecordBatch(data)))
        }
        None => {
            // No records in |data|. Keep all as remainder.
            *rem = data;
            Ok(None)
        }
    }
}
