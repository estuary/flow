//! Decoded rows are the protobuf messages themselves. `ReadRows` already
//! defines the shape a row takes, so re-deriving a parallel set of structs
//! would only buy a conversion at the boundary.
use futures_core::Stream;
use std::convert::From;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use tonic::Streaming;

use googleapis_tonic_google_bigtable_v2::google::bigtable::v2;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_range;

use crate::bigtable::decoder::decode_read_rows_response_stream;
use crate::bigtable::errors::Error;

// re-export these types for downstream consumers to use to
// build filters and requests
pub type RowRange = v2::RowRange;
pub type RowSet = v2::RowSet;
pub type StartKey = row_range::StartKey;
pub type EndKey = row_range::EndKey;
pub type ReadRowsRequest = v2::ReadRowsRequest;

pub type RowKey = Vec<u8>;
pub type Row = v2::Row;
pub type Family = v2::Family;
pub type Column = v2::Column;
pub type Cell = v2::Cell;

/// A `ReadRows` response stream decoded into whole rows.
pub struct RowStream {
    inner: Pin<Box<dyn Stream<Item = Result<Row, Error>> + Send + 'static>>,
}

impl RowStream {
    pub fn new(inner: impl Stream<Item = Result<Row, Error>> + Send + 'static) -> Self {
        Self {
            inner: Box::pin(inner),
        }
    }
}

impl Stream for RowStream {
    type Item = Result<Row, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().inner.as_mut().poll_next(cx)
    }
}

impl From<Streaming<v2::ReadRowsResponse>> for RowStream {
    fn from(responses: Streaming<v2::ReadRowsResponse>) -> Self {
        Self::new(decode_read_rows_response_stream(responses))
    }
}
