//! Decodes the Bigtable `ReadRows` chunk protocol into whole rows.
//!
//! The server streams `ReadRowsResponse` messages, each carrying a batch of
//! `CellChunk`s. A chunk may begin a row, begin a cell, continue a cell's
//! value, reset the in-progress row, or commit it. Rows and even individual
//! cell values may be split across `ReadRowsResponse` boundaries, so decoder
//! state must persist for the life of the stream rather than per response.

use crate::bigtable::errors::Error;
use crate::bigtable::types::{Cell, Column, Family, Row, RowKey};

use futures_core::Stream;
use futures_util::StreamExt;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::ReadRowsResponse;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::read_rows_response::CellChunk;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::read_rows_response::cell_chunk::RowStatus;

/// Accumulator for a row that has begun but not yet been committed.
/// Becomes a Row once a CommitRow(true) chunk arrives.
#[derive(Debug)]
struct PartialRow {
    key: RowKey,
    families: Vec<Family>,
    /// Family and qualifier most recently named by a chunk. The protocol omits
    /// them on continuation chunks, so they carry forward until replaced, and
    /// they name the column that the next completed cell belongs to.
    family_name: String,
    qualifier: Vec<u8>,
}

/// Decodes a `ReadRows` response stream into committed `Row`s.
///
/// If a chunk or RPC error occurs while streaming, the error is yielded and the stream ended.
/// Rows committed before the error are would have already been yielded. A caller
/// that retries must therefore narrow its request past the last yielded key.
pub fn decode_read_rows_response_stream<S>(responses: S) -> impl Stream<Item = Result<Row, Error>>
where
    S: Stream<Item = Result<ReadRowsResponse, tonic::Status>>,
{
    coroutines::coroutine(move |mut co| async move {
        let mut responses = std::pin::pin!(responses);
        let mut decoder = RowDecoder::default();

        let result: Result<(), Error> = async {
            while let Some(response) = responses.next().await {
                for chunk in response?.chunks {
                    if let Some(row) = decoder.push(chunk)? {
                        () = co.yield_(Ok(row)).await;
                    }
                }
            }
            decoder.finish()
        }
        .await;

        // Surface the error as the final item, then end the stream. Yielding
        // rather than returning it (as `try_coroutine` would) means consumers
        // see `None` afterwards instead of a panic on the completed future.
        if let Err(err) = result {
            () = co.yield_(Err(err)).await;
        }
    })
}

#[derive(Debug, Default)]
enum DecoderState {
    /// Represents the state where the decoder is currently not processing a row,
    /// meaning nothing is buffered, and the next chunk must be the first chunk
    /// of a fresh row.
    ///
    /// The decoder enters this state in three scenarios:
    ///   - At the start of the stream.
    ///   - After a row is committed
    ///   - After a row is reset.
    #[default]
    BetweenRows,
    /// Represents the state where a new row has started and not yet completed,
    /// so new cells contribute to that row. It also indicates that the last cell
    /// value is complete, so the decoder will start to proces a fresh cell.
    ///
    /// The decoder enters this state whenever a chunk finishes a cell
    /// (`value_size` == 0) without committing the row.
    InRow(PartialRow),
    /// Represents the state where a cells value is still being processed, i.e. the
    /// cell value spans multiple chunks an the decoder needs to continue accumulating
    /// the value. This is indicated by the previous chunk setting the `value_size`.
    InCell { row: PartialRow, cell: Cell },
}

/// Incremental chunk decoder. Every chunk of a `ReadRows` stream is passed
/// to the `push` method, updating the decoder state. When the stream is finished
/// the `finish` method must be called to finalize the decoding.
///
/// This is the three-state reader used by the official Bigtable clients:
/// between rows, inside a row between cells, or inside a cell whose value
/// is still arriving.
#[derive(Debug, Default)]
pub struct RowDecoder {
    state: DecoderState,
    /// Key of the most recently committed row. Bigtable returns rows in
    /// strictly increasing key order for forward scans, which allows identifying
    /// duplicates and reorders without having to store every key.
    last_committed: Option<RowKey>,
}

impl RowDecoder {
    /// Fold one chunk into the decoder. Returns the completed row when
    /// `chunk` commits it. After an `Err` the decoder is reset and the
    /// stream should be abandoned.
    pub fn push(&mut self, chunk: CellChunk) -> Result<Option<Row>, Error> {
        let state = std::mem::take(&mut self.state);
        let (state, row) = step(state, self.last_committed.as_deref(), chunk)?;
        self.state = state;

        if let Some(row) = &row {
            self.last_committed = Some(row.key.clone());
        }
        Ok(row)
    }

    /// Validate a clean end-of-stream: no row may be mid-assembly.
    pub fn finish(self) -> Result<(), Error> {
        match self.state {
            DecoderState::BetweenRows => Ok(()),
            DecoderState::InRow(_) | DecoderState::InCell { .. } => {
                Err(Error::ChunkError("stream ended with an uncommitted row"))
            }
        }
    }
}

fn step(
    state: DecoderState,
    last_committed: Option<&[u8]>,
    chunk: CellChunk,
) -> Result<(DecoderState, Option<Row>), Error> {
    let CellChunk {
        row_key,
        family_name,
        qualifier,
        timestamp_micros,
        labels,
        value,
        value_size,
        row_status,
    } = chunk;

    // Only the `true` variants carry meaning; the protocol allows the server
    // to send an explicit `false`, which is the same as no status.
    let reset = matches!(row_status, Some(RowStatus::ResetRow(true)));
    let commit = matches!(row_status, Some(RowStatus::CommitRow(true)));

    let has_cell_metadata = !row_key.is_empty()
        || family_name.is_some()
        || qualifier.is_some()
        || timestamp_micros != 0
        || !labels.is_empty();

    if reset {
        if has_cell_metadata || value_size != 0 || !value.is_empty() {
            return Err(Error::ChunkError("ResetRow chunk carries other fields"));
        }
        return match state {
            DecoderState::BetweenRows => Err(Error::ChunkError("ResetRow with no row in progress")),
            DecoderState::InRow(_) | DecoderState::InCell { .. } => {
                Ok((DecoderState::BetweenRows, None))
            }
        };
    }
    if commit && value_size > 0 {
        return Err(Error::ChunkError(
            "CommitRow while a cell value is still being chunked",
        ));
    }

    let (mut row, cell) = match state {
        DecoderState::BetweenRows => {
            if row_key.is_empty() {
                return Err(Error::ChunkError("first chunk of a row has no row key"));
            }
            if last_committed.is_some_and(|last| row_key.as_slice() <= last) {
                return Err(Error::ChunkError("row keys must be strictly increasing"));
            }
            let (Some(family_name), Some(qualifier)) = (family_name, qualifier) else {
                return Err(Error::ChunkError(
                    "first chunk of a row has no family name or qualifier",
                ));
            };
            let row = PartialRow {
                key: row_key,
                families: Vec::new(),
                family_name,
                qualifier,
            };
            let cell = Cell {
                timestamp_micros,
                labels,
                value,
            };

            (row, cell)
        }
        DecoderState::InRow(mut row) => {
            if !row_key.is_empty() && row_key != row.key {
                return Err(Error::ChunkError("row key changed without CommitRow"));
            }
            if family_name.is_some() && qualifier.is_none() {
                return Err(Error::ChunkError("new column family without a qualifier"));
            }
            if let Some(family_name) = family_name {
                row.family_name = family_name;
            }
            if let Some(qualifier) = qualifier {
                row.qualifier = qualifier;
            }
            let cell = Cell {
                timestamp_micros,
                labels,
                value,
            };

            (row, cell)
        }
        DecoderState::InCell { row, mut cell } => {
            if has_cell_metadata {
                return Err(Error::ChunkError(
                    "continuation of a cell value carries cell metadata",
                ));
            }
            cell.value.extend(value);
            (row, cell)
        }
    };

    if value_size > 0 {
        return Ok((DecoderState::InCell { row, cell }, None));
    }
    push_cell(&mut row.families, &row.family_name, &row.qualifier, cell);

    if !commit {
        return Ok((DecoderState::InRow(row), None));
    }
    let row = Row {
        key: row.key,
        families: row.families,
    };
    Ok((DecoderState::BetweenRows, Some(row)))
}

/// Appends `cell` to the `(family_name, qualifier)` column, opening a `Family`
/// or `Column` when the chunk stream moves on to the next one.
///
/// `Row` requires a family's columns to be sorted by increasing qualifier,
/// which is the order Bigtable emits a row's chunks in. Appending to the
/// trailing family and column therefore reproduces the required structure
/// without a lookup or a sort. A server that interleaved a family's columns
/// with another family's would yield that family twice rather than an error.
fn push_cell(families: &mut Vec<Family>, family_name: &str, qualifier: &[u8], cell: Cell) {
    if families
        .last()
        .is_none_or(|family| family.name != family_name)
    {
        families.push(Family {
            name: family_name.to_owned(),
            columns: Vec::new(),
        });
    }
    let columns = &mut families.last_mut().expect("just pushed").columns;

    if columns
        .last()
        .is_none_or(|column| column.qualifier != qualifier)
    {
        columns.push(Column {
            qualifier: qualifier.to_owned(),
            cells: Vec::new(),
        });
    }
    columns.last_mut().expect("just pushed").cells.push(cell);
}

#[cfg(test)]
mod tests {
    use super::*;

    type Responses = Vec<Result<ReadRowsResponse, tonic::Status>>;

    #[tokio::test]
    async fn decodes_chunk_sequences() {
        let a = "acmeCo/a";
        let b = "acmeCo/b";

        let cases: Vec<(&str, Responses)> = vec![
            ("empty stream", vec![]),
            (
                "single chunk row",
                vec![resp(vec![commit(first(a, "q", "v"))])],
            ),
            (
                "multi-cell row carries family and qualifier forward",
                vec![resp(vec![
                    first(a, "q1", "v1"),
                    cell("q2", "v2"),
                    commit(with_ts(value_only("v3", 0), 7)),
                ])],
            ),
            (
                "cells group by family, then by qualifier",
                vec![resp(vec![
                    first(a, "q1", "v1"),
                    value_only("v2", 0), // a second cell of f:q1
                    cell("q2", "v3"),
                    commit(family("g", "q1", "v4")),
                ])],
            ),
            (
                "cell split across chunks in one response",
                vec![resp(vec![
                    with_size(with_ts(first(a, "q", "he"), 9), 5),
                    value_only("ll", 5),
                    commit(value_only("o", 0)),
                ])],
            ),
            (
                "cell split across responses",
                vec![
                    resp(vec![with_size(first(a, "q", "he"), 5)]),
                    resp(vec![value_only("ll", 5)]),
                    resp(vec![commit(value_only("o", 0))]),
                ],
            ),
            (
                "row split across responses at a cell boundary",
                vec![
                    resp(vec![first(a, "q1", "v1")]),
                    resp(vec![commit(cell("q2", "v2"))]),
                ],
            ),
            (
                "two rows in one response",
                vec![resp(vec![
                    commit(first(a, "q", "va")),
                    commit(first(b, "q", "vb")),
                ])],
            ),
            (
                "ResetRow in row discards buffered cells",
                vec![resp(vec![
                    first(a, "q", "garbled"),
                    reset(),
                    commit(first(a, "q", "v")),
                ])],
            ),
            (
                "ResetRow in cell discards partial value",
                vec![resp(vec![
                    with_size(first(a, "q", "ga"), 7),
                    reset(),
                    commit(first(a, "q", "v")),
                ])],
            ),
            (
                "CommitRow(false) is not a commit",
                vec![resp(vec![
                    with_status(first(a, "q1", "v1"), RowStatus::CommitRow(false)),
                    commit(cell("q2", "v2")),
                ])],
            ),
            (
                "empty heartbeat response between rows",
                vec![
                    resp(vec![commit(first(a, "q", "va"))]),
                    resp(vec![]),
                    resp(vec![commit(first(b, "q", "vb"))]),
                ],
            ),
            (
                "present but empty family and qualifier",
                vec![resp(vec![commit(CellChunk {
                    row_key: a.into(),
                    family_name: Some(String::new()),
                    qualifier: Some(Vec::new()),
                    value: b"v".to_vec(),
                    ..Default::default()
                })])],
            ),
            (
                "rows before an RPC error are yielded first",
                vec![
                    resp(vec![commit(first(a, "q", "va"))]),
                    Err(tonic::Status::unavailable("boom")),
                    resp(vec![commit(first(b, "q", "vb"))]),
                ],
            ),
            // Error cases: exactly one Err, and it is the final item.
            (
                "err: first chunk without row key",
                vec![resp(vec![commit(with_key(first(a, "q", "v"), ""))])],
            ),
            (
                "err: first chunk without family",
                vec![resp(vec![commit(CellChunk {
                    row_key: a.into(),
                    qualifier: Some(b"q".to_vec()),
                    value: b"v".to_vec(),
                    ..Default::default()
                })])],
            ),
            (
                "err: first chunk without qualifier",
                vec![resp(vec![commit(CellChunk {
                    row_key: a.into(),
                    family_name: Some("f".to_owned()),
                    value: b"v".to_vec(),
                    ..Default::default()
                })])],
            ),
            (
                "err: new family without qualifier mid-row",
                vec![resp(vec![
                    first(a, "q", "v"),
                    commit(CellChunk {
                        family_name: Some("g".to_owned()),
                        value: b"v".to_vec(),
                        ..Default::default()
                    }),
                ])],
            ),
            (
                "err: commit while cell is chunked",
                vec![resp(vec![commit(with_size(first(a, "q", "v"), 10))])],
            ),
            (
                "err: row key changed without commit",
                vec![resp(vec![first(a, "q", "v"), commit(first(b, "q", "v"))])],
            ),
            (
                "err: duplicate row key",
                vec![resp(vec![
                    commit(first(a, "q", "v")),
                    commit(first(a, "q", "v")),
                ])],
            ),
            (
                "err: decreasing row key",
                vec![resp(vec![
                    commit(first(b, "q", "v")),
                    commit(first(a, "q", "v")),
                ])],
            ),
            (
                "err: ResetRow with value",
                vec![resp(vec![
                    first(a, "q", "v"),
                    with_status(value_only("x", 0), RowStatus::ResetRow(true)),
                ])],
            ),
            (
                "err: ResetRow with row key",
                vec![resp(vec![first(a, "q", "v"), with_key(reset(), a)])],
            ),
            ("err: ResetRow between rows", vec![resp(vec![reset()])]),
            (
                "err: cell continuation with qualifier",
                vec![resp(vec![
                    with_size(first(a, "q", "he"), 5),
                    commit(cell("q", "llo")),
                ])],
            ),
            (
                "err: cell continuation with timestamp",
                vec![resp(vec![
                    with_size(first(a, "q", "he"), 5),
                    commit(with_ts(value_only("llo", 0), 5)),
                ])],
            ),
            (
                "err: stream ends in row",
                vec![resp(vec![first(a, "q", "v")])],
            ),
            (
                "err: stream ends in cell",
                vec![resp(vec![with_size(first(a, "q", "he"), 5)])],
            ),
        ];

        let mut snapshots: Vec<(&str, Vec<String>)> = Vec::new();

        for (label, responses) in cases {
            let stream = decode_read_rows_response_stream(futures::stream::iter(responses));
            let item_snapshots: Vec<_> = stream.map(render_to_snapshot).collect().await;
            snapshots.push((label, item_snapshots));
        }

        insta::assert_debug_snapshot!(snapshots);
    }

    // Renders the family / column / cell nesting, so that a regression in how
    // cells are grouped shows up in the snapshot and not just a reordering.
    fn render_to_snapshot(item: Result<Row, Error>) -> String {
        let row = match item {
            Err(err) => return format!("Err({err})"),
            Ok(row) => row,
        };

        let families: Vec<String> = row
            .families
            .iter()
            .map(|family| {
                let columns: Vec<String> = family
                    .columns
                    .iter()
                    .map(|column| {
                        let cells: Vec<String> = column
                            .cells
                            .iter()
                            .map(|cell| {
                                format!(
                                    "@{}={} {:?}",
                                    cell.timestamp_micros,
                                    String::from_utf8_lossy(&cell.value),
                                    cell.labels,
                                )
                            })
                            .collect();
                        format!(
                            "{}[{}]",
                            String::from_utf8_lossy(&column.qualifier),
                            cells.join(", "),
                        )
                    })
                    .collect();
                format!("{}[{}]", family.name, columns.join(", "))
            })
            .collect();

        format!(
            "{} {}",
            String::from_utf8_lossy(&row.key),
            families.join(", "),
        )
    }

    fn resp(chunks: Vec<CellChunk>) -> Result<ReadRowsResponse, tonic::Status> {
        Ok(ReadRowsResponse {
            chunks,
            ..Default::default()
        })
    }

    // First chunk of a row: carries key, family "f", and qualifier.
    fn first(key: &str, qualifier: &str, value: &str) -> CellChunk {
        CellChunk {
            row_key: key.into(),
            family_name: Some("f".to_owned()),
            ..cell(qualifier, value)
        }
    }

    // Starts a new cell in a new column family.
    fn family(name: &str, qualifier: &str, value: &str) -> CellChunk {
        CellChunk {
            family_name: Some(name.to_owned()),
            ..cell(qualifier, value)
        }
    }

    // Starts a new cell in the carried family.
    fn cell(qualifier: &str, value: &str) -> CellChunk {
        CellChunk {
            qualifier: Some(qualifier.into()),
            ..value_only(value, 0)
        }
    }

    // Carries only a value: either a continuation of a chunked cell, or a
    // new cell in the carried column when the previous cell is complete.
    fn value_only(value: &str, value_size: i32) -> CellChunk {
        CellChunk {
            value: value.into(),
            value_size,
            ..Default::default()
        }
    }

    fn reset() -> CellChunk {
        with_status(CellChunk::default(), RowStatus::ResetRow(true))
    }

    fn commit(chunk: CellChunk) -> CellChunk {
        with_status(chunk, RowStatus::CommitRow(true))
    }

    fn with_status(mut chunk: CellChunk, status: RowStatus) -> CellChunk {
        chunk.row_status = Some(status);
        chunk
    }

    fn with_size(mut chunk: CellChunk, value_size: i32) -> CellChunk {
        chunk.value_size = value_size;
        chunk
    }

    fn with_ts(mut chunk: CellChunk, timestamp_micros: i64) -> CellChunk {
        chunk.timestamp_micros = timestamp_micros;
        chunk
    }

    fn with_key(mut chunk: CellChunk, key: &str) -> CellChunk {
        chunk.row_key = key.into();
        chunk
    }
}
