# gcp

Client for the Google Cloud APIs. Currently only Bigtable reads are supported.

Protobuf and gRPC stubs come from `googleapis-tonic-google-bigtable-v2`, so
nothing here is code-generated. What this crate adds is authentication, a
decoder for the `ReadRows` wire protocol, and various helpers to make interacting
with Bigtable a bit easier.

## Layout

- `transport/` — `Channel` wraps `tonic::transport::Channel` and runs async
  `RequestInterceptor`s over each outgoing request. `AuthInterceptor` injects a
  GCP access token as a bearer auth header.
- `bigtable/client.rs` — the `Client` trait (mockable for tests) and `ClientImpl`,
  plus aliases for the request and filter messages.
- `bigtable/decoder.rs` — reassembles the `ReadRows` chunk stream into rows.
  The bulk of the modules's logic.
- `bigtable/types.rs` — aliases to the protobuf row messages, and provides `RowStream`,
  which performs the decoding from the underlying Bigtable wire protocol.
- `bigtable/filters.rs` — constructors for the `RowFilter`s we use in requests to Bigtable,
  primarily to reduce the verbosity of writing filters.
- `scopes` in `lib.rs` — OAuth scope constants.

## Things worth knowing

**Rows are the protobuf messages.** `Row`, `Family`, `Column` and `Cell` are
aliases for `v2::*`, not parallel structs, so there is no conversion at the
boundary. Cells nest as row → family → column → cell.

**The decoder spans the whole stream, not one response.** A row, and even a
single cell's value, can exist across `ReadRowsResponse` boundaries, so `RowDecoder`
has to outlive any one response. It is the three-state reader (state machine) the
official clients use: between rows, in a row, in a cell.

**Errors are yielded, not returned.** A chunk or RPC failure arrives as the
stream's final item, after any rows already committed. A caller that retries has
to narrow its request past the last key it saw.

**`Client::read_rows` returns decoded rows.** It hands back a `RowStream`, not
the raw `Streaming<ReadRowsResponse>`; the `From` conversion runs the decoder.

**Filter patterns are escaped.** Bigtable evaluates them as full matches, and
`filters::columns` escapes each name before joining them with `|`, so a `.` or
`|` in a family or column name cannot change what the pattern means.

## Tests

Unit tests live beside the code; the decoder uses an `insta` snapshot.
