use anyhow::Context;
use futures::StreamExt;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    self as bt, bigtable_client,
    read_rows_response::{self, cell_chunk},
    row_filter, row_range, value_range,
};
use tonic::body;
use tonic::codegen;
use tonic::codegen::http;
use tonic::transport;
use tuple::TuplePack;

#[cfg(feature = "test_util")]
pub mod test_util;

pub use ops::catalog_stats::{CatalogStats, Grain, StatsSummary, TaskStats};

// Fixed column family written by `materialize-bigtable`.
const COLUMN_FAMILY: &str = "f";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("gRPC code: {:?}, message: {}", .0.code(), .0.message())]
    Grpc(#[from] tonic::Status),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

/// RetryError is an Error encountered during a retry-able operation.
#[derive(Debug)]
pub struct RetryError {
    /// Number of operation attempts since the last success.
    pub attempt: usize,
    /// Error encountered with this attempt.
    pub inner: Error,
}

impl Error {
    pub fn with_attempt(self, attempt: usize) -> RetryError {
        RetryError {
            attempt,
            inner: self,
        }
    }

    pub fn is_transient(&self) -> bool {
        match self {
            // These retryable codes are consistent with retry handling in the
            // official Go Bigtable client library.
            Error::Grpc(status) => matches!(
                status.code(),
                tonic::Code::Unavailable | tonic::Code::DeadlineExceeded | tonic::Code::Aborted,
            ),
            Error::Internal(_) => false,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// RetryResult is a single Result of a retry-able operation.
pub type RetryResult<T> = std::result::Result<T, RetryError>;

/// BigTable-specific configuration. When `emulator_host` is `Some`, the
/// transport connects to `http://{host}` and skips ADC auth; otherwise it
/// connects to `https://bigtable.googleapis.com` with an ADC bearer token.
#[derive(Debug, Clone)]
pub struct BigtableConfig {
    pub project: String,
    pub instance: String,
    pub emulator_host: Option<String>,
}

#[derive(Clone)]
pub struct Client {
    client: bigtable_client::BigtableClient<AuthChannel>,
    project: String,
    instance: String,
}

impl Client {
    pub async fn connect(cfg: &BigtableConfig) -> anyhow::Result<Self> {
        let endpoint = match &cfg.emulator_host {
            Some(h) => format!("http://{h}"),
            None => "https://bigtable.googleapis.com".to_string(),
        };

        let ep = transport::Channel::from_shared(endpoint.clone())
            .with_context(|| format!("invalid BigTable endpoint {endpoint:?}"))?;

        let ep = if cfg.emulator_host.is_some() {
            ep
        } else {
            ep.tls_config(
                transport::ClientTlsConfig::new()
                    .with_native_roots()
                    .assume_http2(true),
            )
            .context("configuring Bigtable TLS")?
        };

        let channel = ep
            .connect()
            .await
            .with_context(|| format!("connecting to BigTable {endpoint:?}"))?;
        let use_auth = cfg.emulator_host.is_none();
        let client =
            bigtable_client::BigtableClient::new(AuthChannel::new(channel, use_auth).await?);

        Ok(Self {
            client,
            project: cfg.project.clone(),
            instance: cfg.instance.clone(),
        })
    }

    /// Streams every `catalog_stats_<grain>` row whose `catalog_name` is
    /// one of `names`, and whose `ts` falls in the half-open interval
    /// `[range.start, range.end)`.
    ///
    /// Timestamps should be aligned to the grain boundaries. Sub-ms precision
    /// is truncated.
    ///
    /// Results are in lexicographic order by `names`, then ascending by `ts`
    /// within each name.
    pub fn fetch_range_for_names(
        &self,
        names: &[&str],
        grain: Grain,
        range: std::ops::Range<chrono::DateTime<chrono::Utc>>,
    ) -> impl futures_core::Stream<Item = RetryResult<CatalogStats>> + '_ {
        let row_set = bt::RowSet {
            row_keys: vec![],
            row_ranges: names
                .iter()
                .map(|name| pack_row_range(name, &range))
                .collect(),
        };

        self.read_rows(grain, row_set, vec![])
    }

    /// Streams the row at exactly `(name, ts)` from `catalog_stats_<grain>` for
    /// each name.
    ///
    /// Timestamps should be aligned to the grain boundaries. Sub-ms precision
    /// is truncated.
    ///
    /// Results are in lexicographic order by `names`.
    pub fn fetch_at_for_names(
        &self,
        names: &[&str],
        grain: Grain,
        ts: chrono::DateTime<chrono::Utc>,
    ) -> impl futures_core::Stream<Item = RetryResult<CatalogStats>> + '_ {
        let row_set = bt::RowSet {
            row_keys: names.iter().map(|name| pack_row_key(name, ts)).collect(),
            row_ranges: vec![],
        };

        self.read_rows(grain, row_set, vec![])
    }

    /// Streams every `catalog_stats_<grain>` row whose `catalog_name` starts
    /// with `prefix`, and whose `ts` falls in the half-open interval
    /// `[range.start, range.end)`.
    ///
    /// Includes rollup rows — callers that want only individual tasks must
    /// filter names ending in `/` themselves.
    ///
    /// Timestamps should be aligned to the grain boundaries. Sub-ms precision
    /// is truncated.
    ///
    /// Performance: `ts` is enforced server-side as a value filter, so Bigtable
    /// scans every row in the prefix range regardless of `range`. The cost is
    /// least significant on coarser grains like `Monthly` but grows on finer
    /// grains.
    ///
    /// Results are in lexicographic order by prefix, then ascending by `ts`
    /// within each prefix.
    pub fn fetch_range_for_prefix(
        &self,
        prefix: &str,
        grain: Grain,
        range: std::ops::Range<chrono::DateTime<chrono::Utc>>,
    ) -> impl futures_core::Stream<Item = RetryResult<CatalogStats>> + '_ {
        let row_set = bt::RowSet {
            row_keys: vec![],
            row_ranges: pack_name_prefix_range(prefix).into_iter().collect(),
        };

        let ts_match = rf(row_filter::Filter::Condition(Box::new(
            row_filter::Condition {
                predicate_filter: Some(Box::new(chain_filter(vec![
                    rf(row_filter::Filter::FamilyNameRegexFilter(
                        COLUMN_FAMILY.to_string(),
                    )),
                    rf(row_filter::Filter::ColumnQualifierRegexFilter(
                        b"ts".to_vec(),
                    )),
                    rf(row_filter::Filter::ValueRangeFilter(bt::ValueRange {
                        start_value: Some(value_range::StartValue::StartValueClosed(
                            format_ts(range.start).into_bytes(),
                        )),
                        end_value: Some(value_range::EndValue::EndValueOpen(
                            format_ts(range.end).into_bytes(),
                        )),
                    })),
                ]))),
                true_filter: Some(Box::new(rf(row_filter::Filter::PassAllFilter(true)))),
                false_filter: None,
            },
        )));

        self.read_rows(grain, row_set, vec![ts_match])
    }

    fn table_name(&self, grain: Grain) -> String {
        format!(
            "projects/{}/instances/{}/tables/catalog_stats_{grain}",
            self.project, self.instance,
        )
    }

    fn read_rows(
        &self,
        grain: Grain,
        row_set: bt::RowSet,
        additional_filters: Vec<bt::RowFilter>,
    ) -> impl futures_core::Stream<Item = RetryResult<CatalogStats>> + '_ {
        let mut client = self.client.clone();
        let mut read = ReadRows::new(self.table_name(grain), row_set, additional_filters);
        let mut attempt: usize = 0;

        coroutines::coroutine(move |mut co| async move {
            // Each iteration issues one ReadRows RPC; on failure the inner
            // state has already trimmed `row_set` past the watermark so we
            // only re-read rows still owed.
            loop {
                let Some(request) = read.next_request() else {
                    return;
                };

                let stream = client
                    .read_rows(request)
                    .await
                    .map(|response| response.into_inner());
                let mut stream = std::pin::pin!(read.handle_stream(stream));

                while let Some(action) = stream.next().await {
                    match action {
                        ReadResult::Yield(stats_doc) => {
                            () = co.yield_(Ok(stats_doc)).await;
                            attempt = 0;
                        }
                        ReadResult::Done => return,
                        ReadResult::Failed(err) => {
                            // Surface error to the caller, who can either drop
                            // to cancel or poll to retry. Non-transient errors
                            // (decode failures, protocol violations) end the
                            // stream immediately — retrying won't help.
                            let transient = err.is_transient();
                            () = co.yield_(Err(err.with_attempt(attempt))).await;
                            if !transient {
                                return;
                            }
                            () = tokio::time::sleep(backoff(attempt)).await;
                            attempt += 1;
                            break;
                        }
                    }
                }
            }
        })
    }
}

fn backoff(attempt: usize) -> std::time::Duration {
    match attempt {
        0 => std::time::Duration::ZERO,
        1 => std::time::Duration::from_millis(100),
        2 | 3 => std::time::Duration::from_millis(500),
        _ => std::time::Duration::from_secs(1),
    }
}

// `%.3f` matches the JS `toISOString()` format that the L1 derivation uses.
fn format_ts(ts: chrono::DateTime<chrono::Utc>) -> String {
    ts.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

fn pack_row_key(name: &str, ts: chrono::DateTime<chrono::Utc>) -> Vec<u8> {
    let key = (name, &format_ts(ts));
    key.pack_to_vec()
}

fn pack_row_range(
    name: &str,
    range: &std::ops::Range<chrono::DateTime<chrono::Utc>>,
) -> bt::RowRange {
    bt::RowRange {
        start_key: Some(row_range::StartKey::StartKeyClosed(pack_row_key(
            name,
            range.start,
        ))),
        end_key: Some(row_range::EndKey::EndKeyOpen(pack_row_key(name, range.end))),
    }
}

fn pack_name_prefix_range(prefix: &str) -> Option<bt::RowRange> {
    if prefix.is_empty() {
        // Return `None` for an empty prefix: the FDB encoding of `""` would
        // produce a range covering every string-typed row in the table — i.e.
        // "scan everything".
        return None;
    }

    let mut start_key = (prefix,).pack_to_vec();
    start_key.pop().expect("non-empty packed prefix"); // strip 0x00 terminator

    // FDB escapes any NUL byte in the string as `0x00 0xff`, so a `prefix`
    // ending in NUL produces an encoded form that ends in `0xff` once the
    // FDB terminator is stripped. Walk past trailing `0xff` bytes before
    // incrementing to compute the lex successor; the leading `0x02`
    // string-type tag keeps the strip from emptying the key.
    let mut end_key = start_key.clone();
    while let Some(&0xff) = end_key.last() {
        end_key.pop();
    }
    *end_key.last_mut().expect("type tag prevents empty key") += 1;

    Some(bt::RowRange {
        start_key: Some(row_range::StartKey::StartKeyClosed(start_key)),
        end_key: Some(row_range::EndKey::EndKeyOpen(end_key)),
    })
}

fn rf(filter: row_filter::Filter) -> bt::RowFilter {
    bt::RowFilter {
        filter: Some(filter),
    }
}

fn chain_filter(mut filters: Vec<bt::RowFilter>) -> bt::RowFilter {
    if filters.len() == 1 {
        filters.pop().unwrap()
    } else {
        rf(row_filter::Filter::Chain(row_filter::Chain { filters }))
    }
}

struct ReadRows {
    table_name: String,
    filter: Option<bt::RowFilter>,
    row_set: bt::RowSet,
    doc: Vec<u8>,
    current_key: Vec<u8>,
    watermark: Option<Vec<u8>>,
}

#[derive(Debug)]
enum ReadResult {
    Yield(CatalogStats),
    Failed(Error),
    Done,
}

impl ReadRows {
    fn new(
        table_name: String,
        row_set: bt::RowSet,
        additional_filters: Vec<bt::RowFilter>,
    ) -> Self {
        // `CellsPerColumnLimitFilter(1)`: materialize-bigtable writes
        // under MaxVersions(2); narrow to the latest cell.
        let mut filters = vec![rf(row_filter::Filter::CellsPerColumnLimitFilter(1))];
        filters.extend(additional_filters);
        filters.push(rf(row_filter::Filter::ColumnQualifierRegexFilter(
            b"flow_document".to_vec(),
        )));

        Self {
            table_name,
            filter: Some(chain_filter(filters)),
            row_set,
            doc: Vec::new(),
            current_key: Vec::new(),
            watermark: None,
        }
    }

    fn next_request(&self) -> Option<bt::ReadRowsRequest> {
        if self.row_set.row_keys.is_empty() && self.row_set.row_ranges.is_empty() {
            // Request is complete.
            return None;
        }

        Some(bt::ReadRowsRequest {
            table_name: self.table_name.clone(),
            rows: Some(self.row_set.clone()),
            filter: self.filter.clone(),
            ..Default::default()
        })
    }

    fn handle_stream<'a, S>(
        &'a mut self,
        stream: std::result::Result<S, tonic::Status>,
    ) -> impl futures_core::Stream<Item = ReadResult> + 'a
    where
        S: futures_core::Stream<Item = std::result::Result<bt::ReadRowsResponse, tonic::Status>>
            + Unpin
            + 'a,
    {
        coroutines::coroutine(move |mut co| async move {
            let mut stream = match stream {
                Ok(s) => s,
                Err(status) => {
                    // Initial RPC failed before any data arrived.
                    () = co.yield_(ReadResult::Failed(Error::Grpc(status))).await;
                    return;
                }
            };

            // Drain the response stream. Each response carries N chunks;
            // chunks may begin, continue, reset, or commit a row.
            while let Some(res) = stream.next().await {
                let message = match res {
                    Ok(m) => m,
                    Err(status) => {
                        // Mid-stream failure may be retried. Drop any partial
                        // row buffer and trim `row_set` past the last yielded
                        // key.
                        self.doc.clear();
                        if let Some(w) = &self.watermark {
                            self.row_set = trim_row_set(std::mem::take(&mut self.row_set), w);
                        }
                        () = co.yield_(ReadResult::Failed(Error::Grpc(status))).await;
                        return;
                    }
                };
                for chunk in message.chunks {
                    // `on_chunk` returns `Some` only on `CommitRow`.
                    match self.on_chunk(chunk) {
                        Ok(Some(stats)) => () = co.yield_(ReadResult::Yield(stats)).await,
                        Ok(None) => {}
                        Err(err) => {
                            () = co.yield_(ReadResult::Failed(err)).await;
                            return;
                        }
                    }
                }
            }

            // Clean end-of-stream: no row should be mid-assembly.
            if !self.doc.is_empty() {
                let buffered = self.doc.len();
                () = co
                    .yield_(ReadResult::Failed(Error::Internal(anyhow::anyhow!(
                        "ReadRows stream ended with {buffered} bytes buffered for an uncommitted row",
                    ))))
                    .await;
                return;
            }

            () = co.yield_(ReadResult::Done).await;
        })
    }

    /// Fold `chunk` into the in-progress row buffer. Returns the
    /// decoded row when `chunk` carries `CommitRow`.
    fn on_chunk(&mut self, chunk: read_rows_response::CellChunk) -> Result<Option<CatalogStats>> {
        // The first chunk of each row carries `row_key`; subsequent
        // chunks for the same row leave it empty.
        if !chunk.row_key.is_empty() {
            self.current_key = chunk.row_key;
        }
        // The server-side filter strips all columns except
        // `flow_document`, so every chunk's `value` belongs to that
        // single cell.
        self.doc.extend_from_slice(&chunk.value);

        match chunk.row_status {
            Some(cell_chunk::RowStatus::ResetRow(true)) => {
                // `ResetRow` discards in-progress state — the server is
                // retrying this row from scratch.
                self.doc.clear();
                Ok(None)
            }
            Some(cell_chunk::RowStatus::CommitRow(true)) => {
                let result = serde_json::from_slice(&self.doc)
                    .context("decoding flow_document")
                    .map_err(Error::Internal)?;
                self.doc.clear();

                // This assumes Bigtable returns results in sorted order, which
                // it does.
                self.watermark = Some(std::mem::take(&mut self.current_key));

                Ok(Some(result))
            }
            _ => Ok(None), // Continue accumulation
        }
    }
}

/// Narrows `row_set` to exclude every row at or before `after`.
///
/// Called from the retry path: `after` is the highest row key already
/// yielded, so the returned `RowSet` covers only the rows still owed.
///
/// Discrete `row_keys` are filtered point-wise. Each `row_range` is then
/// dropped, clamped, or kept as-is depending on where it sits relative
/// to `after`:
///
/// ```text
///                          after
///                            │
///   range A:  [─────)        │              drop  (end ≤ after)
///   range B:  [──────────────┼──────)       clamp start to Open(after)
///   range C:                 │   [─────)    keep as-is
/// ```
fn trim_row_set(row_set: bt::RowSet, after: &[u8]) -> bt::RowSet {
    let row_keys = row_set
        .row_keys
        .into_iter()
        .filter(|k| k.as_slice() > after)
        .collect();

    let row_ranges = row_set
        .row_ranges
        .into_iter()
        .filter_map(|mut range| {
            match &range.end_key {
                Some(row_range::EndKey::EndKeyClosed(k))
                | Some(row_range::EndKey::EndKeyOpen(k))
                    if k.as_slice() <= after =>
                {
                    // Range ends at or below `after`; no rows remain.
                    return None;
                }
                _ => {}
            }

            // Narrow the start when it still includes anything `<= after`.
            let narrow = match &range.start_key {
                None => true,
                Some(row_range::StartKey::StartKeyClosed(k)) => k.as_slice() <= after,
                Some(row_range::StartKey::StartKeyOpen(k)) => k.as_slice() < after,
            };
            if narrow {
                range.start_key = Some(row_range::StartKey::StartKeyOpen(after.to_vec()));
            }

            Some(range)
        })
        .collect();

    bt::RowSet {
        row_keys,
        row_ranges,
    }
}

const BIGTABLE_DATA_SCOPE: &str = "https://www.googleapis.com/auth/bigtable.data";

#[derive(Clone)]
struct AuthChannel {
    channel: transport::Channel,
    provider: Option<std::sync::Arc<dyn gcp_auth::TokenProvider>>,
}

impl AuthChannel {
    async fn new(channel: transport::Channel, use_auth: bool) -> anyhow::Result<Self> {
        let provider = if use_auth {
            Some(
                gcp_auth::provider()
                    .await
                    .context("acquiring GCP credentials provider")?,
            )
        } else {
            None
        };
        Ok(Self { channel, provider })
    }
}

impl codegen::Service<http::Request<body::Body>> for AuthChannel {
    type Response = http::Response<body::Body>;
    type Error = codegen::StdError;
    type Future = std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = std::result::Result<http::Response<body::Body>, codegen::StdError>,
                > + Send,
        >,
    >;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        self.channel.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, mut req: http::Request<body::Body>) -> Self::Future {
        let next = self.channel.clone();
        let mut channel = std::mem::replace(&mut self.channel, next);
        let provider = self.provider.clone();
        Box::pin(async move {
            if let Some(provider) = provider {
                let token = provider.token(&[BIGTABLE_DATA_SCOPE]).await?;
                let mut header =
                    http::HeaderValue::from_str(&format!("Bearer {}", token.as_str()))?;
                header.set_sensitive(true);
                req.headers_mut()
                    .insert(http::header::AUTHORIZATION, header);
            }
            channel.call(req).await.map_err(Into::into)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn format_ts_matches_l2_iso_string() {
        let ts = chrono::Utc.with_ymd_and_hms(2026, 5, 5, 18, 0, 0).unwrap();
        assert_eq!(format_ts(ts), "2026-05-05T18:00:00.000Z");

        // Whole-millisecond precision round-trips through the formatter.
        let ts_ms = ts + chrono::Duration::milliseconds(123);
        assert_eq!(format_ts(ts_ms), "2026-05-05T18:00:00.123Z");

        // Sub-ms precision is silently truncated, not rounded.
        let ts_us = ts + chrono::Duration::microseconds(123_999);
        assert_eq!(format_ts(ts_us), "2026-05-05T18:00:00.123Z");
    }

    #[test]
    fn name_prefix_range_brackets_matching_row_keys() {
        let ts = chrono::Utc.with_ymd_and_hms(2026, 5, 5, 18, 0, 0).unwrap();
        let cases: &[(&str, &[(&str, bool)])] = &[
            (
                "acmeCo/",
                &[
                    ("acmeCo/", true),     // tenant rollup row
                    ("acmeCo/foo", true),  // per-task row under the tenant
                    ("acmeCo0foo", false), // sibling exactly at the exclusive end-key boundary (`/`+1 == `0`)
                    ("acmeCo:foo", false), // sibling just past the boundary (`:` > `0`)
                ],
            ),
            (
                "acm",
                &[
                    ("acmeCo/foo", true), // partial-name match
                    ("acn", false),       // `m`+1 == `n`, just past the range
                ],
            ),
            (
                "acmé",
                &[
                    ("acmé/foo", true), // under the prefix
                    ("acmé", true),     // exact match
                    ("acmê", false),    // next Unicode codepoint, sibling
                ],
            ),
            (
                "foo\0",
                &[
                    ("foo\0bar", true), // longer match through the NUL escape
                    ("foo\0", true),    // exact match
                    ("foo", false),     // shorter, falls before the prefix
                    ("foo\x01", false), // sibling — 0x01 sorts after the escape
                ],
            ),
        ];

        for (prefix, candidates) in cases {
            let bt::RowRange {
                start_key: Some(row_range::StartKey::StartKeyClosed(start)),
                end_key: Some(row_range::EndKey::EndKeyOpen(end)),
            } = pack_name_prefix_range(prefix).expect("non-empty prefix")
            else {
                panic!("expected [closed, open) range for prefix {prefix:?}");
            };

            for (name, want_in) in *candidates {
                let key = pack_row_key(name, ts);
                assert_eq!(
                    key >= start && key < end,
                    *want_in,
                    "prefix {prefix:?}, name {name:?}"
                );
            }
        }
    }

    #[test]
    fn name_prefix_range_empty_prefix_yields_none() {
        assert!(pack_name_prefix_range("").is_none());
    }

    #[test]
    fn trim_row_set_drops_ranges_ending_at_or_before_after() {
        let mk_range =
            |start: Option<row_range::StartKey>, end: Option<row_range::EndKey>| bt::RowRange {
                start_key: start,
                end_key: end,
            };
        let closed_start = |k: &[u8]| Some(row_range::StartKey::StartKeyClosed(k.to_vec()));
        let closed_end = |k: &[u8]| Some(row_range::EndKey::EndKeyClosed(k.to_vec()));
        let open_end = |k: &[u8]| Some(row_range::EndKey::EndKeyOpen(k.to_vec()));

        let after = b"m";
        let cases: &[(&str, bt::RowRange)] = &[
            (
                "open end strictly before after",
                mk_range(closed_start(b"a"), open_end(b"c")),
            ),
            (
                "open end at after (range ends below after)",
                mk_range(closed_start(b"a"), open_end(after)),
            ),
            (
                "closed end at after (only row is `after` itself)",
                mk_range(closed_start(b"a"), closed_end(after)),
            ),
        ];

        for (name, range) in cases {
            let got = trim_row_set(
                bt::RowSet {
                    row_keys: vec![],
                    row_ranges: vec![range.clone()],
                },
                after,
            );
            assert!(
                got.row_ranges.is_empty(),
                "{name}: expected dropped range, got {got:?}",
            );
        }
    }

    #[test]
    fn trim_row_set_narrows_starts_at_or_before_after() {
        let mk_range =
            |start: Option<row_range::StartKey>, end: Option<row_range::EndKey>| bt::RowRange {
                start_key: start,
                end_key: end,
            };
        let closed_start = |k: &[u8]| Some(row_range::StartKey::StartKeyClosed(k.to_vec()));
        let open_start = |k: &[u8]| Some(row_range::StartKey::StartKeyOpen(k.to_vec()));
        let closed_end = |k: &[u8]| Some(row_range::EndKey::EndKeyClosed(k.to_vec()));
        let open_end = |k: &[u8]| Some(row_range::EndKey::EndKeyOpen(k.to_vec()));

        let after = b"m";
        let cases: &[(&str, bt::RowRange, Option<row_range::StartKey>)] = &[
            (
                "unbounded start narrows to open(after)",
                mk_range(None, open_end(b"z")),
                open_start(after),
            ),
            (
                "closed start exactly at after narrows (would otherwise re-emit it)",
                mk_range(closed_start(after), open_end(b"z")),
                open_start(after),
            ),
            (
                "closed start past after is left alone",
                mk_range(closed_start(b"p"), open_end(b"z")),
                closed_start(b"p"),
            ),
            (
                "open start strictly before after narrows",
                mk_range(open_start(b"a"), open_end(b"z")),
                open_start(after),
            ),
            (
                "open start exactly at after is already correct",
                mk_range(open_start(after), open_end(b"z")),
                open_start(after),
            ),
            (
                "closed end past after keeps the range and narrows start",
                mk_range(closed_start(b"a"), closed_end(b"z")),
                open_start(after),
            ),
        ];

        for (name, range, expected_start) in cases {
            let got = trim_row_set(
                bt::RowSet {
                    row_keys: vec![],
                    row_ranges: vec![range.clone()],
                },
                after,
            );
            assert_eq!(got.row_ranges.len(), 1, "{name}");
            assert_eq!(&got.row_ranges[0].start_key, expected_start, "{name}");
            assert_eq!(
                got.row_ranges[0].end_key, range.end_key,
                "{name}: end_key must not be mutated",
            );
        }
    }

    #[test]
    fn trim_row_set_filters_discrete_keys() {
        let trimmed = trim_row_set(
            bt::RowSet {
                row_keys: vec![b"a".to_vec(), b"m".to_vec(), b"n".to_vec(), b"z".to_vec()],
                row_ranges: vec![],
            },
            b"m",
        );
        assert_eq!(trimmed.row_keys, vec![b"n".to_vec(), b"z".to_vec()]);
    }

    // ────────────────────────────────────────────────────────────────────
    // `ReadRows` state-machine tests
    //
    // Drive `handle_stream` with a scripted RPC outcome and observe the
    // resulting `ReadResult` actions and post-attempt `next_request`
    // shape.
    // ────────────────────────────────────────────────────────────────────

    fn base_ts() -> chrono::DateTime<chrono::Utc> {
        chrono::Utc.with_ymd_and_hms(2026, 5, 5, 18, 0, 0).unwrap()
    }

    fn mk_stats(name: &str, ts: chrono::DateTime<chrono::Utc>) -> CatalogStats {
        CatalogStats {
            meta: ops::Meta {
                uuid: "00000000-0000-0000-0000-000000000000".to_string(),
            },
            catalog_name: name.to_string(),
            ts,
            stats_summary: StatsSummary::default(),
            task_stats: TaskStats::default(),
        }
    }

    fn mk_chunk(
        row_key: &[u8],
        value: &[u8],
        status: Option<cell_chunk::RowStatus>,
    ) -> read_rows_response::CellChunk {
        read_rows_response::CellChunk {
            row_key: row_key.to_vec(),
            value: value.to_vec(),
            row_status: status,
            ..Default::default()
        }
    }

    fn commit(key: &[u8], stats: &CatalogStats) -> read_rows_response::CellChunk {
        let doc = serde_json::to_vec(stats).unwrap();
        mk_chunk(key, &doc, Some(cell_chunk::RowStatus::CommitRow(true)))
    }

    fn reset_chunk() -> read_rows_response::CellChunk {
        mk_chunk(b"", b"", Some(cell_chunk::RowStatus::ResetRow(true)))
    }

    fn mk_response(chunks: Vec<read_rows_response::CellChunk>) -> bt::ReadRowsResponse {
        bt::ReadRowsResponse {
            chunks,
            ..Default::default()
        }
    }

    // Serialize `stats` and split it across `n` chunks; only the first carries
    // `key`, only the last carries `CommitRow(true)`.
    fn split_commit(
        key: &[u8],
        stats: &CatalogStats,
        n: usize,
    ) -> Vec<read_rows_response::CellChunk> {
        let doc = serde_json::to_vec(stats).unwrap();
        (0..n)
            .map(|i| {
                let start = i * doc.len() / n;
                let end = (i + 1) * doc.len() / n;
                let row_key: &[u8] = if i == 0 { key } else { b"" };
                let status = (i + 1 == n).then_some(cell_chunk::RowStatus::CommitRow(true));
                mk_chunk(row_key, &doc[start..end], status)
            })
            .collect()
    }

    fn read_over_name_range(name: &str, hours: i64) -> ReadRows {
        let base = base_ts();
        let row_set = bt::RowSet {
            row_keys: vec![],
            row_ranges: vec![pack_row_range(
                name,
                &(base..base + chrono::Duration::hours(hours)),
            )],
        };
        ReadRows::new("test-table".to_string(), row_set, vec![])
    }

    fn next_request_range(state: &ReadRows) -> bt::RowRange {
        let req = state.next_request().expect("state should not be done");
        let mut row_set = req.rows.expect("request must have rows");
        assert_eq!(row_set.row_ranges.len(), 1, "single-range fixture");
        row_set.row_ranges.pop().unwrap()
    }

    async fn drive(
        state: &mut ReadRows,
        stream: std::result::Result<
            Vec<std::result::Result<bt::ReadRowsResponse, tonic::Status>>,
            tonic::Status,
        >,
    ) -> Vec<ReadResult> {
        let stream = stream.map(futures::stream::iter);
        let res = state.handle_stream(stream);
        let mut res = std::pin::pin!(res);
        let mut out = Vec::new();
        while let Some(action) = res.next().await {
            out.push(action);
        }
        out
    }

    fn assert_yields_then_done(label: &str, actions: &[ReadResult], expected: &[CatalogStats]) {
        assert_eq!(
            actions.len(),
            expected.len() + 1,
            "{label}: action count mismatch: {actions:?}",
        );
        for (i, (got, want)) in actions.iter().zip(expected).enumerate() {
            let ReadResult::Yield(s) = got else {
                panic!("{label}: action {i} is not Yield: {got:?}");
            };
            assert_eq!(s, want, "{label}: yielded row {i}");
        }
        assert!(
            matches!(actions.last(), Some(ReadResult::Done)),
            "{label}: trailing action must be Done, got {actions:?}",
        );
    }

    #[tokio::test]
    async fn happy_path_streams() {
        let ts = base_ts();
        let stats = mk_stats("foo", ts);
        let key = pack_row_key("foo", ts);
        let stats_a = mk_stats("acmeCo/a", ts);
        let stats_b = mk_stats("acmeCo/b", ts);
        let key_a = pack_row_key("acmeCo/a", ts);
        let key_b = pack_row_key("acmeCo/b", ts);
        let split2 = split_commit(&key, &stats, 2);

        // (label, name range, responses, expected rows)
        let cases: Vec<(&str, &str, Vec<bt::ReadRowsResponse>, Vec<CatalogStats>)> = vec![
            ("empty stream", "foo", vec![], vec![]),
            (
                "single committed row",
                "foo",
                vec![mk_response(vec![commit(&key, &stats)])],
                vec![stats.clone()],
            ),
            (
                "doc split across chunks in one response",
                "foo",
                vec![mk_response(split_commit(&key, &stats, 3))],
                vec![stats.clone()],
            ),
            (
                "doc split across two responses",
                "foo",
                vec![
                    mk_response(vec![split2[0].clone()]),
                    mk_response(vec![split2[1].clone()]),
                ],
                vec![stats.clone()],
            ),
            (
                "ResetRow discards partial buffer; retry commits cleanly",
                "foo",
                vec![mk_response(vec![
                    mk_chunk(&key, b"{garbled", None),
                    reset_chunk(),
                    commit(&key, &stats),
                ])],
                vec![stats.clone()],
            ),
            (
                "consecutive rows yield independently",
                "acmeCo",
                vec![mk_response(vec![
                    commit(&key_a, &stats_a),
                    commit(&key_b, &stats_b),
                ])],
                vec![stats_a.clone(), stats_b.clone()],
            ),
        ];

        for (label, name, responses, expected) in cases {
            let mut state = read_over_name_range(name, 1);
            let rpc: Vec<std::result::Result<bt::ReadRowsResponse, tonic::Status>> =
                responses.into_iter().map(Ok).collect();
            let actions = drive(&mut state, Ok(rpc)).await;
            assert_yields_then_done(label, &actions, &expected);
        }
    }

    #[tokio::test]
    async fn initial_status_yields_failed() {
        let mut read = read_over_name_range("foo", 1);
        let actions = drive(&mut read, Err(tonic::Status::unavailable("nope"))).await;
        let [ReadResult::Failed(Error::Grpc(status))] = &actions[..] else {
            panic!("expected [Failed(Grpc)], got {actions:?}");
        };
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn resume_after_mid_range_yield() {
        let stats = mk_stats("foo", base_ts());
        let key = pack_row_key("foo", base_ts());

        let mut read = read_over_name_range("foo", 3);
        let stream = drive(
            &mut read,
            Ok(vec![
                Ok(mk_response(vec![commit(&key, &stats)])),
                Err(tonic::Status::unavailable("")),
            ]),
        )
        .await;

        let [
            ReadResult::Yield(s),
            ReadResult::Failed(Error::Grpc(status)),
        ] = &stream[..]
        else {
            panic!("expected [Yield, Failed(Grpc)], got {stream:?}");
        };
        assert_eq!(s, &stats);
        assert_eq!(status.code(), tonic::Code::Unavailable);

        let range = next_request_range(&read);
        assert_eq!(
            range.start_key,
            Some(row_range::StartKey::StartKeyOpen(key)),
            "resume must start strictly past yielded key",
        );
    }

    #[tokio::test]
    async fn resume_after_yield_at_range_end_exhausts_row_set() {
        let stats = mk_stats("foo", base_ts() + chrono::Duration::hours(1));
        let key = pack_row_key("foo", base_ts() + chrono::Duration::hours(1));

        let mut read = read_over_name_range("foo", 1);
        let stream = drive(
            &mut read,
            Ok(vec![
                Ok(mk_response(vec![commit(&key, &stats)])),
                Err(tonic::Status::unavailable("")),
            ]),
        )
        .await;

        let [
            ReadResult::Yield(s),
            ReadResult::Failed(Error::Grpc(status)),
        ] = &stream[..]
        else {
            panic!("expected [Yield, Failed(Grpc)], got {stream:?}");
        };
        assert_eq!(s, &stats);
        assert_eq!(status.code(), tonic::Code::Unavailable);

        assert!(
            read.next_request().is_none(),
            "trim past range end must leave row_set empty",
        );
    }
}
