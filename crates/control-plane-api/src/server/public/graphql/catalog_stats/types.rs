//! GraphQL projections of the catalog stats model, and the pure functions that
//! build them.
//!
//! The types here mirror `ops::catalog_stats`. They are copies rather than
//! derives on the originals for three reasons: the leaf types are prost
//! generated in `proto-flow` and are rewritten by `build:rust-protobufs`; the
//! orphan rule forbids implementing `async_graphql::OutputType` on them from
//! this crate; and `ops` is a dependency of the data-plane runtime, which
//! should not link async-graphql.
//!
//! Nothing in this file performs IO. The resolver in the parent module does the
//! authorization and the BigTable read, then hands values here for conversion.

use super::super::UInt64;
use std::collections::BTreeMap;

/// Time grain at which a bucket aggregates.
// `remote` generates `From` in both directions against the `ops` enum, so no
// hand-written conversion is needed. Item casing follows the GraphQL convention
// of SCREAMING_SNAKE_CASE.
#[derive(Debug, Clone, Copy, PartialEq, Eq, async_graphql::Enum)]
#[graphql(remote = "ops::catalog_stats::Grain")]
pub enum CatalogStatsGrain {
    Hourly,
    Daily,
    Monthly,
}

/// A single stored reporting bucket. The stats for a single catalog name over a
/// single window of the requested time grain.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct CatalogStats {
    /// The name of the catalog that this stats bucket is associated with.
    /// The name is either a task, a collection, or a prefix that rolls up both.
    pub catalog_name: String,
    /// Time grain at which this bucket is aggregated over.
    pub grain: CatalogStatsGrain,
    /// Start of the window this bucket covers, always on a `grain` boundary:
    /// the hour for `HOURLY`, midnight for `DAILY`, and day 1 at midnight for
    /// `MONTHLY`. The window runs up to, but does not include, the next one.
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Combined totals across every task and collection contributing to the row.
    pub stats_summary: CatalogStatsSummary,
    /// Binding-level detail, present only on exact task rows. Prefix rollup
    /// rows aggregate across tasks and carry none.
    pub task_stats: Option<CatalogTaskStats>,
}

/// Combined totals across every task and collection contributing to a bucket.
///
/// A bucket's name is a task, a collection, or a prefix that rolls up both, so
/// which counters are non-zero depends on what the name refers to. The `ByMe`
/// pair counts what a task moved; the `FromMe` and `ToMe` pair counts what
/// flowed through a collection.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct CatalogStatsSummary {
    /// Documents a task read from the collections it sources from. Captures
    /// read from no collection, so this covers materializations and
    /// derivations.
    pub read_by_me: DocsAndBytes,
    /// Documents read out of a collection by the tasks that source from it.
    pub read_from_me: DocsAndBytes,
    /// Documents a task wrote to the collections it produces. Materializations
    /// write to an endpoint rather than a collection, so this covers captures
    /// and derivations.
    pub written_by_me: DocsAndBytes,
    /// Documents written into a collection by the tasks that produce it.
    pub written_to_me: DocsAndBytes,
    /// Total number of logged warnings.
    pub warnings: UInt64,
    /// Total number of logged errors.
    pub errors: UInt64,
    /// Total number of shard failures.
    pub failures: UInt64,
    /// Cumulative number of metered seconds of task usage, which is what
    /// usage billing is drawn from.
    pub usage_seconds: UInt64,
    /// Total number of transactions that have been successfully processed.
    pub txn_count: UInt64,
    /// The most recent publish timestamp of documents in this collection.
    pub last_published_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<ops::catalog_stats::StatsSummary> for CatalogStatsSummary {
    fn from(v: ops::catalog_stats::StatsSummary) -> Self {
        Self {
            read_by_me: v.read_by_me.into(),
            read_from_me: v.read_from_me.into(),
            written_by_me: v.written_by_me.into(),
            written_to_me: v.written_to_me.into(),
            warnings: v.warnings.into(),
            errors: v.errors.into(),
            failures: v.failures.into(),
            usage_seconds: v.usage_seconds.into(),
            txn_count: v.txn_count.into(),
            last_published_at: v.last_published_at.into(),
        }
    }
}

/// A count of documents and their cumulative size.
#[derive(Debug, Clone, Copy, async_graphql::SimpleObject)]
pub struct DocsAndBytes {
    /// The count of JSON documents.
    pub docs_total: UInt64,
    /// Cumulative total size of the documents, in bytes.
    pub bytes_total: UInt64,
}

impl From<ops::stats::DocsAndBytes> for DocsAndBytes {
    fn from(v: ops::stats::DocsAndBytes) -> Self {
        Self {
            docs_total: v.docs_total.into(),
            bytes_total: v.bytes_total.into(),
        }
    }
}

/// Per-task-kind breakouts. A given row populates at most one of these, since a
/// task is a capture, a derivation, or a materialization.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct CatalogTaskStats {
    /// List of stats for bindings of a capture task.
    /// Each entry represents stats for a single binding.
    pub capture: Vec<CaptureBindingStats>,
    /// Derivation stats.
    pub derive: Option<DeriveStats>,
    /// List of stats for bindings of a materialization task.
    /// Each entry represents stats for a single binding.
    pub materialize: Vec<MaterializeBindingStats>,
}

/// Stats for one binding of a capture, keyed by the collection it writes to.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct CaptureBindingStats {
    /// The collection name.
    pub collection: String,
    /// Documents from the connector, before combining.
    pub right: Option<DocsAndBytes>,
    /// Documents written to the collection, after combining.
    pub out: Option<DocsAndBytes>,
    /// The most recent publish timestamp of documents captured by this binding.
    pub last_published_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Stats for one binding of a materialization, keyed by its source collection.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct MaterializeBindingStats {
    /// The collection name.
    pub collection: String,
    /// Documents loaded from the endpoint.
    pub left: Option<DocsAndBytes>,
    /// Documents read from the source collection.
    pub right: Option<DocsAndBytes>,
    /// Documents stored to the endpoint.
    pub out: Option<DocsAndBytes>,
    /// The most recent publish timestamp from the source documents that were
    /// read by this binding.
    pub last_source_published_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Bytes behind across the binding's source journals. A historical row
    /// that predates this counter reports zero, indistinguishably from a
    /// binding that is genuinely caught up.
    pub bytes_behind: UInt64,
}

/// Stats for a derivation. A derivation has a single output collection, so
/// unlike captures and materializations there is one block rather than a list.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct DeriveStats {
    /// List of metrics for the transforms in this derivation.
    pub transforms: Vec<DeriveTransformStats>,
    /// Documents published by the connector, before combining.
    pub published: Option<DocsAndBytes>,
    /// Documents written to the derived collection, after combining.
    pub out: Option<DocsAndBytes>,
    /// The most recent publish timestamp of documents written to the derived collection.
    pub last_published_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<ops::stats::Derive> for DeriveStats {
    fn from(v: ops::stats::Derive) -> Self {
        Self {
            transforms: v
                .transforms
                .into_iter()
                .map(|(transform, t)| transform_entry(transform, t))
                .collect(),
            published: v.published.map(Into::into),
            out: v.out.map(Into::into),
            last_published_at: timestamp(v.last_published_at),
        }
    }
}

/// Stats for one transform of a derivation. Unlike the capture and materialize
/// breakouts, these are keyed by transform name, and the collection read is a
/// separate field.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct DeriveTransformStats {
    /// The transform name.
    pub transform: String,
    /// The name of the collection that this transform sourced from.
    pub source: String,
    /// Input documents that were read by this transform.
    pub input: Option<DocsAndBytes>,
    /// The most recent publish timestamp from the source documents that were read by this transform.
    pub last_source_published_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Bytes behind across the transform's source journals. A historical row
    /// that predates this counter reports zero, indistinguishable from a
    /// transform that is genuinely caught up.
    pub bytes_behind: UInt64,
}

/// Protobuf timestamps carry seconds and nanos, which may not land inside
/// chrono's range. These fields are connector-reported and informational, so an
/// unrepresentable value reads as unknown rather than failing the whole query.
fn timestamp(v: Option<proto_flow::Timestamp>) -> Option<chrono::DateTime<chrono::Utc>> {
    v.and_then(|ts| chrono::DateTime::<chrono::Utc>::try_from(ts).ok())
}

fn capture_entry(collection: String, b: ops::stats::CaptureBinding) -> CaptureBindingStats {
    CaptureBindingStats {
        collection,
        right: b.right.map(Into::into),
        out: b.out.map(Into::into),
        last_published_at: timestamp(b.last_published_at),
    }
}

fn materialize_entry(
    collection: String,
    b: ops::stats::MaterializeBinding,
) -> MaterializeBindingStats {
    MaterializeBindingStats {
        collection,
        left: b.left.map(Into::into),
        right: b.right.map(Into::into),
        out: b.out.map(Into::into),
        last_source_published_at: timestamp(b.last_source_published_at),
        bytes_behind: b.bytes_behind.into(),
    }
}

fn transform_entry(transform: String, t: ops::stats::derive::Transform) -> DeriveTransformStats {
    DeriveTransformStats {
        transform,
        source: t.source,
        input: t.input.map(Into::into),
        last_source_published_at: timestamp(t.last_source_published_at),
        bytes_behind: t.bytes_behind.into(),
    }
}

/// `TaskStats` is always present on the wire but is simply empty for prefix
/// rollups. Collapse that to `None` so clients can tell a task row from a
/// rollup without inspecting three sub-fields.
fn task_stats(v: ops::catalog_stats::TaskStats) -> Option<CatalogTaskStats> {
    let ops::catalog_stats::TaskStats {
        capture,
        derive,
        materialize,
    } = v;

    if capture.is_empty() && derive.is_none() && materialize.is_empty() {
        return None;
    }

    Some(CatalogTaskStats {
        capture: entries(capture, capture_entry),
        derive: derive.map(Into::into),
        materialize: entries(materialize, materialize_entry),
    })
}

/// Projects a keyed map to a list of entries that carry their own key.
/// `BTreeMap` iterates in key order, so the result is deterministic without a
/// sort, which keeps snapshots stable.
fn entries<T, U>(map: BTreeMap<String, T>, f: impl Fn(String, T) -> U) -> Vec<U> {
    map.into_iter().map(|(key, value)| f(key, value)).collect()
}

/// Builds one bucket. `grain` comes from the query rather than the row, because
/// a stored row records only its name and timestamp.
pub fn bucket(grain: CatalogStatsGrain, row: ops::catalog_stats::CatalogStats) -> CatalogStats {
    // `_meta.uuid` is deliberately dropped: it identifies the materialization
    // that wrote the row and differs between the BigTable and Postgres copies
    // of the same data, so it is an implementation artifact.
    CatalogStats {
        catalog_name: row.catalog_name,
        grain,
        timestamp: row.ts,
        stats_summary: row.stats_summary.into(),
        task_stats: task_stats(row.task_stats),
    }
}

/// Validates the given start-end range for the specified time grain. Returns an Error if
/// `end` is before `start` or if the time range does not align with the time grain boundary.
///
/// Alignment matters because the Bigtable scan is a half-open range over row keys that
/// pack the formatted timestamp as a string. A `start` of 10:30 against a daily
/// grain sorts after that day's `00:00` key and will silently drop the `00:00` bucket.
pub fn validate_range(
    grain: CatalogStatsGrain,
    start: chrono::DateTime<chrono::Utc>,
    end: chrono::DateTime<chrono::Utc>,
) -> async_graphql::Result<()> {
    if start >= end {
        return Err(async_graphql::Error::new(format!(
            "`start` ({}) must be before `end` ({})",
            start.to_rfc3339(),
            end.to_rfc3339(),
        )));
    }
    for (label, ts) in [("start", start), ("end", end)] {
        if !is_aligned(grain, ts) {
            return Err(async_graphql::Error::new(format!(
                "`{label}` ({}) is not aligned to the {} grain; a bucket that straddles it \
                 would be omitted from the results",
                ts.to_rfc3339(),
                ops::catalog_stats::Grain::from(grain),
            )));
        }
    }
    Ok(())
}

fn is_aligned(grain: CatalogStatsGrain, ts: chrono::DateTime<chrono::Utc>) -> bool {
    use chrono::{Datelike as _, Timelike as _};

    let sub_hour = ts.minute() == 0 && ts.second() == 0 && ts.nanosecond() == 0;

    match grain {
        CatalogStatsGrain::Hourly => sub_hour,
        CatalogStatsGrain::Daily => sub_hour && ts.hour() == 0,
        CatalogStatsGrain::Monthly => sub_hour && ts.hour() == 0 && ts.day() == 1,
    }
}

/// Collects the rows of a `ReadRows` stream into buckets. Returns an error if the number
/// of rows in the stream exceeds the specified `max_buckets`. Will attempt to retry on
/// transient error up to `max_attempts` times.
pub async fn collect_buckets(
    stream: impl futures::Stream<Item = catalog_stats::RetryResult<ops::catalog_stats::CatalogStats>>,
    grain: CatalogStatsGrain,
    max_buckets: usize,
    max_attempts: usize,
) -> async_graphql::Result<Vec<CatalogStats>> {
    let mut stream = std::pin::pin!(stream);
    let mut out = Vec::new();

    while let Some(item) = futures::StreamExt::next(&mut stream).await {
        let err = match item {
            Ok(row) => {
                if out.len() == max_buckets {
                    // Dropping the stream here cancels the in-flight read.
                    return Err(async_graphql::Error::new(format!(
                        "query matches more than {max_buckets} buckets; narrow `names` or the \
                         time range, or request a coarser grain"
                    )));
                }
                out.push(bucket(grain, row));
                continue;
            }
            Err(err) => err,
        };

        // Need to check if the client error is not transient. The client yields transient failures
        // and keeps the stream alive for another poll, having already applied its own backoff. A
        // non-transient failure is yielded and then *ends* the stream. Without this check non-transient
        // failure would look like a truncated success.
        if !err.inner.is_transient() || err.attempt >= max_attempts {
            return Err(async_graphql::Error::new(format!(
                "reading catalog stats: {}",
                err.inner
            )));
        }
        tracing::warn!(
            attempt = err.attempt,
            error = %err.inner,
            "retrying catalog stats read",
        );
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone as _;

    fn ts(hour: u32) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc
            .with_ymd_and_hms(2026, 5, 5, hour, 0, 0)
            .unwrap()
    }

    fn docs_and_bytes(docs: u64, bytes: u64) -> ops::stats::DocsAndBytes {
        ops::stats::DocsAndBytes {
            docs_total: docs,
            bytes_total: bytes,
        }
    }

    fn proto_ts(seconds: i64) -> Option<proto_flow::Timestamp> {
        Some(proto_flow::Timestamp { seconds, nanos: 0 })
    }

    fn row(
        name: &str,
        task_stats: ops::catalog_stats::TaskStats,
    ) -> ops::catalog_stats::CatalogStats {
        ops::catalog_stats::CatalogStats {
            meta: ops::Meta {
                uuid: "00000000-0000-0000-0000-000000000000".to_string(),
            },
            catalog_name: name.to_string(),
            ts: ts(18),
            stats_summary: ops::catalog_stats::StatsSummary {
                read_by_me: docs_and_bytes(1, 2),
                read_from_me: docs_and_bytes(3, 4),
                written_by_me: docs_and_bytes(5, 6),
                // Above 2^53, where a JS number stops being exact. The
                // `UInt64` scalar sends it as a string, so it round-trips.
                written_to_me: docs_and_bytes(7, 9_007_199_254_740_993),
                warnings: 1,
                errors: 2,
                failures: 3,
                usage_seconds: 4,
                txn_count: 5,
                last_published_at: Some(ts(18)),
            },
            task_stats,
        }
    }

    /// A `u64` above 2^53 cannot be represented exactly by a JSON number, which
    /// is what a JS client parses into. Executing a real query rather than
    /// inspecting the struct is the point: it is the scalar's `to_value` that
    /// has to emit a string, and only serialization exercises it.
    #[tokio::test]
    async fn large_counters_serialize_as_strings_without_losing_precision() {
        struct StubQuery;

        #[async_graphql::Object]
        impl StubQuery {
            async fn stats(&self) -> CatalogStats {
                bucket(
                    CatalogStatsGrain::Hourly,
                    row("acmeCo/task", Default::default()),
                )
            }
        }

        let schema = async_graphql::Schema::new(
            StubQuery,
            async_graphql::EmptyMutation,
            async_graphql::EmptySubscription,
        );
        let response = schema
            .execute("{ stats { statsSummary { writtenToMe { bytesTotal } } } }")
            .await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);

        let data = &serde_json::to_value(&response.data).unwrap();
        let bytes_total = &data["stats"]["statsSummary"]["writtenToMe"]["bytesTotal"];

        // A JSON number here would round to 9007199254740992 in JS client.
        // Expect the string to maintain precision.
        assert_eq!(bytes_total, &serde_json::json!("9007199254740993"));
        assert!(
            bytes_total.is_string(),
            "expected a string, got {bytes_total}"
        );
    }

    #[test]
    fn rollup_rows_have_no_task_stats() {
        let bucket = bucket(CatalogStatsGrain::Daily, row("acmeCo/", Default::default()));
        assert!(bucket.task_stats.is_none());
        assert_eq!(bucket.grain, CatalogStatsGrain::Daily);
    }

    #[test]
    fn task_rows_project_binding_maps_to_entries() {
        let mut capture = BTreeMap::new();
        // Inserted out of order to show the output follows key order.
        capture.insert(
            "acmeCo/second".to_string(),
            ops::stats::CaptureBinding {
                right: Some(docs_and_bytes(1, 2)),
                out: None,
                last_published_at: None,
            },
        );
        capture.insert(
            "acmeCo/first".to_string(),
            ops::stats::CaptureBinding {
                right: None,
                out: Some(docs_and_bytes(3, 4)),
                last_published_at: proto_ts(1_700_000_000),
            },
        );

        let bucket = bucket(
            CatalogStatsGrain::Hourly,
            row(
                "acmeCo/capture",
                ops::catalog_stats::TaskStats {
                    capture,
                    ..Default::default()
                },
            ),
        );

        insta::assert_debug_snapshot!(bucket.task_stats.unwrap().capture);
    }

    #[test]
    fn derive_transforms_are_keyed_by_transform_not_collection() {
        let mut transforms = BTreeMap::new();
        transforms.insert(
            "my-transform".to_string(),
            ops::stats::derive::Transform {
                source: "acmeCo/source".to_string(),
                input: Some(docs_and_bytes(1, 2)),
                last_source_published_at: None,
                bytes_behind: 99,
            },
        );

        let derive = DeriveStats::from(ops::stats::Derive {
            transforms,
            published: None,
            out: Some(docs_and_bytes(3, 4)),
            last_published_at: None,
        });

        let entry = &derive.transforms[0];
        assert_eq!(entry.transform, "my-transform");
        assert_eq!(entry.source, "acmeCo/source");
        assert_eq!(entry.bytes_behind, UInt64(99));
    }

    #[test]
    fn unrepresentable_timestamps_read_as_unknown() {
        assert!(timestamp(None).is_none());
        assert!(timestamp(proto_ts(1_700_000_000)).is_some());
        // Negative nanos cannot be converted, and must not fail the query.
        assert!(
            timestamp(Some(proto_flow::Timestamp {
                seconds: 0,
                nanos: -1,
            }))
            .is_none()
        );
    }

    #[test]
    fn range_must_be_non_empty_and_grain_aligned() {
        let day = |d: u32| chrono::Utc.with_ymd_and_hms(2026, 5, d, 0, 0, 0).unwrap();

        assert!(validate_range(CatalogStatsGrain::Daily, day(1), day(2)).is_ok());
        assert!(validate_range(CatalogStatsGrain::Daily, day(2), day(2)).is_err());
        assert!(validate_range(CatalogStatsGrain::Daily, day(3), day(2)).is_err());

        // An hour offset is fine hourly, but straddles a daily bucket.
        assert!(validate_range(CatalogStatsGrain::Hourly, ts(1), ts(2)).is_ok());
        assert!(validate_range(CatalogStatsGrain::Daily, ts(1), day(6)).is_err());

        // Monthly additionally requires the first of the month.
        let month = |m: u32| chrono::Utc.with_ymd_and_hms(2026, m, 1, 0, 0, 0).unwrap();
        assert!(validate_range(CatalogStatsGrain::Monthly, month(5), month(6)).is_ok());
        assert!(validate_range(CatalogStatsGrain::Monthly, day(2), month(6)).is_err());

        // Sub-minute precision is misaligned at every grain.
        let offset = ts(1) + chrono::Duration::seconds(30);
        assert!(validate_range(CatalogStatsGrain::Hourly, offset, ts(2)).is_err());
    }

    fn transient() -> catalog_stats::RetryError {
        catalog_stats::Error::Grpc(tonic::Status::unavailable("try again")).with_attempt(0)
    }

    fn fatal() -> catalog_stats::RetryError {
        catalog_stats::Error::Internal(anyhow::anyhow!("bad row")).with_attempt(0)
    }

    async fn collect(
        items: Vec<catalog_stats::RetryResult<ops::catalog_stats::CatalogStats>>,
        max_buckets: usize,
        max_attempts: usize,
    ) -> async_graphql::Result<Vec<CatalogStats>> {
        collect_buckets(
            futures::stream::iter(items),
            CatalogStatsGrain::Hourly,
            max_buckets,
            max_attempts,
        )
        .await
    }

    #[tokio::test]
    async fn collects_rows_and_recovers_from_transient_errors() {
        let items = vec![
            Ok(row("acmeCo/a", Default::default())),
            Err(transient()),
            Ok(row("acmeCo/b", Default::default())),
        ];
        let got = collect(items, 10, 3).await.unwrap();

        let names: Vec<&str> = got.iter().map(|b| b.catalog_name.as_str()).collect();
        assert_eq!(names, ["acmeCo/a", "acmeCo/b"]);
    }

    #[tokio::test]
    async fn exhausting_the_retry_budget_errors() {
        let items = vec![Err(catalog_stats::Error::Grpc(tonic::Status::unavailable(
            "",
        ))
        .with_attempt(3))];
        assert!(collect(items, 10, 3).await.is_err());
    }

    #[tokio::test]
    async fn a_fatal_error_is_not_a_truncated_success() {
        // The client yields a non-transient error and then ends the stream.
        // Treating every error as retryable would turn this into an empty Ok,
        // which is the failure mode this test exists to catch.
        let items = vec![Ok(row("acmeCo/a", Default::default())), Err(fatal())];
        assert!(collect(items, 10, 3).await.is_err());
    }

    #[tokio::test]
    async fn exceeding_the_bucket_cap_errors_rather_than_truncating() {
        let items = vec![
            Ok(row("acmeCo/a", Default::default())),
            Ok(row("acmeCo/b", Default::default())),
            Ok(row("acmeCo/c", Default::default())),
        ];
        let err = collect(items, 2, 3).await.unwrap_err();
        assert!(
            err.message.contains("more than 2 buckets"),
            "{}",
            err.message
        );
    }
}
