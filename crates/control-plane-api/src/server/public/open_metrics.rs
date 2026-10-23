use chrono::{Datelike, TimeZone};
use futures::StreamExt;
use std::fmt::Write;

#[axum::debug_handler(state=std::sync::Arc<crate::App>)]
pub async fn handle_get_metrics(
    axum::extract::State(app): axum::extract::State<std::sync::Arc<crate::App>>,
    env: crate::Envelope,
    axum::extract::Path(prefix): axum::extract::Path<String>,
) -> Result<axum::response::Response, crate::ApiError> {
    if !prefix.ends_with('/') {
        return Err(tonic::Status::invalid_argument(format!(
            "prefix {prefix:?} must end with a trailing '/' slash"
        ))
        .into());
    }

    let policy_result = crate::evaluate_names_authorization(
        env.snapshot(),
        env.claims()?,
        models::Capability::Read,
        [&prefix],
    );
    let (_expiry, ()) = env.authorization_outcome(policy_result).await?;

    let Some(client) = app.catalog_stats.as_deref() else {
        return Err(tonic::Status::internal("catalog stats are not configured").into());
    };

    // Map `started` to midnight at the open of the current month.
    let started = env.started;
    let started_month = chrono::Utc
        .with_ymd_and_hms(started.year(), started.month(), 1, 0, 0, 0)
        .unwrap();
    let next_month = started_month
        .checked_add_months(chrono::Months::new(1))
        .ok_or_else(|| tonic::Status::invalid_argument("started month is out of range"))?;

    let scrape_at = started.timestamp_micros() as f64 / 1_000_000f64;

    // Stream the month's stats under `prefix` from BigTable, encoding into metrics.
    // Some OpenMetrics clients (Python, grr) expect that metric metadata
    // and all samples appear contiguously, so we jump through some hoops
    // to efficiently accumulate chunked buffers that we can yield in the
    // correct order once we're done.
    let mut buf = BufferParts::new();

    for metric in REGISTRY {
        metric.declare(&mut buf);
    }

    let stats = client.fetch_range_for_prefix(
        &prefix,
        catalog_stats::Grain::Monthly,
        started_month..next_month,
    );
    let mut stats = std::pin::pin!(stats);

    while let Some(item) = stats.next().await {
        // Incomplete scrapes would under-report counters.
        let row = item.map_err(|err| {
            tonic::Status::unavailable(format!("reading catalog stats: {}", err.inner))
        })?;
        // Rows named with a trailing '/' are rollups of the prefix and its
        // sub-prefixes, which aggregate the tasks and collections below them.
        if !row.catalog_name.ends_with('/') {
            encode_metrics(&mut buf, scrape_at, row);
        }
    }

    let stream = coroutines::try_coroutine(move |mut co| async move {
        for group in buf.groups {
            for chunk in group {
                () = co.yield_(chunk.freeze()).await;
            }
        }
        () = co.yield_(bytes::Bytes::from_static(b"# EOF\n")).await;

        Ok::<(), std::convert::Infallible>(())
    });

    Ok(axum::response::Response::builder()
        .header(
            axum::http::header::CONTENT_TYPE,
            // Use Prometheus format, not OpenMetrics, because parser support is immature:
            "text/plain; version=1.0.0; charset=utf-8",
            // Alternative OpenMetrics Content-Type which we're not using:
            // "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )
        .body(axum::body::Body::from_stream(stream))
        .expect("response headers are valid"))
}

fn encode_metrics(buf: &mut BufferParts, _scrape_at: f64, stats: catalog_stats::CatalogStats) {
    let catalog_stats::CatalogStats {
        catalog_name,
        meta: ops::Meta { uuid: _ },
        ts: _,
        stats_summary:
            catalog_stats::StatsSummary {
                errors,
                warnings,
                failures,
                read_by_me,
                read_from_me,
                written_by_me,
                written_to_me,
                usage_seconds,
                txn_count,
                last_published_at: _,
            },
        task_stats:
            catalog_stats::TaskStats {
                capture,
                derive,
                materialize,
            },
    } = stats;

    let l_collection = format!("collection={catalog_name:?}");
    let l_task = format!("task={catalog_name:?}");

    LOGGED_WARNINGS.counter(buf, &l_task, warnings);
    LOGGED_ERRORS.counter(buf, &l_task, errors);
    LOGGED_FAILURES.counter(buf, &l_task, failures);
    TXN_COUNT.counter(buf, &l_task, txn_count);

    // Task-centric roll-ups.
    READ_BY_ME_BYTES.counter(buf, &l_task, read_by_me.bytes_total);
    READ_BY_ME_DOCS.counter(buf, &l_task, read_by_me.docs_total);
    WRITTEN_BY_ME_BYTES.counter(buf, &l_task, written_by_me.bytes_total);
    WRITTEN_BY_ME_DOCS.counter(buf, &l_task, written_by_me.docs_total);
    USAGE_SECONDS.counter(buf, &l_task, usage_seconds);

    // Collection-centric roll-ups.
    READ_FROM_ME_BYTES.counter(buf, &l_collection, read_from_me.bytes_total);
    READ_FROM_ME_DOCS.counter(buf, &l_collection, read_from_me.docs_total);
    WRITTEN_TO_ME_BYTES.counter(buf, &l_collection, written_to_me.bytes_total);
    WRITTEN_TO_ME_DOCS.counter(buf, &l_collection, written_to_me.docs_total);

    for (
        collection,
        ops::stats::CaptureBinding {
            out,
            right,
            last_published_at,
        },
    ) in capture
    {
        let l_task_collection = format!("task={catalog_name:?},collection={collection:?}");

        if let Some(m) = last_published_at {
            let ts = to_time_seconds(m);
            CAPTURED_LAST_PUBLISHED_AT.gauge(buf, &l_task_collection, ts, false);
        }
        if let Some(m) = right {
            CAPTURED_IN_BYTES.counter(buf, &l_task_collection, m.bytes_total);
            CAPTURED_IN_DOCS.counter(buf, &l_task_collection, m.docs_total);
        }
        if let Some(m) = out {
            CAPTURED_OUT_BYTES.counter(buf, &l_task_collection, m.bytes_total);
            CAPTURED_OUT_DOCS.counter(buf, &l_task_collection, m.docs_total);
        }
    }

    if let Some(ops::stats::Derive {
        out,
        published,
        transforms,
        last_published_at,
    }) = derive
    {
        if let Some(m) = last_published_at {
            let ts = to_time_seconds(m);
            DERIVED_LAST_PUBLISHED_AT.gauge(buf, &l_task, ts, false);
        }
        for (
            transform,
            ops::stats::derive::Transform {
                last_source_published_at,
                source: collection,
                input,
                bytes_behind,
            },
        ) in transforms
        {
            let l_task_collection_transform =
                format!("task={catalog_name:?},collection={collection:?},transform={transform:?}");

            if let Some(m) = last_source_published_at {
                let ts = to_time_seconds(m);
                DERIVED_LAST_SOURCE_PUBLISHED_AT.gauge(
                    buf,
                    &l_task_collection_transform,
                    ts,
                    false,
                );
            }
            DERIVED_BYTES_BEHIND.gauge(
                buf,
                &l_task_collection_transform,
                bytes_behind as f64,
                // TODO(whb): Emit explicit zero values at some point in the
                // future when stats have been widely populated with
                // bytesBehind. For now, suppress zero values to avoid
                // presenting a false 0 for historical stats where bytesBehind
                // is absent.
                false,
            );
            if let Some(m) = input {
                DERIVED_IN_BYTES.counter(buf, &l_task_collection_transform, m.bytes_total);
                DERIVED_IN_DOCS.counter(buf, &l_task_collection_transform, m.docs_total);
            }
        }
        if let Some(m) = out {
            DERIVED_OUT_BYTES.counter(buf, &l_task, m.bytes_total);
            DERIVED_OUT_DOCS.counter(buf, &l_task, m.docs_total);
        }
        if let Some(m) = published {
            DERIVED_YIELD_BYTES.counter(buf, &l_task, m.bytes_total);
            DERIVED_YIELD_DOCS.counter(buf, &l_task, m.docs_total);
        }
    }

    for (
        collection,
        ops::stats::MaterializeBinding {
            last_source_published_at,
            bytes_behind,
            left,
            right,
            out,
        },
    ) in materialize
    {
        let l_task_collection = format!("task={catalog_name:?},collection={collection:?}");

        if let Some(m) = last_source_published_at {
            let ts = to_time_seconds(m);
            MATERIALIZED_LAST_SOURCE_PUBLISHED_AT.gauge(buf, &l_task_collection, ts, false);
        }
        // TODO(whb): Emit explicit zero values at some point in the future when
        // stats have been widely populated with bytesBehind. For now, suppress
        // zero values to avoid presenting a false 0 for historical stats where
        // bytesBehind is absent.
        MATERIALIZED_BYTES_BEHIND.gauge(buf, &l_task_collection, bytes_behind as f64, false);
        if let Some(m) = right {
            MATERIALIZED_IN_BYTES.counter(buf, &l_task_collection, m.bytes_total);
            MATERIALIZED_IN_DOCS.counter(buf, &l_task_collection, m.docs_total);
        }
        if let Some(m) = left {
            MATERIALIZED_LOAD_BYTES.counter(buf, &l_task_collection, m.bytes_total);
            MATERIALIZED_LOAD_DOCS.counter(buf, &l_task_collection, m.docs_total);
        }
        if let Some(m) = out {
            MATERIALIZED_OUT_BYTES.counter(buf, &l_task_collection, m.bytes_total);
            MATERIALIZED_OUT_DOCS.counter(buf, &l_task_collection, m.docs_total);
        }
    }
}

fn to_time_seconds(pbts: proto_flow::Timestamp) -> f64 {
    pbts.seconds as f64 + (pbts.nanos as f64 / 1_000_000_000.0)
}

struct Metric {
    index: usize,
    name: &'static str,
    help: &'static str,
    type_: &'static str,
}

impl Metric {
    fn declare(&self, buf: &mut BufferParts) {
        let buf = buf.get(self);
        let Self {
            index: _,
            name,
            help,
            type_,
        } = self;

        writeln!(buf, "# HELP {name} {help}").unwrap();
        writeln!(buf, "# TYPE {name} {type_}").unwrap();
    }

    fn counter(&self, buf: &mut BufferParts, labels: &str, value: u64) {
        let Self { name, .. } = self;

        if value != 0 {
            writeln!(buf.get(self), "{name}{{{labels}}} {value}").unwrap();
        }
    }

    fn gauge(&self, buf: &mut BufferParts, labels: &str, value: f64, emit_zero: bool) {
        let Self { name, .. } = self;

        if value != 0.0 || emit_zero {
            writeln!(buf.get(self), "{name}{{{labels}}} {value}").unwrap();
        }
    }
}

struct BufferParts {
    groups: Vec<Vec<bytes::BytesMut>>,
}

impl BufferParts {
    const BUF_CAP: usize = 16 * 1024;
    const BUF_TGT: usize = (Self::BUF_CAP * 85) / 100;

    fn new() -> Self {
        Self {
            groups: vec![Default::default(); REGISTRY.len()],
        }
    }

    fn get<'s>(&'s mut self, metric: &Metric) -> &'s mut bytes::BytesMut {
        match self.groups[metric.index].last() {
            Some(buf) if buf.len() < Self::BUF_TGT => {}
            Some(_ /* full */) | None => {
                self.groups[metric.index].push(bytes::BytesMut::with_capacity(Self::BUF_CAP));
            }
        };
        self.groups[metric.index].last_mut().unwrap()
    }
}

macro_rules! define_metrics {
    // Entry rule: match one or more Metric definitions separated by commas.
    (
        $(
            $ident:ident = Metric {
                $($field:ident : $value:expr),* $(,)?
            }
        ),+ $(,)?
    ) => {
        // Step 1: Recursively define each metric const with an incrementing index.
        define_metrics!(@define_consts 0; $($ident = { $($field : $value),* }),+);

        // Step 2: Build the REGISTRY array in the same order.
        define_metrics!(@define_registry [ $($ident),+ ]);
    };

    // Base case for recursion: no more items to define.
    (@define_consts $_idx:expr; ) => {};

    // Recursive case: define the next metric using `$_idx` as `index`,
    // then increment and recurse for the rest.
    (@define_consts $idx:expr; $ident:ident = { $($field:ident : $value:expr),* } $(, $($rest:tt)+)?) => {
        const $ident: Metric = Metric {
            index: $idx,
            $($field: $value),*
        };
        define_metrics!(@define_consts $idx + 1; $($($rest)+)?);
    };

    // Finally, define the REGISTRY slice referencing them in order.
    (@define_registry [ $($ident:ident),+ ]) => {
        const REGISTRY: &'static [Metric] = &[
            $($ident),+
        ];
    };
}

// When testing changes to metrics, feed an example scrape as stdin of:
//   docker run --rm -i --entrypoint '' prom/prometheus:latest promtool check metrics
define_metrics! {
    LOGGED_WARNINGS= Metric {
        name: "logged_warnings_total",
        type_: COUNTER,
        help: "Total log lines at level WARN, by task",
    },
    LOGGED_ERRORS= Metric {
        name: "logged_errors_total",
        type_: COUNTER,
        help: "Total log lines at level ERROR, by task",
    },
    LOGGED_FAILURES= Metric {
        name: "logged_failures_total",
        type_: COUNTER,
        help: "Total log lines indicating task failure, by task",
    },
    TXN_COUNT= Metric {
        name: "txn_count_total",
        type_: COUNTER,
        help: "Total number of transactions processed by this task, by task",
    },
    READ_BY_ME_BYTES= Metric {
        name: "read_by_me_bytes_total",
        type_: COUNTER,
        help: "Total number of collection bytes read by this task, by task",
    },
    READ_BY_ME_DOCS= Metric {
        name: "read_by_me_docs_total",
        type_: COUNTER,
        help: "Total number of collection documents read by this task, by task",
    },
    WRITTEN_BY_ME_BYTES= Metric {
        name: "written_by_me_bytes_total",
        type_: COUNTER,
        help: "Total number of collection bytes written by this task, by task",
    },
    WRITTEN_BY_ME_DOCS= Metric {
        name: "written_by_me_docs_total",
        type_: COUNTER,
        help: "Total number of collection documents written by this task, by task",
    },
    USAGE_SECONDS= Metric {
        name: "usage_seconds_total",
        type_: COUNTER,
        help: "Total number of billable seconds of connector usage time, by task",
    },
    READ_FROM_ME_BYTES= Metric {
        name: "read_from_me_bytes_total",
        type_: COUNTER,
        help: "Total number of collection bytes read from this source, by collection",
    },
    READ_FROM_ME_DOCS= Metric {
        name: "read_from_me_docs_total",
        type_: COUNTER,
        help: "Total number of collection documents read from this source, by collection",
    },
    WRITTEN_TO_ME_BYTES= Metric {
        name: "written_to_me_bytes_total",
        type_: COUNTER,
        help: "Total number of collection bytes written to this target, by collection",
    },
    WRITTEN_TO_ME_DOCS= Metric {
        name: "written_to_me_docs_total",
        type_: COUNTER,
        help: "Total number of collection documents written to this target, by collection",
    },
    CAPTURED_LAST_PUBLISHED_AT= Metric {
        name: "captured_last_published_at_time_seconds",
        help: "Publication timestamp of the most recently captured document, given as seconds since the unix epoch, by task and target collection",
        type_: GAUGE,
    },
    CAPTURED_IN_BYTES= Metric {
        name: "captured_in_bytes_total",
        type_: COUNTER,
        help:
            "Total number of pre-combine bytes captured by the connector, by task and target collection",
    },
    CAPTURED_IN_DOCS= Metric {
        name: "captured_in_docs_total",
        type_: COUNTER,
        help: "Total number of pre-combine documents captured by the connector, by task and target collection",
    },
    CAPTURED_OUT_BYTES= Metric {
        name: "captured_out_bytes_total",
        help: "Total number of post-combine bytes captured by the connector, by task and target collection",
        type_: COUNTER,
    },
    CAPTURED_OUT_DOCS= Metric {
        name: "captured_out_docs_total",
        help: "Total number of post-combine documents captured by the connector, by task and target collection",
        type_: COUNTER,
    },
    DERIVED_LAST_PUBLISHED_AT= Metric {
        name: "derived_last_published_at_time_seconds",
        help: "Publication timestamp of the most recently derived document, given as seconds since the unix epoch, by task",
        type_: GAUGE,
    },
    DERIVED_LAST_SOURCE_PUBLISHED_AT= Metric {
        name: "derived_last_source_published_at_time_seconds",
        help: "Publication timestamp of the most recent source collection document that was processed by the derivation, given as seconds since the unix epoch",
        type_: GAUGE,
    },
    DERIVED_BYTES_BEHIND= Metric {
        name: "derived_bytes_behind",
        help: "Bytes behind of the source journals for this transform, by task, source collection, and transform",
        type_: GAUGE,
    },
    DERIVED_IN_BYTES= Metric {
        name: "derived_in_bytes_total",
        help: "Total number of pre-reduce bytes read from the source collection, by task, source collection, and transform",
        type_: COUNTER,
    },
    DERIVED_IN_DOCS = Metric {
        name: "derived_in_docs_total",
        help: "Total number of pre-reduce documents read from the source collection, by task, source collection, and transform",
        type_: COUNTER,
    },
    DERIVED_OUT_BYTES= Metric {
        name: "derived_out_bytes_total",
        help: "Total number of post-combine bytes published by derivation transforms, by task",
        type_: COUNTER,
    },
    DERIVED_OUT_DOCS= Metric {
        name: "derived_out_docs_total",
        help: "Total number of post-combine documents published by derivation transforms, by task",
        type_: COUNTER,
    },
    DERIVED_YIELD_BYTES= Metric {
        name: "derived_yield_bytes_total",
        help: "Total number of pre-combine bytes published by derivation transforms, by task",
        type_: COUNTER,
    },
    DERIVED_YIELD_DOCS= Metric {
        name: "derived_yield_docs_total",
        help: "Total number of pre-combine documents published by derivation transforms, by task",
        type_: COUNTER,
    },
    MATERIALIZED_LAST_SOURCE_PUBLISHED_AT= Metric {
        name: "materialized_last_source_published_at_time_seconds",
        help: "Publication timestamp of the most recent source collection document that was materialized, given as seconds since the unix epoch",
        type_: GAUGE,
    },
    MATERIALIZED_BYTES_BEHIND= Metric {
        name: "materialized_bytes_behind",
        help: "Bytes behind of the source journals for this binding, by task and source collection",
        type_: GAUGE,
    },
    MATERIALIZED_IN_BYTES= Metric {
        name: "materialized_in_bytes_total",
        help: "Total number of pre-reduce bytes read from the source collection, by task and source collection",
        type_: COUNTER,
    },
    MATERIALIZED_IN_DOCS= Metric {
        name: "materialized_in_docs_total",
        help:
            "Total number of pre-reduce documents read from the source collection, by task and source collection",
        type_: COUNTER,
    },
    MATERIALIZED_LOAD_BYTES= Metric {
        name: "materialized_load_bytes_total",
        help: "Total number of pre-reduce bytes loaded from the target, by task and source collection",
        type_: COUNTER,
    },
    MATERIALIZED_LOAD_DOCS = Metric {
        name: "materialized_load_docs_total",
        help:
            "Total number of pre-reduce documents loaded from the target, by task and source collection",
        type_: COUNTER,
    },
    MATERIALIZED_OUT_BYTES = Metric {
        name: "materialized_out_bytes_total",
        help: "Total number of post-reduce bytes stored to the target, by task and source collection",
        type_: COUNTER,
    },
    MATERIALIZED_OUT_DOCS = Metric {
        name: "materialized_out_docs_total",
        help:
            "Total number of post-reduce documents stored to the target, by task and source collection",
        type_: COUNTER,
    }
}

const COUNTER: &str = "counter";
const GAUGE: &str = "gauge";

#[cfg(test)]
mod tests {
    use crate::test_server;
    use chrono::TimeZone as _;

    fn docs_and_bytes(docs: u64, bytes: u64) -> ops::stats::DocsAndBytes {
        ops::stats::DocsAndBytes {
            docs_total: docs,
            bytes_total: bytes,
        }
    }

    fn stats(
        name: String,
        month: u32,
        stats_summary: catalog_stats::StatsSummary,
        task_stats: catalog_stats::TaskStats,
    ) -> (catalog_stats::Grain, catalog_stats::CatalogStats) {
        let stats = catalog_stats::CatalogStats {
            meta: ops::Meta {
                uuid: "00000000-0000-0000-0000-000000000000".to_string(),
            },
            catalog_name: name,
            ts: chrono::Utc
                .with_ymd_and_hms(2026, month, 1, 0, 0, 0)
                .unwrap(),
            stats_summary,
            task_stats,
        };
        (catalog_stats::Grain::Monthly, stats)
    }

    async fn get_metrics(
        server: &test_server::TestServer,
        prefix: &str,
        token: &str,
        started: &str,
    ) -> (u16, String) {
        let url = server
            .base_url()
            .join(&format!("/api/v1/metrics/{prefix}"))
            .unwrap();
        let response = reqwest::Client::new()
            .get(url)
            .query(&[("started", started)])
            .bearer_auth(token)
            .send()
            .await
            .unwrap();

        (response.status().as_u16(), response.text().await.unwrap())
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_open_metrics(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let client =
            std::sync::Arc::new(catalog_stats::test_util::connect_emulator("estuary-local").await);
        // Clear only this test's prefix to preserve other tests' emulator rows.
        let prefix = "aliceCo/ctest-open-metrics/";
        catalog_stats::test_util::delete_rows_with_prefix(&client, prefix)
            .await
            .unwrap();

        let capture = std::collections::BTreeMap::from([(
            format!("{prefix}data/events"),
            ops::stats::CaptureBinding {
                right: Some(docs_and_bytes(2, 200)),
                out: Some(docs_and_bytes(2, 180)),
                last_published_at: Some(proto_flow::Timestamp {
                    seconds: 1_778_000_000,
                    nanos: 0,
                }),
            },
        )]);
        let materialize = std::collections::BTreeMap::from([(
            format!("{prefix}data/events"),
            ops::stats::MaterializeBinding {
                left: Some(docs_and_bytes(1, 90)),
                right: Some(docs_and_bytes(2, 180)),
                out: Some(docs_and_bytes(3, 270)),
                last_source_published_at: Some(proto_flow::Timestamp {
                    seconds: 1_778_000_000,
                    nanos: 0,
                }),
                bytes_behind: 42,
            },
        )]);

        let rows = vec![
            // The prefix rollup is not encoded.
            stats(
                prefix.to_string(),
                5,
                catalog_stats::StatsSummary {
                    written_by_me: docs_and_bytes(9, 900),
                    ..Default::default()
                },
                Default::default(),
            ),
            stats(
                format!("{prefix}in/capture"),
                5,
                catalog_stats::StatsSummary {
                    written_by_me: docs_and_bytes(2, 180),
                    usage_seconds: 3600,
                    txn_count: 4,
                    warnings: 1,
                    ..Default::default()
                },
                catalog_stats::TaskStats {
                    capture,
                    ..Default::default()
                },
            ),
            // Exercise both month boundaries for the same task.
            stats(
                format!("{prefix}in/capture"),
                4,
                catalog_stats::StatsSummary {
                    written_by_me: docs_and_bytes(7, 700),
                    ..Default::default()
                },
                Default::default(),
            ),
            stats(
                format!("{prefix}in/capture"),
                6,
                catalog_stats::StatsSummary {
                    written_by_me: docs_and_bytes(8, 800),
                    ..Default::default()
                },
                Default::default(),
            ),
            stats(
                format!("{prefix}data/events"),
                5,
                catalog_stats::StatsSummary {
                    written_to_me: docs_and_bytes(2, 180),
                    read_from_me: docs_and_bytes(3, 270),
                    ..Default::default()
                },
                Default::default(),
            ),
            stats(
                format!("{prefix}out/materialize"),
                5,
                catalog_stats::StatsSummary {
                    read_by_me: docs_and_bytes(2, 180),
                    usage_seconds: 1800,
                    errors: 2,
                    ..Default::default()
                },
                catalog_stats::TaskStats {
                    materialize,
                    ..Default::default()
                },
            ),
        ];
        catalog_stats::test_util::seed_rows(&client, &rows)
            .await
            .unwrap();

        let server = test_server::TestServer::start_with_catalog_stats(
            pool.clone(),
            test_server::snapshot(pool, false).await,
            client,
        )
        .await;
        let token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);

        let (status, body) = get_metrics(&server, prefix, &token, "2026-05-05T18:00:00Z").await;
        assert_eq!(status, 200, "{body}");
        assert!(body.ends_with("# EOF\n"));
        insta::assert_snapshot!(body);

        let (status, body) = get_metrics(&server, prefix, &token, "+262142-12-01T00:00:00Z").await;
        insta::assert_snapshot!(format!("{status}: {body}"), @"400: started month is out of range");
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_open_metrics_unconfigured(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let server =
            test_server::TestServer::start(pool.clone(), test_server::snapshot(pool, false).await)
                .await;
        let token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);

        let (status, body) = get_metrics(&server, "aliceCo/", &token, "2026-05-05T18:00:00Z").await;
        insta::assert_snapshot!(format!("{status}: {body}"), @"500: catalog stats are not configured");
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_open_metrics_read_error(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let client = std::sync::Arc::new(
            catalog_stats::test_util::connect_emulator("ctest-open-metrics-missing").await,
        );
        let server = test_server::TestServer::start_with_catalog_stats(
            pool.clone(),
            test_server::snapshot(pool, false).await,
            client,
        )
        .await;
        let token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);

        let (status, body) = get_metrics(&server, "aliceCo/", &token, "2026-05-05T18:00:00Z").await;
        assert_eq!(status, 503, "{body}");
        assert!(body.starts_with("reading catalog stats:"), "{body}");
        assert!(
            !body.contains("# HELP") && !body.contains("# EOF"),
            "{body}"
        );
    }
}
