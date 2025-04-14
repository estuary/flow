use crate::api::{public::ApiErrorExt, ApiError, App, ControlClaims};
use axum::http::StatusCode;
use chrono::{Datelike, TimeZone};
use futures::StreamExt;
use ops::stats::DocsAndBytes;
use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::Arc;

#[axum::debug_handler]
pub async fn handle_get_metrics(
    state: axum::extract::State<Arc<App>>,
    axum::Extension(claims): axum::Extension<ControlClaims>,
    axum::extract::Path(prefix): axum::extract::Path<String>,
) -> Result<axum::response::Response, ApiError> {
    if !prefix.ends_with('/') {
        return Err(
            anyhow::anyhow!("prefix {prefix:?} must end with a trailing '/' slash")
                .with_status(StatusCode::BAD_REQUEST),
        );
    }
    let prefixes = state
        .0
        .verify_user_authorization(&claims, vec![prefix], models::Capability::Read)
        .await?;

    let pg_pool = state.pg_pool.clone();
    let now = chrono::Utc::now();

    // Map `now` to midnight at the open of the current month.
    let now_month = chrono::Utc
        .with_ymd_and_hms(now.year(), now.month(), 1, 0, 0, 0)
        .unwrap();

    let scrape_at = now.timestamp_micros() as f64 / 1_000_000f64;

    // Stream all `catalog_stats` rows from the DB, encoding into metrics.
    // Some OpenMetrics clients (Python, grr) expect that metric metadata
    // and all samples appear contiguously, so we jump through some hoops
    // to efficiently accumulate chunked buffers that we can yield in the
    // correct order once we're done.
    let mut buf = BufferParts::new();

    for metric in REGISTRY {
        metric.declare(&mut buf);
    }

    let mut stats = sqlx::query!(
        r#"
        SELECT flow_document AS "stats: sqlx::types::Json<CatalogStats>"
        FROM   catalog_stats
        WHERE  starts_with(catalog_name, $1) AND right(catalog_name, 1) != '/'
        AND    grain = 'monthly'
        AND    ts = $2
        "#,
        &prefixes[0],
        now_month,
    )
    .fetch(&pg_pool);

    loop {
        match stats.next().await {
            Some(Ok(stat)) => encode_metrics(&mut buf, scrape_at, stat.stats.0),
            Some(Err(err)) => return Err(err.with_status(StatusCode::INTERNAL_SERVER_ERROR)),
            None => break,
        }
    }

    let stream = coroutines::try_coroutine(move |mut co| async move {
        for group in buf.groups {
            for chunk in group {
                () = co.yield_(chunk.freeze()).await;
            }
        }
        () = co.yield_(bytes::Bytes::from_static(b"# EOF\n")).await;

        Ok::<(), sqlx::Error>(())
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

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct CatalogStats {
    #[serde(rename = "_meta")]
    meta: ops::Meta,
    catalog_name: String,
    stats_summary: StatsSummary,
    #[serde(default)]
    task_stats: TaskStats,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct StatsSummary {
    #[serde(default)]
    read_by_me: DocsAndBytes,
    #[serde(default)]
    read_from_me: DocsAndBytes,
    #[serde(default)]
    written_by_me: DocsAndBytes,
    #[serde(default)]
    written_to_me: DocsAndBytes,
    #[serde(default)]
    warnings: u64,
    #[serde(default)]
    errors: u64,
    #[serde(default)]
    failures: u64,
    #[serde(default)]
    usage_seconds: u64,
    #[serde(default)]
    txn_count: u64,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct TaskStats {
    #[serde(default)]
    capture: BTreeMap<String, ops::stats::Binding>,
    #[serde(default)]
    derive: Option<ops::stats::Derive>,
    #[serde(default)]
    materialize: BTreeMap<String, ops::stats::Binding>,
}

fn encode_metrics(buf: &mut BufferParts, _scrape_at: f64, stats: CatalogStats) {
    let CatalogStats {
        catalog_name,
        meta: ops::Meta { uuid: _ },
        stats_summary:
            StatsSummary {
                errors,
                warnings,
                failures,
                read_by_me,
                read_from_me,
                written_by_me,
                written_to_me,
                usage_seconds,
                txn_count,
            },
        task_stats:
            TaskStats {
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

    for (collection, ops::stats::Binding { out, right, .. }) in capture {
        let l_task_collection = format!("task={catalog_name:?},collection={collection:?}");

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
    }) = derive
    {
        for (
            transform,
            ops::stats::derive::Transform {
                last_source_published_at,
                source: collection,
                input,
            },
        ) in transforms
        {
            let l_task_collection_transform =
                format!("task={catalog_name:?},collection={collection:?},transform={transform:?}");

            if let Some(m) = last_source_published_at {
                let ts = to_time_seconds(m);
                DERIVED_LAST_SOURCE_PUBLISHED_AT.gauge(buf, &l_task_collection_transform, ts);
            }
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
        ops::stats::Binding {
            last_source_published_at,
            left,
            right,
            out,
        },
    ) in materialize
    {
        let l_task_collection = format!("task={catalog_name:?},collection={collection:?}");

        if let Some(m) = last_source_published_at {
            let ts = to_time_seconds(m);
            MATERIALIZED_LAST_SOURCE_PUBLISHED_AT.gauge(buf, &l_task_collection, ts);
        }
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

    #[allow(dead_code)] // TODO(johnny): Will be used for timestamps and ages.
    fn gauge(&self, buf: &mut BufferParts, labels: &str, value: f64) {
        let Self { name, .. } = self;

        if value != 0.0 {
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
    DERIVED_LAST_SOURCE_PUBLISHED_AT= Metric {
        name: "derived_last_source_published_at_time_seconds",
        help: "Publication timestamp of the most recent source collection document that was processed by the derivation, given as seconds since the unix epoch",
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
