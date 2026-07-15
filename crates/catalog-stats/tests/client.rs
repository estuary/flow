use catalog_stats::test_util;
use chrono::TimeZone;
use futures::TryStreamExt;

async fn connect() -> catalog_stats::Client {
    // The emulator's host port is dynamic per stack; FLOW_PORT_BIGTABLE is set
    // by mise/tasks/local/stack-env. Run via mise so the ambient env is present.
    let port = std::env::var("FLOW_PORT_BIGTABLE")
        .expect("FLOW_PORT_BIGTABLE must be set — run via 'mise run' (see local:bigtable)");
    catalog_stats::Client::connect(&catalog_stats::BigtableConfig {
        project: "estuary-local".to_string(),
        instance: "estuary-local".to_string(),
        emulator_host: Some(format!("localhost:{port}")),
    })
    .await
    .expect("BigTable emulator must be running: `mise run local:bigtable`")
}

fn base_ts() -> chrono::DateTime<chrono::Utc> {
    chrono::Utc.with_ymd_and_hms(2026, 5, 5, 18, 0, 0).unwrap()
}

async fn fresh_prefix(client: &catalog_stats::Client, test: &str) -> String {
    let prefix = format!("ct/{test}/");
    test_util::delete_rows_with_prefix(client, &prefix)
        .await
        .unwrap();
    prefix
}

fn stats(
    name: impl Into<String>,
    ts: chrono::DateTime<chrono::Utc>,
) -> catalog_stats::CatalogStats {
    catalog_stats::CatalogStats {
        meta: ops::Meta {
            uuid: "00000000-0000-0000-0000-000000000000".to_string(),
        },
        catalog_name: name.into(),
        ts,
        stats_summary: catalog_stats::StatsSummary::default(),
        task_stats: catalog_stats::TaskStats::default(),
    }
}

async fn names_of(
    stream: impl futures_core::Stream<Item = catalog_stats::RetryResult<catalog_stats::CatalogStats>>,
) -> Vec<String> {
    let rows: Vec<catalog_stats::CatalogStats> = Box::pin(stream).try_collect().await.unwrap();
    rows.into_iter().map(|s| s.catalog_name).collect()
}

async fn pairs_of(
    stream: impl futures_core::Stream<Item = catalog_stats::RetryResult<catalog_stats::CatalogStats>>,
) -> Vec<(String, chrono::DateTime<chrono::Utc>)> {
    let rows: Vec<catalog_stats::CatalogStats> = Box::pin(stream).try_collect().await.unwrap();
    rows.into_iter().map(|s| (s.catalog_name, s.ts)).collect()
}

#[tokio::test]
async fn fetch_at_for_names() {
    struct Case {
        name: &'static str,
        seed: &'static [&'static str],
        query: &'static [&'static str],
        query_ts_us: i64,
        expected: &'static [&'static str],
    }

    let client = connect().await;
    let base = base_ts();

    for case in [
        Case {
            name: "returns_seeded_rows_in_lex_order",
            seed: &["c", "a", "b"],
            query: &["c", "a", "b"],
            query_ts_us: 0,
            expected: &["a", "b", "c"],
        },
        Case {
            name: "empty_input_yields_empty_stream",
            seed: &[],
            query: &[],
            query_ts_us: 0,
            expected: &[],
        },
        Case {
            name: "omits_unknown_names",
            seed: &["present"],
            query: &["missing", "present"],
            query_ts_us: 0,
            expected: &["present"],
        },
        Case {
            name: "truncates_sub_ms_query_timestamp",
            seed: &["foo"],
            query: &["foo"],
            query_ts_us: 999,
            expected: &["foo"],
        },
    ] {
        let prefix = fresh_prefix(&client, case.name).await;
        let rows: Vec<_> = case
            .seed
            .iter()
            .map(|n| {
                (
                    catalog_stats::Grain::Hourly,
                    stats(format!("{prefix}{n}"), base),
                )
            })
            .collect();
        test_util::seed_rows(&client, &rows).await.unwrap();

        let full_query: Vec<String> = case.query.iter().map(|n| format!("{prefix}{n}")).collect();
        let query_ref: Vec<&str> = full_query.iter().map(String::as_str).collect();
        let got = names_of(client.fetch_at_for_names(
            &query_ref,
            catalog_stats::Grain::Hourly,
            base + chrono::Duration::microseconds(case.query_ts_us),
        ))
        .await;

        let expected: Vec<String> = case
            .expected
            .iter()
            .map(|n| format!("{prefix}{n}"))
            .collect();
        assert_eq!(got, expected, "case: {}", case.name);
    }
}

#[tokio::test]
async fn fetch_range_for_names() {
    struct Case {
        name: &'static str,
        seed: &'static [(&'static str, i64)],
        query: &'static [&'static str],
        range_hours: std::ops::Range<i64>,
        expected: &'static [(&'static str, i64)],
    }

    let client = connect().await;
    let base = base_ts();
    let at = |h: i64| base + chrono::Duration::hours(h);

    for case in [
        Case {
            name: "inclusive_start_exclusive_end",
            seed: &[("foo", 0), ("foo", 1), ("foo", 2), ("foo", 3)],
            query: &["foo"],
            range_hours: 0..3,
            expected: &[("foo", 0), ("foo", 1), ("foo", 2)],
        },
        Case {
            name: "orders_by_name_then_ts",
            seed: &[("b", 1), ("a", 1), ("b", 0), ("a", 0)],
            query: &["b", "a"],
            range_hours: 0..2,
            expected: &[("a", 0), ("a", 1), ("b", 0), ("b", 1)],
        },
        Case {
            name: "empty_names_yields_empty_stream",
            seed: &[],
            query: &[],
            range_hours: 0..1,
            expected: &[],
        },
        Case {
            name: "degenerate_range_yields_empty_stream",
            seed: &[("any", 0)],
            query: &["any"],
            range_hours: 0..0,
            expected: &[],
        },
    ] {
        let prefix = fresh_prefix(&client, case.name).await;
        let rows: Vec<_> = case
            .seed
            .iter()
            .map(|(n, h)| {
                (
                    catalog_stats::Grain::Hourly,
                    stats(format!("{prefix}{n}"), at(*h)),
                )
            })
            .collect();
        test_util::seed_rows(&client, &rows).await.unwrap();

        let full_query: Vec<String> = case.query.iter().map(|n| format!("{prefix}{n}")).collect();
        let query_ref: Vec<&str> = full_query.iter().map(String::as_str).collect();

        let got = pairs_of(client.fetch_range_for_names(
            &query_ref,
            catalog_stats::Grain::Hourly,
            at(case.range_hours.start)..at(case.range_hours.end),
        ))
        .await;

        let expected: Vec<_> = case
            .expected
            .iter()
            .map(|(n, h)| (format!("{prefix}{n}"), at(*h)))
            .collect();
        assert_eq!(got, expected, "case: {}", case.name);
    }
}

#[tokio::test]
async fn fetch_range_for_prefix() {
    struct Case {
        name: &'static str,
        seed: &'static [(&'static str, i64)],
        sub_prefix: &'static str,
        range_hours: std::ops::Range<i64>,
        expected: &'static [(&'static str, i64)],
    }

    let client = connect().await;
    let base = base_ts();
    let at = |h: i64| base + chrono::Duration::hours(h);

    for case in [
        Case {
            name: "includes_rollups",
            seed: &[("tenant/", 0), ("tenant/a", 0), ("tenant/b", 0)],
            sub_prefix: "tenant/",
            range_hours: 0..1,
            expected: &[("tenant/", 0), ("tenant/a", 0), ("tenant/b", 0)],
        },
        Case {
            name: "excludes_sibling_names",
            seed: &[("tenant/foo", 0), ("tenant:foo", 0)],
            sub_prefix: "tenant/",
            range_hours: 0..1,
            expected: &[("tenant/foo", 0)],
        },
        Case {
            name: "filters_rows_outside_ts_range",
            seed: &[("tenant/foo", 1), ("tenant/foo", 3)],
            sub_prefix: "tenant/",
            range_hours: 0..2,
            expected: &[("tenant/foo", 1)],
        },
    ] {
        let prefix = fresh_prefix(&client, case.name).await;
        let rows: Vec<_> = case
            .seed
            .iter()
            .map(|(n, h)| {
                (
                    catalog_stats::Grain::Hourly,
                    stats(format!("{prefix}{n}"), at(*h)),
                )
            })
            .collect();
        test_util::seed_rows(&client, &rows).await.unwrap();

        let query_prefix = format!("{prefix}{}", case.sub_prefix);
        let got = pairs_of(client.fetch_range_for_prefix(
            &query_prefix,
            catalog_stats::Grain::Hourly,
            at(case.range_hours.start)..at(case.range_hours.end),
        ))
        .await;

        let expected: Vec<_> = case
            .expected
            .iter()
            .map(|(n, h)| (format!("{prefix}{n}"), at(*h)))
            .collect();
        assert_eq!(got, expected, "case: {}", case.name);
    }
}

#[tokio::test]
async fn fetch_range_for_prefix_empty_prefix_yields_empty_stream() {
    let client = connect().await;
    let base = base_ts();
    let got = names_of(client.fetch_range_for_prefix(
        "",
        catalog_stats::Grain::Hourly,
        base..base + chrono::Duration::hours(1),
    ))
    .await;
    assert!(got.is_empty());
}

#[tokio::test]
async fn fetches_target_only_the_requested_grain() {
    let client = connect().await;
    let prefix = fresh_prefix(&client, "grain_isolation").await;
    let ts = base_ts();
    let hourly_name = format!("{prefix}hourly");
    let daily_name = format!("{prefix}daily");
    let monthly_name = format!("{prefix}monthly");

    test_util::seed_rows(
        &client,
        &[
            (catalog_stats::Grain::Hourly, stats(&hourly_name, ts)),
            (catalog_stats::Grain::Daily, stats(&daily_name, ts)),
            (catalog_stats::Grain::Monthly, stats(&monthly_name, ts)),
        ],
    )
    .await
    .unwrap();

    for (grain, expected) in [
        (catalog_stats::Grain::Hourly, vec![hourly_name.clone()]),
        (catalog_stats::Grain::Daily, vec![daily_name.clone()]),
        (catalog_stats::Grain::Monthly, vec![monthly_name.clone()]),
    ] {
        let got = names_of(client.fetch_at_for_names(
            &[
                hourly_name.as_str(),
                daily_name.as_str(),
                monthly_name.as_str(),
            ],
            grain,
            ts,
        ))
        .await;
        assert_eq!(got, expected, "grain: {grain:?}");
    }
}
