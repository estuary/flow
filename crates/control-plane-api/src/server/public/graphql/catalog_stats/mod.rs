//! The `catalogStats` query. Reads rolled-up catalog stats out of BigTable.

mod types;

use async_graphql::connection;
use base64::Engine as _;

pub use types::CatalogStatsGrain;

/// The query returns a Relay connection so that pagination can be added later
/// without a breaking schema change. It is not paginated today: every call
/// returns one complete page, and `first` / `after` arguments are deliberately
/// absent rather than accepted and ignored.
pub type PaginatedCatalogStats = connection::Connection<
    String,
    types::CatalogStats,
    connection::EmptyFields,
    connection::EmptyFields,
    connection::DefaultConnectionName,
    connection::DefaultEdgeName,
    connection::DisableNodesField,
>;

/// Upper bound on buckets returned by one query. Exceeding it is an error
/// rather than a truncation, because a silently short result makes a usage
/// total quietly wrong with nothing in the response to say so.
const MAX_BUCKETS: usize = 10_000;
/// Consecutive transient read failures tolerated before giving up. The client
/// already applies its own backoff, so a few attempts add little latency to a
/// request someone is waiting on; past that, retrying the whole query is the
/// better trade.
const MAX_ATTEMPTS: usize = 3;

#[derive(Debug, Default)]
pub struct CatalogStatsQuery;

#[derive(Debug, Clone, async_graphql::InputObject)]
pub struct CatalogStatsBy {
    /// Exact catalog names. A prefix rollup is addressed by its own
    /// trailing-slash name, such as `acmeCo/`. A minimum of of 1 name is
    /// required. A maximum of 100 names are supported.
    #[graphql(validator(min_items = 1, max_items = 100))]
    pub names: Vec<String>,
    /// Time grain to retrieve aggregates stats for.
    pub grain: CatalogStatsGrain,
    /// Inclusive lower bound, which must fall on a `grain` boundary.
    pub start: chrono::DateTime<chrono::Utc>,
    /// Exclusive upper bound, which must fall on a `grain` boundary.
    pub end: chrono::DateTime<chrono::Utc>,
}

#[async_graphql::Object]
impl CatalogStatsQuery {
    /// Returns stored reporting buckets for the given catalog names, at the
    /// given grain, over the half-open window `[start, end)`.
    async fn catalog_stats(
        &self,
        ctx: &async_graphql::Context<'_>,
        by: CatalogStatsBy,
    ) -> async_graphql::Result<PaginatedCatalogStats> {
        let env = ctx.data::<crate::Envelope>()?;
        let CatalogStatsBy {
            mut names,
            grain,
            start,
            end,
        } = by;

        // Duplicates would build overlapping row ranges and count twice
        // against the bucket cap.
        names.sort();
        names.dedup();

        // Authorize before validating anything else, so that error messages
        // cannot be used to probe for names the caller cannot see.
        let policy_result = crate::server::evaluate_names_authorization(
            env.snapshot(),
            env.claims()?,
            models::Capability::Read,
            names.iter().map(String::as_str),
        );
        let (_expiry, ()) = env.authorization_outcome(policy_result).await?;

        () = types::validate_range(grain, start, end)?;

        let client = client(ctx)?;
        let names: Vec<&str> = names.iter().map(String::as_str).collect();

        // Fetch the stats as a stream, collecting them into a vector of buckets.
        // An error is raised (closing the stream) if the number of buckets exceeds the maximum,
        // in order to limit response sizes.
        let stream = client.fetch_range_for_names(&names, grain.into(), start..end);
        let buckets = types::collect_buckets(stream, grain, MAX_BUCKETS, MAX_ATTEMPTS).await?;

        // For now, only a single page of stats is ever returned. Cursor-based pagination
        // can be added if required in the future.
        let mut conn = PaginatedCatalogStats::new(false, false);
        conn.edges = buckets
            .into_iter()
            .map(|bucket| connection::Edge::new(cursor_of(&bucket), bucket))
            .collect();

        Ok(conn)
    }
}

/// Encodes the `(name, ts)` pair that BigTable sorts rows by.
///
/// Clients treat a cursor as opaque, so this is base64 rather than something
/// readable. Encoding the sort key means a future paginated implementation can
/// decode a cursor straight into a row key and resume from it, instead of
/// having to change the cursor format and break in-flight clients. `\0`
/// separates the two parts because a catalog name cannot contain one.
fn cursor_of(bucket: &types::CatalogStats) -> String {
    let key = format!("{}\0{}", bucket.catalog_name, bucket.timestamp.to_rfc3339());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key)
}

/// The client is absent when the deployment has no BigTable configured, in
/// which case this query is the only thing that stops working.
fn client(
    ctx: &async_graphql::Context<'_>,
) -> async_graphql::Result<std::sync::Arc<catalog_stats::Client>> {
    ctx.data::<std::sync::Arc<catalog_stats::Client>>()
        .cloned()
        .map_err(|_| async_graphql::Error::new("catalog stats are not configured"))
}

#[cfg(test)]
mod tests {
    use crate::test_server;
    use chrono::TimeZone as _;

    /// The emulator's port is per-stack, so the ambient mise env is required.
    async fn emulator() -> std::sync::Arc<catalog_stats::Client> {
        let port = std::env::var("FLOW_PORT_BIGTABLE")
            .expect("FLOW_PORT_BIGTABLE must be set — run via 'mise run' (see local:bigtable)");

        let client = catalog_stats::Client::connect(&catalog_stats::BigtableConfig {
            project: "estuary-local".to_string(),
            instance: "estuary-local".to_string(),
            emulator_host: Some(format!("localhost:{port}")),
        })
        .await
        .expect("BigTable emulator must be running: `mise run local:bigtable`");

        std::sync::Arc::new(client)
    }

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

    fn stats(
        name: String,
        at: chrono::DateTime<chrono::Utc>,
        task_stats: ops::catalog_stats::TaskStats,
    ) -> ops::catalog_stats::CatalogStats {
        ops::catalog_stats::CatalogStats {
            meta: ops::Meta {
                uuid: "00000000-0000-0000-0000-000000000000".to_string(),
            },
            catalog_name: name,
            ts: at,
            stats_summary: ops::catalog_stats::StatsSummary {
                written_by_me: docs_and_bytes(2, 200),
                usage_seconds: 60,
                ..Default::default()
            },
            task_stats,
        }
    }

    /// Seeds a rollup row and a task row, under a prefix that is both unique to
    /// this test and covered by Alice's `aliceCo/` grant. The emulator is
    /// shared and never truncated, so the prefix is cleared first.
    async fn seed(client: &catalog_stats::Client, prefix: &str) {
        catalog_stats::test_util::delete_rows_with_prefix(client, prefix)
            .await
            .unwrap();

        let mut capture = std::collections::BTreeMap::new();
        capture.insert(
            format!("{prefix}data/events"),
            ops::stats::CaptureBinding {
                right: Some(docs_and_bytes(2, 200)),
                out: Some(docs_and_bytes(2, 180)),
                last_published_at: None,
            },
        );

        let rows = vec![
            (
                catalog_stats::Grain::Hourly,
                stats(prefix.to_string(), ts(18), Default::default()),
            ),
            (
                catalog_stats::Grain::Hourly,
                stats(
                    format!("{prefix}in/capture-events"),
                    ts(18),
                    ops::catalog_stats::TaskStats {
                        capture,
                        ..Default::default()
                    },
                ),
            ),
            // A later bucket, to show the window bounds are applied.
            (
                catalog_stats::Grain::Hourly,
                stats(prefix.to_string(), ts(20), Default::default()),
            ),
        ];
        catalog_stats::test_util::seed_rows(client, &rows)
            .await
            .unwrap();
    }

    const QUERY: &str = r#"
        query ($names: [String!]!, $start: DateTime!, $end: DateTime!) {
            catalogStats(by: { names: $names, grain: HOURLY, start: $start, end: $end }) {
                pageInfo { hasNextPage hasPreviousPage }
                edges {
                    cursor
                    node {
                        catalogName
                        grain
                        timestamp
                        statsSummary { writtenByMe { docsTotal bytesTotal } usageSeconds }
                        taskStats {
                            capture { collection right { docsTotal } out { docsTotal } }
                            materialize { collection }
                            derive { transforms { transform } }
                        }
                    }
                }
            }
        }
    "#;

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_graphql_catalog_stats(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let client = emulator().await;
        let prefix = "aliceCo/ctest-catalog-stats/";
        seed(&client, prefix).await;

        let server = test_server::TestServer::start_with_catalog_stats(
            pool.clone(),
            // The resolver goes through Envelope::authorization_outcome, so
            // gate the Snapshot to exercise the refresh-and-retry path.
            test_server::snapshot(pool, true).await,
            client,
        )
        .await;
        let token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);

        // The rollup row and the task row, bounded to exclude the 20:00 bucket.
        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": QUERY,
                    "variables": {
                        "names": [prefix, format!("{prefix}in/capture-events")],
                        "start": "2026-05-05T18:00:00Z",
                        "end": "2026-05-05T19:00:00Z",
                    },
                }),
                Some(&token),
            )
            .await;
        insta::assert_json_snapshot!("buckets", response);

        // A name Alice holds no grant on fails the whole query.
        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": QUERY,
                    "variables": {
                        "names": ["bobCo/nope"],
                        "start": "2026-05-05T18:00:00Z",
                        "end": "2026-05-05T19:00:00Z",
                    },
                }),
                Some(&token),
            )
            .await;
        insta::assert_json_snapshot!("unauthorized", response);

        // A start that is not on an hourly boundary would silently drop the
        // bucket it falls inside.
        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": QUERY,
                    "variables": {
                        "names": [prefix],
                        "start": "2026-05-05T18:30:00Z",
                        "end": "2026-05-05T19:00:00Z",
                    },
                }),
                Some(&token),
            )
            .await;
        insta::assert_json_snapshot!("misaligned", response);
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_graphql_catalog_stats_unconfigured(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        // A server with no BigTable client, as in a deployment that has none.
        let server =
            test_server::TestServer::start(pool.clone(), test_server::snapshot(pool, true).await)
                .await;
        let token = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);

        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": QUERY,
                    "variables": {
                        "names": ["aliceCo/anything"],
                        "start": "2026-05-05T18:00:00Z",
                        "end": "2026-05-05T19:00:00Z",
                    },
                }),
                Some(&token),
            )
            .await;
        insta::assert_json_snapshot!("unconfigured", response);
    }
}
