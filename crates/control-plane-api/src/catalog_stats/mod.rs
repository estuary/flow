use anyhow::Context;
use std::sync::Arc;

#[derive(Clone, Debug, clap::Parser)]
pub struct CatalogStatsConfig {
    /// GCP project of the BigTable instance holding rolled-up catalog stats.
    #[clap(long = "bigtable-project", env = "BIGTABLE_PROJECT")]
    bigtable_project: String,
    /// BigTable instance holding rolled-up catalog stats.
    #[clap(long = "bigtable-instance", env = "BIGTABLE_INSTANCE")]
    bigtable_instance: String,
    /// Connect to a BigTable emulator at this host:port instead of Google's
    /// endpoint, skipping credentials. Set by the local stack.
    #[clap(long = "bigtable-emulator-host", env = "BIGTABLE_EMULATOR_HOST")]
    bigtable_emulator_host: Option<String>,
    /// How long to wait for the initial BigTable connection before giving up.
    /// Bounded so that an unreachable endpoint surfaces as a startup failure
    /// rather than hanging past a deployment's startup probe window.
    /// Defaults to 30s.
    #[clap(
        long = "bigtable-connect-timeout",
        env = "BIGTABLE_CONNECT_TIMEOUT",
        default_value = "30s"
    )]
    #[arg(value_parser = humantime::parse_duration)]
    bigtable_connect_timeout: std::time::Duration,
}

/// Builds the catalog stats read client from deployment configuration.
///
/// This lives here rather than in the binary so that callers pass plain
/// configuration and never name the BigTable client themselves, and so that
/// the connect sits beside the [`App`] field it fills.
///
/// Connecting fails startup rather than degrading to `None`: a deployment that
/// meant to serve catalog stats and cannot reach BigTable should say so at
/// boot, not answer "catalog stats are not configured" to every caller.
pub async fn connect(config: CatalogStatsConfig) -> anyhow::Result<Arc<catalog_stats::Client>> {
    let CatalogStatsConfig {
        bigtable_project,
        bigtable_instance,
        bigtable_connect_timeout,
        bigtable_emulator_host,
    } = config;

    let config = catalog_stats::BigtableConfig {
        project: bigtable_project,
        instance: bigtable_instance,
        emulator_host: bigtable_emulator_host,
    };

    // Requires the `roles/bigtable.reader` IAM role for the Bigtable instance
    // provided in the above configuration.
    let client = tokio::time::timeout(
        bigtable_connect_timeout,
        catalog_stats::Client::connect(&config),
    )
    .await
    .context("timed out connecting to BigTable for catalog stats")?
    .context("could not connect to BigTable for catalog stats")?;

    Ok(Arc::new(client))
}

/// Collects a complete stats read, with at most three retries of consecutive
/// transient failures. Callers set the deadline for their operation.
pub async fn collect(
    stream: impl futures::Stream<Item = catalog_stats::RetryResult<catalog_stats::CatalogStats>>,
) -> anyhow::Result<Vec<catalog_stats::CatalogStats>> {
    const MAX_READ_RETRIES: usize = 3;
    let mut stream = std::pin::pin!(stream);
    let mut rows = Vec::new();

    while let Some(item) = futures::StreamExt::next(&mut stream).await {
        match item {
            Ok(row) => rows.push(row),
            // Transient failures leave the stream alive for another attempt.
            // Other failures end it and must not be mistaken for a complete read.
            Err(err) if err.inner.is_transient() && err.attempt < MAX_READ_RETRIES => {
                tracing::warn!(attempt = err.attempt, error = %err.inner, "retrying catalog stats read");
            }
            Err(err) => return Err(anyhow::Error::new(err.inner).context("reading catalog stats")),
        }
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn collects_complete_reads() {
        let row = catalog_stats::CatalogStats {
            meta: ops::Meta {
                uuid: "00000000-0000-0000-0000-000000000000".to_string(),
            },
            catalog_name: "acmeCo/".to_string(),
            ts: chrono::DateTime::from_timestamp(0, 0).unwrap(),
            stats_summary: Default::default(),
            task_stats: Default::default(),
        };
        let next_row = catalog_stats::CatalogStats {
            catalog_name: "acmeCo/task".to_string(),
            ..row.clone()
        };
        let transient = |attempt| {
            catalog_stats::Error::Grpc(tonic::Status::unavailable("retry")).with_attempt(attempt)
        };
        let rows = collect(futures::stream::iter([
            Ok(row.clone()),
            Err(transient(0)),
            Err(transient(1)),
            Err(transient(2)),
            Ok(next_row.clone()),
        ]))
        .await
        .unwrap();
        assert_eq!(rows, [row.clone(), next_row]);

        for error in [
            catalog_stats::Error::Grpc(tonic::Status::permission_denied("denied")).with_attempt(0),
            transient(3),
        ] {
            assert!(
                collect(futures::stream::iter([Ok(row.clone()), Err(error)]))
                    .await
                    .is_err()
            );
        }
    }
}
