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
/// Controllers read catalog stats to evaluate alerts, so a deployment that
/// cannot reach BigTable fails at startup rather than in every controller run.
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
