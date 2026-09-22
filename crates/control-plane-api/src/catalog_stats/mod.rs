use anyhow::Context;
use std::sync::Arc;

#[derive(Clone, Debug, clap::Parser)]
pub struct CatalogStatsConfig {
    /// GCP project of the BigTable instance holding rolled-up catalog stats.
    /// Must be given together with --bigtable-instance. Without both, the
    /// `catalogStats` GraphQL query reports that it is not configured.
    #[clap(
        long = "bigtable-project",
        env = "BIGTABLE_PROJECT",
        requires = "bigtable_instance"
    )]
    bigtable_project: Option<String>,
    /// BigTable instance holding rolled-up catalog stats.
    /// Must be given together with --bigtable-project. Without both, the
    /// `catalogStats` GraphQL query reports that it is not configured.
    #[clap(
        long = "bigtable-instance",
        env = "BIGTABLE_INSTANCE",
        requires = "bigtable_project"
    )]
    bigtable_instance: Option<String>,
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

/// Builds the catalog stats read client from deployment configuration,
/// returning `None` when BigTable is not configured at all.
///
/// This lives here rather than in the binary so that callers pass plain
/// configuration and never name the BigTable client themselves, and so that
/// the connect sits beside the [`App`] field it fills.
///
/// Connecting fails startup rather than degrading to `None`: a deployment that
/// meant to serve catalog stats and cannot reach BigTable should say so at
/// boot, not answer "catalog stats are not configured" to every caller.
pub async fn connect(
    config: CatalogStatsConfig,
) -> anyhow::Result<Option<Arc<catalog_stats::Client>>> {
    let CatalogStatsConfig {
        bigtable_project,
        bigtable_instance,
        bigtable_connect_timeout,
        bigtable_emulator_host,
    } = config;

    // check required parameters
    let (bigtable_project, bigtable_instance) = match (bigtable_project, bigtable_instance) {
        (Some(bigtable_project), Some(bigtable_instance)) => (bigtable_project, bigtable_instance),
        (None, None) => return Ok(None),
        _ => anyhow::bail!("BigTable project and instance must be configured together"),
    };

    let config = catalog_stats::BigtableConfig {
        project: bigtable_project,
        instance: bigtable_instance,
        emulator_host: bigtable_emulator_host,
    };

    let client = tokio::time::timeout(
        bigtable_connect_timeout,
        catalog_stats::Client::connect(&config),
    )
    .await
    .context("timed out connecting to BigTable for catalog stats")?
    .context("could not connect to BigTable for catalog stats")?;

    Ok(Some(Arc::new(client)))
}
