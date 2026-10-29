use anyhow::Context;
use chrono::Datelike;
use std::io::Write;

/// Calculate invoices and print the results as JSON.
#[derive(Debug, clap::Args)]
pub struct GenerateInvoices {
    #[clap(flatten)]
    args: InvoiceArgs,
    /// Comma-separated tenant names, including their trailing slash.
    #[clap(long, value_delimiter = ',', required_unless_present("all_tenants"))]
    tenants: Vec<String>,
    /// Calculate invoices for every tenant.
    #[clap(long, conflicts_with("tenants"))]
    all_tenants: bool,
}

/// Recalculate and freeze invoices, retaining invoices already frozen.
#[derive(Debug, clap::Args)]
pub struct FreezeInvoices {
    #[clap(flatten)]
    args: InvoiceArgs,
}

#[derive(Debug, clap::Args)]
struct InvoiceArgs {
    /// Control-plane database connection string.
    #[clap(long)]
    connection_string: String,
    /// Billing month: YYYY-MM or YYYY-MM-01, in UTC.
    #[clap(long, value_parser = crate::parse_month)]
    month: chrono::NaiveDate,
    /// GCP project containing the rolled-up catalog statistics.
    #[clap(long, env = "BIGTABLE_PROJECT")]
    bigtable_project: String,
    /// Bigtable instance containing the catalog statistics.
    #[clap(long, env = "BIGTABLE_INSTANCE")]
    bigtable_instance: String,
    /// Bigtable application profile; defaults to the instance's default profile.
    #[clap(long, env = "BIGTABLE_APP_PROFILE")]
    bigtable_app_profile: Option<String>,
    /// Bigtable emulator host:port; skips Google authentication.
    #[clap(long, env = "BIGTABLE_EMULATOR_HOST")]
    bigtable_emulator_host: Option<String>,
}

pub async fn run(command: &GenerateInvoices) -> anyhow::Result<()> {
    let (pool, stats) = connect(&command.args).await?;
    let invoices = billing_types::invoices::generate(
        &pool,
        &stats,
        command.args.month.year(),
        chrono::Month::try_from(command.args.month.month() as u8).unwrap(),
        (!command.all_tenants).then_some(command.tenants.as_slice()),
    )
    .await?;

    let mut output_stream = std::io::BufWriter::new(std::io::stdout().lock());
    serde_json::to_writer_pretty(&mut output_stream, &invoices)?;
    writeln!(output_stream)?;
    output_stream.flush()?;
    Ok(())
}

pub async fn freeze(command: &FreezeInvoices) -> anyhow::Result<()> {
    let (pool, stats) = connect(&command.args).await?;
    let inserted = billing_types::invoices::freeze(
        &pool,
        &stats,
        command.args.month.year(),
        chrono::Month::try_from(command.args.month.month() as u8).unwrap(),
    )
    .await?;
    println!(
        "{inserted} newly frozen invoices for {}",
        command.args.month.format("%Y-%m")
    );
    Ok(())
}

async fn connect(command: &InvoiceArgs) -> anyhow::Result<(sqlx::PgPool, catalog_stats::Client)> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&command.connection_string)
        .await
        .context("connecting to the control-plane database")?;
    let config = catalog_stats::BigtableConfig {
        project: command.bigtable_project.clone(),
        instance: command.bigtable_instance.clone(),
        app_profile_id: command.bigtable_app_profile.clone(),
        emulator_host: command.bigtable_emulator_host.clone(),
    };
    let stats = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        catalog_stats::Client::connect(&config),
    )
    .await
    .context("timed out connecting to Bigtable")?
    .context("connecting to Bigtable")?;
    Ok((pool, stats))
}
