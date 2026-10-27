use anyhow::Context;
use chrono::Datelike;
use futures::TryStreamExt;
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
    /// Include the PostgreSQL calculation alongside each Rust result for inspection.
    #[clap(long)]
    compare_sql: bool,
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
    let invoices = billing::invoices::generate(
        &pool,
        &stats,
        command.args.month.year(),
        chrono::Month::try_from(command.args.month.month() as u8).unwrap(),
        (!command.all_tenants).then_some(command.tenants.as_slice()),
    )
    .await?;

    let output = if command.compare_sql {
        #[derive(serde::Serialize)]
        struct Comparison {
            rust: billing::Invoice,
            postgres: serde_json::Value,
        }
        let mut tx = pool.begin().await?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *tx)
            .await?;
        let comparisons = Vec::with_capacity(invoices.len());
        let (tx, comparisons) =
            futures::stream::iter(invoices.into_iter().map(Ok::<_, anyhow::Error>))
                .try_fold(
                    (tx, comparisons),
                    |(mut tx, mut comparisons), invoice| async move {
                        let postgres = sqlx::query_scalar(
                            "SELECT internal.billing_report_202308(\
                             $1::catalog_prefix, $2::timestamptz)",
                        )
                        .bind(&invoice.billed_prefix)
                        .bind(command.args.month.and_hms_opt(0, 0, 0).unwrap().and_utc())
                        .fetch_one(&mut *tx)
                        .await
                        .with_context(|| {
                            format!(
                                "reading PostgreSQL invoice calculation for {}",
                                invoice.billed_prefix
                            )
                        })?;
                        comparisons.push(Comparison {
                            rust: invoice,
                            postgres,
                        });
                        Ok::<_, anyhow::Error>((tx, comparisons))
                    },
                )
                .await?;
        tx.rollback().await?;
        serde_json::to_value(comparisons)?
    } else {
        serde_json::to_value(invoices)?
    };
    let mut output_stream = std::io::BufWriter::new(std::io::stdout().lock());
    serde_json::to_writer_pretty(&mut output_stream, &output)?;
    writeln!(output_stream)?;
    output_stream.flush()?;
    Ok(())
}

pub async fn freeze(command: &FreezeInvoices) -> anyhow::Result<()> {
    let (pool, stats) = connect(&command.args).await?;
    let inserted = billing::invoices::freeze(
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
        // `--compare-sql` runs `billing_report_202308`, which truncates the
        // month and assigns usage to days in the session time zone.
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SET TIME ZONE 'UTC'")
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
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
