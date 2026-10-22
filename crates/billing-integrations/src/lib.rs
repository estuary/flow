use clap::Parser;

mod publish;
mod send;
#[cfg(test)]
mod stripe_stub;
mod stripe_utils;

#[derive(Debug, Parser)]
#[clap(version)]
pub struct Cli {
    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Debug, clap::Subcommand)]
#[clap(rename_all = "kebab-case")]
pub enum Command {
    PublishInvoices(publish::PublishInvoice),
    SendInvoices(send::SendInvoices),
}

impl Cli {
    pub async fn run(&self) -> anyhow::Result<()> {
        match &self.cmd {
            Command::PublishInvoices(publish_invoice) => {
                publish::do_publish_invoices(publish_invoice).await
            }
            Command::SendInvoices(send_invoices) => send::do_send_invoices(send_invoices).await,
        }
    }
}

fn parse_month(arg: &str) -> anyhow::Result<chrono::NaiveDate> {
    use chrono::Datelike;
    let date = chrono::NaiveDate::parse_from_str(
        &if arg.len() == 7 {
            format!("{arg}-01")
        } else {
            arg.to_owned()
        },
        "%Y-%m-%d",
    )?;
    anyhow::ensure!(date.day() == 1, "expected YYYY-MM or YYYY-MM-01");
    Ok(date)
}

/// Billing queries can exceed the default statement timeout.
async fn connect_db(connection_string: &str, max_connections: u32) -> anyhow::Result<sqlx::PgPool> {
    use anyhow::Context;
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(max_connections)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                // Raise the statement timeout to 10 minutes
                sqlx::query("set statement_timeout to 600000")
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect(connection_string)
        .await
        .context("Connecting to billing database")
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    #[test]
    fn billing_month_and_recreate_alias() {
        assert_eq!(
            super::parse_month("2026-08").unwrap(),
            chrono::NaiveDate::from_ymd_opt(2026, 8, 1).unwrap()
        );
        for command in ["publish-invoices", "send-invoices"] {
            for month in ["2026-08", "2026-08-01", "2026-08-15", "2026-13", "invalid"] {
                let args = vec![
                    "billing-integrations",
                    command,
                    "--stripe-api-key",
                    "test",
                    "--connection-string",
                    "test",
                    "--all-tenants",
                    "--month",
                    month,
                ];
                assert_eq!(
                    super::Cli::try_parse_from(args).is_ok(),
                    matches!(month, "2026-08" | "2026-08-01"),
                    "{command}: {month}"
                );
            }
        }
        for flag in ["--recreate-existing", "--recreate-finalized"] {
            assert!(
                super::Cli::try_parse_from([
                    "billing-integrations",
                    "publish-invoices",
                    "--stripe-api-key",
                    "test",
                    "--connection-string",
                    "test",
                    "--all-tenants",
                    "--month",
                    "2026-08",
                    flag
                ])
                .is_ok()
            );
        }
    }
}
