use clap::Parser;

mod publish;
mod send;
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
                let mut args = vec![
                    "billing-integrations",
                    command,
                    "--stripe-api-key",
                    "test",
                    "--all-tenants",
                    "--month",
                    month,
                ];
                if command == "publish-invoices" {
                    args.extend(["--connection-string", "test"]);
                }
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
