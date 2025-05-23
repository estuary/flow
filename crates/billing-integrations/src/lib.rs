use clap::Parser;

mod publish;
mod stripe_utils;
mod send;

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
