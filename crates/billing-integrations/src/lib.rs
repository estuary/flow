use clap::Parser;

mod stripe;

#[derive(Debug, Parser)]
#[clap(version)]
pub struct Cli {
    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Debug, clap::Subcommand)]
#[clap(rename_all = "kebab-case")]
pub enum Command {
    Stripe(stripe::PublishInvoice),
}

impl Cli {
    pub async fn run(&self) -> anyhow::Result<()> {
        match &self.cmd {
            Command::Stripe(publish_invoice) => stripe::do_publish_invoices(publish_invoice).await,
        }
    }
}
