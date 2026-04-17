pub mod configs;
pub mod subscriptions;

#[derive(Debug, clap::Args)]
pub struct Alerts {
    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Debug, clap::Subcommand)]
#[clap(rename_all = "kebab-case")]
pub enum Command {
    /// View and manage subscriptions to alerts and notifications
    Subscriptions(subscriptions::AlertSubscriptions),
    /// View and manage per-prefix alert configuration
    Configs(configs::Configs),
}

impl Alerts {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
        match &self.cmd {
            Command::Subscriptions(s) => s.run(ctx).await,
            Command::Configs(c) => c.run(ctx).await,
        }
    }
}
