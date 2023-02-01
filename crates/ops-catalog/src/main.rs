use std::{env, fmt::Display};

use clap::Parser;
use ops_catalog::{generate::GenerateArgs, monitor::MonitorArgs};
use tracing_subscriber::filter::{EnvFilter, LevelFilter};

#[derive(Debug, clap::Parser)]
#[clap(author, name = "ops-catalog", version = env!("FLOW_VERSION"))]
pub struct Args {
    #[clap(subcommand)]
    pub subcommand: Subcommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum Subcommand {
    /// Monitor the ops catalog for changes and apply updates as needed.
    Monitor(MonitorArgs),
    /// Generate local specs for inspection and testing from a jsonl tenant list via stdin.
    Generate(GenerateArgs),
}

impl Subcommand {
    async fn run(&self) -> anyhow::Result<()> {
        match self {
            Subcommand::Monitor(args) => args.run().await,
            Subcommand::Generate(args) => args.run(),
        }
    }
}

impl Display for Subcommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Subcommand::Monitor(_) => f.write_str("monitor"),
            Subcommand::Generate(_) => f.write_str("generate"),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting tracing default failed");

    let args = Args::parse();

    args.subcommand.run().await
}
