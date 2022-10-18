use std::fmt::Display;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use tracing_subscriber::filter::{EnvFilter, LevelFilter};

use clap::Parser;

use schema_inference::analyze::{self};
use schema_inference::server::ServeArgs;

/// Reads JSON documents and infers a basic schema from its structure.
#[derive(Debug, clap::Parser)]
#[clap(author, name = "flow-schema-inference", version = env!("FLOW_VERSION"))]
pub struct Args {
    #[clap(subcommand)]
    pub subcommand: Subcommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum Subcommand {
    /// Perform basic analysis over one or more documents and print the inferred
    /// schema to stdout.
    Analyze(AnalyzeArgs),
    /// Stand up a gRPC server that exposes an API to infer a schema from a Flow collection
    Serve(ServeArgs),
}

impl Subcommand {
    async fn run(&self) -> Result<(), anyhow::Error> {
        match self {
            Subcommand::Analyze(args) => args.run(),
            Subcommand::Serve(args) => args.run().await,
        }
    }
}

impl Display for Subcommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Subcommand::Analyze(_) => f.write_str("analyze"),
            Subcommand::Serve(_) => f.write_str("serve"),
        }
    }
}

#[derive(Debug, clap::Args)]
pub struct AnalyzeArgs {
    /// Path to a file with the data to parse. If no file is given, reads
    /// from stdin.
    file: Option<PathBuf>,
}

impl AnalyzeArgs {
    fn run(&self) -> Result<(), anyhow::Error> {
        let input: Box<dyn BufRead> = match &self.file {
            Some(path) => Box::new(BufReader::new(File::open(path)?)),
            None => Box::new(std::io::stdin().lock()),
        };

        let schema = analyze::infer_schema(input)?;

        serde_json::to_writer_pretty(std::io::stdout().lock(), &schema)?;

        Ok(())
    }
}
#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
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
