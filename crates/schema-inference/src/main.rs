use std::fmt::Display;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use clap::Parser;

use schema_inference::analyze;

/// Reads JSON documents and infers a basic schema from its structure.
#[derive(Debug, clap::Parser)]
#[clap(author, name = "flow-schema-inference", version = env!("FLOW_VERSION"))]
pub struct Args {
    #[clap(subcommand)]
    pub subcommand: Subcommand,
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

#[derive(Debug, clap::Subcommand)]
pub enum Subcommand {
    // Perform basic analysis over one or more documents and print the inferred
    // schema to stdout.
    Analyze(AnalyzeArgs),
}

impl Subcommand {
    fn run(&self) -> Result<(), anyhow::Error> {
        match self {
            Subcommand::Analyze(args) => args.run(),
        }
    }
}

impl Display for Subcommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Subcommand::Analyze(_) => f.write_str("analyze"),
        }
    }
}

fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();

    args.subcommand.run()
}
