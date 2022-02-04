mod combine;
mod external;

use clap::Parser;
use external::{exec_go_flowctl, ExternalSubcommand};

/// flowctl is a CLI for interacting with Flow data planes (and soon, control planes).
#[derive(Debug, Parser)]
struct Cli {
    #[clap(subcommand)]
    subcommand: Subcommand,
}

#[derive(Debug, clap::Subcommand)]
#[clap(rename_all = "kebab-case")]
enum Subcommand {
    #[clap(flatten)]
    Internal(InternalSubcommand),
    #[clap(flatten)]
    External(ExternalSubcommand),
}

#[derive(Debug, clap::Subcommand)]
#[clap(rename_all = "kebab-case")]
enum InternalSubcommand {
    /// Reduce JSON documents by key and print the results to stdout
    Combine(combine::CombineArgs),
}

fn main() -> Result<(), anyhow::Error> {
    // calling parse will automatically handle --help and --version flags that were provided as
    // top-level arguments. If it does, then it will exit(0) automatically. This will not be the
    // case for external subcommands, though, as they handle their own --help and --version flags.
    let cli = Cli::parse();
    run_subcommand(cli.subcommand)
}

fn run_subcommand(subcommand: Subcommand) -> Result<(), anyhow::Error> {
    match subcommand {
        Subcommand::Internal(internal) => execute_internal_subcommand(internal),
        Subcommand::External(external) => {
            let (subcommand, external_args) = external.into_subcommand_and_args();
            let err = exec_go_flowctl(subcommand, &external_args);
            Err(err.into())
        }
    }
}

fn execute_internal_subcommand(subcommand: InternalSubcommand) -> Result<(), anyhow::Error> {
    match subcommand {
        InternalSubcommand::Combine(args) => combine::run(args),
    }
}
