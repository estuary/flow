mod combine;
mod external;

use clap::Parser;
use external::{exec_go_flowctl, ExternalArgs};

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
enum ExternalSubcommand {
    /// Low-level APIs for automation
    Api(ExternalArgs),
    /// Check a Flow catalog for errors
    Check(ExternalArgs),
    /// Build a catalog and deploy it to a data plane
    Deploy(ExternalArgs),
    /// Discover available captures of an endpoint
    Discover(ExternalArgs),
    /// Interact with broker journals
    Journals(ExternalArgs),
    /// Print the catalog JSON schema
    JsonSchema(ExternalArgs),
    /// Print combined configuration and exit
    PrintConfig(ExternalArgs),
    /// Serve a component of Flow
    Serve(ExternalArgs),
    /// Interact with consumer shards
    Shards(ExternalArgs),
    /// Run an ephemeral, temporary local data plane
    TempDataPlane(ExternalArgs),
    /// Locally test a Flow catalog
    Test(ExternalArgs),
}

impl ExternalSubcommand {
    fn subcommand_and_args(&self) -> (&'static str, &ExternalArgs) {
        use ExternalSubcommand::*;
        match self {
            Api(a) => ("api", a),
            Check(a) => ("check", a),
            Deploy(a) => ("deploy", a),
            Discover(a) => ("discover", a),
            Journals(a) => ("journals", a),
            JsonSchema(a) => ("json-schema", a),
            PrintConfig(a) => ("print-config", a),
            Serve(a) => ("serve", a),
            Shards(a) => ("shards", a),
            TempDataPlane(a) => ("temp-data-plane", a),
            Test(a) => ("test", a),
        }
    }
}

#[derive(Debug, clap::Subcommand)]
#[clap(rename_all = "kebab-case")]
enum InternalSubcommand {
    /// Reduce JSON documents by key and print the results to stdout
    Combine(combine::CombineArgs),
}

fn main() -> Result<(), std::io::Error> {
    // calling parse will automatically handle --help and --version flags that were provided as
    // top-level arguments. If it does, then it will exit(0) automatically. This will not be the
    // case for external subcommands, though, as they handle their own --help and --version flags.
    let cli = Cli::parse();

    match cli.subcommand {
        Subcommand::Internal(internal) => execute_internal_subcommand(internal),
        Subcommand::External(external) => {
            let (subcommand, external_args) = external.subcommand_and_args();
            let err = exec_go_flowctl(subcommand, external_args);
            Err(err)
        }
    }
}

fn execute_internal_subcommand(subcommand: InternalSubcommand) -> Result<(), std::io::Error> {
    match subcommand {
        InternalSubcommand::Combine(args) => {
            eprintln!("combine: {:?}", args);
            Ok(())
        }
    }
}
