mod combine;
mod external;

use clap::Parser;
use external::{exec_go_flowctl, ExternalSubcommand};
use flow_cli_common::{init_logging, LogArgs, NoArgs};

// Note that the FLOW_VERSION referenced below is set in Makefile, but there is a default also
// provided in .cargo/config

/// flowctl is a CLI for interacting with Flow data planes (and soon, control planes).
#[derive(Debug, Parser)]
#[clap(name = "flowctl-rs", version = env!("FLOW_VERSION"))]
struct Flowctl {
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

/// A wrapper type for internal subcommand arguments, which defines logging arguments that apply to
/// all internal subcommands.
#[derive(Debug, clap::Args)]
pub struct InternalSubcommandArgs<T: clap::Args + std::fmt::Debug> {
    #[clap(flatten)]
    log_args: LogArgs,

    #[clap(flatten)]
    args: T,
}

#[derive(Debug, clap::Subcommand)]
#[clap(rename_all = "kebab-case")]
enum InternalSubcommand {
    /// Reduce JSON documents by key and print the results to stdout
    Combine(InternalSubcommandArgs<combine::CombineArgs>),

    /// Generates TypeScript types from JSON Schemas
    SchemaToTypescript(InternalSubcommandArgs<schemalate::typescript::Args>),
    /// Generates Markdown documentation for the given JSON schema
    SchemaToMarkdown(InternalSubcommandArgs<NoArgs>),
}

fn main() -> Result<(), anyhow::Error> {
    // calling parse will automatically handle --help and --version flags that were provided as
    // top-level arguments. If it does, then it will exit(0) automatically. This will not be the
    // case for external subcommands, though, as they handle their own --help and --version flags.
    let cli = Flowctl::parse();
    run_subcommand(cli.subcommand)
}

fn run_subcommand(subcommand: Subcommand) -> Result<(), anyhow::Error> {
    use InternalSubcommand::*;
    use Subcommand::*;

    match subcommand {
        Internal(Combine(args)) => run_internal(args, combine::run),
        Internal(SchemaToTypescript(args)) => run_internal(args, schemalate::typescript::run),
        Internal(SchemaToMarkdown(args)) => run_internal(args, schemalate::markdown::run),

        External(external) => {
            let (subcommand, external_args) = external.into_subcommand_and_args();
            let err = exec_go_flowctl(subcommand, &external_args);
            Err(err.into())
        }
    }
}

fn run_internal<T, F>(
    subcommand_args: InternalSubcommandArgs<T>,
    run_fn: F,
) -> Result<(), anyhow::Error>
where
    T: clap::Args + std::fmt::Debug,
    F: FnOnce(T) -> Result<(), anyhow::Error>,
{
    let InternalSubcommandArgs { log_args, args } = subcommand_args;

    init_logging(&log_args);
    let result = run_fn(args);
    if let Err(err) = result.as_ref() {
        tracing::error!(error = ?err, "subcommand failed");
    }
    result
}
