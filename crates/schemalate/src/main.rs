use clap::Parser;
use flow_cli_common::{LogArgs, init_logging};

/// Schemalate (Schema + Translate) generates things from JSON schemas.
///
/// Each subcommand reads a JSON schema from stdin and writes its output to stdout. Some
/// subcommands have additional arguments to control the generated output.
#[derive(Debug, clap::Parser)]
#[clap(author, name = "flow-schemalate", version = env!("FLOW_VERSION"))]
struct Args {
    #[clap(subcommand)]
    subcommand: Subcommand,

    #[clap(flatten)]
    log_args: LogArgs,
}

#[derive(Debug, clap::Subcommand)]
enum Subcommand {
    /// Generates Markdown documentation of the fields in a schema.
    Markdown(schemalate::markdown::Args),
    // Generates a Firebolt table schema
    FireboltSchema(schemalate::firebolt::Args),
}

fn main() -> Result<(), anyhow::Error> {
    let Args {
        subcommand,
        log_args,
    } = Args::parse();
    init_logging(&log_args);
    run_subcommand(subcommand)
}

fn run_subcommand(subcommand: Subcommand) -> Result<(), anyhow::Error> {
    let result = match subcommand {
        Subcommand::Markdown(md_args) => schemalate::markdown::run(md_args),
        Subcommand::FireboltSchema(fb_args) => schemalate::firebolt::run(fb_args),
    };

    if let Err(err) = result.as_ref() {
        tracing::error!(error = ?err, "subcommand failed");
    }
    result
}
