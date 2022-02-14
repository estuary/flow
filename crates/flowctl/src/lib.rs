pub mod combine;
pub mod go_flowctl;
pub mod logs;

use clap::Parser;
use flow_cli_common::{init_logging, ExecExternal, ExternalArgs, LogArgs, Success};
use go_flowctl::{FlowctlGoSubcommand, GO_FLOWCTL};

// Note that the FLOW_VERSION referenced below is set in Makefile, but there is a default also
// provided in .cargo/config

/// flowctl is a CLI for interacting with Flow data planes (and soon, control planes).
#[derive(Debug, Parser)]
#[clap(author, name = "flowctl-rs", version = env!("FLOW_VERSION"))]
pub struct Flowctl {
    #[clap(subcommand)]
    pub subcommand: Subcommand,
}

/// The name of the flow-schemalate binary. This must be on the PATH.
pub const FLOW_SCHEMALATE: &str = "flow-schemalate";

#[derive(Debug, clap::Subcommand)]
#[clap(rename_all = "kebab-case")]
pub enum Subcommand {
    #[clap(flatten)]
    Internal(InternalSubcommand), // Executed as a function call
    /// Read the logs collections of Flow tasks
    Logs(logs::LogsArgs), // delegated to flowctl journals read
    #[clap(flatten)]
    FlowctlGo(FlowctlGoSubcommand), // delegated to the go flowctl binary
    /// Tools for generating various things from JSON schemas
    Schemalate(ExternalArgs), // delegated to the flow-schemalate binary
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
pub enum InternalSubcommand {
    /// Reduce JSON documents by key and print the results to stdout
    Combine(InternalSubcommandArgs<combine::CombineArgs>),
}

pub fn run_subcommand(subcommand: Subcommand) -> Result<Success, anyhow::Error> {
    use InternalSubcommand::*;
    use Subcommand::*;

    match subcommand {
        Internal(Combine(args)) => run_internal(args, combine::run).map(Into::into),
        Logs(alias_args) => alias_args.try_into_exec_external().map(Into::into),
        FlowctlGo(external) => {
            let args = external.into_flowctl_args();
            Ok(Success::Exec(ExecExternal::from((GO_FLOWCTL, args))))
        }
        Schemalate(args) => Ok(Success::Exec(ExecExternal::from((
            FLOW_SCHEMALATE,
            args.args,
        )))),
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
