use std::os::unix::process::CommandExt;
use std::process::Command;

// These settings are applied to ExternalArgs to prevent clap from automatically using its own help
// handling, so that --help and -h are simply forwarded to the external subcommand.
use clap::AppSettings::{AllowHyphenValues, DisableHelpFlag, NoAutoHelp};

/// The name of the go-based flowctl binary. This must be on the PATH.
const GO_FLOWCTL: &str = "flowctl";

const FLOW_SCHEMALATE: &str = "flow-schemalate";

/// A simple arguments container that simply takes everything as a plain string value.
/// This is used by all the external subcommands to allow all their argument parsing to be handled
/// by the external binary.
#[derive(Debug, clap::Args)]
#[clap(setting = NoAutoHelp | DisableHelpFlag | AllowHyphenValues)]
pub struct ExternalArgs {
    pub args: Vec<String>,
}

/// External subcommands are those that are provided by the flowctl Go-based binary.
#[derive(Debug, clap::Subcommand)]
#[clap(rename_all = "kebab-case")]
pub enum FlowctlGoSubcommand {
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

impl FlowctlGoSubcommand {
    pub fn into_subcommand_and_args(self) -> (&'static str, ExternalArgs) {
        use FlowctlGoSubcommand::*;
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

/// Executes the given external subcommand. This function will not return unless there's an error.
/// In the normal case, the current process will be replaced by the process of the subcommand.
/// This is to remove a seemingly uncessary parent process from the hierarchy, since Go flowctl
/// subcommands are themselves liable to spawn child processes.
pub fn exec_go_flowctl(subcommand: &str, args: &ExternalArgs) -> std::io::Error {
    Command::new(GO_FLOWCTL)
        .arg(subcommand)
        .args(args.args.as_slice())
        .exec()
}

/// Executes `flow-schemalate` with the given arguments. This function will not return unless
/// there's an error starting that process. In the normal case, the current process will be
/// replaced by the process of the subcommand.
pub fn exec_flow_schemalate(args: &ExternalArgs) -> std::io::Error {
    Command::new(FLOW_SCHEMALATE)
        .args(args.args.as_slice())
        .exec()
}
