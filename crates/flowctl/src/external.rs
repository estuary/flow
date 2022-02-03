use std::os::unix::process::CommandExt;
use std::process::Command;

// These settings are applied to ExternalArgs to prevent clap from automatically using its own help
// handling, so that --help and -h are simply forwarded to the external subcommand.
use clap::AppSettings::{AllowHyphenValues, DisableHelpFlag, NoAutoHelp};

/// The name of the go-based flowctl binary. This must be on the PATH.
const GO_FLOWCTL: &str = "flowctl";

/// A simple arguments container that simply takes everything as a plain string value.
/// This is used by all the external subcommands to allow all their argument parsing to be handled
/// by the external binary.
#[derive(Debug, clap::Args)]
#[clap(setting = NoAutoHelp | DisableHelpFlag | AllowHyphenValues)]
pub struct ExternalArgs {
    pub args: Vec<String>,
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
