//! Contains helpers and things that are used by all Flow rust executables.
mod logging;

pub use logging::{init_logging, LogArgs, LogFormat, LogLevel};

/// An arguments container that accepts all arguments verbatim. This is used by external
/// subcommands to allow all their argument parsing to be handled by the external binary.
// These settings prevent clap from automatically using its own help
// handling, so that --help and -h are simply forwarded to the external subcommand.
#[derive(Debug, clap::Args)]
#[clap(
    disable_help_subcommand = true,
    disable_help_flag = true,
    allow_hyphen_values = true
)]
pub struct ExternalArgs {
    pub args: Vec<String>,
}

/// The flowctl CLI boilerplate requires that all subcommands have a type for thier arguments, so
/// this is a type to use with subcommands that accept no arguments.
#[derive(Debug, clap::Args)]
pub struct NoArgs;

/// A return type indicating that a function cannot return.
pub enum Never {}

/// Executes the given `run_fn` and handles the result by either exiting or replacing the current
/// process. This function will never return, so a typical `fn main` will have this as the last
/// line. Argument parsing is left up to the caller.
/// The `run_fn` can be any function or closure that returns a Result having a `Ok` that can be
/// converted into a `Success`. Returning an `Err` will cause the error to be logged and the
/// application will exit with a code of `1`. Note that it's intentionally allowed for a function
/// to return `Success::Exit` with a non-zero code, in cases where the subcommand wishes to opt out
/// of the default error handling.
pub fn run_cli_main<A, S, F>(args: A, run_fn: F) -> Never
where
    F: FnOnce(A) -> Result<S, anyhow::Error>,
    S: Into<Success>,
{
    use std::os::unix::process::CommandExt;

    let error = match run_fn(args).map(Into::<Success>::into) {
        Ok(Success::Exit(code)) => {
            std::process::exit(code);
        }
        Ok(Success::Exec(external)) => {
            let resolved = std::env::current_exe()
                .expect("failed to fetch current exec path")
                .canonicalize()
                .expect("failed to make the exec path canonical")
                .parent()
                .expect("failed to extract exec directory")
                .join(&external.program);

            tracing::debug!(command = ?external, ?resolved, "process will be replaced by command");

            std::process::Command::new(resolved)
                .args(&external.args)
                .exec()
                .into()
        }
        Err(err) => err,
    };

    tracing::error!(error = %error, "execution failed");
    std::process::exit(1);
}

/// Represents the successful execution of a program or subcommand, and allows handler functions to
/// delegate exiting or replacing the process.
#[derive(Debug)]
pub enum Success {
    /// Program should immediately exit with the given code.
    Exit(i32),
    /// Program should use an execvp syscall to replace the current process with the program and
    /// arguments given. If there's a need, we can add env variables here and use `execvpe`, but I
    /// can't think of a use case for it right now.
    Exec(ExecExternal),
}

/// Represents the desire to replace the current process with the given program and arguments.
#[derive(Debug, PartialEq)]
pub struct ExecExternal {
    pub program: String,
    pub args: Vec<String>,
}

impl From<()> for Success {
    fn from(_: ()) -> Self {
        Success::Exit(0)
    }
}

impl From<i32> for Success {
    fn from(code: i32) -> Self {
        Success::Exit(code)
    }
}

impl<T> From<T> for Success
where
    T: Into<ExecExternal>,
{
    fn from(ex: T) -> Self {
        Success::Exec(ex.into())
    }
}

impl<P, A, T> From<(P, T)> for ExecExternal
where
    P: Into<String>,
    A: Into<String>,
    T: IntoIterator<Item = A>,
    // It's Pat!
{
    fn from((program, args): (P, T)) -> Self {
        ExecExternal {
            program: program.into(),
            args: args.into_iter().map(Into::into).collect(),
        }
    }
}
