//! Contains helpers and things that are used by all Flow rust executables.

mod logging;

pub use logging::{init_logging, LogArgs, LogFormat};

/// The flowctl CLI boilerplate requires that all subcommands have a type for thier arguments, so
/// this is a type to use with subcommands that accept no arguments.
#[derive(Debug, clap::Args)]
pub struct NoArgs;
