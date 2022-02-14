use clap::Parser;
use flowctl::{run_subcommand, Flowctl};

fn main() {
    // calling parse will automatically handle --help and --version flags that were provided as
    // top-level arguments. If it does, then it will exit(0) automatically. This will not be the
    // case for external subcommands, though, as they handle their own --help and --version flags.
    let cli = Flowctl::parse();
    flow_cli_common::run_cli_main(cli.subcommand, run_subcommand);
    unreachable!();
}
