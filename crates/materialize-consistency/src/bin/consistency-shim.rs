//! `consistency-shim -- <connector> [args...]`
//!
//! Named as the `local:` command of a materialization under test, with the real
//! connector binary as its trailing argument. See `shim.rs`.

use anyhow::Context;
use clap::Parser;
use materialize_consistency::protocol::{ENV_FAULTS, ENV_RUN_DIR};
use materialize_consistency::shim::Shim;

#[derive(Parser)]
#[command(
    about = "Interpose on a materialization connector's protocol stream, tracing it and injecting faults."
)]
struct Args {
    /// Speak the length-prefixed protobuf codec rather than newline-delimited
    /// JSON. Must match the `local:` endpoint's `protobuf` setting.
    #[arg(long)]
    protobuf: bool,

    /// The connector to run, and its arguments.
    #[arg(trailing_var_arg = true, required = true)]
    connector: Vec<String>,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let codec = if args.protobuf {
        connector_init::Codec::Proto
    } else {
        connector_init::Codec::Json
    };

    let run_dir = std::env::var(ENV_RUN_DIR)
        .with_context(|| format!("{ENV_RUN_DIR} must name this run's directory"))?;
    let faults = std::env::var(ENV_FAULTS).ok();

    let shim = Shim::new(run_dir, faults, codec)?;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let status = runtime.block_on(shim.run(args.connector))?;

    // Mirror the connector's own exit status: the runtime distinguishes a clean
    // end-of-session from a failure by exactly this.
    std::process::exit(status.code().unwrap_or(1));
}
