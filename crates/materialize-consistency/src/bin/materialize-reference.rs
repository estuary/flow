//! The reference materialization: a `local:` connector over a SQLite
//! destination, whose consistency class and defects are chosen by configuration.
//! See `reference/mod.rs`.

use anyhow::Context;
use clap::{Parser, Subcommand};
use materialize_consistency::reference;

#[derive(Parser)]
#[command(about = "A reference materialization with switchable consistency defects.")]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Read every row of a materialized resource as newline-delimited JSON.
    ///
    /// The harness reads destinations through the connector rather than reaching
    /// into them, mirroring `materialize-boilerplate`'s `read` subcommand so that
    /// one code path serves the reference connector and real ones alike.
    Read {
        /// Endpoint configuration, as a JSON document or a path to one.
        #[arg(long)]
        config: String,
        #[arg(long)]
        table: String,
        /// Read the resource as an append-only log, preserving delivery order.
        #[arg(long)]
        delta: bool,
    },
}

fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(err) => {
            // As one structured line, because the reactor parses a connector's stderr
            // as logs and discards anything that is not — so an `anyhow` chain printed
            // plainly reaches nobody, and the failure surfaces two layers up as
            // "connector exited with no log output". That is how 21 restarts of this
            // connector were observed without ever seeing why it died.
            let line = serde_json::json!({
                "level": "error",
                "msg": "the reference connector failed",
                "fields": {
                    "error": format!("{err:#}"),
                },
            });
            eprintln!("{line}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn run() -> anyhow::Result<()> {
    let args = Args::parse();

    match args.command {
        Some(Command::Read {
            config,
            table,
            delta,
        }) => {
            let config = load_config(&config)?;
            reference::read(&config, &table, delta)
        }
        // No subcommand: serve the materialization protocol on stdio, which is
        // how the runtime invokes a `local:` connector.
        None => reference::serve(connector_init::Codec::Json),
    }
}

fn load_config(arg: &str) -> anyhow::Result<reference::EndpointConfig> {
    let json = if arg.trim_start().starts_with('{') {
        arg.to_string()
    } else {
        std::fs::read_to_string(arg).with_context(|| format!("reading config {arg}"))?
    };
    serde_json::from_str(&json).context("parsing endpoint configuration")
}
