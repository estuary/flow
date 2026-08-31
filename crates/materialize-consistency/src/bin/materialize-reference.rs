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
    Read {
        /// Path to a YAML file holding the endpoint configuration.
        #[arg(long)]
        config: String,
        /// Path to a YAML file holding the resource configuration.
        #[arg(long)]
        resource: String,
    },
}

fn main() -> std::process::ExitCode {
    install_panic_hook();

    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(err) => {
            // Report the failure as one structured line. The reactor reads each stderr line as
            // a separate warning, so here we accumulate all of the error and emit the whole
            // multi-line error as a JSON object in a single line to keep it tidy.
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

/// Report a panic as one structured line, for the same reason errors are.
fn install_panic_hook() {
    let default = std::panic::take_hook();

    std::panic::set_hook(Box::new(move |info| {
        let line = serde_json::json!({
            "level": "error",
            "msg": "the reference connector panicked",
            "fields": {
                "panic": info.to_string(),
            },
        });
        eprintln!("{line}");
        default(info);
    }));
}

fn run() -> anyhow::Result<()> {
    let args = Args::parse();

    match args.command {
        Some(Command::Read { config, resource }) => {
            let config = load_config::<reference::EndpointConfig>(&config)?;
            let resource: reference::ResourceConfig = load_config(&resource)?;
            reference::read(&config, &resource.table, resource.delta)
        }
        // No subcommand: serve the materialization protocol on stdio.
        None => reference::serve(connector_init::Codec::Json),
    }
}

/// Load a YAML configuration file.
fn load_config<T: serde::de::DeserializeOwned>(path: &str) -> anyhow::Result<T> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("reading {path}"))?;
    serde_yaml::from_str(&raw).with_context(|| format!("parsing {path}"))
}
