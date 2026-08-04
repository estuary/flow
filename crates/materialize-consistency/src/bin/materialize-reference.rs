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
    /// The harness reads destinations through connector code rather than reaching into them,
    /// because it has no client for an arbitrary endpoint and should not grow one. This
    /// subcommand serves *this* connector only: a real subject is read through
    /// `tests/materialize/testctl` in the connectors repository, so there are deliberately two
    /// paths — see `harness::stack::ReadVia`. A subcommand is fine here because this binary
    /// lives in the flow repository and nothing but this suite runs it.
    Read {
        /// Path to the endpoint configuration, as JSON or YAML.
        #[arg(long)]
        config: String,
        /// Path to the resource configuration, as JSON or YAML.
        #[arg(long)]
        resource: String,
    },
}

fn main() -> std::process::ExitCode {
    install_panic_hook();

    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(err) => {
            // As one structured line, because the reactor parses a connector's stderr as
            // logs and discards anything that is not: an `anyhow` chain printed plainly
            // reaches nobody, and the failure surfaces two layers up as the far less
            // useful "connector exited with no log output".
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

/// Report a panic as one structured line, for the same reason errors are: the reactor
/// parses a connector's stderr as logs and drops what is not, so the default hook's
/// plain text is discarded and the death surfaces as "connector exited with no log
/// output" — indistinguishable from an injected crash. `at-least-once-never-loses`
/// failed three consecutive suite runs with exactly that message, at a point where its
/// crash fault could not yet have armed.
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
            let config = load_json::<reference::EndpointConfig>(&config)?;
            let resource: reference::ResourceConfig = load_json(&resource)?;
            reference::read(&config, &resource.table, resource.delta)
        }
        // No subcommand: serve the materialization protocol on stdio, which is
        // how the runtime invokes a `local:` connector.
        None => reference::serve(connector_init::Codec::Json),
    }
}

/// Load a config file as JSON or YAML.
///
/// YAML because that is how the connectors repository writes the endpoint configs its
/// integration tests use, and because it subsumes JSON — so the same reader serves a
/// harness passing a temporary JSON file and a person passing `config.local.yaml`.
fn load_json<T: serde::de::DeserializeOwned>(path: &str) -> anyhow::Result<T> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("reading {path}"))?;
    serde_yaml::from_str(&raw).with_context(|| format!("parsing {path}"))
}
