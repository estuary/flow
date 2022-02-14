use clap::Parser;
use flow_cli_common::LogArgs;
use std::net::TcpListener;

use control::config;
use control::config::app_env::{self, AppEnv};
use control::startup;

/// Runs the control plane api server in development mode.
#[derive(Debug, Parser)]
#[clap(author, name = "control", version = "dev", about)]
struct Args {
    #[clap(flatten)]
    pub log_args: LogArgs,
}

/// Runs the development server. This sets a few defaults:
/// * Runs in `AppEnv::Development` mode
/// * Loads application configuration from `config/development.toml`
/// * Connects to a local postgres database. This is necessary for compilation.
///
/// See `tests/it/main.rs` to launch the server in test mode.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    app_env::force_env(AppEnv::Development);

    let args = Args::parse();
    flow_cli_common::init_logging(&args.log_args);

    let settings = config::load_settings("config/development.toml")?;
    let listener = TcpListener::bind(settings.application.address())?;
    let db = startup::connect_to_postgres(&settings.database).await;
    let server = startup::run(listener, db)?;

    // The server runs until it receives a shutdown signal.
    server.await?;

    Ok(())
}
