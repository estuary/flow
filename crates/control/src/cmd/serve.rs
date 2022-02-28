use std::net::TcpListener;

use crate::cmd::{async_runtime, ConfigArgs};
use crate::config;
use crate::startup;

#[derive(clap::Args, Debug)]
pub struct Args {
    #[clap(flatten)]
    config: ConfigArgs,
}

/// Runs the control plane server.
pub fn run(args: Args) -> anyhow::Result<()> {
    config::load_settings(args.config.config_path)?;
    let runtime = async_runtime()?;
    let listener = TcpListener::bind(config::settings().application.address())?;

    runtime.block_on(async move {
        // Run the server until it decides to shut down
        serve(listener).await
    })
}

async fn serve(listener: TcpListener) -> anyhow::Result<()> {
    let db = startup::connect_to_postgres(&config::settings().database).await;
    let (put_builds, fetch_builds) = startup::init_builds_root(&config::settings().builds_root)?;
    let server = startup::run(listener, db, put_builds, fetch_builds)?;

    // The server runs until it receives a shutdown signal.
    server.await?;

    Ok(())
}
