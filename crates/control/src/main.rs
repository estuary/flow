use std::net::TcpListener;

use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    info!("Running in {} mode", control::config::app_env().as_str());

    let settings = control::config::settings();
    let listener = TcpListener::bind(settings.application.address())?;
    let server = control::run(listener)?;

    info!("Listening on http://{}", settings.application.address());

    // The server runs until it receives a shutdown signal.
    server.await?;

    Ok(())
}
