use clap::Parser;
use tracing_subscriber::{filter::LevelFilter, EnvFilter};

fn main() -> Result<(), anyhow::Error> {
    let cli = flowctl::Cli::parse();

    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::WARN.into()) // Otherwise it's ERROR.
        .from_env_lossy();

    tracing_subscriber::fmt::fmt()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .init();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to start runtime");

    let result = runtime.block_on(async move { cli.run().await });

    // We must call `shutdown_background()` because otherwise an incomplete spawned future
    // could block indefinitely.
    runtime.shutdown_background();
    result
}
