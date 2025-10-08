use billing_integrations::Cli;
use clap::Parser;
use tracing_subscriber::{EnvFilter, filter::LevelFilter};

fn main() -> Result<(), anyhow::Error> {
    // Colorization support for Win 10.
    #[cfg(windows)]
    let _ = colored_json::enable_ansi_support();

    let cli = Cli::parse();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to start runtime");

    // Use reasonable defaults for printing structured logs to stderr.
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .compact()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .without_time()
        .with_target(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting tracing default failed");

    let result = runtime.block_on(async move { cli.run().await });

    // We must call `shutdown_background()` because otherwise an incomplete spawned future
    // could block indefinitely.
    runtime.shutdown_background();
    result
}
