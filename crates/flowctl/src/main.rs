use clap::Parser;
use tracing_subscriber::{filter::LevelFilter, EnvFilter};

fn main() -> Result<(), anyhow::Error> {
    let cli = flowctl::Cli::parse();

    // Required in order for libraries to use `rustls` for TLS.
    // See: https://docs.rs/rustls/latest/rustls/crypto/struct.CryptoProvider.html
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install default crypto provider");

    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::WARN.into()) // Otherwise it's ERROR.
        .from_env_lossy();

    tracing_subscriber::fmt::fmt()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .init();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .expect("failed to start runtime");

    let handle = runtime.spawn(async move { cli.run().await });
    let result = runtime.block_on(handle);

    // We must call `shutdown_background()` because otherwise an incomplete spawned future
    // could block indefinitely.
    runtime.shutdown_background();

    result.unwrap()
}
