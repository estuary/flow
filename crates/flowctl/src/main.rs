use clap::Parser;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, filter::LevelFilter};

fn main() -> Result<(), anyhow::Error> {
    let cli = flowctl::Cli::parse();

    // Required in order for libraries to use `rustls` for TLS.
    // See: https://docs.rs/rustls/latest/rustls/crypto/struct.CryptoProvider.html
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install default crypto provider");

    // Process-global handler registry. Service-kit's trace layer consults this
    // for per-handler verbosity overrides, and its event layer records opt-in
    // `event!` breadcrumbs onto registered handlers. Both are inert unless a
    // command (today: `raw preview-next --debug-port`) actually registers
    // handlers and serves the admin surface that lets an operator flip them on.
    let registry = service_kit::Registry::new();

    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::WARN.into()) // Otherwise it's ERROR.
        .from_env_lossy();

    let fmt_layer = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);

    tracing_subscriber::registry()
        .with(fmt_layer.with_filter(service_kit::trace::layer_filter(
            env_filter,
            registry.clone(),
        )))
        .with(service_kit::event::layer(registry.clone()))
        .init();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to start runtime");

    let handle = runtime.spawn(async move { cli.run(registry).await });
    let result = runtime.block_on(handle);

    // We must call `shutdown_background()` because otherwise an incomplete spawned future
    // could block indefinitely.
    runtime.shutdown_background();

    result.unwrap()
}
