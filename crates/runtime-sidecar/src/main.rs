use clap::Parser;

fn main() -> Result<(), anyhow::Error> {
    // Required for libraries that use rustls (tonic TLS, gazette client TLS).
    // See https://docs.rs/rustls/latest/rustls/crypto/struct.CryptoProvider.html
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install default crypto provider");

    let args = runtime_sidecar::Args::parse();
    // The handler registry is shared between the tracing subscriber (which
    // consults per-handler trace overrides) and the services (which populate it
    // and expose it via the admin surface).
    let registry = service_kit::Registry::new();
    install_tracing(args.log_format, registry.clone());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let result = runtime.block_on(runtime.spawn(runtime_sidecar::run(args, registry)));
    runtime.shutdown_timeout(std::time::Duration::from_secs(5));
    result?
}

/// Install a tracing subscriber that writes structured application logs to
/// stderr. The base `EnvFilter` (`RUST_LOG`, default `info`) is composed with
/// `service_kit::trace`'s per-handler override filter, so an operator can raise
/// a handler's verbosity at runtime via the admin dashboard; `service_kit::event`
/// additionally captures opt-in `event!` breadcrumbs into per-handler tracks
/// shown on the dashboard's handler drill-down page.
fn install_tracing(log_format: runtime_sidecar::LogFormat, registry: service_kit::Registry) {
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    // `fmt` layer, boxed so the JSON and text variants share one assembly path.
    let fmt_layer: Box<dyn Layer<tracing_subscriber::Registry> + Send + Sync> = match log_format {
        runtime_sidecar::LogFormat::Json => Box::new(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(std::io::stderr),
        ),
        runtime_sidecar::LogFormat::Text => {
            let no_color = matches!(std::env::var("NO_COLOR"), Ok(v) if v == "1");
            Box::new(
                tracing_subscriber::fmt::layer()
                    .with_ansi(!no_color)
                    .with_writer(std::io::stderr),
            )
        }
    };

    tracing_subscriber::registry()
        .with(fmt_layer.with_filter(service_kit::trace::layer_filter(
            env_filter,
            registry.clone(),
        )))
        .with(service_kit::event::layer(registry))
        .init();
}
