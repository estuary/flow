use clap::Parser;

fn main() -> anyhow::Result<()> {
    // The TLS of the broker connections a tenure may ask for needs this.
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install default crypto provider");

    let args = disk_daemon::args::Args::parse();

    // The shared tracing layers require a registry even without handlers.
    let registry = service_kit::Registry::new();
    install_tracing(args.log_format, registry);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let served = runtime.spawn(disk_daemon::daemon::run(args));
    let result = runtime.block_on(served);
    runtime.shutdown_timeout(std::time::Duration::from_secs(5));

    result?
}

/// Write structured logs to stderr using `RUST_LOG` (default `info`) and the
/// shared service-kit tracing layers. No admin handlers are registered yet.
fn install_tracing(log_format: disk_daemon::args::LogFormat, registry: service_kit::Registry) {
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    // `fmt` layer, boxed so the JSON and text variants share one assembly path.
    let fmt_layer: Box<dyn Layer<tracing_subscriber::Registry> + Send + Sync> = match log_format {
        disk_daemon::args::LogFormat::Json => Box::new(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(std::io::stderr),
        ),
        disk_daemon::args::LogFormat::Text => {
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
