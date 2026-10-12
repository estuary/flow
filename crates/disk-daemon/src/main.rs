use clap::Parser as _;

fn main() -> anyhow::Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install default crypto provider");

    let args = disk_daemon::daemon::Args::parse();

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

fn install_tracing(log_format: disk_daemon::daemon::LogFormat, registry: service_kit::Registry) {
    use tracing_subscriber::Layer as _;
    use tracing_subscriber::layer::SubscriberExt as _;
    use tracing_subscriber::util::SubscriberInitExt as _;

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    let fmt_layer: Box<dyn tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync> =
        match log_format {
            disk_daemon::daemon::LogFormat::Json => Box::new(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_writer(std::io::stderr),
            ),
            disk_daemon::daemon::LogFormat::Text => {
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
