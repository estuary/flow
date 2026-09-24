//! `flow-disk-daemon`: serves block devices whose durable state lives in
//! Gazette journals.
//!
//! It needs `CAP_SYS_ADMIN` to serve a `ublk` device and to mount a filesystem, and
//! `CAP_SYS_RESOURCE` to mark each disk's owner thread an I/O flusher. It also needs
//! to own `/dev/ublk-control` and the `/dev/ublkc*` nodes it opens. Running as root
//! grants all of these. A dedicated UID needs the capabilities and a udev rule
//! instead.

use clap::Parser;

fn main() -> anyhow::Result<()> {
    // The TLS of the broker connections a tenure may ask for needs this.
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install default crypto provider");

    let args = disk_daemon::args::Args::parse();

    // The daemon registers no handlers, but the subscriber takes a registry all
    // the same: its per-handler layers cost one atomic load per event while
    // nothing is registered, and the registry is the handle an admin surface
    // would later hang off.
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

/// Install a tracing subscriber that writes structured application logs to
/// stderr. The base `EnvFilter` (`RUST_LOG`, default `info`) is composed with
/// `service_kit::trace`'s per-handler override filter, so an operator can raise
/// a handler's verbosity at runtime via the admin dashboard; `service_kit::event`
/// additionally records opt-in `event!` breadcrumbs into per-handler tracks
/// shown on the dashboard's handler drill-down page.
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
