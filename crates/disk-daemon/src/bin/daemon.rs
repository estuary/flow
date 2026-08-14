//! `flow-disk-daemon`: serves block devices whose durable state lives in
//! Gazette journals.
//!
//! It needs `CAP_SYS_ADMIN` to serve a `ublk` device and to mount a filesystem,
//! plus ownership of `/dev/ublk-control` and the `/dev/ublkc*` nodes it opens,
//! which running as root gives and a dedicated UID needs a udev rule for.

use clap::Parser;

fn main() -> anyhow::Result<()> {
    // Required by the TLS of the broker connections a session may ask for.
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install default crypto provider");

    let args = disk_daemon::args::Args::parse();

    // Shared between the tracing subscriber, which consults per-handler trace
    // overrides, and the session service, which populates it.
    let registry = service_kit::Registry::new();
    () = install_tracing(args.log_format, registry.clone());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let result = runtime.block_on(runtime.spawn(disk_daemon::daemon::run(args, registry)));
    runtime.shutdown_timeout(std::time::Duration::from_secs(5));

    result?
}

/// Write structured logs to stderr. The base `EnvFilter` (`RUST_LOG`, `info` by
/// default) composes with `service_kit::trace`'s override filter, so an operator
/// can raise one session's verbosity from the admin surface.
fn install_tracing(log_format: disk_daemon::args::LogFormat, registry: service_kit::Registry) {
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    let format: Box<dyn Layer<tracing_subscriber::Registry> + Send + Sync> = match log_format {
        disk_daemon::args::LogFormat::Json => Box::new(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(std::io::stderr),
        ),
        // Colour only an interactive run, so escape codes never reach a log
        // collector.
        disk_daemon::args::LogFormat::Text => Box::new(
            tracing_subscriber::fmt::layer()
                .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
                .with_writer(std::io::stderr),
        ),
    };

    tracing_subscriber::registry()
        .with(format.with_filter(service_kit::trace::layer_filter(
            env_filter,
            registry.clone(),
        )))
        .with(service_kit::event::layer(registry))
        .init();
}
