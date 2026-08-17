//! `flow-disk-daemon`: serves block devices whose durable state lives in
//! Gazette journals.
//!
//! It needs `CAP_SYS_ADMIN` to serve a `ublk` device and to mount a filesystem,
//! plus ownership of `/dev/ublk-control` and the `/dev/ublkc*` nodes it opens,
//! which running as root gives and a dedicated UID needs a udev rule for.

use clap::Parser;
use disk_daemon::args::{Command, LogFormat};

fn main() -> anyhow::Result<()> {
    // Required by the TLS of the broker connections a session may ask for.
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install default crypto provider");

    let args = disk_daemon::args::Args::parse();

    // Shared between the tracing subscriber, which consults per-handler trace
    // overrides, and the session service, which populates it.
    let registry = service_kit::Registry::new();

    let log_format = match &args.command {
        Command::Serve(serve) => serve.log_format,
        // The client is a terminal tool, and its own output is stdout.
        Command::Client(_) => LogFormat::Text,
    };
    () = install_tracing(log_format, registry.clone());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let served = match args.command {
        Command::Serve(serve) => runtime.spawn(disk_daemon::daemon::run(serve, registry)),
        Command::Client(client) => runtime.spawn(disk_daemon::client::run(client)),
    };
    let result = runtime.block_on(served);
    runtime.shutdown_timeout(std::time::Duration::from_secs(5));

    result?
}

/// Write structured logs to stderr. The base `EnvFilter` (`RUST_LOG`, `info` by
/// default) composes with `service_kit::trace`'s override filter, so an operator
/// can raise one session's verbosity from the admin surface.
fn install_tracing(log_format: LogFormat, registry: service_kit::Registry) {
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    let format: Box<dyn Layer<tracing_subscriber::Registry> + Send + Sync> = match log_format {
        LogFormat::Json => Box::new(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(std::io::stderr),
        ),
        // Colour only an interactive run, so escape codes never reach a log
        // collector.
        LogFormat::Text => Box::new(
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
