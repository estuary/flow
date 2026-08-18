//! `flow-disk-daemon`: serves block devices whose durable state lives in
//! Gazette journals.
//!
//! It needs `CAP_SYS_ADMIN` to serve a `ublk` device and to mount a filesystem. It
//! also needs to own `/dev/ublk-control` and the `/dev/ublkc*` nodes it opens.
//! Running as root grants both. A dedicated UID needs a udev rule instead.

use clap::Parser;
use disk_daemon::args::{Command, LogFormat};

fn main() -> anyhow::Result<()> {
    // The TLS of the broker connections a session may ask for needs this.
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install default crypto provider");

    let Command::Serve(serve) = disk_daemon::args::Args::parse().command;

    // Two things share this. The tracing subscriber reads its per-handler trace
    // overrides, and the session service populates it.
    let registry = service_kit::Registry::new();
    () = install_tracing(serve.log_format, registry.clone());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let served = runtime.spawn(disk_daemon::daemon::run(serve, registry));
    let result = runtime.block_on(served);
    runtime.shutdown_timeout(std::time::Duration::from_secs(5));

    result?
}

/// Write structured logs to stderr. The base `EnvFilter` reads `RUST_LOG` and
/// defaults to `info`. It composes with `service_kit::trace`'s override filter, so
/// an operator can raise one session's verbosity from the admin surface.
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
        // Colour an interactive run only, so no escape code reaches a log
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
