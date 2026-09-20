//! `flow-disk-daemon`: serves block devices whose durable state lives in
//! Gazette journals.
//!
//! It needs `CAP_SYS_ADMIN` to serve a `ublk` device and to mount a filesystem. It
//! also needs to own `/dev/ublk-control` and the `/dev/ublkc*` nodes it opens.
//! Running as root grants both. A dedicated UID needs a udev rule instead.

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
    () = service_kit::trace::init(args.log_format, registry);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let served = runtime.spawn(disk_daemon::daemon::run(args));
    let result = runtime.block_on(served);
    runtime.shutdown_timeout(std::time::Duration::from_secs(5));

    result?
}
