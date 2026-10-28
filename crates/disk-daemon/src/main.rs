use anyhow::Context;
use clap::Parser as _;

fn main() -> anyhow::Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install default crypto provider");

    let args = disk_daemon::daemon::Args::parse();
    install_tracing(args.log_format);

    // Each promoted disk holds several file descriptors, so the default soft
    // limit of 1024 caps the daemon at a few hundred disks.
    let nofile = rustix::process::getrlimit(rustix::process::Resource::Nofile);
    rustix::process::setrlimit(
        rustix::process::Resource::Nofile,
        rustix::process::Rlimit {
            current: nofile.maximum,
            ..nofile
        },
    )
    .context("raising the open file limit")?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let served = runtime.spawn(disk_daemon::daemon::run(args));
    let result = runtime.block_on(served);
    runtime.shutdown_timeout(std::time::Duration::from_secs(5));

    result?
}

fn install_tracing(log_format: disk_daemon::daemon::LogFormat) {
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
                Box::new(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
            }
        };

    tracing_subscriber::registry()
        .with(fmt_layer.with_filter(env_filter))
        .init();
}
