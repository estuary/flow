use clap::Parser;

#[derive(clap::Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(subcommand)]
    mode: Mode,
}

#[derive(clap::Subcommand, Debug)]
enum Mode {
    /// Run as a job (dispatcher) - polls automations framework and dispatches HTTP requests
    Job(data_plane_controller::job::JobArgs),
    /// Run as a service (worker) - receives HTTP requests and executes work
    Service(data_plane_controller::service::ServiceArgs),
}

fn main() -> Result<(), anyhow::Error> {
    // Required in order for libraries to use `rustls` for TLS.
    // See: https://docs.rs/rustls/latest/rustls/crypto/struct.CryptoProvider.html
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install default crypto provider");

    // Use reasonable defaults for printing structured logs to stderr.
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(if matches!(std::env::var("NO_COLOR"), Ok(v) if v == "1") {
            false
        } else {
            true
        })
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting tracing default failed");

    let cli = Cli::parse();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let result = runtime.block_on(runtime.spawn(async move {
        match cli.mode {
            Mode::Job(args) => data_plane_controller::job::run_job(args).await,
            Mode::Service(args) => data_plane_controller::service::run_service(args).await,
        }
    }));

    runtime.shutdown_timeout(std::time::Duration::from_secs(5));
    result?
}
