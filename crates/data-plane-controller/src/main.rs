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
    /// Act as a git credential helper, minting GitHub App installation tokens.
    GitCredential(data_plane_controller::git_credential::GitCredentialArgs),
}

fn main() -> Result<(), anyhow::Error> {
    let cli = Cli::parse();

    // Required in order for libraries to use `rustls` for TLS.
    // See: https://docs.rs/rustls/latest/rustls/crypto/struct.CryptoProvider.html
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install default crypto provider");

    // The git-credential helper speaks git's credential protocol over stdout,
    // so its logs must go to stderr or they corrupt that stream. Other modes
    // keep logging to stdout.
    let is_git_credential = matches!(cli.mode, Mode::GitCredential(_));
    let builder = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(!is_git_credential && !matches!(std::env::var("NO_COLOR"), Ok(v) if v == "1"));
    if is_git_credential {
        tracing::subscriber::set_global_default(builder.with_writer(std::io::stderr).finish())
            .expect("setting tracing default failed");
    } else {
        tracing::subscriber::set_global_default(builder.finish())
            .expect("setting tracing default failed");
    }

    // The credential helper does trivial work per invocation (git spawns it
    // afresh each time), so it runs on a single-threaded runtime rather than
    // paying to spin up a worker pool.
    let runtime = if is_git_credential {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
    } else {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
    };

    let result = runtime.block_on(runtime.spawn(async move {
        match cli.mode {
            Mode::Job(args) => data_plane_controller::job::run_job(args).await,
            Mode::Service(args) => data_plane_controller::service::run_service(args).await,
            Mode::GitCredential(args) => {
                data_plane_controller::git_credential::run_git_credential(args).await
            }
        }
    }));

    runtime.shutdown_timeout(std::time::Duration::from_secs(5));
    result?
}
