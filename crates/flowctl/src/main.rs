use clap::Parser;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), anyhow::Error> {
    // Colorization support for Win 10.
    #[cfg(windows)]
    let _ = colored_json::enable_ansi_support();

    let cli = flowctl::Cli::parse();

    // Use reasonable defaults for printing structured logs to stderr.
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting tracing default failed");

    cli.run().await
}
