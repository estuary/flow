use tracing_subscriber::EnvFilter;

fn main() {
    tracing_subscriber::fmt::fmt()
        .with_env_filter(EnvFilter::from_env("LOG_LEVEL"))
        .with_writer(std::io::stderr)
        .json()
        .init();

    // Use anyhow's compact format, as the runtime's log decoder cannot structure a multi-line Debug.
    if let Err(error) = capture_python::run() {
        tracing::error!(error = format!("{error:#}"), "capture connector failed");
        std::process::exit(1);
    }
}
