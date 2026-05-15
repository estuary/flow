use clap::Parser;

fn main() -> Result<(), anyhow::Error> {
    // Required for libraries that use rustls (tonic TLS, gazette client TLS).
    // See https://docs.rs/rustls/latest/rustls/crypto/struct.CryptoProvider.html
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install default crypto provider");

    let args = runtime_sidecar::Args::parse();
    install_tracing(args.log_format);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let result = runtime.block_on(runtime.spawn(runtime_sidecar::run(args)));
    runtime.shutdown_timeout(std::time::Duration::from_secs(5));
    result?
}

/// Install a tracing subscriber that writes structured application logs to
/// stderr. The plan mandates application-style logs only, so we do not
/// install `ops::tracing::Layer`.
fn install_tracing(log_format: runtime_sidecar::LogFormat) {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    if log_format == runtime_sidecar::LogFormat::Json {
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_writer(std::io::stderr)
            .json()
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("setting tracing default failed");
    } else {
        let no_color = matches!(std::env::var("NO_COLOR"), Ok(v) if v == "1");
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_writer(std::io::stderr)
            .with_ansi(!no_color)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("setting tracing default failed");
    }
}
