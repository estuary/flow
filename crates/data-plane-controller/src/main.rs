use clap::Parser;

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

    let args = data_plane_controller::Args::parse();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let result =
        runtime.block_on(runtime.spawn(async move { data_plane_controller::run(args).await }));

    runtime.shutdown_timeout(std::time::Duration::from_secs(5));
    result?
}
