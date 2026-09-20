use clap::Parser;

fn main() -> Result<(), anyhow::Error> {
    // Required for libraries that use rustls (tonic TLS, gazette client TLS).
    // See https://docs.rs/rustls/latest/rustls/crypto/struct.CryptoProvider.html
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install default crypto provider");

    let args = runtime_sidecar::Args::parse();
    // The handler registry is shared between the tracing subscriber (which
    // consults per-handler trace overrides) and the services (which populate it
    // and expose it via the admin surface).
    let registry = service_kit::Registry::new();
    service_kit::trace::init(args.log_format, registry.clone());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let result = runtime.block_on(runtime.spawn(runtime_sidecar::run(args, registry)));
    runtime.shutdown_timeout(std::time::Duration::from_secs(5));
    result?
}
