mod extract;
mod metrics;
mod service;
mod task_service;
mod task_service_v2;
mod upper_case;

fn install_crypto_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();

    ONCE.call_once(|| {
        // `bindings` is linked into Go binaries through CGO, so Rust binary
        // entrypoints cannot install rustls' process-wide provider for us.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}
