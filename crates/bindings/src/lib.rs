mod extract;
mod metrics;
mod service;
mod task_service;
mod task_service_v2;
mod upper_case;

/// `connector::REPORTED_IDENTITY`, NUL-terminated: the build this process's
/// VMM launches pair with a VMM image's own.
#[unsafe(no_mangle)]
pub extern "C" fn connector_build_identity() -> *const std::ffi::c_char {
    static IDENTITY: std::sync::LazyLock<std::ffi::CString> = std::sync::LazyLock::new(|| {
        std::ffi::CString::new(connector::REPORTED_IDENTITY).expect("an identity has no NUL")
    });
    IDENTITY.as_ptr()
}

fn install_crypto_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();

    ONCE.call_once(|| {
        // `bindings` is linked into Go binaries through CGO, so Rust binary
        // entrypoints cannot install rustls' process-wide provider for us.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}
