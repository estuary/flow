//! A reproduction lab for `runtime-next` and `shuffle`: see README.md.

pub mod auth;
pub mod catalog;
pub mod cgroup;
pub mod connector;
pub mod controller;
pub mod layout;
pub mod reactor;
pub mod sidecar;
pub mod topology;

/// Journal clients of the run's broker, for production's publisher. The
/// broker disables AuthZ, so every client is the same, and carries no token.
pub fn journal_client_factory(broker: String) -> gazette::journal::ClientFactory {
    let fragment_client = gazette::journal::Client::new_fragment_client();
    let router = gazette::Router::new("local");

    std::sync::Arc::new(move |_subject, _object| {
        gazette::journal::Client::new(
            broker.clone(),
            fragment_client.clone(),
            proto_grpc::Metadata::new(),
            router.clone(),
        )
    })
}

/// Name the calling thread, as tokio names its workers. Host processes name
/// their main thread (their `comm`, as `top` shows it) after their host.
pub fn set_thread_name(name: &str) {
    let name = std::ffi::CString::new(name).expect("thread name has no NUL");
    // Safety: PR_SET_NAME reads a NUL-terminated string of at most 16 bytes,
    // and the kernel truncates a longer one.
    unsafe { libc::prctl(libc::PR_SET_NAME, name.as_ptr()) };
}

/// Resolve on SIGTERM or SIGINT, with the signal's name.
pub async fn await_signal() -> &'static str {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("installing SIGTERM handler");
    let name = tokio::select! {
        _ = term.recv() => "SIGTERM",
        _ = tokio::signal::ctrl_c() => "SIGINT",
    };
    tracing::info!(signal = name, "signal received");
    name
}

/// Install a tracing subscriber writing to stderr, composed with service-kit's
/// per-handler trace overrides and event tracks (as `runtime-sidecar` does).
/// The base filter is `RUST_LOG`, defaulting to `info`.
pub fn install_tracing(registry: service_kit::Registry) {
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    // Host process output goes to log files, where color escapes are noise.
    let ansi = std::io::IsTerminal::is_terminal(&std::io::stderr());

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(ansi)
                .with_writer(std::io::stderr)
                .with_filter(service_kit::trace::layer_filter(
                    env_filter,
                    registry.clone(),
                )),
        )
        .with(service_kit::event::layer(registry))
        .init();
}
