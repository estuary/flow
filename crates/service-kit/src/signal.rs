//! The process signals which drain a service.

/// Resolve once this process has been asked to shut down, logging which signal
/// asked.
///
/// SIGTERM is what a supervisor sends and SIGINT is Ctrl+C at a terminal; both
/// mean the same thing to a service. What that shutdown then reaches is the
/// caller's own, because every service holds its in-flight work differently: a
/// `CancellationToken`, a broadcast to each server, or simply returning from
/// `main`.
pub async fn shutdown_signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("install SIGTERM handler");

    tokio::select! {
        _ = term.recv() => tracing::info!("SIGTERM received"),
        _ = tokio::signal::ctrl_c() => tracing::info!("SIGINT received"),
    }
}
