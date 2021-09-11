pub(crate) mod service;

mod build;
mod combine;
mod derive;
mod extract;
mod metrics;
mod schema;
mod upper_case;

/// Setup a global tracing subscriber using the RUST_LOG env variable.
pub fn setup_env_tracing() {
    static SUBSCRIBE: std::sync::Once = std::sync::Once::new();

    SUBSCRIBE.call_once(|| {
        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .finish();
        tracing::subscriber::set_global_default(subscriber).unwrap();
    });
}
