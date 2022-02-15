use std::io;

pub fn init_tracing() {
    tracing_subscriber::fmt()
        .with_writer(io::stderr)
        .with_env_filter("info")
        .json()
        .flatten_event(true)
        .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .with_current_span(true)
        .with_span_list(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_target(false)
        .init();
}

pub trait Must<T> {
    fn or_bail(self, message: &str) -> T;
}

impl<T, E> Must<T> for Result<T, E>
where
    E: std::fmt::Display + std::fmt::Debug,
{
    fn or_bail(self, message: &str) -> T {
        match self {
            Ok(t) => t,
            Err(e) => {
                tracing::debug!(error_details = ?e, message);
                tracing::error!(error = %e, message);
                std::process::exit(1);
            }
        }
    }
}