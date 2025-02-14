use crate::log_appender::{self, GazetteWriter, TaskForwarder};
use futures::Future;
use lazy_static::lazy_static;
use rand::Rng;
use tracing::{level_filters::LevelFilter, Instrument};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};

// These are accessible anywhere inside the call stack of a future wrapped with [`forward_logs()`].
// The relationship between LogForwarder and log journal is one-to-one. That means that all logs
// from the point at which you call `forward_logs()` downwards will get forwarded to the same journal.
tokio::task_local! {
    static TASK_FORWARDER: TaskForwarder<GazetteWriter>;
    static LOG_LEVEL: std::cell::Cell<ops::LogLevel>;
}

pub fn install() {
    // Build a tracing_subscriber::Filter which uses our dynamic log level.
    let log_filter = tracing_subscriber::filter::DynFilterFn::new(move |metadata, _cx| {
        if metadata
            .fields()
            .iter()
            .any(|f| f.name() == log_appender::EXCLUDE_FROM_TASK_LOGGING)
        {
            return false;
        }

        let cur_level = match metadata.level().as_str() {
            "TRACE" => ops::LogLevel::Trace as i32,
            "DEBUG" => ops::LogLevel::Debug as i32,
            "INFO" => ops::LogLevel::Info as i32,
            "WARN" => ops::LogLevel::Warn as i32,
            "ERROR" => ops::LogLevel::Error as i32,
            _ => ops::LogLevel::UndefinedLevel as i32,
        };

        cur_level
            <= LOG_LEVEL
                .try_with(|log_level| log_level.get())
                .unwrap_or(ops::LogLevel::Info)
                .into()
    });

    // We want to be able to control Dekaf's own logging output via the RUST_LOG environment variable like usual.
    let fmt_layer = tracing_subscriber::fmt::Layer::default()
        .with_writer(std::io::stderr)
        .with_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(LevelFilter::WARN.into()) // Otherwise it's ERROR.
                .from_env_lossy(),
        );

    let registry = tracing_subscriber::registry()
        .with(tracing_record_hierarchical::HierarchicalRecord::default())
        .with(
            ops::tracing::Layer::new(
                |log| {
                    let _ = TASK_FORWARDER.try_with(|f| f.send_log_message(log.clone()));
                },
                std::time::SystemTime::now,
            )
            .with_filter(log_filter),
        )
        .with(fmt_layer);

    registry.init();
}

lazy_static! {
    // Producer IDs should change infrequently, so we should create one as early as possible and use it for the lifetime of the process
    static ref PRODUCER: gazette::uuid::Producer = {
        // There's probably a neat bit-banging way to do this with i64 and masks, but I'm just not that fancy.
        let mut producer_id = rand::thread_rng().gen::<[u8; 6]>();
        producer_id[0] |= 0x01;
        gazette::uuid::Producer::from_bytes(producer_id)
    };
}

/// Capture all log messages emitted by the passed future and all of its descendants, and writes them out
/// based on the behavior of the provided writer. Initially, log messages will get buffered in a circular
/// queue until such time as the forwarder is informed of the name of the journal to emit them into. Then,
/// all buffered logs as well as all new incoming logs will be written out to that journal.
///
/// The log forwarder can be configured (i.e to inform it of the log journal, once it's known) via [`get_log_forwarder()`].
///  - Note: This will panic if called from outside the context of a future wrapped by [`forward_logs()`]!
/// The level filter can be dynamically configured for new messages via [`set_log_level()`].
pub fn forward_logs<F, O>(writer: GazetteWriter, fut: F) -> impl Future<Output = O>
where
    F: Future<Output = O>,
{
    let forwarder = TaskForwarder::new(PRODUCER.to_owned(), writer);

    LOG_LEVEL.scope(
        ops::LogLevel::Info.into(),
        TASK_FORWARDER.scope(
            forwarder,
            fut.instrument(tracing::info_span!(
                // Attach these empty fields so that later on we can use tracing_record_hierarchical
                // to set them, effectively adding a field to every event emitted inside a Session.
                "dekaf_session",
                { log_appender::SESSION_CLIENT_ID_FIELD_MARKER } = tracing::field::Empty,
                { log_appender::SESSION_TASK_NAME_FIELD_MARKER } = tracing::field::Empty,
            )),
        ),
    )
}

/// By default, `tokio::task::LocalKey`s don't propagate into futures passed to `tokio::spawn()`.
/// This allows us to create new futures that can be executed later by `tokio::spawn()` while still
/// referring to the same task-local values as the parent.
pub fn propagate_task_forwarder<F, O>(fut: F) -> impl Future<Output = O>
where
    F: Future<Output = O>,
{
    let current_level = LOG_LEVEL.get();
    let current_forwarder = TASK_FORWARDER.get();

    LOG_LEVEL.scope(
        current_level,
        TASK_FORWARDER.scope(current_forwarder, fut.in_current_span()),
    )
}

pub fn get_log_forwarder() -> TaskForwarder<GazetteWriter> {
    TASK_FORWARDER.get()
}

pub fn set_log_level(level: ops::LogLevel) {
    LOG_LEVEL.with(|cell| cell.set(level))
}
