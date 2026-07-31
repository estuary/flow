//! `TestLogger`: the [`runtime_next::Logger`] seam for the catalog-test runner.
//!
//! It does one job — sink the task's log stream through a caller-provided
//! handler. Nothing else: the transaction-commit signal comes from the
//! [`Publisher`](runtime_next::Publisher) seam (see [`crate::publish`]), whose
//! contract *is* the transaction lifecycle. `LogEvent` is an observability
//! channel, and both it and its variants are `#[non_exhaustive]`, so scheduling
//! must not depend on what it happens to emit.

use std::sync::Arc;

/// A clonable ops-log sink. The user-visible logs of a run — connector output and
/// flattened runtime events alike — flow through it. `flowctl test` installs a
/// tracing / stderr handler; the control-plane agent installs one that streams to
/// a publication's job logs.
pub type LogHandler = Arc<dyn Fn(&ops::Log) + Send + Sync>;

/// [`runtime_next::LoggerFactory`] producing [`TestLogger`]s over one shared
/// handler.
#[derive(Clone)]
pub struct TestLoggerFactory {
    log_handler: LogHandler,
}

impl TestLoggerFactory {
    pub fn new(log_handler: LogHandler) -> Self {
        Self { log_handler }
    }
}

impl runtime_next::LoggerFactory for TestLoggerFactory {
    type Logger = TestLogger;

    fn open(&self, _task_name: &str) -> TestLogger {
        TestLogger {
            log_handler: self.log_handler.clone(),
        }
    }
}

/// Per-session logger. Cheap to clone; the connector log pump holds its own.
#[derive(Clone)]
pub struct TestLogger {
    log_handler: LogHandler,
}

impl runtime_next::Logger for TestLogger {
    fn log(&self, log: &ops::Log) {
        (self.log_handler)(log)
    }

    fn event(&self, event: runtime_next::LogEvent<'_>) {
        // Preserve the canonical rendering of every event; add no behavior.
        if let Some(log) = event.to_log() {
            self.log(&log);
        }
    }
}
