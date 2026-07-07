//! `TestLogger`: the [`runtime_next::Logger`] seam for the catalog-test runner.
//!
//! It sinks the task's log stream through a caller-provided handler (so the
//! agent can forward user-visible logs), and — critically — turns the leader's
//! committing [`LogEvent::Persist`](runtime_next::LogEvent::Persist) into the
//! runner's transaction-commit signal. The leader emits exactly one committing
//! `Persist` per derivation transaction, so the runner awaits one signal per
//! Stat it drives.

use std::sync::Arc;
use tokio::sync::mpsc;

/// A clonable ops-log sink. The runner's user-visible logs — connector output
/// and flattened runtime events alike — flow through it. `flowctl test` installs
/// a tracing / stderr handler; the control-plane agent installs one that streams
/// to a publication's `logs_tx`.
pub type LogHandler = Arc<dyn Fn(&ops::Log) + Send + Sync>;

/// [`runtime_next::LoggerFactory`] producing commit-signaling loggers. The
/// `commit_tx` is shared by the leader and every shard logger; only the leader
/// emits a committing `Persist`, so exactly one signal fires per transaction.
#[derive(Clone)]
pub struct TestLoggerFactory {
    commit_tx: mpsc::UnboundedSender<()>,
    log_handler: LogHandler,
}

impl TestLoggerFactory {
    pub fn new(commit_tx: mpsc::UnboundedSender<()>, log_handler: LogHandler) -> Self {
        Self {
            commit_tx,
            log_handler,
        }
    }
}

impl runtime_next::LoggerFactory for TestLoggerFactory {
    type Logger = TestLogger;

    fn open(&self, _task_name: &str) -> TestLogger {
        TestLogger {
            commit_tx: self.commit_tx.clone(),
            log_handler: self.log_handler.clone(),
        }
    }
}

/// Per-session logger. Cheap to clone; the connector log pump holds its own.
#[derive(Clone)]
pub struct TestLogger {
    commit_tx: mpsc::UnboundedSender<()>,
    log_handler: LogHandler,
}

impl runtime_next::Logger for TestLogger {
    fn log(&self, log: &ops::Log) {
        (self.log_handler)(log)
    }

    fn event(&self, event: runtime_next::LogEvent<'_>) {
        // A committing Persist (its `committed_frontier` set) marks a transaction
        // commit — the runner's signal that its Stat completed. Apply-state and
        // ACK-only persists carry no committed frontier and are not commits.
        if let runtime_next::LogEvent::Persist { persist, .. } = &event {
            if persist.committed_frontier.is_some() {
                let _ = self.commit_tx.send(());
            }
        }
        // Preserve the canonical log rendering for every event (Persist included,
        // when it carries a connector-state delta).
        if let Some(log) = event.to_log() {
            self.log(&log);
        }
    }
}
