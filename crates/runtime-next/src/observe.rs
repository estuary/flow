//! Out-of-band reporting seam: the runtime's user-facing event channel.
//!
//! This is the third host-facing seam of the crate, alongside [`Publisher`]
//! (document output) and [`ShuffleSession`] (checkpoint input). It carries
//! everything that is *eligible for surfacing to a human*: the connector log
//! stream, connector-state persists, connector Apply actions, and (in later
//! changes) inferred-schema updates and connector lifecycle events.
//!
//! It is deliberately distinct from two adjacent surfaces:
//!
//! - From `service_kit` (the [`Registry`](service_kit::Registry) + `event!`
//!   macro), which is the *operator/admin* surface — in-flight handler phases,
//!   breadcrumb rings, an admin dashboard. That never reaches user task logs;
//!   this seam is the one that does.
//! - From [`Publisher`], which emits the task's *data* (captured / derived
//!   collection documents). An [`Observer`] reports *about* the runtime, not
//!   the data it moves.
//!
//! The seam is generic, not dynamic: each leader / shard `Service` is
//! monomorphized over its concrete [`ObserverFactory`].
//!
//! [`Observer`] both sinks the connector log stream ([`Observer::log`], folded
//! in from the former `LogHandler` trait) and reports high-level events. The
//! event routines all carry a tracing default, so an Observer that overrides
//! none of them reproduces the legacy task-log lines; a harness overrides them
//! to render differently:
//!
//! - Production shards install an [`FnObserverFactory`] wrapping the task's
//!   encoded-JSON log writer: [`Observer::log`] writes connector logs to the
//!   task-log file, and the event routines keep their tracing defaults (which
//!   the Go runtime forwards to the task's ops-log journal).
//! - `flowctl preview` shards install an [`FnObserverFactory`] over its
//!   stderr/tracing log handler; a later change overrides the event routines to
//!   render preview lines.
//! - Leaders (which run no connector, so [`Observer::log`] is never called) and
//!   actor unit tests install [`NoopObserver`].
//!
//! The leader sidecar is a standalone process whose tracing is *not* forwarded
//! to task ops-logs the way the Go-hosted shard's is; bridging that gap (an
//! async ops-log publishing Observer) is a later change.
//!
//! [`Publisher`]: crate::Publisher
//! [`ShuffleSession`]: crate::ShuffleSession

use crate::proto;

/// Per-session observer: the runtime's user-facing event channel. The leader and
/// shards obtain one from an [`ObserverFactory`] at the start of each session —
/// *before* the connector starts and *before* any [`Publisher`](crate::Publisher)
/// is opened, so it is in hand for startup-time Apply observations and connector
/// logs. Cheap to clone (the connector log pump holds its own handle).
///
/// Every method is synchronous and off the hot path; any async publication is
/// the implementation's internal concern (a background drain), never an `await`
/// at the call site.
///
/// The event routines ([`persist`](Observer::persist), [`applied`](Observer::applied),
/// ...) default to the tracing lines they replaced, so the production Observer
/// reproduces legacy task-log behavior by overriding only [`log`](Observer::log).
pub trait Observer: Clone + Send + Sync + 'static {
    /// Sink one connector log line. Folded in from the former `LogHandler`
    /// trait: this is the per-session connector log stream. Required (no
    /// default) so no installer silently drops connector logs.
    fn log(&self, log: &ops::Log);

    /// Report a connector-state [`proto::Persist`] at the point it's emitted.
    ///
    /// Emitted from every point a `Persist` originates: the leader's committing
    /// transaction and its Apply loop (derive / materialize), and the capture
    /// shard's committing transaction and its Apply loop. The default skips
    /// persists carrying no connector-state delta (idempotent replays, ACK-only
    /// persists, startup checkpoint reconciliation) and otherwise logs at debug,
    /// matching the legacy `"applied an updated connector state"` line.
    fn persist(&self, persist: &proto::Persist) {
        if persist.connector_patches_json.is_empty() {
            return;
        }
        tracing::debug!(
            patches = %String::from_utf8_lossy(&persist.connector_patches_json),
            "persisted connector-state delta",
        );
    }

    /// Report a connector Apply action description, once per Apply iteration as
    /// the Apply loop converges (before any session [`Publisher`](crate::Publisher)
    /// exists). The default logs at info, matching the legacy
    /// `"capture/materialization was applied"` line.
    fn applied(&self, action_description: &str) {
        tracing::info!(%action_description, "connector applied");
    }

    /// Report that a collection's inferred write-schema widened this transaction.
    /// `binding` is the source binding index for captures (multiple bindings per
    /// task) and `None` for derivations (a single derived collection). The default
    /// logs at info, matching the legacy `"inferred schema updated"` line — so it
    /// surfaces in task logs whenever the task's log level is info or finer.
    ///
    /// `schema` is the representative JSON Schema of the widened write-shape, as
    /// produced by [`doc::shape::schema::to_schema`]; an Observer that forwards
    /// it structurally avoids re-parsing a `serde_json::Value`.
    fn inferred_schema(
        &self,
        collection_name: &str,
        binding: Option<usize>,
        schema: &schemars::Schema,
    ) {
        tracing::info!(
            schema = ?ops::DebugJson(schema.as_value()),
            %collection_name,
            ?binding,
            "inferred schema updated",
        );
    }

    /// Report that a connector container has started and is dialed. The default
    /// logs at info; lower-level network/codec detail is logged separately at
    /// debug by `container::start`.
    fn container_started(&self, image: &str, container: &proto::Container) {
        tracing::info!(
            %image,
            container = ?ops::DebugJson(container),
            "started connector container",
        );
    }

    /// Report that a connector container is being torn down (its [`Guard`] was
    /// dropped at session end or on error). The default logs at debug.
    ///
    /// [`Guard`]: crate::container::Guard
    fn container_stopped(&self, image: &str) {
        tracing::debug!(%image, "stopped connector container");
    }

    /// Report a transient image-pull failure that will be retried. The default
    /// logs at warn, matching the legacy `"transient error pulling image"` line.
    fn image_pull_retry(&self, image: &str, attempt: u32, error: &str) {
        tracing::warn!(%image, attempt, %error, "transient error pulling image (will retry)");
    }
}

/// Opens an [`Observer`] for each leader / shard session. Held by the leader
/// [`Service`](crate::leader::Service) and shard [`Service`](crate::shard::Service),
/// which are monomorphized over it.
pub trait ObserverFactory: Clone + Send + Sync + 'static {
    /// Concrete per-session observer this factory produces.
    type Observer: Observer;

    /// Open an [`Observer`] bound to the given task. `task_name` identifies the
    /// task whose events (and, in the future, ops-log journal) the observer
    /// reports; the no-op and `Fn` observers ignore it.
    fn open(&self, task_name: &str) -> Self::Observer;
}

/// [`Observer`] whose [`log`](Observer::log) forwards to a `Fn(&ops::Log)` and
/// whose event routines keep their tracing defaults. The shard install for both
/// production (the `Fn` is the encoded-JSON task-log writer) and the interim
/// `flowctl preview` (the `Fn` is its stderr/tracing handler).
#[derive(Clone)]
pub struct FnObserver<F>(F);

impl<F: Fn(&ops::Log) + Clone + Send + Sync + 'static> Observer for FnObserver<F> {
    fn log(&self, log: &ops::Log) {
        (self.0)(log)
    }
}

/// [`ObserverFactory`] producing [`FnObserver`]s. Each session's observer is a
/// clone of the wrapped log handler; the handler is shared, the per-session
/// observer is a cheap clone.
#[derive(Clone)]
pub struct FnObserverFactory<F>(F);

impl<F: Fn(&ops::Log) + Clone + Send + Sync + 'static> FnObserverFactory<F> {
    pub fn new(log_handler: F) -> Self {
        Self(log_handler)
    }
}

impl<F: Fn(&ops::Log) + Clone + Send + Sync + 'static> ObserverFactory for FnObserverFactory<F> {
    type Observer = FnObserver<F>;

    fn open(&self, _task_name: &str) -> FnObserver<F> {
        FnObserver(self.0.clone())
    }
}

/// Inert [`Observer`]: connector logs are dropped and events keep their tracing
/// defaults. Installed by leaders (which run no connector, so [`log`](Observer::log)
/// is never called) and by actor unit tests.
#[derive(Clone)]
pub struct NoopObserver;

impl Observer for NoopObserver {
    fn log(&self, _log: &ops::Log) {}
}

/// [`ObserverFactory`] opening [`NoopObserver`]s. The default install for the
/// leader `Service`.
#[derive(Clone)]
pub struct NoopObserverFactory;

impl ObserverFactory for NoopObserverFactory {
    type Observer = NoopObserver;

    fn open(&self, _task_name: &str) -> NoopObserver {
        NoopObserver
    }
}
