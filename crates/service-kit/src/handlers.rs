use crate::event;
use std::borrow::Cow;
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

/// Number of recently-finished handlers retained for the dashboard.
const RECENT_CAPACITY: usize = 64;

/// Target of the per-handler `tracing` span opened by [`Registry::register`].
/// [`crate::trace`]'s filter recognizes spans with this target, hangs the
/// handler's trace-override atomic off them, and walks the span scope to decide
/// whether an otherwise-filtered event should be admitted.
pub(crate) const HANDLER_SPAN_TARGET: &str = "service_kit::handler";

/// Registry tracks the lifecycle of in-flight units of work — gRPC handlers,
/// jobs, connections, whatever a service spawns — so an operator (via
/// [`crate::admin`]) can see what the process is doing right now.
///
/// It's cheap to construct and clone; callers that don't expose an admin
/// surface construct one and ignore it.
#[derive(Clone, Default)]
pub struct Registry(Arc<Mutex<Inner>>);

#[derive(Default)]
struct Inner {
    next_id: u64,
    live: BTreeMap<u64, Arc<Slot>>,
    // Newest-last ring of handlers that have finished.
    recent: VecDeque<FinishedView>,
}

struct Slot {
    id: u64,
    kind: &'static str,
    started_at: SystemTime,
    // Verbosity floor for an operator-set trace override: 0 = none,
    // 1..=5 = ERROR..TRACE (see `level_to_u8`). Read on the tracing hot path
    // by `crate::trace`, written by `Registry::set_trace_override`.
    trace_override: Arc<AtomicU8>,
    // Per-handler set of event tracks (named ring buffers) emitted within the
    // handler span. Populated on the tracing hot path by `crate::event`, hung
    // off the handler span by it too; snapshotted for the admin surface and,
    // on drop, into the recently-finished ring. See `crate::event`.
    tracks: Arc<event::Tracks>,
    // Mutable as the handler learns its identity and advances through phases.
    state: Mutex<SlotState>,
}

struct SlotState {
    label: String,
    phase: Cow<'static, str>,
    phase_since: SystemTime,
    fields: Vec<(&'static str, String)>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new handler of the given `kind` (a stable, short identifier
    /// like `"leader.materialize"` or `"shuffle.session"`). The returned guard
    /// removes the handler from the registry when dropped, recording it in the
    /// recently-finished ring.
    ///
    /// The guard also carries a [`tracing::Span`] (see [`HandlerGuard::span`]):
    /// the handler should run its body inside it (`.instrument(handler.span())`)
    /// so that operator-directed trace overrides reach the handler's events.
    ///
    /// Call `register` *inside* the spawned task, never before a `tokio::spawn`
    /// that carries the guard in: a span captures the current dispatcher at
    /// creation and `tokio::spawn` doesn't propagate it, so a span created under
    /// one subscriber (e.g. a test's `set_default`) but closed on a worker
    /// thread routes its close to the wrong registry and panics.
    pub fn register(&self, kind: &'static str) -> HandlerGuard {
        let now = SystemTime::now();

        let (id, slot) = {
            let mut inner = self.0.lock().unwrap();
            let id = inner.next_id;
            inner.next_id += 1;

            let slot = Arc::new(Slot {
                id,
                kind,
                started_at: now,
                trace_override: Arc::new(AtomicU8::new(0)),
                tracks: Arc::new(event::Tracks::default()),
                state: Mutex::new(SlotState {
                    label: String::new(),
                    phase: Cow::Borrowed("new"),
                    phase_since: now,
                    fields: Vec::new(),
                }),
            });
            inner.live.insert(id, slot.clone());
            (id, slot)
        };

        // Root span carrying the handler's identity, so every log line a handler
        // emits is attributable. Created with the registry lock released — span creation re-enters
        // the registry via `crate::trace`'s `on_new_span` (`override_handle`).
        let span = tracing::info_span!(
            target: HANDLER_SPAN_TARGET,
            parent: None,
            "handler",
            id,
            kind,
            label = tracing::field::Empty,
        );

        HandlerGuard {
            registry: self.clone(),
            slot,
            span,
            terminal: None,
        }
    }

    /// Snapshot the current registry contents for presentation.
    pub fn snapshot(&self) -> Snapshot {
        let now = SystemTime::now();
        let inner = self.0.lock().unwrap();

        let live = inner
            .live
            .values()
            .map(|slot| {
                let state = slot.state.lock().unwrap();
                HandlerView {
                    id: slot.id,
                    kind: slot.kind,
                    label: state.label.clone(),
                    phase: state.phase.to_string(),
                    age_seconds: secs_since(now, slot.started_at),
                    phase_age_seconds: secs_since(now, state.phase_since),
                    fields: state.fields.clone(),
                    trace_override: u8_to_level_name(slot.trace_override.load(Ordering::Relaxed)),
                }
            })
            .collect();

        Snapshot {
            live,
            recent: inner.recent.iter().cloned().collect(),
        }
    }

    /// Set (or, with `None`, clear) the trace-verbosity override of the live
    /// handler `id`. Returns false if no live handler has that id. Events at
    /// `level` or above emitted within the handler's span are admitted even if
    /// the process's base filter would drop them; this is additive — it never
    /// suppresses events the base filter would keep.
    pub fn set_trace_override(&self, id: u64, level: Option<tracing::Level>) -> bool {
        let inner = self.0.lock().unwrap();
        let Some(slot) = inner.live.get(&id) else {
            return false;
        };
        let value = level.map(|l| level_to_u8(&l)).unwrap_or(0);
        slot.trace_override.store(value, Ordering::Relaxed);
        true
    }

    /// The trace-override atomic of live handler `id`, for [`crate::trace`] to
    /// stash on the handler span when it's created.
    pub(crate) fn override_handle(&self, id: u64) -> Option<Arc<AtomicU8>> {
        let inner = self.0.lock().unwrap();
        inner.live.get(&id).map(|slot| slot.trace_override.clone())
    }

    /// The event tracks of live handler `id`, for [`crate::event`] to stash on
    /// the handler span when it's created.
    pub(crate) fn tracks_handle(&self, id: u64) -> Option<Arc<event::Tracks>> {
        let inner = self.0.lock().unwrap();
        inner.live.get(&id).map(|slot| slot.tracks.clone())
    }

    /// Snapshot one handler for the drill-down view: a live handler (with its
    /// current phase, fields, trace override, and event tracks) or, failing
    /// that, a recently-finished one (with its final phase and the event tracks
    /// captured at the moment it ended). Returns `None` if neither has that `id`.
    pub fn handler_detail(&self, id: u64) -> Option<HandlerDetail> {
        let now = SystemTime::now();
        let inner = self.0.lock().unwrap();

        if let Some(slot) = inner.live.get(&id) {
            let state = slot.state.lock().unwrap();
            return Some(HandlerDetail {
                id: slot.id,
                kind: slot.kind,
                label: state.label.clone(),
                phase: state.phase.to_string(),
                finished: false,
                age_seconds: secs_since(now, slot.started_at),
                started_at_rfc3339: Some(rfc3339_millis(slot.started_at)),
                phase_age_seconds: Some(secs_since(now, state.phase_since)),
                phase_since_rfc3339: Some(rfc3339_millis(state.phase_since)),
                fields: state.fields.clone(),
                trace_override: u8_to_level_name(slot.trace_override.load(Ordering::Relaxed)),
                tracks: slot.tracks.snapshot(),
            });
        }

        let finished = inner.recent.iter().find(|v| v.id == id)?;
        Some(HandlerDetail {
            id: finished.id,
            kind: finished.kind,
            label: finished.label.clone(),
            phase: finished.final_phase.clone(),
            finished: true,
            age_seconds: finished.age_seconds,
            // `FinishedView` carries only enough to list a finished handler;
            // start time and current phase aren't retained.
            started_at_rfc3339: None,
            phase_age_seconds: None,
            phase_since_rfc3339: None,
            fields: Vec::new(),
            trace_override: None,
            tracks: finished.tracks.clone(),
        })
    }

    fn finish(&self, id: u64, view: FinishedView) {
        let mut inner = self.0.lock().unwrap();
        inner.live.remove(&id);
        if inner.recent.len() == RECENT_CAPACITY {
            inner.recent.pop_front();
        }
        inner.recent.push_back(view);
    }
}

/// HandlerGuard is the RAII handle returned by [`Registry::register`]. The
/// handler updates its label / phase / fields through this guard; dropping it
/// (when the handler future completes, for any reason) removes the live entry
/// and appends a [`FinishedView`] to the recently-finished ring.
pub struct HandlerGuard {
    registry: Registry,
    slot: Arc<Slot>,
    // Per-handler `tracing` span; see `Registry::register` and `Self::span`.
    span: tracing::Span,
    // Terminal phase recorded by `finish_ok`/`finish_err`. When unset, `Drop`
    // synthesizes one from the last phase, so abrupt error or early-return
    // paths still leave a breadcrumb.
    terminal: Option<Cow<'static, str>>,
}

impl HandlerGuard {
    /// The handler's `tracing` span. Run the handler body inside it — typically
    /// `some_inner_future.instrument(handler.span()).await` — so that operator
    /// trace overrides (and the handler's identity fields) apply to its events.
    pub fn span(&self) -> tracing::Span {
        self.span.clone()
    }

    pub fn set_label(&self, label: impl Into<String>) {
        let label = label.into();
        self.span.record("label", label.as_str());
        self.slot.state.lock().unwrap().label = label;
    }

    pub fn set_phase(&self, phase: impl Into<Cow<'static, str>>) {
        let mut state = self.slot.state.lock().unwrap();
        state.phase = phase.into();
        state.phase_since = SystemTime::now();
    }

    pub fn set_field(&self, name: &'static str, value: impl std::fmt::Display) {
        let value = value.to_string();
        let mut state = self.slot.state.lock().unwrap();
        match state.fields.iter_mut().find(|(n, _)| *n == name) {
            Some((_, v)) => *v = value,
            None => state.fields.push((name, value)),
        }
    }

    pub fn finish_ok(&mut self) {
        self.terminal = Some(Cow::Borrowed("done"));
    }

    pub fn finish_err(&mut self, error: &str) {
        self.terminal = Some(Cow::Owned(format!("error: {error}")));
    }
}

impl Drop for HandlerGuard {
    fn drop(&mut self) {
        let now = SystemTime::now();
        // Snapshot the event tracks before taking any registry/slot lock — the
        // handler span (and its `Tracks` extension) is about to go away, so
        // this is the last chance to preserve them for the drill-down view.
        let tracks = self.slot.tracks.snapshot();
        let view = {
            let state = self.slot.state.lock().unwrap();
            FinishedView {
                id: self.slot.id,
                kind: self.slot.kind,
                label: state.label.clone(),
                final_phase: self
                    .terminal
                    .clone()
                    .unwrap_or_else(|| Cow::Owned(format!("ended ({})", state.phase)))
                    .into_owned(),
                age_seconds: secs_since(now, self.slot.started_at),
                tracks,
            }
        };
        self.registry.finish(self.slot.id, view);
    }
}

#[derive(serde::Serialize)]
pub struct Snapshot {
    pub live: Vec<HandlerView>,
    pub recent: Vec<FinishedView>,
}

#[derive(serde::Serialize)]
pub struct HandlerView {
    pub id: u64,
    pub kind: &'static str,
    pub label: String,
    pub phase: String,
    pub age_seconds: u64,
    pub phase_age_seconds: u64,
    pub fields: Vec<(&'static str, String)>,
    /// Current operator trace override, as a level name (`"TRACE"`, ...), or
    /// `None` when the handler runs at the process's base verbosity.
    pub trace_override: Option<&'static str>,
}

#[derive(Clone, serde::Serialize)]
pub struct FinishedView {
    pub id: u64,
    pub kind: &'static str,
    pub label: String,
    pub final_phase: String,
    pub age_seconds: u64,
    /// Event tracks as of the moment the handler finished; see [`crate::event`].
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub tracks: BTreeMap<String, Vec<event::EventView>>,
}

/// One handler in detail, for the drill-down view: a live handler, or a
/// recently-finished one. Produced by [`Registry::handler_detail`].
#[derive(serde::Serialize)]
pub struct HandlerDetail {
    pub id: u64,
    pub kind: &'static str,
    pub label: String,
    /// The current phase of a live handler, or the final phase of a finished one.
    pub phase: String,
    /// True if this is a recently-finished handler rather than a live one.
    pub finished: bool,
    pub age_seconds: u64,
    /// Absolute start time as RFC-3339 (millisecond precision); `None` for
    /// finished handlers (the start time isn't retained past finish).
    pub started_at_rfc3339: Option<String>,
    /// Time spent in the current phase; `None` for finished handlers.
    pub phase_age_seconds: Option<u64>,
    /// Absolute time of the last phase change as RFC-3339; `None` for finished
    /// handlers (no current phase).
    pub phase_since_rfc3339: Option<String>,
    /// Identifier fields; always empty for finished handlers (not retained).
    pub fields: Vec<(&'static str, String)>,
    /// Current operator trace override, or `None` (always `None` if finished).
    pub trace_override: Option<&'static str>,
    /// Recent events captured per track; see [`crate::event`].
    pub tracks: BTreeMap<String, Vec<event::EventView>>,
}

fn secs_since(now: SystemTime, then: SystemTime) -> u64 {
    now.duration_since(then).unwrap_or_default().as_secs()
}

/// RFC-3339 / ISO-8601 in UTC with `Z` suffix and millisecond precision —
/// what the drill-down's zulu mode renders.
pub(crate) fn rfc3339_millis(t: SystemTime) -> String {
    chrono::DateTime::<chrono::Utc>::from(t).to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Verbosity floor for a trace override, where larger is more verbose:
/// `1 = ERROR` … `5 = TRACE`. (`0`, used elsewhere, means "no override".)
pub(crate) fn level_to_u8(level: &tracing::Level) -> u8 {
    if *level == tracing::Level::ERROR {
        1
    } else if *level == tracing::Level::WARN {
        2
    } else if *level == tracing::Level::INFO {
        3
    } else if *level == tracing::Level::DEBUG {
        4
    } else {
        5 // TRACE
    }
}

/// Inverse of [`level_to_u8`]; `0` (no override) maps to `None`.
pub(crate) fn u8_to_level_name(v: u8) -> Option<&'static str> {
    match v {
        1 => Some("ERROR"),
        2 => Some("WARN"),
        3 => Some("INFO"),
        4 => Some("DEBUG"),
        5 => Some("TRACE"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_snapshot_and_drop() {
        let registry = Registry::new();
        assert!(registry.snapshot().live.is_empty());

        let handler_id;
        {
            let mut h = registry.register("shuffle.session");
            handler_id = registry.snapshot().live[0].id;
            h.set_label("acmeCo/widgets");
            h.set_field("session_id", 42u32);
            h.set_field("shards", 3usize);
            h.set_phase("running");
            // Last write wins for a repeated field.
            h.set_field("shards", 4usize);

            assert!(registry.set_trace_override(handler_id, Some(tracing::Level::TRACE)));

            let snap = registry.snapshot();
            assert_eq!(snap.live.len(), 1);
            let v = &snap.live[0];
            assert_eq!(v.kind, "shuffle.session");
            assert_eq!(v.label, "acmeCo/widgets");
            assert_eq!(v.phase, "running");
            assert_eq!(v.trace_override, Some("TRACE"));
            assert_eq!(
                v.fields,
                vec![
                    ("session_id", "42".to_string()),
                    ("shards", "4".to_string())
                ]
            );

            assert!(registry.set_trace_override(handler_id, None));
            assert_eq!(registry.snapshot().live[0].trace_override, None);

            h.finish_ok();
        }

        // Override of a finished handler is reported as not-found.
        assert!(!registry.set_trace_override(handler_id, Some(tracing::Level::TRACE)));

        let snap = registry.snapshot();
        assert!(snap.live.is_empty());
        assert_eq!(snap.recent.len(), 1);
        assert_eq!(snap.recent[0].final_phase, "done");
        assert_eq!(snap.recent[0].label, "acmeCo/widgets");
    }

    #[test]
    fn drop_without_finish_records_last_phase() {
        let registry = Registry::new();
        {
            let h = registry.register("leader.materialize");
            h.set_phase("joining");
        }
        let snap = registry.snapshot();
        assert_eq!(snap.recent[0].final_phase, "ended (joining)");
    }

    #[test]
    fn handler_detail_covers_live_and_finished() {
        let registry = Registry::new();
        assert!(registry.handler_detail(0).is_none());

        let id;
        {
            let mut h = registry.register("leader.materialize");
            id = registry.snapshot().live[0].id;
            h.set_label("acmeCo/widgets");
            h.set_field("shards", 3usize);
            h.set_phase("running");
            registry.set_trace_override(id, Some(tracing::Level::DEBUG));

            let d = registry.handler_detail(id).expect("live");
            assert!(!d.finished);
            assert_eq!(d.kind, "leader.materialize");
            assert_eq!(d.label, "acmeCo/widgets");
            assert_eq!(d.phase, "running");
            assert_eq!(d.trace_override, Some("DEBUG"));
            assert_eq!(d.fields, vec![("shards", "3".to_string())]);
            assert!(d.phase_age_seconds.is_some());
            assert!(d.tracks.is_empty()); // no `crate::event` layer installed here

            h.finish_ok();
        }

        let d = registry.handler_detail(id).expect("finished");
        assert!(d.finished);
        assert_eq!(d.phase, "done");
        assert_eq!(d.trace_override, None);
        assert!(d.phase_age_seconds.is_none());
        assert!(d.fields.is_empty());
    }

    #[test]
    fn recent_ring_evicts_oldest() {
        let registry = Registry::new();
        for i in 0..(RECENT_CAPACITY + 5) {
            let mut h = registry.register("shuffle.log");
            h.set_label(format!("dir-{i}"));
            h.finish_ok();
        }
        let snap = registry.snapshot();
        assert_eq!(snap.recent.len(), RECENT_CAPACITY);
        // Oldest (`dir-0`..`dir-4`) were evicted; newest is last.
        assert_eq!(snap.recent.first().unwrap().label, "dir-5");
        assert_eq!(
            snap.recent.last().unwrap().label,
            format!("dir-{}", RECENT_CAPACITY + 4)
        );
    }
}
