//! Dynamic, per-handler `tracing` verbosity.
//!
//! A long-running service runs at a modest base level (typically `INFO`). When
//! one in-flight handler needs debugging, an operator can raise *its* verbosity at
//! runtime — via the [`crate::admin`] surface, which calls
//! [`Registry::set_trace_override`] — and see what it's doing without restarting
//! the process or drowning in every other handler's output.
//!
//! Mechanism: each handler runs inside a [`tracing::Span`] owned by its
//! [`crate::HandlerGuard`] (with the target `service_kit::handler`).
//! [`OverrideFilter`] — a [`tracing_subscriber`] per-layer filter, composed with
//! the service's base filter via [`layer_filter`] — stashes that handler's
//! override atomic on the span when it's created, and for every otherwise-
//! filtered event walks the current span scope: if an ancestor handler span's
//! override admits the event's level, it passes. The override is *additive* —
//! it never suppresses an event the base filter would keep.
//!
//! Cost when no override is active: [`OverrideFilter`]'s `max_level_hint` is
//! `TRACE`, so disabled `trace!`/`debug!` callsites do one extra `enabled()`
//! check (an atomic load, plus — only inside a handler span — a short scope
//! walk) rather than being statically skipped.

use crate::Registry;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

/// Compose `base` with an [`OverrideFilter`] over `registry`, yielding a filter
/// to attach to a `fmt` (or other) layer via `Layer::with_filter`. Events pass
/// if `base` admits them *or* an active handler trace-override does.
pub fn layer_filter<S>(
    base: tracing_subscriber::EnvFilter,
    registry: Registry,
) -> impl tracing_subscriber::layer::Filter<S> + 'static
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    use tracing_subscriber::filter::FilterExt as _;
    base.or(OverrideFilter { registry })
}

/// A [`tracing_subscriber`] per-layer filter that admits events at or above the
/// trace-override level of an enclosing handler span. See the module docs;
/// normally used through [`layer_filter`] rather than directly.
pub struct OverrideFilter {
    registry: Registry,
}

/// Span extension: the trace-override atomic of the handler whose span this is.
struct TraceOverride(Arc<AtomicU8>);

impl<S> tracing_subscriber::layer::Filter<S> for OverrideFilter
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn enabled(
        &self,
        meta: &tracing::Metadata<'_>,
        cx: &tracing_subscriber::layer::Context<'_, S>,
    ) -> bool {
        // Handler spans must always be created — they're where overrides live.
        if meta.target() == crate::handlers::HANDLER_SPAN_TARGET {
            return true;
        }
        let want = crate::handlers::level_to_u8(meta.level());
        let Some(span) = cx.lookup_current() else {
            return false;
        };
        span.scope().any(|span| {
            span.extensions()
                .get::<TraceOverride>()
                .is_some_and(|ov| want <= ov.0.load(Ordering::Relaxed))
        })
    }

    fn callsite_enabled(
        &self,
        meta: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        if meta.target() == crate::handlers::HANDLER_SPAN_TARGET {
            tracing::subscriber::Interest::always()
        } else {
            // An override set later may admit this callsite, so we can't cache
            // a static decision: ask `enabled` per event.
            tracing::subscriber::Interest::sometimes()
        }
    }

    fn max_level_hint(&self) -> Option<tracing_subscriber::filter::LevelFilter> {
        Some(tracing_subscriber::filter::LevelFilter::TRACE)
    }

    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if attrs.metadata().target() != crate::handlers::HANDLER_SPAN_TARGET {
            return;
        }
        let mut visitor = HandlerIdVisitor(None);
        attrs.record(&mut visitor);

        let Some(handler_id) = visitor.0 else { return };
        let Some(handle) = self.registry.override_handle(handler_id) else {
            return;
        };
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(TraceOverride(handle));
        }
    }
}

/// Pulls the `id` field out of a handler span's attributes. Also used by
/// [`crate::event`], which hangs its own extension off the same handler span.
pub(crate) struct HandlerIdVisitor(pub(crate) Option<u64>);

impl tracing::field::Visit for HandlerIdVisitor {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if field.name() == "id" {
            self.0 = Some(value);
        }
    }
    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use tracing_subscriber::prelude::*;

    /// Counts the events that reach it (after its filter).
    #[derive(Clone, Default)]
    struct CountLayer(Arc<AtomicUsize>);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CountLayer {
        fn on_event(
            &self,
            _event: &tracing::Event<'_>,
            _cx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn override_admits_handler_trace_events() {
        let registry = Registry::new();
        let count = Arc::new(AtomicUsize::new(0));
        let n = || count.load(Ordering::Relaxed);

        let subscriber =
            tracing_subscriber::registry().with(CountLayer(count.clone()).with_filter(
                layer_filter(tracing_subscriber::EnvFilter::new("info"), registry.clone()),
            ));

        tracing::subscriber::with_default(subscriber, || {
            let mut handler = registry.register("test.kind");
            let id = registry.snapshot().live[0].id;
            let span = handler.span();
            let _entered = span.enter();

            tracing::trace!("dropped: below base, no override");
            assert_eq!(n(), 0);

            // INFO always passes — it's at the base level.
            tracing::info!("kept: at base level");
            assert_eq!(n(), 1);

            assert!(registry.set_trace_override(id, Some(tracing::Level::TRACE)));
            tracing::trace!("kept: admitted by override");
            tracing::debug!("kept: admitted by override");
            assert_eq!(n(), 3);

            // A nested non-handler span doesn't break scope-walking.
            tracing::info_span!("inner").in_scope(|| {
                tracing::trace!("kept: override reached via ancestor handler span");
            });
            assert_eq!(n(), 4);

            assert!(registry.set_trace_override(id, None));
            tracing::trace!("dropped: override cleared");
            assert_eq!(n(), 4);

            handler.finish_ok();
        });

        // Trace events outside any handler span are unaffected by overrides.
        let count2 = Arc::new(AtomicUsize::new(0));
        let n2 = || count2.load(Ordering::Relaxed);
        let subscriber =
            tracing_subscriber::registry().with(CountLayer(count2.clone()).with_filter(
                layer_filter(tracing_subscriber::EnvFilter::new("info"), registry.clone()),
            ));
        tracing::subscriber::with_default(subscriber, || {
            tracing::trace!("dropped: no enclosing handler span");
            assert_eq!(n2(), 0);
        });
    }
}
