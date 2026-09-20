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

//! [`init`] assembles the whole subscriber such a service installs at startup:
//! that filter, a `fmt` layer in the operator's chosen [`LogFormat`], and the
//! event layer the drill-down page reads.

use crate::Registry;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

/// Encoding of the application logs a service writes to stderr. A service
/// exposes this as a `clap` argument.
#[derive(Debug, Clone, Copy, PartialEq, clap::ValueEnum)]
pub enum LogFormat {
    Text,
    Json,
}

/// Install the whole stderr subscriber a service runs under: a `fmt` layer in
/// the operator's chosen [`LogFormat`], the base `EnvFilter` (`RUST_LOG`,
/// default `info`) composed with this module's per-handler override, so an
/// operator can raise one handler's verbosity at runtime via the admin
/// dashboard, and [`crate::event`]'s layer, which captures opt-in `event!`
/// breadcrumbs into per-handler tracks shown on the dashboard's handler
/// drill-down page.
///
/// A service which registers no handlers passes a [`Registry`] all the same:
/// the override filter then costs one atomic load per event, and the registry
/// is the handle its admin surface would hang off.
pub fn init(log_format: LogFormat, registry: Registry) {
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    tracing_subscriber::registry()
        .with(fmt_layer(log_format).with_filter(layer_filter(env_filter(), registry.clone())))
        .with(crate::event::layer(registry))
        .init();
}

/// The base filter every install starts from.
fn env_filter() -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
}

/// The stderr `fmt` layer of `log_format`, boxed so the JSON and text variants
/// share one assembly path.
fn fmt_layer(
    log_format: LogFormat,
) -> Box<dyn tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync> {
    match log_format {
        LogFormat::Json => Box::new(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(std::io::stderr),
        ),
        // Colour an interactive run only, so no escape code reaches a log
        // collector. `NO_COLOR` still vetoes it: an explicit `with_ansi`
        // overrides what the `fmt` layer would otherwise infer from that
        // variable itself, so this has to repeat the check.
        LogFormat::Text => Box::new(
            tracing_subscriber::fmt::layer()
                .with_ansi(
                    std::io::IsTerminal::is_terminal(&std::io::stderr())
                        && std::env::var_os("NO_COLOR").is_none_or(|value| value.is_empty()),
                )
                .with_writer(std::io::stderr),
        ),
    }
}

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
