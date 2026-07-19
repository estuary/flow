//! Per-handler event tracks: an opt-in, in-memory tail of recent events —
//! IO completions, RX/TX boundaries, future completions, anything notable
//! happening inside an asynchronous actor or handler — grouped onto named
//! ring-buffer **tracks**, hung off the enclosing handler's span, and surfaced
//! on the [`crate::admin`] handler drill-down page.
//!
//! Emit one with the [`event!`](crate::event!) macro, whose shape mirrors
//! `tracing`'s event macros — leading `name = value` fields, a message literal,
//! then positional `{}` interpolation args. A bare identifier is shorthand for
//! `name = name` — same as `tracing::debug!` et al.:
//!
//! ```ignore
//! service_kit::event!(tracing::Level::DEBUG, "io", "opened connection");
//! service_kit::event!(tracing::Level::DEBUG, "io", bytes = n, offset = off, "read chunk");
//! service_kit::event!(tracing::Level::DEBUG, "io", bytes, offset, "read chunk"); // shorthand
//! service_kit::event!(tracing::Level::INFO,  "io", "read {} bytes at {}", n, off);
//! ```
//!
//! `event!` does two things, cheaply: it appends to the **track** named by its
//! second argument on the handler span the call site is running within (found
//! via the current `tracing` span — outside any handler span it's a no-op),
//! *and* it emits an ordinary `tracing` event at the given level. That event is
//! usually filtered out, but it surfaces in the log stream when the process —
//! or just that handler, via [`crate::trace`] — is turned up; rendering the
//! message for it is deferred to that moment.
//!
//! Pair `event!` with `tracing::trace!`: emit an `event!` at RX/TX boundaries
//! and at future-completion points (the events that define *what happened*),
//! and use `tracing::trace!` for periodic fine-grain logging of internal
//! state and decisions. When debugging a stuck task an operator can then turn
//! the log level up on the admin dashboard to see ongoing tracing of internal
//! state, *and* review the historical event tracks captured before the turn-up
//! — the sequence of events that led the task to its current state.
//!
//! The track side reaches the handler's buffers by downcasting the active
//! `tracing` dispatcher to a [`tracing_subscriber::registry::Registry`] — so
//! it's silently a no-op under any other subscriber, even though the `tracing`
//! event still fires. In practice that's fine: a process that wants track
//! capture installs [`layer`] on a `tracing_subscriber` registry stack anyway.
//!
//! Laziness — the track capture path does as little as possible, because most
//! tracks are never looked at:
//! - a literal message is stored as the `&'static str` it already is — no
//!   allocation, no formatting;
//! - an interpolated message is stored as `(template, captured args)` and
//!   assembled by [`render_args`] only when it's actually read — by the
//!   drill-down page, or by the (usually-filtered) log event;
//! - field/argument values are stored in [`Captured`] — scalars stay scalars,
//!   `&'static str` stays static — so capture allocates only the (small) vectors
//!   holding them, and only when there are any;
//! - a value whose *text* rendering is itself expensive — or that's simply not
//!   one of the cheap types above — can be a [`lazy`] thunk (`event::lazy(|| …)`,
//!   [`json`] for a `serde::Serialize`, or [`debug`] for a `std::fmt::Debug`):
//!   the `Fn() -> String` is stored as-is and called only if the event is read —
//!   a drill-down snapshot, or the usually-filtered log event — possibly more
//!   than once.
//!
//! The contract that buys this (see [`IntoCaptured`]): an `event!` value must
//! be a number, `bool`, `&'static str`, `String`, `Arc<str>`,
//! `proto_gazette::uuid::Clock`, or a [`lazy`]/[`json`]/[`debug`] thunk. Anything
//! else — notably a borrowed `&str` or a `?x`/`%x`-style value — must be turned
//! into one of those at the call site (`x.to_string()` for a `Display`,
//! `event::debug(x)` for a `Debug`), making the cost visible.

use crate::Registry;
use crate::trace::HandlerIdVisitor;
use std::borrow::Cow;
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::SystemTime;

/// Events retained per track.
const TRACK_CAPACITY: usize = 10;

/// A handler's set of event tracks. Created by [`Registry::register`], hung off
/// the handler's span by the [`layer`] this module installs (so
/// [`event!`](crate::event!) reaches it via the current span), and snapshotted
/// for the admin surface and the recently-finished ring.
///
/// Keyed by track name — a `&'static str`, because [`event!`](crate::event!)
/// only accepts a string literal there — so there's one entry per distinct
/// `event!` track name in the handler's code, a handful; a `Vec` with linear
/// scan beats a map.
#[derive(Default)]
pub struct Tracks(std::sync::Mutex<Vec<(&'static str, VecDeque<Event>)>>);

struct Event {
    at: SystemTime,
    level: tracing::Level,
    message: Message,
    fields: Vec<(&'static str, Captured)>,
}

/// An event's message: a bare literal (kept as-is) or a deferred interpolation
/// (template + captured args, assembled by [`render_args`] at read time). Built
/// by [`record`] from the `(template, args)` the macro passes.
enum Message {
    Static(&'static str),
    Formatted {
        template: &'static str,
        args: Vec<Captured>,
    },
}

/// A cheaply-owned value captured for an event field or interpolation argument;
/// see [`IntoCaptured`]. Rendered to text via its [`std::fmt::Display`] impl
/// only when the drill-down page (or the deferred `tracing` event) needs it.
pub enum Captured {
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    Static(&'static str),
    Owned(String),
    Shared(Arc<str>),
    Clock(proto_gazette::uuid::Clock),
    Lazy(Box<dyn Fn() -> String + Send>),
}

impl std::fmt::Display for Captured {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Captured::Bool(v) => v.fmt(f),
            Captured::I64(v) => v.fmt(f),
            Captured::U64(v) => v.fmt(f),
            Captured::F64(v) => v.fmt(f),
            Captured::Static(v) => f.write_str(v),
            Captured::Owned(v) => f.write_str(v),
            Captured::Shared(v) => f.write_str(v),
            Captured::Clock(c) => f.write_str(&crate::handlers::rfc3339_millis(c.to_time())),
            Captured::Lazy(render) => f.write_str(&render()),
        }
    }
}

/// Capture a value whose *text* rendering is expensive enough to defer: `f` is
/// stored as-is and called only if the event is read — the drill-down page, or
/// the `event!`-emitted `tracing` event when it isn't filtered out — and then
/// possibly more than once. Use it for an [`event!`](crate::event!) field or
/// interpolation argument whose rendering you don't want to pay for on the
/// common path where the track is never looked at:
///
/// ```ignore
/// service_kit::event!(
///     tracing::Level::DEBUG, "rpc",
///     body = service_kit::event::lazy(move || serde_json::to_string(&msg).unwrap()),
///     "received {}", kind,
/// );
/// ```
///
/// The closure is `Send + 'static` — the track outlives the `event!` call and
/// is shared across threads — so it must own what it renders; clone or `Arc` a
/// borrow at the call site, keeping that cost visible.
#[inline]
pub fn lazy(f: impl Fn() -> String + Send + 'static) -> Captured {
    Captured::Lazy(Box::new(f))
}

/// A [`lazy`] thunk that compact-encodes `value` as JSON when the event is read
/// — for an `event!` field or interpolation argument that's a `serde::Serialize`
/// you don't want to pay to serialize on the common path. `value` is moved into
/// the thunk (it's `Send + 'static`, see [`lazy`]); a serialization error
/// renders as a placeholder rather than panicking on the read path.
#[inline]
pub fn json(value: impl serde::Serialize + Send + 'static) -> Captured {
    Captured::Lazy(Box::new(move || match serde_json::to_string(&value) {
        Ok(s) => s,
        Err(err) => format!("<unserializable: {err}>"),
    }))
}

/// A [`lazy`] thunk that renders `value` with its [`std::fmt::Debug`] impl when
/// the event is read. `value` is moved into the thunk (it's `Send + 'static`, see
/// [`lazy`]); if the type is `Clone` and held by reference at the call site,
/// clone it in (`event::debug(x.clone())`) — keeping the cost visible.
#[inline]
pub fn debug(value: impl std::fmt::Debug + Send + 'static) -> Captured {
    Captured::Lazy(Box::new(move || format!("{value:?}")))
}

/// Conversion into [`Captured`] — implemented only for values that are cheap to
/// own (no allocation, or at most a small move/clone). This is what restricts
/// [`event!`](crate::event!) to lazily-capturable values.
pub trait IntoCaptured {
    fn into_captured(self) -> Captured;
}

impl IntoCaptured for bool {
    #[inline]
    fn into_captured(self) -> Captured {
        Captured::Bool(self)
    }
}
impl IntoCaptured for &'static str {
    #[inline]
    fn into_captured(self) -> Captured {
        Captured::Static(self)
    }
}
impl IntoCaptured for String {
    #[inline]
    fn into_captured(self) -> Captured {
        Captured::Owned(self)
    }
}
impl IntoCaptured for Arc<str> {
    #[inline]
    fn into_captured(self) -> Captured {
        Captured::Shared(self)
    }
}
impl IntoCaptured for Captured {
    /// Identity — lets [`lazy`] (and any explicit [`Captured`]) flow through the
    /// `event!` macro's per-value `into_captured` call unchanged.
    #[inline]
    fn into_captured(self) -> Captured {
        self
    }
}
impl IntoCaptured for proto_gazette::uuid::Clock {
    #[inline]
    fn into_captured(self) -> Captured {
        Captured::Clock(self)
    }
}
impl IntoCaptured for f32 {
    #[inline]
    fn into_captured(self) -> Captured {
        Captured::F64(self as f64)
    }
}
impl IntoCaptured for f64 {
    #[inline]
    fn into_captured(self) -> Captured {
        Captured::F64(self)
    }
}
macro_rules! impl_into_captured_int {
    ($($signed:ty),+ ; $($unsigned:ty),+) => {
        $( impl IntoCaptured for $signed { #[inline] fn into_captured(self) -> Captured { Captured::I64(self as i64) } } )+
        $( impl IntoCaptured for $unsigned { #[inline] fn into_captured(self) -> Captured { Captured::U64(self as u64) } } )+
    };
}
impl_into_captured_int!(i8, i16, i32, i64, isize ; u8, u16, u32, u64, usize);

/// Append an event to the per-handler track `$track`, *and* emit it as a
/// `tracing` event at `$level`. See the [`event`](crate::event) module docs.
///
/// The shape mirrors `tracing`'s event macros — leading `name = value` fields,
/// a message literal, then positional `{}` interpolation args. A bare
/// identifier is shorthand for `name = name`:
/// - `event!(level, track, "literal")`
/// - `event!(level, track, "fmt {} {}", a, b)` — positional `{}` interpolation
/// - `event!(level, track, k1 = v1, k2 = v2, "literal")` — structured fields
/// - `event!(level, track, k1, k2 = v2, "literal")` — `k1` is sugar for `k1 = k1`
/// - `event!(level, track, k1 = v1, "fmt {}", a)` — fields *and* interpolation
/// - `event!(level, track, k1 = v1, k2 = v2)` — fields only, no message
///
/// `track` must be a string literal — the track topology of a handler is a
/// deliberate, greppable set of call sites, not a dynamically-derived thing.
/// `level` is any `tracing::Level` *expression* — unlike [`tracing::event!`],
/// which requires a constant because it bakes the level into a `static`
/// callsite. A runtime level (e.g. quieter on the first attempt, louder on
/// retry) is supported by fanning out to one constant-level `tracing::event!`
/// per level and selecting at runtime; only the taken branch renders, so a
/// literal level still collapses to a single callsite. Interpolation is
/// positional `{}` only: a `{name}`,
/// `{0}`, or `{:spec}` is **rejected at compile time** (via
/// [`check_template`](crate::event::check_template)) — write `{}` and pass the
/// value positionally. The `tracing` event renders the same text as the track,
/// so use `Display`-friendly values; a placeholder/argument count mismatch
/// trips a `debug_assert!`. Each `event!` value must implement
/// [`IntoCaptured`](crate::event::IntoCaptured) (number, `bool`, `&'static str`,
/// `String`, `Arc<str>`, `proto_gazette::uuid::Clock`); turn anything else into
/// one of those at the call site, or defer its rendering until — and only if —
/// the event is read with
/// [`lazy`](crate::event::lazy) (a `Fn() -> String`),
/// [`json`](crate::event::json) (a `serde::Serialize`), or
/// [`debug`](crate::event::debug) (a `std::fmt::Debug`).
///
/// The `tracing` event always carries a structured `track = <name>` field so an
/// `fmt` consumer can group/filter log lines by track. `track` is therefore
/// reserved.
#[macro_export]
macro_rules! event {
    // Zero or more leading `name [= value]` fields, a message literal, then
    // zero or more positional `{}` interpolation args. Each field is either
    // `name = val,` or the bare `name,` shorthand for `name = name` — same as
    // `tracing::debug!` et al. (The trailing comma on every field — including
    // the one before `$msg` — lets the repetition end cleanly at the literal
    // without a separator-comma ambiguity.)
    ($level:expr, $track:literal, $($name:ident $(= $val:expr)?,)* $msg:literal $(, $arg:expr)* $(,)?) => {{
        // Compile-time: the template must use only bare `{}` placeholders. Its
        // placeholder count is also the expected argument count, debug-checked
        // below against what was actually passed.
        const __EVENT_PLACEHOLDERS: usize = $crate::event::check_template($msg);
        let __event_fields: ::std::vec::Vec<(&'static str, $crate::event::Captured)> = ::std::vec![
            $( (
                ::std::stringify!($name),
                $crate::event::IntoCaptured::into_captured($crate::__event_value!($name $(= $val)?)),
            ), )*
        ];
        let __event_args: ::std::vec::Vec<$crate::event::Captured> = ::std::vec![
            $( $crate::event::IntoCaptured::into_captured($arg), )*
        ];
        ::std::debug_assert_eq!(
            __EVENT_PLACEHOLDERS, __event_args.len(),
            "event!: format string {:?} has {} placeholder(s) but {} argument(s) were passed",
            $msg, __EVENT_PLACEHOLDERS, __event_args.len(),
        );
        // `tracing::event!` bakes its level into a `static` callsite, so a
        // runtime `$level` can't be forwarded to it. Fan the level out to one
        // constant-level callsite each and pick at runtime (`Level: PartialEq`);
        // only the taken branch renders, and a literal `$level` folds away to a
        // single callsite. `record` — the track side — takes the level directly.
        // Bind `$level` once: it may be a side-effecting expression, and is
        // otherwise re-evaluated by each comparison and again by `record`.
        let __event_level = $level;
        if __event_level == ::tracing::Level::ERROR {
            $crate::__event_at_level!(::tracing::Level::ERROR, $track, __event_fields, __event_args, $msg $(, $name)*);
        } else if __event_level == ::tracing::Level::WARN {
            $crate::__event_at_level!(::tracing::Level::WARN, $track, __event_fields, __event_args, $msg $(, $name)*);
        } else if __event_level == ::tracing::Level::INFO {
            $crate::__event_at_level!(::tracing::Level::INFO, $track, __event_fields, __event_args, $msg $(, $name)*);
        } else if __event_level == ::tracing::Level::DEBUG {
            $crate::__event_at_level!(::tracing::Level::DEBUG, $track, __event_fields, __event_args, $msg $(, $name)*);
        } else {
            $crate::__event_at_level!(::tracing::Level::TRACE, $track, __event_fields, __event_args, $msg $(, $name)*);
        }
        $crate::event::record(__event_level, $track, $msg, __event_args, __event_fields);
    }};
    // Fields only, no message. (Defers to the form above with an empty message,
    // as `tracing`'s `info!(field = v)` produces an event with no message.)
    ($level:expr, $track:literal, $($name:ident $(= $val:expr)?),+ $(,)?) => {
        $crate::event!($level, $track, $($name $(= $val)?,)+ "")
    };
}

/// Emit the [`event!`](crate::event!)-mirroring `tracing` event at a *constant*
/// `$level`. Factored out so [`event!`](crate::event!) can fan a runtime level
/// across one invocation per level — `tracing::event!` requires a constant level
/// because it bakes it into a `static` callsite. `$fields`/`$args` are the field
/// and interpolation-argument vectors already built by the caller; the trailing
/// `$name`s re-attach each captured field to the `tracing` event by name. Macro
/// plumbing — not part of the intended API.
#[doc(hidden)]
#[macro_export]
macro_rules! __event_at_level {
    ($level:expr, $track:literal, $fields:ident, $args:ident, $msg:literal $(, $name:ident)* $(,)?) => {
        ::tracing::event!(
            $level,
            track = $track,
            $( $name = %$crate::event::lookup_field(&$fields, ::std::stringify!($name)), )*
            "{}", $crate::event::render_args($msg, &$args)
        )
    };
}

/// Pick the value for an `event!` field: the explicit `$val` when given,
/// otherwise the identifier itself (the `foo` → `foo = foo` shorthand). Lifted
/// to its own macro because `macro_rules` can't expand an `$(= $val:expr)?`
/// repetition into "use `$val` if present, else `$name`" inline.
#[doc(hidden)]
#[macro_export]
macro_rules! __event_value {
    ($name:ident) => {
        $name
    };
    ($name:ident = $val:expr) => {
        $val
    };
}

/// Append an event to the current handler's track `track` — a literal message
/// (`args` empty) or a deferred interpolation of `template` by `args`. Used by
/// the [`event!`](crate::event!) macro; no-op outside a handler span.
#[doc(hidden)]
#[inline]
pub fn record(
    level: tracing::Level,
    track: &'static str,
    template: &'static str,
    args: Vec<Captured>,
    fields: Vec<(&'static str, Captured)>,
) {
    let Some(tracks) = current_handler_tracks() else {
        return;
    };
    let message = if args.is_empty() {
        Message::Static(template)
    } else {
        Message::Formatted { template, args }
    };
    tracks.record(
        track,
        Event {
            at: SystemTime::now(),
            level,
            message,
            fields,
        },
    );
}

/// Look a field's value up by name within an `event!`-built field vector. The
/// name is always present (the macro just inserted it); this exists only so the
/// expanded [`tracing::event!`] can name fields without re-evaluating their
/// values. Macro plumbing — not part of the intended API.
#[doc(hidden)]
pub fn lookup_field<'a>(fields: &'a [(&'static str, Captured)], name: &str) -> &'a Captured {
    fields
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, v)| v)
        .expect("event! just inserted this field")
}

/// Compile-time check, invoked by [`event!`](crate::event!), that `template`
/// uses only bare `{}` placeholders (it returns their count, used for a
/// debug-time placeholder/argument count assertion). A `{name}`, `{0}`, or
/// `{:spec}` would be taken as a plain positional slot with its contents
/// silently dropped — and a stray `}` would be a literal — so all of those are
/// rejected at the call site. `{{` / `}}` are escaped literal braces.
#[doc(hidden)]
pub const fn check_template(template: &str) -> usize {
    let b = template.as_bytes();
    let mut count = 0usize;
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'{' {
            if i + 1 < b.len() && b[i + 1] == b'{' {
                i += 2; // escaped `{{`
            } else if i + 1 < b.len() && b[i + 1] == b'}' {
                count += 1;
                i += 2; // bare `{}` placeholder
            } else {
                panic!(
                    "event! format strings allow only bare positional placeholders: \
                     write a brace pair and pass the value as an argument \
                     (named or indexed args and format specs are not supported)"
                );
            }
        } else if b[i] == b'}' {
            if i + 1 < b.len() && b[i + 1] == b'}' {
                i += 2; // escaped `}}`
            } else {
                panic!(
                    "event! format string has an unmatched closing brace; double it for a literal"
                );
            }
        } else {
            i += 1;
        }
    }
    count
}

/// Render an `event!` message: substitute each `{}` in `template` with the next
/// arg's `Display`, collapse `{{` / `}}` to single braces, return `template`
/// borrowed when it has neither. Lazy — called only when the message is read
/// (a drill-down snapshot, or the macro's `tracing` event when unfiltered).
///
/// [`check_template`] has already rejected any non-`{}` placeholder by the time
/// an `event!` template reaches here; the leftover handling (treat any `{…}` as
/// a slot, emit `{?}` when out of args, ignore an unmatched brace) is just so
/// this is total rather than panicking on input that can't actually occur.
#[doc(hidden)]
pub fn render_args<'a>(template: &'a str, args: &[Captured]) -> Cow<'a, str> {
    if !template.contains(['{', '}']) {
        return Cow::Borrowed(template);
    }
    let mut out = String::with_capacity(template.len() + 8 * args.len());
    let mut args = args.iter();
    let mut rest = template;
    while let Some(i) = rest.find(['{', '}']) {
        out.push_str(&rest[..i]);
        let bytes = rest.as_bytes();
        let here = bytes[i];
        if bytes.get(i + 1) == Some(&here) {
            out.push(here as char); // `{{` or `}}`
            rest = &rest[i + 2..];
        } else if here == b'{' {
            // A placeholder `{…}`: skip to its closing `}`, dropping whatever's
            // between (a name / index / format spec — `event!` rejects those).
            rest = match rest[i + 1..].find('}') {
                Some(j) => &rest[i + 1 + j + 1..],
                None => "", // malformed; stop.
            };
            match args.next() {
                Some(v) => {
                    let _ = write!(out, "{v}");
                }
                None => out.push_str("{?}"),
            }
        } else {
            out.push('}'); // stray `}`
            rest = &rest[i + 1..];
        }
    }
    out.push_str(rest);
    Cow::Owned(out)
}

fn render_message(message: &Message) -> Cow<'_, str> {
    match message {
        Message::Static(s) => Cow::Borrowed(s),
        Message::Formatted { template, args } => render_args(template, args),
    }
}

impl Tracks {
    #[inline]
    fn record(&self, track: &'static str, event: Event) {
        let mut tracks = self.0.lock().unwrap();
        let idx = match tracks.iter().position(|(name, _)| *name == track) {
            Some(idx) => idx,
            None => {
                tracks.push((track, VecDeque::with_capacity(TRACK_CAPACITY)));
                tracks.len() - 1
            }
        };
        let events = &mut tracks[idx].1;
        if events.len() == TRACK_CAPACITY {
            events.pop_front();
        }
        events.push_back(event);
    }

    /// Snapshot and render every track (newest event last within each), keyed
    /// by track name, with both an age (relative to now) and an absolute
    /// RFC-3339 timestamp on each event.
    pub fn snapshot(&self) -> BTreeMap<String, Vec<EventView>> {
        let now = SystemTime::now();
        let tracks = self.0.lock().unwrap();
        tracks
            .iter()
            .map(|(name, events)| {
                let views = events
                    .iter()
                    .map(|e| EventView {
                        age_seconds: now.duration_since(e.at).unwrap_or_default().as_secs(),
                        at_rfc3339: crate::handlers::rfc3339_millis(e.at),
                        level: e.level.as_str(),
                        message: render_message(&e.message).into_owned(),
                        fields: e.fields.iter().map(|(n, v)| (*n, v.to_string())).collect(),
                    })
                    .collect();
                (name.to_string(), views)
            })
            .collect()
    }
}

/// One captured event, as presented by the admin surface.
#[derive(Clone, serde::Serialize)]
pub struct EventView {
    pub age_seconds: u64,
    pub at_rfc3339: String,
    pub level: &'static str,
    pub message: String,
    pub fields: Vec<(&'static str, String)>,
}

/// A `tracing_subscriber` layer that hangs each handler's [`Tracks`] off the
/// handler span, so [`event!`](crate::event!) can find it via the current span.
/// Add it to the registry stack:
/// `tracing_subscriber::registry().with(fmt_layer…).with(event::layer(registry))`.
pub fn layer<S>(registry: Registry) -> impl tracing_subscriber::Layer<S>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    use tracing_subscriber::Layer as _;
    EventLayer { registry }.with_filter(EventFilter)
}

struct EventLayer {
    registry: Registry,
}

/// Span extension on a handler span: that handler's event tracks.
struct EventExt(Arc<Tracks>);

impl<S> tracing_subscriber::Layer<S> for EventLayer
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut visitor = HandlerIdVisitor(None);
        attrs.record(&mut visitor);
        let Some(handler_id) = visitor.0 else { return };
        let Some(tracks) = self.registry.tracks_handle(handler_id) else {
            return;
        };
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(EventExt(tracks));
        }
    }
}

/// Per-layer filter: [`EventLayer`] cares only about handler spans (where it
/// hangs the `EventExt`); everything else — every event, every other span —
/// bypasses it entirely.
struct EventFilter;

impl<S> tracing_subscriber::layer::Filter<S> for EventFilter {
    fn callsite_enabled(
        &self,
        meta: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        if meta.is_span() && meta.target() == crate::handlers::HANDLER_SPAN_TARGET {
            tracing::subscriber::Interest::always()
        } else {
            tracing::subscriber::Interest::never()
        }
    }

    fn enabled(
        &self,
        _meta: &tracing::Metadata<'_>,
        _cx: &tracing_subscriber::layer::Context<'_, S>,
    ) -> bool {
        // `callsite_enabled` answers definitively for every callsite.
        true
    }

    fn max_level_hint(&self) -> Option<tracing_subscriber::filter::LevelFilter> {
        // The handler span is an `info_span!`; advertise `INFO` so that callsite
        // stays live, and our `on_new_span` fires for it.
        Some(tracing_subscriber::filter::LevelFilter::INFO)
    }
}

/// The [`Tracks`] of the handler whose span (or a descendant of it) is
/// currently entered, if any — via the active `tracing` dispatcher's span store.
#[inline]
fn current_handler_tracks() -> Option<Arc<Tracks>> {
    use tracing_subscriber::registry::LookupSpan as _;

    tracing::dispatcher::get_default(|dispatch| {
        let id = dispatch.current_span().id()?.clone();
        let registry = dispatch.downcast_ref::<tracing_subscriber::registry::Registry>()?;
        let mut span = registry.span(&id)?;
        loop {
            if let Some(ext) = span.extensions().get::<EventExt>() {
                return Some(ext.0.clone());
            }
            span = span.parent()?;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::prelude::*;

    fn ev(at: SystemTime) -> Event {
        Event {
            at,
            level: tracing::Level::INFO,
            message: Message::Static("x"),
            fields: Vec::new(),
        }
    }

    #[test]
    fn track_evicts_oldest_within_each_name() {
        let tracks = Tracks::default();
        for i in 0..(TRACK_CAPACITY + 3) {
            tracks.record(
                "io",
                Event {
                    message: Message::Formatted {
                        template: "e{}",
                        args: vec![Captured::I64(i as i64)],
                    },
                    ..ev(SystemTime::now())
                },
            );
        }
        tracks.record("rpc", ev(SystemTime::now()));

        let snap = tracks.snapshot();
        assert_eq!(snap["io"].len(), TRACK_CAPACITY);
        assert_eq!(snap["io"].first().unwrap().message, "e3");
        assert_eq!(
            snap["io"].last().unwrap().message,
            format!("e{}", TRACK_CAPACITY + 2)
        );
        // A second distinct track name gets its own buffer.
        assert_eq!(snap["rpc"].len(), 1);
    }

    #[test]
    fn render_args_substitutes_and_escapes_braces() {
        let a = [Captured::I64(3), Captured::Static("ok")];
        assert_eq!(render_args("read {} bytes: {}", &a), "read 3 bytes: ok");
        // `render_args` itself is lenient about specs / stray braces; the
        // `event!` macro rejects them at compile time (see `check_template`).
        assert_eq!(
            render_args("spec {:>8?} ignored", &a[..1]),
            "spec 3 ignored"
        );
        assert_eq!(
            render_args("braces {{}} and {}", &a[..1]),
            "braces {} and 3"
        );
        assert_eq!(render_args("missing {}", &[]), "missing {?}");
        // Nothing to substitute or escape — borrowed, not reallocated.
        assert!(matches!(
            render_args("plain message", &a),
            Cow::Borrowed("plain message")
        ));
    }

    #[test]
    fn lazy_capture_renders_only_when_read() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let calls = Arc::new(AtomicUsize::new(0));
        let tracks = Tracks::default();
        let c = calls.clone();
        tracks.record(
            "io",
            Event {
                fields: vec![(
                    "body",
                    lazy(move || {
                        c.fetch_add(1, Ordering::Relaxed);
                        "rendered".to_string()
                    }),
                )],
                ..ev(SystemTime::now())
            },
        );
        // Captured, but not yet rendered.
        assert_eq!(calls.load(Ordering::Relaxed), 0);

        let snap = tracks.snapshot();
        assert_eq!(snap["io"][0].fields, vec![("body", "rendered".to_string())]);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        // Each read re-runs the thunk.
        let _ = tracks.snapshot();
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn json_capture_renders_compact_json() {
        let tracks = Tracks::default();
        tracks.record(
            "io",
            Event {
                fields: vec![("body", json(serde_json::json!({"n": 3, "ok": true})))],
                ..ev(SystemTime::now())
            },
        );
        assert_eq!(
            tracks.snapshot()["io"][0].fields,
            vec![("body", r#"{"n":3,"ok":true}"#.to_string())]
        );
    }

    #[test]
    fn debug_capture_renders_debug_fmt() {
        let tracks = Tracks::default();
        tracks.record(
            "io",
            Event {
                fields: vec![("v", debug(vec![1u32, 2, 3]))],
                ..ev(SystemTime::now())
            },
        );
        assert_eq!(
            tracks.snapshot()["io"][0].fields,
            vec![("v", "[1, 2, 3]".to_string())]
        );
    }

    #[test]
    fn clock_capture_renders_rfc3339() {
        let tracks = Tracks::default();
        tracks.record(
            "io",
            Event {
                fields: vec![(
                    "at",
                    proto_gazette::uuid::Clock::from_unix(1_700_000_000, 500_000_000)
                        .into_captured(),
                )],
                ..ev(SystemTime::now())
            },
        );
        assert_eq!(
            tracks.snapshot()["io"][0].fields,
            vec![("at", "2023-11-14T22:13:20.500Z".to_string())]
        );
    }

    #[test]
    fn check_template_counts_placeholders() {
        assert_eq!(check_template(""), 0);
        assert_eq!(check_template("plain message"), 0);
        assert_eq!(check_template("read {} bytes at {}"), 2);
        assert_eq!(check_template("escaped {{}} braces, one {} slot"), 1);
        // It's a `const fn` — also usable in const position.
        const N: usize = check_template("a {} b {} c {}");
        assert_eq!(N, 3);
    }

    #[test]
    #[should_panic(expected = "bare positional placeholders")]
    fn check_template_rejects_named_placeholder() {
        check_template("hello {name}");
    }

    #[test]
    #[should_panic(expected = "bare positional placeholders")]
    fn check_template_rejects_format_spec() {
        check_template("padded {:>8}");
    }

    #[test]
    #[should_panic(expected = "unmatched closing brace")]
    fn check_template_rejects_stray_close() {
        check_template("oops }");
    }

    #[test]
    fn macro_captures_into_current_handler_track() {
        let registry = Registry::new();
        let subscriber = tracing_subscriber::registry().with(layer(registry.clone()));

        tracing::subscriber::with_default(subscriber, || {
            let mut handler = registry.register("test.kind");
            let id = registry.snapshot().live[0].id;
            let span = handler.span();
            let entered = span.enter();

            crate::event!(tracing::Level::DEBUG, "io", "opened");
            crate::event!(
                tracing::Level::INFO,
                "io",
                bytes = 3usize,
                eof = false,
                "read chunk"
            );
            // Reached via an intermediate (non-handler) span.
            tracing::info_span!("inner").in_scope(|| {
                crate::event!(tracing::Level::WARN, "io", "slow read {} of {}", 2, 9);
            });
            crate::event!(tracing::Level::DEBUG, "io", tag = "x", "tagged {}", 7);
            crate::event!(tracing::Level::TRACE, "io", attempt = 4u32);

            let detail = registry.handler_detail(id).expect("live");
            let io = &detail.tracks["io"];
            assert_eq!(io.len(), 5);
            assert_eq!((io[0].level, io[0].message.as_str()), ("DEBUG", "opened"));
            assert!(io[0].fields.is_empty());
            assert_eq!(
                (io[1].level, io[1].message.as_str()),
                ("INFO", "read chunk")
            );
            assert_eq!(
                io[1].fields,
                vec![("bytes", "3".to_string()), ("eof", "false".to_string())]
            );
            assert_eq!(
                (io[2].level, io[2].message.as_str()),
                ("WARN", "slow read 2 of 9")
            );
            assert_eq!(
                (io[3].message.as_str(), &io[3].fields[..]),
                ("tagged 7", &[("tag", "x".to_string())][..])
            );
            // Fields-only: empty message, the field is present.
            assert_eq!(
                (io[4].message.as_str(), &io[4].fields[..]),
                ("", &[("attempt", "4".to_string())][..])
            );

            drop(entered);
            handler.finish_ok();
        });

        // The tracks survive into the recently-finished entry.
        let detail = registry.handler_detail(0).expect("finished");
        assert!(detail.finished);
        assert_eq!(detail.tracks["io"].len(), 5);
    }

    #[test]
    fn macro_accepts_bare_ident_shorthand() {
        let registry = Registry::new();
        let subscriber = tracing_subscriber::registry().with(layer(registry.clone()));

        tracing::subscriber::with_default(subscriber, || {
            let mut handler = registry.register("test.kind");
            let id = registry.snapshot().live[0].id;
            let span = handler.span();
            let _entered = span.enter();

            let bytes = 7usize;
            let eof = false;
            // Bare `bytes` is sugar for `bytes = bytes`; mix freely with the
            // `name = value` form, and with a message + positional args.
            crate::event!(
                tracing::Level::INFO,
                "io",
                bytes,
                eof,
                tag = "x",
                "read {} bytes",
                bytes,
            );
            // Fields-only with a bare ident at the tail.
            let attempt = 4u32;
            crate::event!(tracing::Level::TRACE, "io", attempt);

            let detail = registry.handler_detail(id).expect("live");
            let io = &detail.tracks["io"];
            assert_eq!(
                (io[0].message.as_str(), &io[0].fields[..]),
                (
                    "read 7 bytes",
                    &[
                        ("bytes", "7".to_string()),
                        ("eof", "false".to_string()),
                        ("tag", "x".to_string()),
                    ][..],
                ),
            );
            assert_eq!(
                (io[1].message.as_str(), &io[1].fields[..]),
                ("", &[("attempt", "4".to_string())][..]),
            );

            handler.finish_ok();
        });
    }

    #[test]
    fn macro_accepts_runtime_level() {
        let registry = Registry::new();
        let subscriber = tracing_subscriber::registry().with(layer(registry.clone()));

        tracing::subscriber::with_default(subscriber, || {
            let mut handler = registry.register("test.kind");
            let id = registry.snapshot().live[0].id;
            let span = handler.span();
            let _entered = span.enter();

            // The level is a runtime value — quiet on the first attempt, loud on
            // retry — which `tracing::event!` alone can't accept. Fields and
            // message are captured the same regardless of which branch fires.
            for attempt in 0..2u32 {
                let level = if attempt == 0 {
                    tracing::Level::TRACE
                } else {
                    tracing::Level::WARN
                };
                crate::event!(level, "io", attempt, "retrying {}", attempt);
            }

            let detail = registry.handler_detail(id).expect("live");
            let io = &detail.tracks["io"];
            assert_eq!(
                (io[0].level, io[0].message.as_str(), &io[0].fields[..]),
                ("TRACE", "retrying 0", &[("attempt", "0".to_string())][..]),
            );
            assert_eq!(
                (io[1].level, io[1].message.as_str(), &io[1].fields[..]),
                ("WARN", "retrying 1", &[("attempt", "1".to_string())][..]),
            );

            handler.finish_ok();
        });
    }

    #[test]
    fn macro_evaluates_level_expression_once() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let registry = Registry::new();
        let subscriber = tracing_subscriber::registry().with(layer(registry.clone()));

        tracing::subscriber::with_default(subscriber, || {
            let mut handler = registry.register("test.kind");
            let span = handler.span();
            let _entered = span.enter();

            // A side-effecting `$level` must be evaluated exactly once per
            // `event!` — the macro binds it, rather than re-evaluating it in each
            // per-level comparison and again in `record`.
            let calls = AtomicUsize::new(0);
            let level = || {
                calls.fetch_add(1, Ordering::Relaxed);
                tracing::Level::INFO
            };
            crate::event!(level(), "io", "message");

            assert_eq!(calls.load(Ordering::Relaxed), 1);

            handler.finish_ok();
        });
    }

    #[test]
    fn macro_accepts_lazy_thunks() {
        let registry = Registry::new();
        let subscriber = tracing_subscriber::registry().with(layer(registry.clone()));

        tracing::subscriber::with_default(subscriber, || {
            let mut handler = registry.register("test.kind");
            let id = registry.snapshot().live[0].id;
            let span = handler.span();
            let _entered = span.enter();

            // Lazy thunk as a field, and as an interpolation argument.
            crate::event!(
                tracing::Level::DEBUG,
                "io",
                body = crate::event::lazy(|| "the body".to_string()),
                "got {} ({})",
                "hello",
                crate::event::lazy(|| "lazy arg".to_string()),
            );

            let detail = registry.handler_detail(id).expect("live");
            let io = &detail.tracks["io"];
            assert_eq!(io[0].message, "got hello (lazy arg)");
            assert_eq!(io[0].fields, vec![("body", "the body".to_string())]);

            handler.finish_ok();
        });
    }

    #[test]
    fn macro_outside_a_handler_is_a_noop() {
        let registry = Registry::new();
        let subscriber = tracing_subscriber::registry().with(layer(registry.clone()));
        tracing::subscriber::with_default(subscriber, || {
            crate::event!(tracing::Level::INFO, "io", "no handler span");
            tracing::info_span!("plain").in_scope(|| {
                crate::event!(tracing::Level::INFO, "io", "still no handler span");
            });
        });
        assert!(registry.snapshot().live.is_empty());
        assert!(registry.snapshot().recent.is_empty());
    }
}
