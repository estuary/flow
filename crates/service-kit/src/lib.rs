//! Building blocks for productionized async task servers: an operational
//! surface a long-running service exposes over a loopback HTTP port.
//!
//! - [`Registry`] tracks the lifecycle of in-flight units of work (gRPC
//!   handlers, jobs, connections — whatever a service spawns), so an operator
//!   can see what the process is doing right now.
//! - [`admin`] serves that inventory as an HTML dashboard plus a JSON endpoint,
//!   a per-handler drill-down page, and lets an operator raise the trace
//!   verbosity of one handler at runtime.
//! - [`trace`] is the dynamic-verbosity mechanism behind that control: a
//!   [`tracing_subscriber`] filter, composed with the service's base filter via
//!   [`trace::layer_filter`].
//! - [`event!`] appends an opt-in breadcrumb to a small per-handler ring (and
//!   also emits a `tracing` event), surfaced on the drill-down page; install
//!   [`event::layer`] alongside the `fmt` layer. Capture is lazy — see [`event`].
//! - [`metrics`] folds a Prometheus scrape endpoint into the same admin port,
//!   capturing anything emitted via the `metrics` facade.
//!
//! Nothing here is specific to any one service; see `README.md`.

pub mod admin;
pub mod event;
mod handlers;
pub mod metrics;
pub mod trace;

pub use handlers::{FinishedView, HandlerDetail, HandlerGuard, HandlerView, Registry, Snapshot};
