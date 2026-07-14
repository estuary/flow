//! Out-of-band reporting seam: the task's log and event stream.
//!
//! This is the third host-facing seam of the crate, alongside [`Publisher`]
//! (document output) and [`ShuffleSession`] (checkpoint input). It carries the
//! task's *log stream* — everything that is eligible for surfacing to a human.
//! That stream has two sources: the connector's own logs ([`Logger::log`]),
//! and structured runtime [`LogEvent`]s ([`Logger::event`]) which flatten into
//! log records via their canonical [`LogEvent::to_log`] rendering.
//!
//! The membership test for adding a [`LogEvent`] variant is: *would this be a
//! line in the task's ops-log journal in production?* Events are structured
//! ops-log records that haven't been flattened yet — a host may intercept them
//! structurally (with their typed, verbatim payloads) before they degrade into
//! rendered log lines. Host-specific rendering (e.g. `flowctl preview`'s
//! `--output-state` / `--output-apply` lines) lives entirely in the host.
//!
//! The seam is deliberately distinct from two adjacent surfaces:
//!
//! - From `service_kit` (the [`Registry`](service_kit::Registry) + `event!`
//!   macro), which is the *operator/admin* surface — in-flight handler phases,
//!   breadcrumb rings, an admin dashboard. That never reaches user task logs;
//!   this seam is the one that does. Likewise ad-hoc `tracing::*` diagnostics:
//!   anything that need *not* reach the user's task logs stays plain tracing.
//! - From [`Publisher`], which emits the task's *data* (captured / derived
//!   collection documents). A [`Logger`] reports *about* the runtime, not
//!   the data it moves.
//!
//! The seam is generic, not dynamic: each leader / shard `Service` is
//! monomorphized over its concrete [`LoggerFactory`]. Every real installation
//! is an `ops::Log` sink, differing only in where logs go:
//!
//! - Production shards install an [`FnLoggerFactory`] wrapping the task's
//!   encoded-JSON log writer: connector logs and flattened events both land in
//!   the task-log file, which the Go runtime forwards to the task's ops-log
//!   journal.
//! - Leaders and unit tests install [`TracingLoggerFactory`], which renders
//!   logs as tracing events (sidecar stderr / journald). The leader sidecar's
//!   tracing is *not* yet forwarded to task ops-logs; bridging that gap is an
//!   Logger whose [`log`](Logger::log) publishes to the task's ops journal
//!   asynchronously — a later change that needs only that one method.
//! - `flowctl preview` installs a Logger that intercepts the events it
//!   renders to stdout ([`LogEvent::Persist`], [`LogEvent::Applied`]) and flattens
//!   the rest to its chosen log handler.
//!
//! [`Publisher`]: crate::Publisher
//! [`ShuffleSession`]: crate::ShuffleSession

use crate::proto;

/// Per-session logger: the sink for the task's log and event stream. The
/// leader and shards obtain one from a [`LoggerFactory`] at the start of
/// each session. Cheap to clone (the connector log pump holds its own handle).
///
/// Every method is synchronous and off the hot path; any async publication is
/// the implementation's internal concern (a background drain), never an `await`
/// at the call site.
pub trait Logger: Clone + Send + Sync + 'static {
    /// Sink one log of the task's log stream: a connector log line, or a
    /// runtime [`LogEvent`] flattened through [`LogEvent::to_log`]. Required (no
    /// default) so no installer silently drops the stream.
    fn log(&self, log: &ops::Log);

    /// Report a structured runtime [`LogEvent`]. The default flattens it into its
    /// canonical log record and sinks it through [`log`](Logger::log) —
    /// override only to intercept events structurally, and delegate unhandled
    /// events to [`LogEvent::to_log`] to preserve their log-line rendering.
    fn event(&self, event: LogEvent<'_>) {
        if let Some(log) = event.to_log() {
            self.log(&log);
        }
    }
}

/// A structured runtime event: an ops-log record that hasn't been flattened
/// yet. Variants carry borrowed, verbatim payloads so a host can intercept
/// them structurally; [`LogEvent::to_log`] is their canonical log rendering.
///
/// The enum and its variants are `#[non_exhaustive]`: new events and new
/// fields of existing events are non-breaking, so hosts must match with a
/// wildcard arm (delegating to [`LogEvent::to_log`]) and bind fields with `..`.
#[derive(Debug)]
#[non_exhaustive]
pub enum LogEvent<'a> {
    /// A connector-state [`proto::Persist`] at the point it's emitted: the
    /// leader's committing transaction and its Apply loop (derive /
    /// materialize), and the capture shard's committing transaction and its
    /// Apply loop.
    #[non_exhaustive]
    Persist { persist: &'a proto::Persist },

    /// A connector Apply action description, once per Apply iteration as the
    /// Apply loop converges (before any session
    /// [`Publisher`](crate::Publisher) exists).
    #[non_exhaustive]
    Applied { action_description: &'a str },

    /// A collection's inferred write-schema widened this transaction.
    /// `binding` is the source binding index for captures (multiple bindings
    /// per task) and `None` for derivations (a single derived collection).
    /// `schema` is the representative JSON Schema of the widened write-shape,
    /// as produced by [`doc::shape::schema::to_schema`].
    #[non_exhaustive]
    InferredSchema {
        collection_name: &'a str,
        binding: Option<usize>,
        schema: &'a schemars::Schema,
    },

    /// A connector container started and is dialed. Lower-level network /
    /// codec detail is logged separately at debug by `container::start`.
    #[non_exhaustive]
    ContainerStarted {
        image: &'a str,
        container: &'a proto::Container,
    },

    /// A connector container is being torn down (its [`Guard`] was dropped at
    /// session end or on error).
    ///
    /// [`Guard`]: crate::container::Guard
    #[non_exhaustive]
    ContainerStopped { image: &'a str },

    /// A transient image-pull failure that will be retried.
    #[non_exhaustive]
    ImagePullRetry {
        image: &'a str,
        attempt: u32,
        error: &'a str,
    },
}

impl LogEvent<'_> {
    /// Flatten this event into its canonical [`ops::Log`] — the single place
    /// events render as task-log lines.
    /// Returns `None` when the event surfaces nothing (a [`LogEvent::Persist`]
    /// carrying no connector-state delta: idempotent replays, ACK-only
    /// persists, startup checkpoint reconciliation).
    pub fn to_log(&self) -> Option<ops::Log> {
        let mut fields: Vec<(&str, bytes::Bytes)> = Vec::new();

        let (level, message) = match self {
            LogEvent::Persist { persist, .. } => {
                if persist.connector_patches_json.is_empty() {
                    return None;
                }
                // The patch payload is valid JSON (a tab-delimited JSON array;
                // see `crate::patches`), so it embeds verbatim.
                fields.push(("patches", persist.connector_patches_json.clone()));
                (ops::LogLevel::Debug, "persisted connector-state delta")
            }
            LogEvent::Applied {
                action_description, ..
            } => {
                // Action descriptions can be very long (e.g. a large DDL) and
                // could overflow the maximum ops-log line without bounding.
                let action = match action_description.char_indices().nth(1 << 18) {
                    Some((idx, _)) => &action_description[..idx],
                    None => *action_description,
                };
                fields.push(("actionDescription", json_field(&action)));
                (ops::LogLevel::Info, "connector applied")
            }
            LogEvent::InferredSchema {
                collection_name,
                binding,
                schema,
                ..
            } => {
                // Field name is `collection_name` (not `collection`) because the
                // L1 inferred-schemas rollup keys off `$fields->>'collection_name'`
                // (see ops-catalog/data-plane-template.flow.yaml).
                fields.push(("collection_name", json_field(collection_name)));
                if let Some(binding) = binding {
                    fields.push(("binding", json_field(binding)));
                }
                fields.push(("schema", json_field(schema)));
                (ops::LogLevel::Info, "inferred schema updated")
            }
            LogEvent::ContainerStarted {
                image, container, ..
            } => {
                fields.push(("image", json_field(image)));
                fields.push(("container", json_field(container)));
                (ops::LogLevel::Info, "started connector container")
            }
            LogEvent::ContainerStopped { image, .. } => {
                fields.push(("image", json_field(image)));
                (ops::LogLevel::Debug, "stopped connector container")
            }
            LogEvent::ImagePullRetry {
                image,
                attempt,
                error,
                ..
            } => {
                fields.push(("image", json_field(image)));
                fields.push(("attempt", json_field(attempt)));
                fields.push(("error", json_field(error)));
                (
                    ops::LogLevel::Warn,
                    "transient error pulling image (will retry)",
                )
            }
        };

        Some(ops::Log {
            meta: None,
            shard: None,
            timestamp: Some(proto_flow::as_timestamp(std::time::SystemTime::now())),
            level: level as i32,
            message: message.to_string(),
            fields_json_map: fields
                .into_iter()
                .map(|(key, value)| (key.to_string(), value))
                .collect(),
            spans: Vec::new(),
        })
    }
}

fn json_field(value: &impl serde::Serialize) -> bytes::Bytes {
    serde_json::to_vec(value)
        .expect("event field always serializes")
        .into()
}

/// Opens a [`Logger`] for each leader / shard session. Held by the leader
/// [`Service`](crate::leader::Service) and shard [`Service`](crate::shard::Service),
/// which are monomorphized over it.
pub trait LoggerFactory: Clone + Send + Sync + 'static {
    /// Concrete per-session logger this factory produces.
    type Logger: Logger;

    /// Open a [`Logger`] bound to the given task. `task_name` identifies the
    /// task whose logs (and, in the future, ops-log journal) the logger
    /// sinks; the `Fn` and tracing loggers ignore it.
    fn open(&self, task_name: &str) -> Self::Logger;
}

/// [`Logger`] whose [`log`](Logger::log) forwards to a `Fn(&ops::Log)`.
/// Events flatten through the default [`event`](Logger::event) into the same
/// `Fn`. The production shard install: the `Fn` is the task's encoded-JSON
/// log writer, so connector logs and runtime events both reach the task-log
/// file.
#[derive(Clone)]
pub struct FnLogger<F>(F);

impl<F: Fn(&ops::Log) + Clone + Send + Sync + 'static> Logger for FnLogger<F> {
    fn log(&self, log: &ops::Log) {
        (self.0)(log)
    }
}

/// [`LoggerFactory`] producing [`FnLogger`]s. Each session's logger is a
/// clone of the wrapped log handler; the handler is shared, the per-session
/// logger is a cheap clone.
#[derive(Clone)]
pub struct FnLoggerFactory<F>(F);

impl<F: Fn(&ops::Log) + Clone + Send + Sync + 'static> FnLoggerFactory<F> {
    pub fn new(log_handler: F) -> Self {
        Self(log_handler)
    }
}

impl<F: Fn(&ops::Log) + Clone + Send + Sync + 'static> LoggerFactory for FnLoggerFactory<F> {
    type Logger = FnLogger<F>;

    fn open(&self, _task_name: &str) -> FnLogger<F> {
        FnLogger(self.0.clone())
    }
}

/// [`Logger`] rendering the log stream as tracing events
/// ([`ops::tracing_log_handler`]). Installed by leaders — whose logs surface on
/// sidecar stderr until the async ops-log publishing Logger exists — and by
/// unit tests.
#[derive(Clone)]
pub struct TracingLogger;

impl Logger for TracingLogger {
    fn log(&self, log: &ops::Log) {
        ops::tracing_log_handler(log);
    }
}

/// [`LoggerFactory`] opening [`TracingLogger`]s. The default install for
/// the leader `Service`.
#[derive(Clone)]
pub struct TracingLoggerFactory;

impl LoggerFactory for TracingLoggerFactory {
    type Logger = TracingLogger;

    fn open(&self, _task_name: &str) -> TracingLogger {
        TracingLogger
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn event_to_log_renderings() {
        // A Persist carrying no connector-state delta surfaces nothing.
        assert!(
            LogEvent::Persist {
                persist: &proto::Persist::default()
            }
            .to_log()
            .is_none()
        );

        let persist = proto::Persist {
            connector_patches_json: b"[{\"cursor\":\"abc\"}\t,{\"cursor\":\"def\"}\t]"
                .as_slice()
                .into(),
            ..Default::default()
        };
        let schema: schemars::Schema =
            serde_json::from_value(serde_json::json!({"type": "object"})).unwrap();
        let container = proto::Container {
            ip_addr: "10.0.0.2".to_string(),
            ..Default::default()
        };
        let image = "ghcr.io/estuary/source-hello-world:dev";

        let logs: Vec<serde_json::Value> = [
            LogEvent::Persist { persist: &persist },
            LogEvent::Applied {
                action_description: "create table \"foo\"",
            },
            LogEvent::InferredSchema {
                collection_name: "acmeCo/collection",
                binding: Some(2),
                schema: &schema,
            },
            LogEvent::InferredSchema {
                collection_name: "acmeCo/collection",
                binding: None,
                schema: &schema,
            },
            LogEvent::ContainerStarted {
                image,
                container: &container,
            },
            LogEvent::ContainerStopped { image },
            LogEvent::ImagePullRetry {
                image,
                attempt: 2,
                error: "TLS handshake timeout",
            },
        ]
        .iter()
        .map(|event| {
            let mut log = event.to_log().unwrap();
            log.timestamp = None; // Stabilize the snapshot.
            serde_json::to_value(&log).unwrap()
        })
        .collect();

        insta::assert_json_snapshot!(logs, @r#"
        [
          {
            "fields": {
              "patches": [
                {
                  "cursor": "abc"
                },
                {
                  "cursor": "def"
                }
              ]
            },
            "level": "debug",
            "message": "persisted connector-state delta"
          },
          {
            "fields": {
              "actionDescription": "create table \"foo\""
            },
            "level": "info",
            "message": "connector applied"
          },
          {
            "fields": {
              "binding": 2,
              "collection_name": "acmeCo/collection",
              "schema": {
                "type": "object"
              }
            },
            "level": "info",
            "message": "inferred schema updated"
          },
          {
            "fields": {
              "collection_name": "acmeCo/collection",
              "schema": {
                "type": "object"
              }
            },
            "level": "info",
            "message": "inferred schema updated"
          },
          {
            "fields": {
              "container": {
                "ipAddr": "10.0.0.2"
              },
              "image": "ghcr.io/estuary/source-hello-world:dev"
            },
            "level": "info",
            "message": "started connector container"
          },
          {
            "fields": {
              "image": "ghcr.io/estuary/source-hello-world:dev"
            },
            "level": "debug",
            "message": "stopped connector container"
          },
          {
            "fields": {
              "attempt": 2,
              "error": "TLS handshake timeout",
              "image": "ghcr.io/estuary/source-hello-world:dev"
            },
            "level": "warn",
            "message": "transient error pulling image (will retry)"
          }
        ]
        "#);
    }
}
