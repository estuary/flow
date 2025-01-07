use crate::{dekaf_shard_template_id, topology::fetch_dekaf_task_auth, App};
use bytes::Bytes;
use flow_client::fetch_task_authorization;
use futures::TryStreamExt;
use gazette::{
    journal,
    uuid::{self, Producer},
};
use proto_gazette::message_flags;
use serde_json::json;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{span, Event, Id};
use tracing_subscriber::layer::{Context, Layer};

// When a span is created with this as its name, a `LogForwarder` is attached
// to that span that will begin capturing all log messages emitted inside it
// and all of its children. Log messages are buffered until a child span
// with field SESSION_TASK_NAME_FIELD_MARKER is created, at which point all log
// messages, both buffered and ongoing, will get written to the logs journal
// associated with that task.
pub const SESSION_SPAN_NAME_MARKER: &str = "dekaf_session";
// When a span is created with this field name, and is a descendent of a span
// named with the value of `SESSION_SPAN_NAME_MARKER`, it causes events (logs)
// anywhere in that hierarchy to get written to the corresponding logs journal.
pub const SESSION_TASK_NAME_FIELD_MARKER: &str = "session_task_name";
// This marker indicates that its Session is not and will never be associated with a
// task, and we should stop buffering logs as we'll never have anywhere to write them.
pub const SESSION_TASKLESS_FIELD_MARKER: &str = "session_is_taskless";

#[derive(Debug)]
enum LoggingMessage {
    SetTaskName(String),
    Log(ops::Log),
    Shutdown,
}

struct LogForwarder {
    app: Arc<App>,
    producer: gazette::uuid::Producer,
}

impl LogForwarder {
    fn new(app: Arc<App>, producer: Producer) -> Self {
        Self { app, producer }
    }

    async fn forward_logs(
        self,
        mut logs_rx: tokio::sync::mpsc::Receiver<LoggingMessage>,
    ) -> anyhow::Result<()> {
        let mut log_data = Vec::new();

        let (ops_logs_journal_client, ops_logs_journal) = loop {
            match logs_rx.recv().await {
                Some(LoggingMessage::SetTaskName(name)) => {
                    let (client, ops_logs) = self.get_journal_client(name).await?;
                    break (client, ops_logs);
                }
                Some(LoggingMessage::Log(log)) => {
                    log_data.append(&mut self.serialize_log(log));
                }
                Some(LoggingMessage::Shutdown) | None => return Ok(()),
            }
        };

        self.append_log_data(
            log_data.into(),
            ops_logs_journal.as_str(),
            &ops_logs_journal_client,
        )
        .await?;

        while let Some(msg) = logs_rx.recv().await {
            match msg {
                LoggingMessage::SetTaskName(_) => {}
                LoggingMessage::Log(log) => {
                    self.append_log_data(
                        self.serialize_log(log).into(),
                        ops_logs_journal.as_str(),
                        &ops_logs_journal_client,
                    )
                    .await?;
                }
                LoggingMessage::Shutdown => break,
            }
        }

        Ok(())
    }

    async fn append_log_data(
        &self,
        log_data: Bytes,
        journal: &str,
        client: &journal::Client,
    ) -> anyhow::Result<()> {
        let resp = client.append(
            gazette::broker::AppendRequest {
                journal: journal.to_owned(),
                ..Default::default()
            },
            || {
                futures::stream::once({
                    let value = log_data.clone();
                    async move { Ok(value) }
                })
            },
        );

        tokio::pin!(resp);

        let mut attempts = 0;

        loop {
            match resp.try_next().await {
                Err(err) if err.is_transient() && attempts < 3 => {
                    attempts += 1;
                }
                Err(err) => {
                    if err.is_transient() {
                        tracing::warn!(
                            ?err,
                            attempts,
                            "Got recoverable error multiple times while trying to write logs"
                        )
                    }
                    return Err(err.into());
                }
                Ok(_) => return Ok(()),
            }
        }
    }

    fn serialize_log(&self, log: ops::Log) -> Vec<u8> {
        let uuid = gazette::uuid::build(
            self.producer,
            gazette::uuid::Clock::from_time(std::time::SystemTime::now()),
            uuid::Flags(
                (message_flags::MASK | message_flags::OUTSIDE_TXN)
                    .try_into()
                    .unwrap(),
            ),
        );

        let mut val = serde_json::to_value(log).expect("Log always serializes");

        if let Some(obj) = val.as_object_mut() {
            obj.insert("_meta".to_string(), json!({ "uuid": uuid }));
        }

        let mut buf = serde_json::to_vec(&val).expect("Value always serializes");
        buf.push(b'\n');

        buf
    }

    async fn get_journal_client(
        &self,
        task_name: String,
    ) -> anyhow::Result<(journal::Client, String)> {
        let (client, _claims, ops_logs, _ops_stats, _task_spec) = fetch_dekaf_task_auth(
            self.app.client_base.clone(),
            &task_name,
            &self.app.data_plane_fqdn,
            &self.app.data_plane_signer,
        )
        .await?;

        let client = fetch_task_authorization(
            &client,
            &dekaf_shard_template_id(task_name.as_str()),
            &self.app.data_plane_fqdn,
            &self.app.data_plane_signer,
            proto_flow::capability::AUTHORIZE | proto_gazette::capability::APPEND,
            gazette::broker::LabelSelector {
                include: Some(labels::build_set([("name", ops_logs.as_str())])),
                exclude: None,
            },
        )
        .await?;

        Ok((client, ops_logs))
    }
}

#[derive(Clone)]
struct SessionLogger {
    tx: tokio::sync::mpsc::Sender<LoggingMessage>,
    _handle: Arc<JoinHandle<()>>,
}

impl SessionLogger {
    fn new(app: Arc<App>, producer: Producer) -> Self {
        let (log_tx, log_rx) = tokio::sync::mpsc::channel(1000);

        let forwarder = LogForwarder::new(app, producer);
        let handle = tokio::spawn(async move {
            if let Err(e) = forwarder.forward_logs(log_rx).await {
                tracing::error!(error = ?e, "Log forwarding errored");
            }
        });

        Self {
            tx: log_tx,
            _handle: Arc::new(handle),
        }
    }

    fn set_task_name(&self, name: String) {
        if !self.tx.is_closed() {
            self.tx.try_send(LoggingMessage::SetTaskName(name)).unwrap();
        }
    }

    fn send_log_message(&self, log: ops::Log) {
        if !self.tx.is_closed() {
            self.tx.try_send(LoggingMessage::Log(log)).unwrap();
        }
    }

    fn shutdown(&self) {
        if !self.tx.is_closed() {
            self.tx.try_send(LoggingMessage::Shutdown).unwrap();
        }
    }
}

pub struct SessionSubscriberLayer {
    app: Arc<App>,
    producer: uuid::Producer,
}

impl SessionSubscriberLayer {
    pub fn new(app: Arc<App>, producer: uuid::Producer) -> Self {
        Self { app, producer }
    }

    pub fn log_from_metadata(&self, metadata: &tracing::Metadata) -> ops::Log {
        let mut log = ops::Log {
            meta: None,
            timestamp: Some(proto_flow::as_timestamp(std::time::SystemTime::now())),
            level: ops::tracing::level_from_tracing(metadata.level()) as i32,
            message: String::new(),
            fields_json_map: Default::default(),
            shard: None,
            spans: Default::default(),
        };

        log.fields_json_map
            .insert("module".to_string(), json!(metadata.target()).to_string());

        log
    }
}

impl<S> Layer<S> for SessionSubscriberLayer
where
    S: tracing::Subscriber,
    S: for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        // First identify if this message is inside a Session span or sub-span
        let session_logger = if let Some(scope) = ctx.event_scope(event) {
            scope
                .from_root()
                .find_map(|span_ref| span_ref.extensions().get::<SessionLogger>().cloned())
        } else {
            None
        };

        if let Some(logger) = session_logger {
            // We're inside a Session span and we have the marker,
            // so let's build the Log and send the message.
            let mut log = self.log_from_metadata(event.metadata());
            event.record(&mut ops::tracing::FieldVisitor(&mut log));

            // Attach context from parent spans, if any.
            if let Some(scope) = ctx.event_scope(event) {
                for span in scope.from_root() {
                    let extensions = span.extensions();
                    let span = extensions.get::<ops::Log>().unwrap();
                    log.spans.push(span.clone());
                }
            }
            logger.send_log_message(log);
        }
    }

    fn on_record(&self, span_id: &span::Id, values: &span::Record<'_>, ctx: Context<'_, S>) {
        // When we get some new fields for a span, walk through its hierarchy starting from the root
        // and if we find a SessionLogger, Visit it with the new values. This is so that we can
        // record a value of `session_task_name` from within a span or sub-span, and it'll propagate
        // to the outer span for the whole session.
        if let Some(scope) = ctx.span_scope(span_id) {
            for span in scope.from_root() {
                if let Some(visitor) = span.extensions_mut().get_mut::<SessionLogger>() {
                    values.record(visitor);
                    return;
                }
            }
        }
    }

    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        // Check if any parent spans already have a SessionLogger
        if let Some(scope) = ctx.span_scope(id) {
            for span in scope.from_root() {
                if let Some(visitor) = span.extensions_mut().get_mut::<SessionLogger>() {
                    attrs.record(visitor);
                    return;
                }
            }
        }

        // No existing spans had a Session, let's check and see if this one should
        let span = ctx.span(id).unwrap();
        if span.name() == SESSION_SPAN_NAME_MARKER {
            let mut visitor = SessionLogger::new(self.app.clone(), self.producer);

            attrs.record(&mut visitor);

            span.extensions_mut().insert(visitor);
        }
    }
}

impl tracing::field::Visit for SessionLogger {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == SESSION_TASK_NAME_FIELD_MARKER && value.len() > 0 {
            self.set_task_name(value.to_string())
        }
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        if field.name() == SESSION_TASKLESS_FIELD_MARKER && value {
            self.shutdown();
        }
    }

    fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {
        // Do nothing
    }
}
