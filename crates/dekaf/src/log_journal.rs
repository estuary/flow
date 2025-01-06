use crate::{topology::fetch_dekaf_task_auth, App};
use flow_client::fetch_task_authorization;
use gazette::journal;
use serde_json::json;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{span, Event, Id};
use tracing_subscriber::layer::{Context, Layer};

#[derive(Debug)]
enum LoggingMessage {
    SetTaskName(String),
    Log(ops::Log),
    Shutdown,
}

#[derive(Clone)]
struct SessionLogger {
    /// Buffer log messages before we know what our task name is
    tx: Arc<tokio::sync::mpsc::Sender<LoggingMessage>>,
    _handle: Arc<JoinHandle<anyhow::Result<()>>>,
}

impl SessionLogger {
    fn new(app: Arc<App>) -> Self {
        let (log_tx, mut log_rx) = tokio::sync::mpsc::channel(10000);

        let handle = tokio::spawn(async move {
            let mut ops_logs_journal_client = None;
            let mut ops_logs_journal = None;
            let mut logs_buffer = VecDeque::new();

            while let Some(msg) = log_rx.recv().await {
                match msg {
                    LoggingMessage::SetTaskName(name) => {
                        let (client, _claims, ops_logs, _ops_stats, _task_spec) =
                            fetch_dekaf_task_auth(
                                app.client_base.clone(),
                                &name,
                                &app.data_plane_fqdn,
                                &app.data_plane_signer,
                            )
                            .await?;

                        let client = fetch_task_authorization(
                            &client,
                            &ops_logs,
                            &app.data_plane_fqdn,
                            &app.data_plane_signer,
                            proto_flow::capability::AUTHORIZE | proto_gazette::capability::APPEND,
                            gazette::broker::LabelSelector {
                                include: Some(labels::build_set([("name", ops_logs.as_str())])),
                                exclude: None,
                            },
                        )
                        .await?;

                        // Flush buffered logs now that we have a client
                        while let Some(log) = logs_buffer.pop_front() {
                            send_to_journal(log, ops_logs.as_str(), &client).await?;
                        }

                        ops_logs_journal = Some(ops_logs.to_string());
                        ops_logs_journal_client = Some(client);
                    }
                    LoggingMessage::Log(log) => {
                        if let (Some(client), Some(journal)) =
                            (&ops_logs_journal_client, &ops_logs_journal)
                        {
                            send_to_journal(log, journal.as_str(), client).await?;
                        } else {
                            logs_buffer.push_back(log);
                        }
                    }
                    LoggingMessage::Shutdown => {
                        break;
                    }
                }
            }
            Ok(())
        });

        Self {
            tx: Arc::new(log_tx),
            _handle: Arc::new(handle),
        }
    }

    fn set_task_name(&self, name: String) {
        self.tx.try_send(LoggingMessage::SetTaskName(name)).unwrap();
    }
}

async fn send_to_journal(
    message: ops::Log,
    journal: &str,
    client: &journal::Client,
) -> anyhow::Result<()> {
    let mut buf = serde_json::to_vec(&message).expect("Log always serializes");
    buf.push(b'\n');

    let req = proto_gazette::broker::AppendRequest {
        journal: journal.to_string(),
        content: buf,
        ..Default::default()
    };

    client.append(futures::stream::once(async { req })).await?;

    Ok(())
}

pub struct SessionSubscriberLayer {
    app: Arc<App>,
}

impl SessionSubscriberLayer {
    pub fn new(app: Arc<App>) -> Self {
        Self { app }
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
            scope.from_root().find_map(|span_ref| {
                span_ref
                    .extensions()
                    .get::<SessionSpanMarker>()
                    .and_then(|marker| marker.logger.clone())
            })
        } else {
            None
        };

        if let Some(logger) = session_logger {
            // We're inside a Session span and we have the logger,
            // so let's build the Log and send the message.
            let mut log = self.log_from_metadata(event.metadata());
            event.record(&mut ops::tracing::FieldVisitor(&mut log));

            // Collect additional metadata in the form of `ops::Log`s attached
            // to other spans in this hierarchy. These get injected by [`ops::tracing::Layer`]
            if let Some(scope) = ctx.event_scope(event) {
                for span in scope.from_root() {
                    let extensions = span.extensions();
                    let span = extensions.get::<ops::Log>().unwrap();
                    log.spans.push(span.clone());
                }
            }
            logger.tx.try_send(LoggingMessage::Log(log)).unwrap();
        }
    }

    fn on_record(&self, span_id: &span::Id, values: &span::Record<'_>, ctx: Context<'_, S>) {
        // When we get some new fields for a span, walk through its hierarchy starting from the root
        // and if we find a SessionLogger, Visit it with the new values. This is ultimately how we handle
        // SESSION_TASK_NAME_FIELD_MARKER and SESSION_TASKLESS_FIELD_MARKER so that even deep within a
        // sub-span, they'll still propagate to the outer span for the whole session.
        if let Some(scope) = ctx.span_scope(span_id) {
            for span in scope.from_root() {
                if let Some(visitor) = span.extensions_mut().get_mut::<SessionSpanMarker>() {
                    values.record(visitor);
                    return;
                }
            }
        }
    }

    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        // Check if any parent spans already have a SessionSpanMarker
        if let Some(scope) = ctx.span_scope(id) {
            for span in scope.from_root() {
                if let Some(visitor) = span.extensions_mut().get_mut::<SessionSpanMarker>() {
                    attrs.record(visitor);
                    return;
                }
            }
        }

        // No existing spans had a Session, let's check and see if this one should
        let span = ctx.span(id).unwrap();
        let mut visitor = SessionSpanMarker {
            logger: None,
            app: self.app.clone(),
        };

        attrs.record(&mut visitor);

        // If logger is Some, that means we found the is_session marker. Let's set this
        // SessionSpanMarker into the extensions of the current span
        if let Some(_) = visitor.logger {
            span.extensions_mut().insert(visitor);
        }
    }

    fn on_close(&self, id: span::Id, ctx: Context<'_, S>) {
        // When we close out a span containing a SessionSpanMarker, that indicates that
        // the session was closed, and we should shut down the log writer.
        let span = ctx.span(&id).unwrap();
        match span.extensions_mut().get_mut::<SessionSpanMarker>() {
            Some(SessionSpanMarker {
                logger: Some(ref logger),
                ..
            }) => {
                logger.tx.try_send(LoggingMessage::Shutdown).unwrap();
            }
            _ => {}
        };
    }
}

#[derive(Clone)]
struct SessionSpanMarker {
    logger: Option<SessionLogger>,
    app: Arc<App>,
}

impl tracing::field::Visit for SessionSpanMarker {
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        if field.name() == "is_session" && self.logger.is_none() && value {
            self.logger = Some(SessionLogger::new(self.app.clone()))
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        let name = field.name();
        if name == "session_task_name" {
            if let Some(ref mut logger) = self.logger {
                logger.set_task_name(value.to_string())
            } else {
                let logger = SessionLogger::new(self.app.clone());
                logger.set_task_name(value.to_string());
                self.logger = Some(logger)
            }
        }
    }

    fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {
        // Do nothing
    }
}
