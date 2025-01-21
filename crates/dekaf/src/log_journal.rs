use crate::{dekaf_shard_template_id, topology::fetch_dekaf_task_auth, App};
use async_trait::async_trait;
use bytes::Bytes;
use flow_client::fetch_task_authorization;
use futures::{Stream, StreamExt, TryStreamExt};
use gazette::{
    journal,
    uuid::{self, Producer},
};
use proto_gazette::message_flags;
use serde_json::json;
use std::{collections::VecDeque, sync::Arc};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{span, Event, Id};
use tracing_subscriber::layer::{Context, Layer};

/// When a span is created with this as its name, a `LogForwarder` is attached
/// to that span that will begin capturing all log messages emitted inside it
/// and all of its children. Log messages are buffered until a child span
/// with field SESSION_TASK_NAME_FIELD_MARKER is created, at which point all log
/// messages, both buffered and ongoing, will get written to the logs journal
/// associated with that task.
pub const SESSION_SPAN_NAME_MARKER: &str = "dekaf_session";
/// When a span is created with this field name, and is a descendent of a span
/// named with the value of `SESSION_SPAN_NAME_MARKER`, it causes events (logs)
/// anywhere in that hierarchy to get written to the corresponding logs journal.
pub const SESSION_TASK_NAME_FIELD_MARKER: &str = "session_task_name";
/// This marker indicates that its Session is not and will never be associated with a
/// task, and we should stop buffering logs as we'll never have anywhere to write them.
pub const SESSION_TASKLESS_FIELD_MARKER: &str = "session_is_taskless";

pub const SESSION_CLIENT_ID_FIELD_MARKER: &str = "session_client_id";
const WELL_KNOWN_LOG_FIELDS: &'static [&'static str] = &[SESSION_CLIENT_ID_FIELD_MARKER];

#[derive(Debug)]
enum LoggingMessage {
    SetTaskName(String),
    Log(ops::Log),
    Shutdown,
}

#[async_trait]
pub trait LogAppender: Send + Sync {
    async fn append_log_data(&self, log_data: Bytes) -> anyhow::Result<()>;

    async fn set_task_name(&mut self, name: String) -> anyhow::Result<()>;
}

#[derive(Clone)]
pub struct GazetteLogAppender {
    app: Arc<App>,
    client: Option<journal::Client>,
    journal_name: Option<String>,
}

#[async_trait]
impl LogAppender for GazetteLogAppender {
    async fn set_task_name(&mut self, task_name: String) -> anyhow::Result<()> {
        let (client, journal) = self.get_journal_client(task_name).await?;
        self.client.replace(client);
        self.journal_name.replace(journal);
        Ok(())
    }

    async fn append_log_data(&self, log_data: Bytes) -> anyhow::Result<()> {
        let resp = self
            .client
            .as_ref()
            .ok_or(anyhow::anyhow!("missing journal client"))?
            .append(
                gazette::broker::AppendRequest {
                    journal: self
                        .journal_name
                        .as_ref()
                        .ok_or(anyhow::anyhow!("missing journal name"))?
                        .to_owned(),
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
}

impl GazetteLogAppender {
    pub fn new(app: Arc<App>) -> Self {
        Self {
            app: app,
            client: None,
            journal_name: None,
        }
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

#[derive(Default, Clone)]
pub struct MockLogAppender {
    pub logs: Arc<tokio::sync::Mutex<VecDeque<Bytes>>>,
}

#[async_trait::async_trait]
impl LogAppender for MockLogAppender {
    async fn set_task_name(&mut self, _: String) -> anyhow::Result<()> {
        Ok(())
    }

    async fn append_log_data(&self, log_data: Bytes) -> anyhow::Result<()> {
        self.logs.lock().await.push_back(log_data);
        Ok(())
    }
}

struct LogForwarder<A: LogAppender> {
    producer: gazette::uuid::Producer,
    appender: A,
}

impl<A: LogAppender> LogForwarder<A> {
    fn new(producer: Producer, appender: A) -> Self {
        Self { producer, appender }
    }

    async fn forward_logs(
        mut self,
        mut logs_rx: tokio::sync::mpsc::Receiver<LoggingMessage>,
    ) -> anyhow::Result<()> {
        let mut pending_logs = VecDeque::new();

        let task_name = loop {
            match logs_rx.recv().await {
                Some(LoggingMessage::SetTaskName(name)) => {
                    self.appender.set_task_name(name.to_owned()).await?;
                    break name;
                }
                Some(LoggingMessage::Log(log)) => {
                    pending_logs.push_front(log);
                    // Keep at most the latest 100 log messages when in this pending state
                    pending_logs.truncate(100);
                }
                Some(LoggingMessage::Shutdown) | None => return Ok(()),
            }
        };

        let mut event_stream = futures::stream::iter(
            pending_logs
                .into_iter()
                // VecDeque::truncate keeps the first N items, so we use `push_front` + `truncate` to
                // store the most recent items in the front of the queue. We need to reverse
                // that when sending, as logs should be sent in oldest-first order.
                .rev()
                .map(|log| LoggingMessage::Log(log)),
        )
        .chain(ReceiverStream::new(logs_rx));

        while let Some(msg) = event_stream.next().await {
            match msg {
                LoggingMessage::SetTaskName(_) => {}
                LoggingMessage::Log(mut log) => {
                    // Attach the task name to every log from the session, even those that
                    // were emitted before it was known
                    log.fields_json_map.insert(
                        SESSION_TASK_NAME_FIELD_MARKER.to_string(),
                        json!(task_name).to_string(),
                    );

                    // Attach any other present well known fields to the top-level Log's fields
                    for well_known in WELL_KNOWN_LOG_FIELDS {
                        if let Some(value) = log
                            .spans
                            .iter()
                            .find_map(|l| l.fields_json_map.get(&well_known.to_string()))
                        {
                            log.fields_json_map
                                .insert(well_known.to_string(), value.to_string());
                        }
                    }

                    self.appender
                        .append_log_data(self.serialize_log(log).into())
                        .await?;
                }
                LoggingMessage::Shutdown => break,
            }
        }

        Ok(())
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
}

#[derive(Clone)]
struct SessionLogger<A: LogAppender + Send + Sync + 'static> {
    tx: tokio::sync::mpsc::Sender<LoggingMessage>,
    _handle: Arc<JoinHandle<()>>,
    _appender: std::marker::PhantomData<A>,
}

impl<A: LogAppender + Send + Sync + 'static> SessionLogger<A> {
    fn new(producer: Producer, appender: A) -> Self {
        // This should always be read promptly by the logic in `LogForwarder::forward_logs`,
        // so a larger buffer here would just obscure other problems.
        let (log_tx, log_rx) = tokio::sync::mpsc::channel::<LoggingMessage>(50);

        let forwarder = LogForwarder::new(producer, appender);
        let handle = tokio::spawn(async move {
            if let Err(e) = forwarder.forward_logs(log_rx).await {
                tracing::error!(error = ?e, "Log forwarding errored");
            }
        });

        Self {
            tx: log_tx,
            _handle: Arc::new(handle),
            _appender: std::marker::PhantomData,
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

pub struct SessionSubscriberLayer<A: LogAppender + Send + Sync + 'static> {
    producer: uuid::Producer,
    appender: A,
}

impl<A: LogAppender + Send + Sync + 'static> SessionSubscriberLayer<A> {
    pub fn new(producer: uuid::Producer, appender: A) -> Self {
        Self { producer, appender }
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

impl<S, A> Layer<S> for SessionSubscriberLayer<A>
where
    S: tracing::Subscriber,
    S: for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
    A: LogAppender + Send + Sync + Clone + 'static,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let session_logger = if let Some(scope) = ctx.event_scope(event) {
            scope
                .from_root()
                .find_map(|span_ref| span_ref.extensions().get::<SessionLogger<A>>().cloned())
        } else {
            None
        };

        if let Some(logger) = session_logger {
            let mut log = self.log_from_metadata(event.metadata());
            event.record(&mut ops::tracing::FieldVisitor(&mut log));

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
        if let Some(scope) = ctx.span_scope(span_id) {
            for span in scope.from_root() {
                if let Some(visitor) = span.extensions_mut().get_mut::<SessionLogger<A>>() {
                    values.record(visitor);
                    return;
                }
            }
        }
    }

    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        if let Some(scope) = ctx.span_scope(id) {
            for span in scope.from_root() {
                if let Some(visitor) = span.extensions_mut().get_mut::<SessionLogger<A>>() {
                    attrs.record(visitor);
                    return;
                }
            }
        }

        let span = ctx.span(id).unwrap();
        if span.name() == SESSION_SPAN_NAME_MARKER {
            let mut visitor = SessionLogger::new(self.producer, self.appender.clone());

            attrs.record(&mut visitor);

            span.extensions_mut().insert(visitor);
        }
    }
}

impl<A: LogAppender + Send + Sync + 'static> tracing::field::Visit for SessionLogger<A> {
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

    fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::Future;
    use insta::assert_json_snapshot;
    use itertools::Itertools;
    use rand::Rng;
    use std::time::Duration;
    use tracing::instrument::WithSubscriber;
    use tracing::{info, info_span};

    use tracing_record_hierarchical::SpanExt;
    use tracing_subscriber::prelude::*;

    fn gen_producer() -> Producer {
        // There's probably a neat bit-banging way to do this with i64 and masks, but I'm just not that fancy.
        let mut producer_id = rand::thread_rng().gen::<[u8; 6]>();
        producer_id[0] |= 0x01;
        gazette::uuid::Producer::from_bytes(producer_id)
    }

    async fn setup<F, Fut>(f: F)
    where
        F: FnOnce(Arc<tokio::sync::Mutex<VecDeque<Bytes>>>) -> Fut,
        Fut: Future,
    {
        let mock_appender = MockLogAppender::default();
        let logs = mock_appender.logs.clone();

        let producer = gen_producer();
        let layer = SessionSubscriberLayer::new(producer, mock_appender);

        let subscriber = tracing_subscriber::registry()
            .with(tracing_record_hierarchical::HierarchicalRecord::default())
            .with(ops::tracing::Layer::new(|_| {}, std::time::SystemTime::now))
            .with(layer)
            .with(tracing_subscriber::fmt::Layer::default());

        f(logs).with_subscriber(subscriber).await;
    }

    async fn assert_output(name: &str, logs: Arc<tokio::sync::Mutex<VecDeque<Bytes>>>) {
        let captured_log_bytes = logs
            .lock()
            .await
            .clone()
            .into_iter()
            .map(|b| Vec::from(b))
            .flatten()
            .collect::<Vec<u8>>();

        let full_str = String::from_utf8(captured_log_bytes.into()).unwrap();

        let captured_logs = full_str
            .split("\n")
            .filter(|l| l.len() > 0)
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect_vec();

        assert_json_snapshot!(name, captured_logs, {
            ".*._meta.uuid" => "[uuid]",
            ".*.spans.*.ts" => "[ts]",
            ".*.ts" => "[ts]"
        });
    }

    #[tokio::test]
    async fn test_session_subscriber_layer_with_no_session_logger() {
        setup(|logs| async move {
            {
                info!("Test log data, you shouldn't be able to see me");
            }

            tokio::time::sleep(Duration::from_millis(100)).await;

            let captured_logs = logs.lock().await;
            assert!(captured_logs.is_empty());
        })
        .await;
    }

    #[tokio::test]
    async fn test_session_subscriber_layer_with_session_logger_but_no_task_name() {
        setup(|logs| async move {
            {
                info!("Test log data, you shouldn't be able to see me");

                let session_span = info_span!(SESSION_SPAN_NAME_MARKER);
                let _guard = session_span.enter();

                info!("Test log data but with a SessionLogger");
            };

            tokio::time::sleep(Duration::from_millis(100)).await;

            let captured_logs = logs.lock().await;
            assert!(captured_logs.is_empty());
        })
        .await;
    }

    #[tokio::test]
    async fn test_session_subscriber_layer_with_session_logger_and_task_name() {
        setup(|logs| async move {
            {
                info!(
                "Test log data, not associated with any session, you should not be able to see me"
            );

                let session_span = info_span!(SESSION_SPAN_NAME_MARKER);
                let _guard = session_span.enter();

                info!("Test log data but with a SessionLogger, still should see me");

                let session_span = info_span!("", { SESSION_TASK_NAME_FIELD_MARKER } = "my_task",);
                let _guard = session_span.enter();

                info!("Test log data with a task name!");
            };

            tokio::time::sleep(Duration::from_millis(100)).await;

            assert_output("session_logger_and_task_name", logs).await;
        })
        .await;
    }

    #[tokio::test]
    async fn test_session_subscriber_layer_with_session_logger_and_task_name_hierarchical() {
        setup(|logs| async move {
            {
                info!(
                "Test log data, not associated with any session, you should not be able to see me"
            );

                let session_span = info_span!(
                    SESSION_SPAN_NAME_MARKER,
                    { SESSION_TASK_NAME_FIELD_MARKER } = tracing::field::Empty,
                    { SESSION_CLIENT_ID_FIELD_MARKER } = tracing::field::Empty
                );
                let _guard = session_span.enter();

                info!("Test log data but with a SessionLogger, still should see me");

                let session_span = info_span!("child_span");
                let session_guard = session_span.enter();

                info!("Test log data without a task name yet!");

                tracing::Span::current()
                    .record_hierarchical(SESSION_TASK_NAME_FIELD_MARKER, "my-task");
                tracing::Span::current()
                    .record_hierarchical(SESSION_CLIENT_ID_FIELD_MARKER, "my-client-id");

                info!("I should have a client ID");
                drop(session_guard);
                info!("I should also have a client ID");
            };

            tokio::time::sleep(Duration::from_millis(100)).await;

            assert_output("session_logger_and_task_name_hierarchical", logs).await;
        })
        .await;
    }

    #[tokio::test]
    async fn test_session_subscriber_layer_taskless() {
        setup(|logs| async move {
            {
                info!("Before session span");

                let session_span = info_span!(SESSION_SPAN_NAME_MARKER);
                let _guard = session_span.enter();

                info!("Before taskless marker");

                let taskless_span =
                    info_span!("taskless", { SESSION_TASKLESS_FIELD_MARKER } = true);
                let _taskless_guard = taskless_span.enter();

                info!("After taskless marker");
            }

            tokio::time::sleep(Duration::from_millis(100)).await;

            let captured_logs = logs.lock().await;
            assert!(
                captured_logs.is_empty(),
                "Expected no logs for taskless session"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn test_session_subscriber_layer_nested_spans() {
        setup(|logs| async move {
            {
                info!("From before session, should not be visible");

                let session_span = info_span!(SESSION_SPAN_NAME_MARKER);
                let _guard = session_span.enter();

                info!("From inside session but before task_name, should be visible");

                let nested_span = info_span!("nested");
                let nested_guard = nested_span.enter();

                info!("From inside nested span but before task_name, should be visible");

                let task_span =
                    info_span!("task", { SESSION_TASK_NAME_FIELD_MARKER } = "test_task");
                let task_guard = task_span.enter();

                info!("Log from nested span after task name marker");

                drop(task_guard);
                drop(nested_guard);

                info!("Back in session span after task name");

                let new_span = info_span!("new_nested");
                let _new_guard = new_span.enter();

                info!("In child of session span after task name");
            }

            tokio::time::sleep(Duration::from_millis(100)).await;

            assert_output("nested_spans", logs).await;
        })
        .await;
    }
}
