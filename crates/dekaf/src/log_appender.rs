use crate::{dekaf_shard_template_id, topology::fetch_dekaf_task_auth, App};
use async_trait::async_trait;
use bytes::Bytes;
use flow_client::fetch_task_authorization;
use futures::{StreamExt, TryStreamExt};
use gazette::{
    journal,
    uuid::{self, Producer},
    RetryError,
};
use proto_gazette::message_flags;
use rand::Rng;
use std::{collections::VecDeque, marker::PhantomData, sync::Arc, time::Duration};
use tokio_stream::wrappers::ReceiverStream;

#[derive(Debug)]
enum LoggingMessage {
    SetTaskName(String),
    Log(ops::Log),
    Shutdown,
}

// This abstraction exists mostly in order to make testing easier.
#[async_trait]
pub trait LogWriter: Send + Sync {
    async fn append_log_data(&self, log_data: Bytes) -> anyhow::Result<()>;

    async fn set_task_name(&mut self, name: String) -> anyhow::Result<()>;
}

#[derive(Clone)]
pub struct GazetteLogWriter {
    app: Arc<App>,
    client: Option<journal::Client>,
    journal_name: Option<String>,
}

#[async_trait]
impl LogWriter for GazetteLogWriter {
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

        loop {
            match resp.try_next().await {
                Err(RetryError { attempt, inner }) if inner.is_transient() && attempt < 3 => {
                    let wait_ms = rand::thread_rng().gen_range(400..5_000);

                    tokio::time::sleep(Duration::from_millis(wait_ms)).await;
                    continue;
                }
                Err(err) => {
                    tracing::warn!(
                        ?err,
                        "Got recoverable error multiple times while trying to write logs"
                    );
                    return Err(err.inner.into());
                }
                Ok(_) => return Ok(()),
            }
        }
    }
}

impl GazetteLogWriter {
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

#[derive(Clone)]
pub struct LogForwarder<W: LogWriter> {
    logs_tx: tokio::sync::mpsc::Sender<LoggingMessage>,
    _handle: Arc<tokio::task::JoinHandle<()>>,
    _ph: PhantomData<W>,
}

// These well-known tracing field names are used to annotate all log messages within a particular session.
// This is done by using `tracing_record_hierarchical` to update the field value wherever it's defined in the span hierarchy:
//
// tracing::Span::current().record_hierarchical(SESSION_CLIENT_ID_FIELD_MARKER, ...client_id...);
pub const SESSION_TASK_NAME_FIELD_MARKER: &str = "task_name";
pub const SESSION_CLIENT_ID_FIELD_MARKER: &str = "session_client_id";
const WELL_KNOWN_LOG_FIELDS: &'static [&'static str] = &[
    SESSION_TASK_NAME_FIELD_MARKER,
    SESSION_CLIENT_ID_FIELD_MARKER,
];

impl<W: LogWriter + 'static> LogForwarder<W> {
    pub fn new(producer: Producer, writer: W) -> Self {
        let (logs_tx, logs_rx) = tokio::sync::mpsc::channel::<LoggingMessage>(50);

        let handle = tokio::spawn(async move {
            if let Err(e) = Self::forward_logs(logs_rx, writer, producer).await {
                tracing::error!(error = ?e, "Log forwarding errored");
            }
        });

        Self {
            logs_tx,
            _handle: Arc::new(handle),
            _ph: Default::default(),
        }
    }

    async fn forward_logs(
        mut logs_rx: tokio::sync::mpsc::Receiver<LoggingMessage>,
        mut writer: W,
        uuid_producer: Producer,
    ) -> anyhow::Result<()> {
        let mut pending_logs = VecDeque::new();

        loop {
            match logs_rx.recv().await {
                Some(LoggingMessage::SetTaskName(name)) => {
                    writer.set_task_name(name.to_owned()).await?;
                    break;
                }
                Some(LoggingMessage::Log(log)) => {
                    pending_logs.push_front(log);
                    // Keep at most the latest 100 log messages when in this pending state
                    pending_logs.truncate(100);
                }
                Some(LoggingMessage::Shutdown) | None => return Ok(()),
            }
        }

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
                    // Attach any present well known fields to the top-level Log's fields
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

                    writer
                        .append_log_data(Self::serialize_log(uuid_producer, log).into())
                        .await?;
                }
                LoggingMessage::Shutdown => break,
            }
        }

        Ok(())
    }

    fn serialize_log(producer: Producer, mut log: ops::Log) -> Vec<u8> {
        let uuid = gazette::uuid::build(
            producer,
            gazette::uuid::Clock::from_time(std::time::SystemTime::now()),
            uuid::Flags((message_flags::OUTSIDE_TXN).try_into().unwrap()),
        );
        log.meta = Some(ops::Meta {
            uuid: uuid.to_string(),
        });

        let mut buf = serde_json::to_vec(&log).expect("Value always serializes");
        buf.push(b'\n');

        buf
    }

    pub fn set_task_name(&self, name: String) {
        use tracing_record_hierarchical::SpanExt;

        if !self.logs_tx.is_closed() {
            self.logs_tx
                .try_send(LoggingMessage::SetTaskName(name.to_owned()))
                .unwrap();
        }
        // Also set the task name on the parent span so it's included in the logs. This also adds it
        // to the logs that Dekaf writes to stdout, which makes debugging issues much easier.
        tracing::Span::current().record_hierarchical(SESSION_TASK_NAME_FIELD_MARKER, name);
    }

    pub fn send_log_message(&self, log: ops::Log) {
        if !self.logs_tx.is_closed() {
            self.logs_tx.try_send(LoggingMessage::Log(log)).unwrap();
        }
    }

    pub fn shutdown(&self) {
        if !self.logs_tx.is_closed() {
            self.logs_tx.try_send(LoggingMessage::Shutdown).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::Future;
    use insta::assert_json_snapshot;
    use itertools::Itertools;
    use rand::Rng;
    use std::time::Duration;
    use tracing::{info, info_span};
    use tracing::{instrument::WithSubscriber, Instrument};

    use tracing_record_hierarchical::SpanExt;
    use tracing_subscriber::prelude::*;

    #[derive(Default, Clone)]
    struct MockLogWriter {
        pub logs: Arc<tokio::sync::Mutex<VecDeque<Bytes>>>,
    }

    #[async_trait::async_trait]
    impl LogWriter for MockLogWriter {
        async fn set_task_name(&mut self, _: String) -> anyhow::Result<()> {
            Ok(())
        }

        async fn append_log_data(&self, log_data: Bytes) -> anyhow::Result<()> {
            self.logs.lock().await.push_back(log_data);
            Ok(())
        }
    }

    tokio::task_local! {
        static MOCK_LOG_FORWARDER: LogForwarder<MockLogWriter>;
    }

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
        let mock_writer = MockLogWriter::default();
        let logs = mock_writer.logs.clone();

        let producer = gen_producer();

        let subscriber = tracing_subscriber::registry()
            .with(tracing_record_hierarchical::HierarchicalRecord::default())
            .with(ops::tracing::Layer::new(
                |log| MOCK_LOG_FORWARDER.get().send_log_message(log.clone()),
                std::time::SystemTime::now,
            ))
            .with(tracing_subscriber::fmt::Layer::default());

        MOCK_LOG_FORWARDER
            .scope(
                LogForwarder::new(producer, mock_writer),
                async move {
                    f(logs)
                        .instrument(tracing::info_span!(
                            "test_session",
                            { SESSION_TASK_NAME_FIELD_MARKER } = tracing::field::Empty,
                        ))
                        .await
                }
                .with_subscriber(subscriber),
            )
            .await;
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
    async fn test_logging_with_no_task_name() {
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
    async fn test_logging_with_task_name() {
        setup(|logs| async move {
            {
                info!("Test log data before setting name, you should see me");

                MOCK_LOG_FORWARDER
                    .get()
                    .set_task_name("my_task".to_string());

                info!("Test log data with a task name!");
            };

            tokio::time::sleep(Duration::from_millis(100)).await;

            assert_output("session_logger_and_task_name", logs).await;
        })
        .await;
    }

    #[tokio::test]
    async fn test_logging_with_client_id_hierarchical() {
        setup(|logs| async move {
            {
                info!("Test log data before setting name, you should see me");
                let session_span = info_span!(
                    "session_span",
                    { SESSION_CLIENT_ID_FIELD_MARKER } = tracing::field::Empty
                );
                let session_guard = session_span.enter();

                info!("Test log data without a task name yet!");

                MOCK_LOG_FORWARDER
                    .get()
                    .set_task_name("my_task".to_string());

                let child_span = info_span!("child_span");
                let child_guard = child_span.enter();

                tracing::Span::current()
                    .record_hierarchical(SESSION_CLIENT_ID_FIELD_MARKER, "my-client-id");

                info!("I should have a client ID");
                drop(child_guard);
                info!("I should also have a client ID");
                drop(session_guard)
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
                info!("Logged without name, you shouldn't see me because of the shutdown");

                MOCK_LOG_FORWARDER.get().shutdown();

                info!("After shutdown, still shouldn't see me");
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
                info!("From before task name, should be visible");

                let nested_span = info_span!("nested");
                let nested_guard = nested_span.enter();

                info!("From inside nested span but before task_name, should be visible");

                MOCK_LOG_FORWARDER
                    .get()
                    .set_task_name("my_task".to_string());

                info!("Log from nested span after task name marker");

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
