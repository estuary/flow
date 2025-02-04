use crate::{dekaf_shard_template_id, topology::fetch_dekaf_task_auth, App};
use anyhow::Context;
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
use std::{
    collections::{BTreeMap, VecDeque},
    marker::PhantomData,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::mpsc::error::TrySendError;
use tokio_stream::wrappers::ReceiverStream;

#[derive(Debug)]
enum TaskWriterMessage {
    SetTaskName(String),
    Log(ops::Log),
    Stats((String, ops::stats::Binding)),
    Shutdown,
}

/// StatsAggregator aggregates statistics for a particular Session over time. Since a Session maps 1:1 to
/// a task, it's possible to read from any number of bindings within a single session. As a result, we store
/// each binding's stats as an entry in this map, where the key is the name of the binding's target collection.
#[derive(Default)]
pub struct StatsAggregator(BTreeMap<String, ops::stats::Binding>);

impl StatsAggregator {
    /// Add new statistics to the aggregator
    pub fn add(&mut self, collection_name: String, stats: ops::stats::Binding) {
        let binding = self.0.entry(collection_name).or_insert(Default::default());
        if let Some(left) = &stats.left {
            ops::merge_docs_and_bytes(left, &mut binding.left);
        }
        if let Some(right) = &stats.right {
            ops::merge_docs_and_bytes(right, &mut binding.right);
        }
        if let Some(out) = &stats.out {
            ops::merge_docs_and_bytes(out, &mut binding.out);
        }
    }

    // If any stats have been written, return them and reset the counter. Otherwise None
    pub fn take(&mut self) -> Option<BTreeMap<String, ops::stats::Binding>> {
        if self.0.iter().any(|(_, v)| {
            v.left
                .is_some_and(|s| s.bytes_total > 0 || s.docs_total > 0)
                || v.right
                    .is_some_and(|s| s.bytes_total > 0 || s.docs_total > 0)
                || v.out.is_some_and(|s| s.bytes_total > 0 || s.docs_total > 0)
        }) {
            Some(std::mem::take(&mut self.0))
        } else {
            None
        }
    }
}

// This abstraction exists mostly in order to make testing easier.
#[async_trait]
pub trait TaskWriter: Send + Sync {
    async fn append_logs(&self, log_data: Bytes) -> anyhow::Result<()>;
    async fn append_stats(&self, log_data: Bytes) -> anyhow::Result<()>;

    async fn set_task_name(&mut self, name: String) -> anyhow::Result<()>;
}

#[derive(Clone)]
pub struct GazetteWriter {
    app: Arc<App>,
    logs_client: Option<journal::Client>,
    stats_client: Option<journal::Client>,
    logs_journal_name: Option<String>,
    stats_journal_name: Option<String>,
}

#[async_trait]
impl TaskWriter for GazetteWriter {
    async fn set_task_name(&mut self, task_name: String) -> anyhow::Result<()> {
        let (logs_client, stats_client, logs_journal, stats_journal) =
            self.get_journal_client(task_name).await?;
        self.logs_client.replace(logs_client);
        self.stats_client.replace(stats_client);
        self.logs_journal_name.replace(logs_journal);
        self.stats_journal_name.replace(stats_journal);
        Ok(())
    }

    async fn append_logs(&self, data: Bytes) -> anyhow::Result<()> {
        Self::append(
            self.logs_client.as_ref().context("not initialized")?,
            data,
            self.logs_journal_name
                .as_ref()
                .context("Writer is not initialized")?
                .clone(),
        )
        .await
    }

    async fn append_stats(&self, data: Bytes) -> anyhow::Result<()> {
        Self::append(
            self.stats_client.as_ref().context("not initialized")?,
            data,
            self.stats_journal_name
                .as_ref()
                .context("Writer is not initialized")?
                .clone(),
        )
        .await
    }
}

impl GazetteWriter {
    pub fn new(app: Arc<App>) -> Self {
        Self {
            app: app,
            logs_client: None,
            stats_client: None,
            logs_journal_name: None,
            stats_journal_name: None,
        }
    }

    async fn get_journal_client(
        &self,
        task_name: String,
    ) -> anyhow::Result<(journal::Client, journal::Client, String, String)> {
        let (client, _claims, ops_logs, ops_stats, _task_spec) = fetch_dekaf_task_auth(
            self.app.client_base.clone(),
            &task_name,
            &self.app.data_plane_fqdn,
            &self.app.data_plane_signer,
        )
        .await?;

        let template_id = dekaf_shard_template_id(task_name.as_str());

        let (logs_client, stats_client) = tokio::try_join!(
            fetch_task_authorization(
                &client,
                &template_id,
                &self.app.data_plane_fqdn,
                &self.app.data_plane_signer,
                proto_flow::capability::AUTHORIZE | proto_gazette::capability::APPEND,
                gazette::broker::LabelSelector {
                    include: Some(labels::build_set([("name", ops_logs.as_str()),])),
                    exclude: None,
                },
            ),
            fetch_task_authorization(
                &client,
                &template_id,
                &self.app.data_plane_fqdn,
                &self.app.data_plane_signer,
                proto_flow::capability::AUTHORIZE | proto_gazette::capability::APPEND,
                gazette::broker::LabelSelector {
                    include: Some(labels::build_set([("name", ops_stats.as_str()),])),
                    exclude: None,
                },
            )
        )?;

        Ok((logs_client, stats_client, ops_logs, ops_stats))
    }

    async fn append(client: &journal::Client, data: Bytes, journal: String) -> anyhow::Result<()> {
        let resp = client.append(
            gazette::broker::AppendRequest {
                journal,
                ..Default::default()
            },
            || {
                futures::stream::once({
                    let value = data.clone();
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

#[derive(Clone)]
pub struct TaskForwarder<W: TaskWriter> {
    tx: tokio::sync::mpsc::Sender<TaskWriterMessage>,
    _handle: Arc<tokio::task::JoinHandle<()>>,
    _ph: PhantomData<W>,
}

// These well-known tracing field names are used to annotate all log messages within a particular session.
// This is done by using `tracing_record_hierarchical` to update the field value wherever it's defined in the span hierarchy:
//
// tracing::Span::current().record_hierarchical(SESSION_CLIENT_ID_FIELD_MARKER, ...client_id...);
pub const SESSION_TASK_NAME_FIELD_MARKER: &str = "task_name";
pub const SESSION_CLIENT_ID_FIELD_MARKER: &str = "session_client_id";
pub const EXCLUDE_FROM_TASK_LOGGING: &str = "exclude_from_task_logging";
const WELL_KNOWN_LOG_FIELDS: &'static [&'static str] = &[
    SESSION_TASK_NAME_FIELD_MARKER,
    SESSION_CLIENT_ID_FIELD_MARKER,
];
pub const LOG_MESSAGE_QUEUE_SIZE: usize = 50;

impl<W: TaskWriter + 'static> TaskForwarder<W> {
    pub fn new(producer: Producer, writer: W) -> Self {
        let (logs_tx, logs_rx) =
            tokio::sync::mpsc::channel::<TaskWriterMessage>(LOG_MESSAGE_QUEUE_SIZE);

        let handle = tokio::spawn(async move {
            if let Err(e) = Self::start(logs_rx, writer, producer).await {
                tracing::error!(error = ?e, "Log forwarding errored");
            }
        });

        Self {
            tx: logs_tx,
            _handle: Arc::new(handle),
            _ph: Default::default(),
        }
    }

    async fn start(
        mut logs_rx: tokio::sync::mpsc::Receiver<TaskWriterMessage>,
        mut writer: W,
        uuid_producer: Producer,
    ) -> anyhow::Result<()> {
        let mut pending_logs = VecDeque::new();
        let mut stats = StatsAggregator::default();

        let task_name = loop {
            match logs_rx.recv().await {
                Some(TaskWriterMessage::SetTaskName(name)) => {
                    writer.set_task_name(name.to_owned()).await?;
                    break name;
                }
                Some(TaskWriterMessage::Log(log)) => {
                    pending_logs.push_front(log);
                    // Keep at most the latest 100 log messages when in this pending state
                    pending_logs.truncate(100);
                }
                Some(TaskWriterMessage::Stats((collection_name, new_stats))) => {
                    stats.add(collection_name, new_stats);
                }
                // If we shutdown before ever finding out our task name, we have no choice
                // but to throw out our preciously collected logs and stats. Bye bye!
                Some(TaskWriterMessage::Shutdown) | None => return Ok(()),
            }
        };

        let mut event_stream = futures::stream::iter(
            pending_logs
                .into_iter()
                // VecDeque::truncate keeps the first N items, so we use `push_front` + `truncate` to
                // store the most recent items in the front of the queue. We need to reverse
                // that when sending, as logs should be sent in oldest-first order.
                .rev()
                .map(|log| TaskWriterMessage::Log(log)),
        )
        .chain(ReceiverStream::new(logs_rx));

        // TODO(jshearer): Do we want to make this configurable?
        let mut stats_interval = tokio::time::interval(std::time::Duration::from_secs(30));

        loop {
            tokio::select! {
                msg = event_stream.next() => {
                    match msg {
                        Some(TaskWriterMessage::SetTaskName(new_name)) => {
                            anyhow::bail!("You can't change the task name after it has already been set ({task_name} -> {new_name})");
                        },
                        Some(TaskWriterMessage::Log(mut log)) => {
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
                                .append_logs(Self::serialize_log(uuid_producer, log, task_name.to_owned()).into())
                                .await?;
                        }
                        Some(TaskWriterMessage::Stats((collection_name, new_stats))) => {
                            stats.add(collection_name, new_stats);
                        }
                        Some(TaskWriterMessage::Shutdown) => break,
                        None => break,
                    }
                },
                _ = stats_interval.tick() => {
                    // Take current stats and write if non-zero
                    if let Some(current_stats) = stats.take(){
                        let data = Self::serialize_stats(uuid_producer, current_stats, task_name.to_owned());
                        writer.append_stats(data.into()).await?;
                    }
                }
            }
        }

        // Flush any remaining stats after stream ends
        if let Some(remaining_stats) = stats.take() {
            let data = Self::serialize_stats(uuid_producer, remaining_stats, task_name);
            writer.append_stats(data.into()).await?;
        }

        Ok(())
    }

    fn serialize_stats(
        producer: Producer,
        stats: BTreeMap<String, ops::stats::Binding>,
        task_name: String,
    ) -> Vec<u8> {
        let uuid = gazette::uuid::build(
            producer,
            gazette::uuid::Clock::from_time(std::time::SystemTime::now()),
            uuid::Flags((message_flags::OUTSIDE_TXN).try_into().unwrap()),
        );

        let stats_output = ops::Stats {
            capture: Default::default(),
            derive: Default::default(),
            interval: None,
            materialize: stats,
            meta: Some(ops::Meta {
                uuid: uuid.to_string(),
            }),
            open_seconds_total: Default::default(),
            shard: Some(dekaf_shard_ref(task_name)),
            timestamp: Some(proto_flow::as_timestamp(SystemTime::now())),
            txn_count: 0,
        };

        let mut buf = serde_json::to_vec(&stats_output).expect("Value always serializes");
        buf.push(b'\n');

        buf
    }

    fn serialize_log(producer: Producer, mut log: ops::Log, task_name: String) -> Vec<u8> {
        let uuid = gazette::uuid::build(
            producer,
            gazette::uuid::Clock::from_time(std::time::SystemTime::now()),
            uuid::Flags((message_flags::OUTSIDE_TXN).try_into().unwrap()),
        );
        log.meta = Some(ops::Meta {
            uuid: uuid.to_string(),
        });

        log.shard = Some(dekaf_shard_ref(task_name));

        let mut buf = serde_json::to_vec(&log).expect("Value always serializes");
        buf.push(b'\n');

        buf
    }

    pub fn set_task_name(&self, name: String) {
        use tracing_record_hierarchical::SpanExt;

        self.send_message(TaskWriterMessage::SetTaskName(name.to_owned()));

        // Also set the task name on the parent span so it's included in the logs. This also adds it
        // to the logs that Dekaf writes to stdout, which makes debugging issues much easier.
        tracing::Span::current().record_hierarchical(SESSION_TASK_NAME_FIELD_MARKER, name);
    }

    pub fn send_log_message(&self, log: ops::Log) {
        self.send_message(TaskWriterMessage::Log(log))
    }

    pub fn shutdown(&self) {
        self.send_message(TaskWriterMessage::Shutdown);
    }

    pub fn send_stats(&self, collection_name: String, stats: ops::stats::Binding) {
        self.send_message(TaskWriterMessage::Stats((collection_name, stats)))
    }

    fn send_message(&self, msg: TaskWriterMessage) {
        let capacity = self.tx.capacity();
        if self.tx.capacity() < (LOG_MESSAGE_QUEUE_SIZE / 2) {
            tracing::warn!(
                queued_messages = LOG_MESSAGE_QUEUE_SIZE - capacity,
                queue_limit = LOG_MESSAGE_QUEUE_SIZE,
                // Exclude these messages from being written to the task's logs, as otherwise
                // as soon as the queue has <50% capacity, it would immediately get filled up
                // with "messages are queueing" logs, each one causing another until the queue is full.
                { EXCLUDE_FROM_TASK_LOGGING } = true,
                "TaskForwarder messages are queueing. Are we unable to append?"
            )
        }
        match self.tx.try_send(msg) {
            Ok(_) => {}
            Err(TrySendError::Full(msg)) => {
                tracing::error!(
                    ?msg,
                    // Similarly to the "messages are queueing" warning, we can't actually append these
                    // to the task logs as the queue is already full. So instead we just log them noisily
                    {EXCLUDE_FROM_TASK_LOGGING} = true,
                    "TaskForwarder message queue is full, dropping message on the ground! Are we unable to append?"
                );
            }
            Err(TrySendError::Closed(_)) => {
                // This is normal and happens when logs are emitted after calling [`TaskForwarder::shutdown()`]
            }
        }
    }
}

fn dekaf_shard_ref(task_name: String) -> ops::ShardRef {
    ops::ShardRef {
        kind: ops::TaskType::Materialization.into(),
        name: task_name,
        key_begin: "00000000".to_string(),
        r_clock_begin: "00000000".to_string(),
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
        pub stats: Arc<tokio::sync::Mutex<VecDeque<Bytes>>>,
    }

    #[async_trait::async_trait]
    impl TaskWriter for MockLogWriter {
        async fn set_task_name(&mut self, _: String) -> anyhow::Result<()> {
            Ok(())
        }

        async fn append_logs(&self, log_data: Bytes) -> anyhow::Result<()> {
            self.logs.lock().await.push_back(log_data);
            Ok(())
        }
        async fn append_stats(&self, log_data: Bytes) -> anyhow::Result<()> {
            self.stats.lock().await.push_back(log_data);
            Ok(())
        }
    }

    tokio::task_local! {
        static MOCK_LOG_FORWARDER: TaskForwarder<MockLogWriter>;
    }

    fn gen_producer() -> Producer {
        // There's probably a neat bit-banging way to do this with i64 and masks, but I'm just not that fancy.
        let mut producer_id = rand::thread_rng().gen::<[u8; 6]>();
        producer_id[0] |= 0x01;
        gazette::uuid::Producer::from_bytes(producer_id)
    }

    async fn setup<F, Fut>(f: F)
    where
        F: FnOnce(
            Arc<tokio::sync::Mutex<VecDeque<Bytes>>>,
            Arc<tokio::sync::Mutex<VecDeque<Bytes>>>,
        ) -> Fut,
        Fut: Future,
    {
        let mock_writer = MockLogWriter::default();
        let logs = mock_writer.logs.clone();
        let stats = mock_writer.stats.clone();

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
                TaskForwarder::new(producer, mock_writer),
                async move {
                    f(logs, stats)
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
        setup(|logs, _stats| async move {
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
        setup(|logs, _stats| async move {
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
        setup(|logs, _stats| async move {
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
        setup(|logs, _stats| async move {
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
        setup(|logs, _stats| async move {
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

    #[tokio::test]
    async fn test_stats() {
        setup(|_logs, stats| async move {
            {
                MOCK_LOG_FORWARDER
                    .get()
                    .set_task_name("my_task".to_string());

                MOCK_LOG_FORWARDER.get().send_stats(
                    "test_collection".to_string(),
                    ops::stats::Binding {
                        left: Some(ops::stats::DocsAndBytes {
                            docs_total: 1,
                            bytes_total: 2,
                        }),
                        right: Some(ops::stats::DocsAndBytes {
                            docs_total: 3,
                            bytes_total: 4,
                        }),
                        out: Some(ops::stats::DocsAndBytes {
                            docs_total: 5,
                            bytes_total: 6,
                        }),
                    },
                );

                MOCK_LOG_FORWARDER.get().shutdown();
            }

            tokio::time::sleep(Duration::from_millis(100)).await;

            assert_output("test_stats", stats).await;
        })
        .await;
    }
}
