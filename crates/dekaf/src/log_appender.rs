use crate::{dekaf_shard_template_id, topology::fetch_dekaf_task_auth, App};
use anyhow::Context;
use async_trait::async_trait;
use bytes::Bytes;
use flow_client::fetch_task_authorization;
use futures::{Stream, StreamExt, TryStreamExt};
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
    mem,
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
                .is_some_and(|s| s.bytes_total > 0 && s.docs_total > 0)
                || v.right
                    .is_some_and(|s| s.bytes_total > 0 && s.docs_total > 0)
                || v.out.is_some_and(|s| s.bytes_total > 0 && s.docs_total > 0)
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
    async fn append_logs<S>(
        &mut self,
        log_data: impl Fn() -> S + Send + Sync,
    ) -> anyhow::Result<()>
    where
        S: Stream<Item = std::io::Result<bytes::Bytes>> + Send + 'static;
    async fn append_stats<S>(
        &mut self,
        stat_data: impl Fn() -> S + Send + Sync,
    ) -> anyhow::Result<()>
    where
        S: Stream<Item = std::io::Result<bytes::Bytes>> + Send + 'static;

    async fn set_task_name(&mut self, name: String) -> anyhow::Result<()>;
}

#[derive(Clone)]
pub struct GazetteWriter {
    app: Arc<App>,
    logs_appender: Option<GazetteAppender>,
    stats_appender: Option<GazetteAppender>,
    task_name: Option<String>,
}

#[async_trait]
impl TaskWriter for GazetteWriter {
    async fn set_task_name(&mut self, task_name: String) -> anyhow::Result<()> {
        let (logs_appender, stats_appender) = self.get_appenders(task_name.as_str()).await?;
        self.logs_appender.replace(logs_appender);
        self.stats_appender.replace(stats_appender);
        self.task_name.replace(task_name);

        Ok(())
    }

    async fn append_logs<S>(&mut self, log_data: impl Fn() -> S + Send + Sync) -> anyhow::Result<()>
    where
        S: Stream<Item = std::io::Result<bytes::Bytes>> + Send + 'static,
    {
        self.logs_appender
            .as_mut()
            .context("not initialized")?
            .append(log_data)
            .await
    }

    async fn append_stats<S>(
        &mut self,
        stat_data: impl Fn() -> S + Send + Sync,
    ) -> anyhow::Result<()>
    where
        S: Stream<Item = std::io::Result<bytes::Bytes>> + Send + 'static,
    {
        self.stats_appender
            .as_mut()
            .context("not initialized")?
            .append(stat_data)
            .await
    }
}

impl GazetteWriter {
    pub fn new(app: Arc<App>) -> Self {
        Self {
            app: app,
            task_name: None,
            logs_appender: None,
            stats_appender: None,
        }
    }

    async fn get_appenders(
        &self,
        task_name: &str,
    ) -> anyhow::Result<(GazetteAppender, GazetteAppender)> {
        let (_, _, ops_logs, ops_stats, _) = fetch_dekaf_task_auth(
            self.app.client_base.clone(),
            &task_name,
            &self.app.data_plane_fqdn,
            &self.app.data_plane_signer,
        )
        .await?;
        Ok((
            GazetteAppender::try_create(ops_logs, task_name.to_string(), self.app.clone()).await?,
            GazetteAppender::try_create(ops_stats, task_name.to_string(), self.app.clone()).await?,
        ))
    }
}

#[derive(Clone)]
struct GazetteAppender {
    client: journal::Client,
    journal_name: String,
    exp: time::OffsetDateTime,
    app: Arc<App>,
    task_name: String,
}

impl GazetteAppender {
    pub async fn try_create(
        journal_name: String,
        task_name: String,
        app: Arc<App>,
    ) -> anyhow::Result<Self> {
        let (client, exp) = Self::refresh_client(&task_name, &journal_name, app.clone()).await?;

        Ok(Self {
            client,
            exp,
            task_name,
            journal_name,
            app,
        })
    }

    async fn append<S>(&mut self, data: impl Fn() -> S + Send + Sync) -> anyhow::Result<()>
    where
        S: Stream<Item = std::io::Result<bytes::Bytes>> + Send + 'static,
    {
        if (self.exp - SystemTime::now()).whole_seconds() < 60 {
            self.refresh().await?;
        }

        let resp = self.client.append(
            gazette::broker::AppendRequest {
                journal: self.journal_name.clone(),
                ..Default::default()
            },
            data,
        );

        tokio::pin!(resp);
        loop {
            match resp.try_next().await {
                Ok(_) => return Ok(()),
                Err(RetryError { attempt, ref inner }) if inner.is_transient() => {
                    tracing::warn!(
                        ?attempt,
                        ?inner,
                        "Got recoverable error trying to write logs, retrying"
                    );
                    continue;
                }
                Err(err) => {
                    tracing::warn!(?err, "Got fatal error while trying to write logs");
                    return Err(err.inner.into());
                }
            }
        }
    }

    async fn refresh(&mut self) -> anyhow::Result<()> {
        let (client, exp) =
            Self::refresh_client(&self.task_name, &self.journal_name, self.app.clone()).await?;
        self.client = client;
        self.exp = exp;
        Ok(())
    }

    async fn refresh_client(
        task_name: &str,
        journal_name: &str,
        app: Arc<App>,
    ) -> anyhow::Result<(journal::Client, time::OffsetDateTime)> {
        let base_client = app.client_base.clone();
        let data_plane_fqdn = &app.data_plane_fqdn;
        let signer = &app.data_plane_signer;

        let template_id = dekaf_shard_template_id(task_name);

        let (auth_client, _, _, _, _) =
            fetch_dekaf_task_auth(base_client, task_name, data_plane_fqdn, signer).await?;

        let (new_client, new_claims) = fetch_task_authorization(
            &auth_client,
            &template_id,
            data_plane_fqdn,
            signer,
            proto_flow::capability::AUTHORIZE | proto_gazette::capability::APPEND,
            gazette::broker::LabelSelector {
                include: Some(labels::build_set([("name", journal_name)])),
                exclude: None,
            },
        )
        .await?;

        Ok((
            new_client,
            time::OffsetDateTime::UNIX_EPOCH + Duration::from_secs(new_claims.exp),
        ))
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
        let mut pending_logs = Vec::new();

        loop {
            tokio::select! {
                // We always want to start a new append before accumulating more log messages because in
                // the extreme case where we're getting messages faster than we can store them, we don't want
                // to end up with an infinitely growing buffer of `pending_logs`.
                biased;

                Err(append_error) = Self::append_logs_to_writer(
                    &mut writer,
                    &mut pending_logs,
                    task_name.clone(),
                    uuid_producer.clone(),
                ), if pending_logs.len() > 0 => {
                    tracing::error!(?append_error, "Error appending logs");
                }

                _ = stats_interval.tick() => {
                    // Take current stats and write if non-zero
                    if let Some(current_stats) = stats.take(){
                        if let Err(append_error) = writer.append_stats(||{
                            let serialized = Self::serialize_stats(
                                uuid_producer,
                                current_stats.clone(),
                                task_name.to_owned(),
                            );
                            futures::stream::once(async move { Ok(serialized.clone().into()) })
                        }).await {
                            tracing::error!(?append_error, "Error appending stats")
                        }
                    }
                }

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

                            pending_logs.push(log);
                        }
                        Some(TaskWriterMessage::Stats((collection_name, new_stats))) => {
                            stats.add(collection_name, new_stats);
                        }
                        Some(TaskWriterMessage::Shutdown) => break,
                        None => break,
                    }
                },
            }
        }

        // Flush any remaining stats after stream ends
        if let Some(remaining_stats) = stats.take() {
            if let Err(append_error) = writer
                .append_stats(|| {
                    let serialized = Self::serialize_stats(
                        uuid_producer,
                        remaining_stats.clone(),
                        task_name.to_owned(),
                    );
                    futures::stream::once(async move { Ok(serialized.clone().into()) })
                })
                .await
            {
                tracing::error!(?append_error, "Error appending stats")
            };
        }

        Ok(())
    }

    fn serialize_stats(
        producer: Producer,
        stats: BTreeMap<String, ops::stats::Binding>,
        task_name: String,
    ) -> bytes::Bytes {
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

        bytes::Bytes::from(buf)
    }

    fn serialize_log(producer: Producer, mut log: ops::Log, task_name: String) -> bytes::Bytes {
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

        bytes::Bytes::from(buf)
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
        if stats
            .left
            .is_some_and(|s| (s.bytes_total == 0) != (s.docs_total == 0))
            || stats
                .right
                .is_some_and(|s| (s.bytes_total == 0) != (s.docs_total == 0))
            || stats
                .out
                .is_some_and(|s| (s.bytes_total == 0) != (s.docs_total == 0))
        {
            tracing::error!(
                ?stats,
                "Invalid stats document emitted! Cannot emit 0 for just one of `bytes_total` or `docs_total`!"
            );
        } else {
            self.send_message(TaskWriterMessage::Stats((collection_name, stats)))
        }
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

    async fn append_logs_to_writer(
        writer: &mut W,
        pending_logs: &mut Vec<ops::Log>,
        task_name: String,
        uuid_producer: Producer,
    ) -> anyhow::Result<()> {
        let logs_to_append = mem::take(pending_logs);

        writer
            .append_logs(move || {
                futures::stream::iter(logs_to_append.clone().into_iter().map({
                    let value = task_name.clone();
                    move |log| {
                        let serialized = TaskForwarder::<W>::serialize_log(
                            uuid_producer.clone(),
                            log,
                            value.to_owned(),
                        );
                        Ok(serialized)
                    }
                }))
            })
            .await
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

        async fn append_logs<S>(
            &mut self,
            log_data: impl Fn() -> S + Send + Sync,
        ) -> anyhow::Result<()>
        where
            S: Stream<Item = std::io::Result<bytes::Bytes>> + Send + 'static,
        {
            let mut logs = self.logs.lock().await;
            let mut stream = Box::pin(log_data());

            while let Some(Ok(data)) = stream.next().await {
                logs.push_back(data);
            }
            Ok(())
        }
        async fn append_stats<S>(
            &mut self,
            stat_data: impl Fn() -> S + Send + Sync,
        ) -> anyhow::Result<()>
        where
            S: Stream<Item = std::io::Result<bytes::Bytes>> + Send + 'static,
        {
            let mut stats = self.stats.lock().await;
            let mut stream = Box::pin(stat_data());

            while let Some(Ok(data)) = stream.next().await {
                stats.push_back(data);
            }
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

    #[tokio::test]
    async fn test_partial_stats() {
        setup(|logs, stats| async move {
            {
                MOCK_LOG_FORWARDER
                    .get()
                    .set_task_name("my_task".to_string());

                MOCK_LOG_FORWARDER.get().send_stats(
                    "test_collection".to_string(),
                    ops::stats::Binding {
                        left: Some(ops::stats::DocsAndBytes {
                            docs_total: 1,
                            bytes_total: 0,
                        }),
                        ..Default::default()
                    },
                );

                MOCK_LOG_FORWARDER.get().shutdown();
            }

            tokio::time::sleep(Duration::from_millis(100)).await;

            assert_output("test_stats_partial_logs", logs).await;
            assert_output("test_stats_partial_stats", stats).await;
        })
        .await;
    }
}
