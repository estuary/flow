use crate::{
    log_appender::GazetteWriter,
    logging,
    topology::{self, fetch_dekaf_task_auth, Partition},
};
use anyhow::Context;
use futures::StreamExt;
use gazette::{broker, journal};
use itertools::Itertools;
use proto_flow::flow::MaterializationSpec;
use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::sync::watch;

// Define a custom cloneable error type
#[derive(Debug, Clone)]
pub struct TaskManagerError(Arc<String>);

/// Creates a TaskManagerError from anyhow::Error, preserving its chain with Debug format.
impl From<anyhow::Error> for TaskManagerError {
    fn from(error: anyhow::Error) -> Self {
        // Use debug format to capture context and backtrace (if available)
        TaskManagerError(Arc::new(format!("{:?}", error)))
    }
}

impl From<&anyhow::Error> for TaskManagerError {
    fn from(error: &anyhow::Error) -> Self {
        TaskManagerError(Arc::new(format!("{:?}", error)))
    }
}

impl fmt::Display for TaskManagerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// Implement the standard Error trait for TaskManagerError
// Importantly, this allows conversion into `anyhow::Error` via `?`
impl std::error::Error for TaskManagerError {}

pub type TaskManagerResult<T> = Result<T, TaskManagerError>;

#[derive(Debug, Clone)]
pub struct TaskState {
    // Access token
    pub access_token: String,
    pub access_token_claims: topology::AccessTokenClaims,
    // ops_logs_journal
    pub ops_logs_journal: String,
    // ops_stats_journal
    pub ops_stats_journal: String,
    pub spec: proto_flow::flow::MaterializationSpec,
    /// Sorted by collection's partition template name
    pub partitions: Vec<(String, TaskManagerResult<Vec<topology::Partition>>)>,
}

/// A wrapper around a TaskManager receiver that provides a method to get the current state.
/// So long as there is at least one `TaskStateReceiver` listening, the task manager will continue to run.
pub struct TaskStateListener(Arc<watch::Receiver<Option<TaskManagerResult<TaskState>>>>);
impl TaskStateListener {
    /// Gets the current state, waiting if it's not yet available.
    /// Returns a clone of the state or the cached error.
    pub async fn get(&self) -> anyhow::Result<TaskState> {
        let mut temp_rx = (*self.0).clone();
        loop {
            // Scope to force the borrow to end before awaiting
            {
                let current_value = temp_rx.borrow_and_update();
                if let Some(ref result) = *current_value {
                    return result.clone().map_err(anyhow::Error::from);
                }
            }

            temp_rx
                .changed()
                .await
                .map_err(anyhow::Error::from)
                .context("TaskManager's watch channel sender was dropped unexpectedly")?;
        }
    }
}

/// TaskManager manages Dekaf's communication with the rest of Flow, _except_ for Read requests.
/// Many Sessions may ask for the same information, so instead of each one independently fetching
/// it, the TaskManager coordinates periodically fetching it and then distributing it to all the Sessions.
/// A TaskManager is responsible for providing:
///   - Information from `/authorize/dekaf`, refreshed periodically
///     - MaterializationSpec
///     - Control-plane access token and its claims
///     - Ops journal names
///   - Information from data planes about journals and partitions, refreshed periodically
///     - Journal partitions by collection
pub struct TaskManager {
    // Key: materialization/task name
    tasks: std::sync::Mutex<
        HashMap<String, std::sync::Weak<watch::Receiver<Option<TaskManagerResult<TaskState>>>>>,
    >,
    ttl: Duration,
    client: flow_client::Client,
    data_plane_fqdn: String,
    data_plane_signer: jsonwebtoken::EncodingKey,
    router: gazette::Router,
}

impl TaskManager {
    pub fn new(
        ttl: Duration,
        client: flow_client::Client,
        data_plane_fqdn: String,
        data_plane_signer: jsonwebtoken::EncodingKey,
        router: gazette::Router,
    ) -> Self {
        TaskManager {
            tasks: std::sync::Mutex::new(HashMap::new()),
            ttl,
            client,
            data_plane_fqdn,
            data_plane_signer: data_plane_signer,
            router,
        }
    }

    /// Returns a [`tokio::sync::watch::Receiver`] that will receive updates to the task state.
    /// The receiver is weakly referenced, so it may be dropped if no one is listening.
    #[tracing::instrument(skip(self))]
    pub async fn get_listener(self: &std::sync::Arc<Self>, task_name: &str) -> TaskStateListener {
        let mut tasks_guard = self.tasks.lock().unwrap();
        if let Some(weak_receiver) = tasks_guard.get(task_name) {
            if let Some(receiver) = weak_receiver.upgrade() {
                return TaskStateListener(receiver.clone());
            }
        }

        let (sender, receiver) = watch::channel(None);

        let receiver = Arc::new(receiver);

        let weak_receiver = Arc::downgrade(&receiver);
        tasks_guard.insert(task_name.to_string(), weak_receiver.clone());
        drop(tasks_guard);

        tracing::info!("Spawning new task processor");

        // Spawn a task to fetch the task state
        let task_name = task_name.to_string();

        let stop_signal = tokio_util::sync::CancellationToken::new();

        // We can't just use `propagate_task_forwarder` here because the session that first spawns
        // the task manager may not live long enough to see the task manager complete, and any log
        // messages emitted by the task manager after that session is closed would be lost.
        // Instead, we'll create a separate log forwarder for this task manager that will report
        // its logs to the correct task's ops logs, irrespective of the session that spawned it.
        tokio::spawn(logging::forward_logs(
            GazetteWriter::new(
                self.client.clone(),
                self.data_plane_fqdn.clone(),
                self.data_plane_signer.clone(),
                self.router.clone(),
            ),
            stop_signal.clone(),
            self.clone()
                .run_task_manager(weak_receiver, sender, stop_signal, task_name),
        ));

        TaskStateListener(receiver)
    }

    /// Runs the task manager loop until either there are no more receivers or the stop signal is triggered.
    #[tracing::instrument(skip(self, receiver, sender, stop_signal))]
    async fn run_task_manager(
        self: std::sync::Arc<Self>,
        receiver: Weak<watch::Receiver<Option<TaskManagerResult<TaskState>>>>,
        // Hold onto a weak reference to the receiver so we can check if there are still listeners
        sender: watch::Sender<Option<TaskManagerResult<TaskState>>>,
        stop_signal: tokio_util::sync::CancellationToken,
        task_name: String,
    ) -> anyhow::Result<()> {
        let mut interval = tokio::time::interval(self.ttl);

        let journal_clients: Arc<
            std::sync::Mutex<HashMap<String, (journal::Client, proto_gazette::Claims)>>,
        > = Arc::new(std::sync::Mutex::new(HashMap::new()));

        let mut partitions: HashMap<String, anyhow::Result<Vec<Partition>>> = HashMap::new();

        loop {
            // No more receivers, time to shut down this task loop.
            // Note that we only do this after waiting out the interval.
            // This is to provide a grace period for any new receivers to be created
            // before we shut down the task loop and cause any new sessions to have to
            // block while the new task loop fetches its first state.
            if Weak::strong_count(&receiver) == 0 {
                tracing::info!(task_name, "No more receivers, exiting");
                break;
            }

            tracing::info!("Refreshing task spec");

            let loop_result: Result<(), anyhow::Error> = async {
                // For the moment, let's just refresh this every tick in order to have relatively
                // fresh MaterializationSpecs, even if the access token may live for a while.
                let (access_token, access_token_claims, ops_logs_journal, ops_stats_journal, spec) =
                    fetch_dekaf_task_auth(
                        &self.client,
                        &task_name,
                        &self.data_plane_fqdn,
                        &self.data_plane_signer,
                    )
                    .await
                    .context("error fetching dekaf task auth")?;

                let _ = self
                    .update_partition_info(
                        &task_name,
                        &spec,
                        &mut partitions,
                        journal_clients.clone(),
                    )
                    .await?;

                let _ = sender.send(Some(Ok(TaskState {
                    access_token,
                    access_token_claims,
                    ops_logs_journal,
                    ops_stats_journal,
                    spec,
                    partitions: partitions
                        .iter()
                        .sorted_by_key(|(k, _)| k.as_str())
                        .map(|(k, v)| {
                            let mapped_val = match v {
                                Ok(p) => Ok(p.clone()),
                                Err(e) => Err(TaskManagerError::from(e)),
                            };
                            let res = (k.clone(), mapped_val);

                            res
                        })
                        .collect_vec(),
                })));

                Ok(())
            }
            .await;

            if let Err(e) = loop_result {
                tracing::error!(task_name, error=%e, "Error in task manager loop");
                let _ = sender.send(Some(Err(TaskManagerError::from(e))));
            }

            tokio::select! {
                _ = stop_signal.cancelled() => {
                    tracing::info!(task_name, "signalled to stop");
                    break;
                }
                _ = interval.tick() => {}
            }
        }
        drop(receiver);

        Ok(())
    }

    #[tracing::instrument(skip(self, spec, partitions, journal_clients))]
    async fn update_partition_info(
        self: &std::sync::Arc<Self>,
        task_name: &str,
        spec: &MaterializationSpec,
        partitions: &mut HashMap<String, anyhow::Result<Vec<Partition>>>,
        journal_clients: Arc<
            std::sync::Mutex<HashMap<String, (journal::Client, proto_gazette::Claims)>>,
        >,
    ) -> anyhow::Result<()> {
        let mut current_partition_template_names = HashSet::with_capacity(spec.bindings.len());
        let mut tasks = Vec::with_capacity(spec.bindings.len());

        for binding in &spec.bindings {
            let collection_spec = binding
                .collection
                .as_ref()
                .context("expected collection Spec")?;
            let partition_template = collection_spec
                .partition_template
                .as_ref()
                .context("expected partition template")?;

            let partition_selector = binding
                .partition_selector
                .as_ref()
                .context("expected partition selector")?;

            current_partition_template_names.insert(partition_template.name.clone());

            let template_name = partition_template.name.clone();
            let task_name_clone = task_name.to_string();
            let journal_clients_clone = journal_clients.clone();

            tasks.push(async move {
                let journal_client_result = self.get_or_refresh_journal_client(
                    &task_name_clone,
                    &template_name,
                    journal_clients_clone.clone(),
                )
                .await;

                let journal_client = match journal_client_result {
                    Ok(jc) => jc,
                    Err(task_error) => {
                        tracing::warn!(task=%task_name_clone, template=%template_name, error=%task_error, "Failed to get journal client for binding");
                        return (template_name, Err(task_error));
                    }
                };

                let partition_result = fetch_partitions(
                    &journal_client,
                    &collection_spec.name,
                    Some(partition_selector.clone()),
                )
                .await
                .map_err(|e| {
                    e.context(format!("Partition fetch failed for collection '{}'", collection_spec.name))
                });

                // Return the result associated with this template name
                (template_name, partition_result)
            });
        }

        let results_vec = futures::stream::iter(tasks)
            .buffer_unordered(10)
            .collect::<Vec<_>>()
            .await;

        for (template_name, result) in results_vec {
            current_partition_template_names.insert(template_name.clone());
            partitions.insert(template_name, result.map_err(|e| anyhow::anyhow!(e)));
        }

        // Clear out any partition templates that are no longer in the spec
        let mut journal_clients_guard = journal_clients.lock().expect("Mutex poisoned");

        journal_clients_guard.retain(|k, _| current_partition_template_names.contains(k));
        partitions.retain(|k, _| current_partition_template_names.contains(k));

        Ok(())
    }

    /// Gets a journal client from cache or fetches a new one if needed.
    #[tracing::instrument(skip(self, journal_clients_cache))]
    async fn get_or_refresh_journal_client(
        self: &std::sync::Arc<Self>,
        task_name: &str,
        partition_template_name: &str,
        journal_clients_cache: Arc<
            std::sync::Mutex<HashMap<String, (journal::Client, proto_gazette::Claims)>>,
        >,
    ) -> anyhow::Result<journal::Client> {
        // Scope the guard so it doesn't accidentally try to hold across the await point
        {
            let cache_guard = journal_clients_cache.lock().expect("Mutex poisoned");

            if let Some((cached_client, claims)) = cache_guard.get(partition_template_name) {
                let now_unix = time::OffsetDateTime::now_utc().unix_timestamp();
                // Add a buffer to token expiry check
                if claims.exp > now_unix as u64 + 60 {
                    tracing::debug!(task=%task_name, template=%partition_template_name, "Re-using existing journal client.");
                    return Ok(cached_client.clone());
                } else {
                    tracing::debug!(task=%task_name, template=%partition_template_name, "Journal client token expired or nearing expiry.");
                }
            }
        }

        tracing::debug!(task=%task_name, template=%partition_template_name, "Fetching new task authorization for journal client.");
        let auth_result = flow_client::fetch_task_authorization(
            &self.client,
            &crate::dekaf_shard_template_id(task_name),
            &self.data_plane_fqdn,
            &self.data_plane_signer,
            proto_flow::capability::AUTHORIZE
                | proto_gazette::capability::LIST
                | proto_gazette::capability::READ,
            broker::LabelSelector {
                include: Some(labels::build_set([(
                    "name:prefix",
                    format!("{partition_template_name}/").as_str(),
                )])),
                exclude: None,
            },
        )
        .await;

        match auth_result {
            Ok((new_auth, new_claims)) => {
                tracing::info!(task=%task_name, template=%partition_template_name, "Successfully fetched new journal client authorization.");
                let mut cache_guard = journal_clients_cache.lock().expect("Mutex poisoned");
                cache_guard.insert(
                    partition_template_name.to_string(),
                    (new_auth.clone(), new_claims),
                );
                Ok(new_client)
            }
            Err(e) => {
                tracing::warn!(task=%task_name, template=%partition_template_name, error=%e, "Failed to fetch task authorization");
                Err(e.context(format!(
                    "Failed to fetch task authorization for template '{}'",
                    partition_template_name
                )))
            }
        }
    }
}

/// Fetch the journals of a collection and map into stable-order partitions.
#[tracing::instrument(skip(journal_client))]
pub async fn fetch_partitions(
    journal_client: &journal::Client,
    collection: &str,
    partition_selector: Option<broker::LabelSelector>,
) -> anyhow::Result<Vec<topology::Partition>> {
    let request = broker::ListRequest {
        selector: Some(partition_selector.unwrap_or(broker::LabelSelector {
            include: Some(labels::build_set([(labels::COLLECTION, collection)])),
            exclude: None,
        })),
        ..Default::default()
    };

    let response = journal_client.list(request).await?;

    let mut partitions = Vec::with_capacity(response.journals.len());

    for journal in response.journals {
        partitions.push(Partition {
            create_revision: journal.create_revision,
            spec: journal.spec.context("expected journal Spec")?,
            mod_revision: journal.mod_revision,
            route: journal.route.context("expected journal Route")?,
        })
    }

    // Establish stability of exposed partition indices by ordering journals
    // by their created revision, and _then_ by their name.
    partitions
        .sort_by(|l, r| (l.create_revision, &l.spec.name).cmp(&(r.create_revision, &r.spec.name)));

    Ok(partitions)
}
