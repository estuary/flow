use crate::{
    log_appender::GazetteWriter,
    logging,
    topology::{self, Partition},
};
use anyhow::Context;
use futures::StreamExt;
use gazette::{broker, journal};
use itertools::Itertools;
use proto_flow::flow::MaterializationSpec;
use rand::Rng;
use std::{
    collections::HashMap,
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::watch;

// Define a custom cloneable error type
#[derive(Debug, Clone)]
pub struct SharedError(Arc<anyhow::Error>);

impl From<anyhow::Error> for SharedError {
    fn from(error: anyhow::Error) -> Self {
        SharedError(Arc::new(error))
    }
}

// This makes SharedError itself a valid error type.
impl std::error::Error for SharedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

impl fmt::Display for SharedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

pub type Result<T> = core::result::Result<T, SharedError>;

const TASK_TIMEOUT: Duration = Duration::from_secs(60 * 3);

#[derive(Clone)]
pub enum TaskState {
    /// Task is authorized and running in this dataplane
    Authorized {
        // Control-plane access token
        access_token: String,
        access_token_claims: AccessTokenClaims,
        ops_logs_journal: String,
        ops_stats_journal: String,
        spec: proto_flow::flow::MaterializationSpec,
        /// Sorted by collection's partition template name
        partitions: Vec<(
            String,
            Result<(journal::Client, proto_gazette::Claims, Vec<Partition>)>,
        )>,
        ops_logs_client: Result<(journal::Client, proto_gazette::Claims)>,
        ops_stats_client: Result<(journal::Client, proto_gazette::Claims)>,
    },
    /// Task has been migrated to a different dataplane
    Redirect {
        target_dataplane_fqdn: String,
        spec: proto_flow::flow::MaterializationSpec,
    },
}

/// A wrapper around a TaskManager receiver that provides a method to get the current state.
/// So long as there is at least one `TaskStateReceiver` listening, the task manager will continue to run.
#[derive(Clone)]
pub struct TaskStateListener(Arc<watch::Receiver<Option<Result<TaskState>>>>);
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
        HashMap<
            String,
            (
                // Activity signal to keep the task manager alive
                Arc<AtomicBool>,
                std::sync::Weak<watch::Receiver<Option<Result<TaskState>>>>,
            ),
        >,
    >,
    interval: Duration,
    client: flow_client::Client,
    data_plane_fqdn: String,
    data_plane_signer: jsonwebtoken::EncodingKey,
}
impl TaskManager {
    pub fn new(
        interval: Duration,
        client: flow_client::Client,
        data_plane_fqdn: String,
        data_plane_signer: jsonwebtoken::EncodingKey,
    ) -> Self {
        TaskManager {
            tasks: std::sync::Mutex::new(HashMap::new()),
            interval,
            client,
            data_plane_fqdn,
            data_plane_signer: data_plane_signer,
        }
    }

    /// Returns a [`tokio::sync::watch::Receiver`] that will receive updates to the task state.
    /// The receiver is weakly referenced, so it may be dropped if no one is listening.
    #[tracing::instrument(skip(self))]
    pub fn get_listener(self: &std::sync::Arc<Self>, task_name: &str) -> TaskStateListener {
        // Scope to force the `tasks` lock to be released before awaiting
        let (sender, receiver, activity_signal) = {
            let mut tasks_guard = self.tasks.lock().unwrap();
            if let Some((activity, weak_receiver)) = tasks_guard.get(task_name) {
                if let Some(receiver) = weak_receiver.upgrade() {
                    activity.store(true, Ordering::Relaxed);
                    return TaskStateListener(receiver.clone());
                }
            }

            let (sender, receiver) = watch::channel(None);

            let receiver = Arc::new(receiver);

            let activity_signal = Arc::new(AtomicBool::new(true));

            tasks_guard.insert(
                task_name.to_string(),
                (activity_signal.clone(), Arc::downgrade(&receiver)),
            );

            (sender, receiver, activity_signal)
        };

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
            GazetteWriter::new(self.clone()),
            stop_signal.clone(),
            self.clone().run_task_manager(
                receiver.clone(),
                sender,
                stop_signal,
                activity_signal,
                task_name,
            ),
        ));

        TaskStateListener(receiver)
    }

    /// Runs the task manager loop until either there are no more receivers or the stop signal is triggered.
    #[tracing::instrument(skip(self, receiver, sender, stop_signal))]
    async fn run_task_manager(
        self: std::sync::Arc<Self>,
        // Hold onto a strong reference to the receiver so we can keep it alive until the timeout runs out
        receiver: Arc<watch::Receiver<Option<Result<TaskState>>>>,
        sender: watch::Sender<Option<Result<TaskState>>>,
        stop_signal: tokio_util::sync::CancellationToken,
        activity_signal: Arc<AtomicBool>,
        task_name: String,
    ) -> anyhow::Result<()> {
        // Start the loop at some random point between now and the interval duration
        let jittered_start = Duration::from_millis(
            rand::thread_rng().gen_range(0..self.interval.as_millis() as u64),
        );
        let mut interval =
            tokio::time::interval_at(tokio::time::Instant::now() + jittered_start, self.interval);

        let mut partitions_and_clients: HashMap<
            String,
            Result<(journal::Client, proto_gazette::Claims, Vec<Partition>)>,
        > = HashMap::new();

        let mut cached_ops_logs_client: Option<Result<(journal::Client, proto_gazette::Claims)>> =
            None;
        let mut cached_ops_stats_client: Option<Result<(journal::Client, proto_gazette::Claims)>> =
            None;

        let mut timeout_start = None;

        loop {
            // No more receivers except us, time to shut down this task loop.
            // Note that we only do this after waiting out the interval.
            // This is to provide a grace period for any new receivers to be created
            // before we shut down the task loop and cause any new sessions to have to
            // block while the new task loop fetches its first state.
            if Arc::strong_count(&receiver) == 1 && timeout_start.is_none() {
                timeout_start = Some(tokio::time::Instant::now());
            }
            if Arc::strong_count(&receiver) > 1 || activity_signal.load(Ordering::Relaxed) {
                timeout_start = None;
                activity_signal.store(false, Ordering::Relaxed);
            }

            if let Some(start) = timeout_start {
                if start.elapsed() > TASK_TIMEOUT {
                    let waited_for = start.elapsed();
                    tracing::info!(
                        ?waited_for,
                        "TaskManager hasn't had any listeners for a while, shutting down"
                    );
                    break;
                }
            }

            let mut has_been_migrated = false;

            let loop_result: Result<()> = async {
                // For the moment, let's just refresh this every tick in order to have relatively
                // fresh MaterializationSpecs, even if the access token may live for a while.
                let dekaf_auth = fetch_dekaf_task_auth(
                    &self.client,
                    &task_name,
                    &self.data_plane_fqdn,
                    &self.data_plane_signer,
                )
                .await
                .context("error fetching dekaf task auth")?;

                match dekaf_auth {
                    DekafTaskAuth::Redirect {
                        target_dataplane_fqdn,
                        spec,
                    } => {
                        if !has_been_migrated {
                            has_been_migrated = true;

                            tracing::info!(
                                task_name = %task_name,
                                target_dataplane = %target_dataplane_fqdn,
                                "Task has been migrated to different dataplane"
                            );
                        }

                        let _ = sender.send(Some(Ok(TaskState::Redirect {
                            target_dataplane_fqdn: target_dataplane_fqdn,
                            spec,
                        })));

                        Ok(())
                    }
                    DekafTaskAuth::Auth {
                        token: access_token,
                        claims: access_token_claims,
                        ops_logs_journal,
                        ops_stats_journal,
                        spec,
                    } => {
                        // Continue with normal processing
                        partitions_and_clients = update_partition_info(
                            &self.client,
                            &self.data_plane_fqdn,
                            &self.data_plane_signer,
                            &task_name,
                            &spec,
                            std::mem::take(&mut partitions_and_clients),
                        )
                        .await?;

                        let logs_client_result = get_or_refresh_journal_client(
                            &self.client,
                            &self.data_plane_fqdn,
                            &self.data_plane_signer,
                            &task_name,
                            proto_flow::capability::AUTHORIZE | proto_gazette::capability::APPEND,
                            broker::LabelSelector {
                                include: Some(labels::build_set([(
                                    "name",
                                    ops_logs_journal.as_str(),
                                )])),
                                exclude: None,
                            },
                            cached_ops_logs_client
                                .as_ref()
                                .and_then(|r| r.as_ref().ok()),
                        )
                        .await
                        .map_err(SharedError::from);
                        cached_ops_logs_client = Some(logs_client_result);

                        let stats_client_result = get_or_refresh_journal_client(
                            &self.client,
                            &self.data_plane_fqdn,
                            &self.data_plane_signer,
                            &task_name,
                            proto_flow::capability::AUTHORIZE | proto_gazette::capability::APPEND,
                            broker::LabelSelector {
                                include: Some(labels::build_set([(
                                    "name",
                                    ops_stats_journal.as_str(),
                                )])),
                                exclude: None,
                            },
                            cached_ops_stats_client
                                .as_ref()
                                .and_then(|r| r.as_ref().ok()),
                        )
                        .await
                        .map_err(SharedError::from);
                        cached_ops_stats_client = Some(stats_client_result);

                        let _ = sender.send(Some(Ok(TaskState::Authorized {
                            access_token,
                            access_token_claims,
                            ops_logs_journal,
                            ops_stats_journal,
                            spec,
                            partitions: partitions_and_clients
                                .iter()
                                .sorted_by_key(|(k, _)| k.as_str())
                                .map(|(k, v)| {
                                    let mapped_val = match v {
                                        Ok(p) => Ok(p.clone()),
                                        Err(e) => Err(e.clone()),
                                    };
                                    let res = (k.clone(), mapped_val);

                                    res
                                })
                                .collect_vec(),
                            ops_logs_client: cached_ops_logs_client
                                .as_ref()
                                .expect("this is guarinteed to be present")
                                .clone(),
                            ops_stats_client: cached_ops_stats_client
                                .as_ref()
                                .expect("this is guarinteed to be present")
                                .clone(),
                        })));

                        Ok(())
                    }
                } // End of match
            }
            .await;

            if let Err(e) = loop_result {
                tracing::error!(task_name, error=%e, "Error in task manager loop");
                let _ = sender.send(Some(Err(SharedError::from(e))));
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
}

#[tracing::instrument(skip_all, fields(task_name))]
async fn update_partition_info(
    flow_client: &flow_client::Client,
    data_plane_fqdn: &str,
    data_plane_signer: &jsonwebtoken::EncodingKey,
    task_name: &str,
    spec: &MaterializationSpec,
    mut info: HashMap<String, Result<(journal::Client, proto_gazette::Claims, Vec<Partition>)>>,
) -> anyhow::Result<HashMap<String, Result<(journal::Client, proto_gazette::Claims, Vec<Partition>)>>>
{
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

        let template_name = partition_template.name.clone();
        let task_name_clone = task_name.to_string();

        let existing_client = match info.remove(template_name.as_str()) {
            Some(Ok((client, claims, _))) => Some((client, claims)),
            _ => None,
        };

        tasks.push(async move {
            let journal_client_result = get_or_refresh_journal_client(
                flow_client,
                data_plane_fqdn,
                data_plane_signer,
                &task_name_clone,
                proto_flow::capability::AUTHORIZE | proto_gazette::capability::LIST | proto_gazette::capability::READ,
                broker::LabelSelector {
                    include: Some(labels::build_set([("name:prefix", format!("{}/", template_name).as_str())])),
                    exclude: None,
                },
                existing_client.as_ref(),
            )
            .await;

            let (journal_client, claims) = match journal_client_result {
                Ok(jc) => jc,
                Err(task_error) => {
                    tracing::warn!(task=%task_name_clone, template=%template_name, error=%task_error, "Failed to get journal client for binding");
                    return (template_name, Err(SharedError::from(task_error)));
                }
            };

            let partition_result = fetch_partitions(
                &journal_client,
                &collection_spec.name,
                Some(partition_selector.clone()),
            )
            .await
            .map(|partitions| {
                (journal_client, claims, partitions)
            })
            .map_err(|e| {
                SharedError::from(e.context(format!("Partition fetch failed for collection '{}'", collection_spec.name)))
            });

            // Return the result associated with this template name
            (template_name, partition_result)
        });
    }

    Ok(futures::stream::iter(tasks)
        .buffer_unordered(10)
        .collect::<HashMap<String, _>>()
        .await)
}

#[tracing::instrument(skip_all, fields(task_name, identifier))]
async fn get_or_refresh_journal_client(
    flow_client: &flow_client::Client,
    data_plane_fqdn: &str,
    data_plane_signer: &jsonwebtoken::EncodingKey,
    task_name: &str,
    capability: u32,
    selector: broker::LabelSelector,
    cached_client_and_claims: Option<&(journal::Client, proto_gazette::Claims)>,
) -> anyhow::Result<(journal::Client, proto_gazette::Claims)> {
    if let Some((cached_client, claims)) = cached_client_and_claims {
        let now_unix = time::OffsetDateTime::now_utc().unix_timestamp();
        // Add a buffer to token expiry check
        if claims.exp > now_unix as u64 + 60 {
            tracing::debug!(task=%task_name, "Re-using existing journal client.");
            return Ok((cached_client.clone(), claims.clone()));
        } else {
            tracing::debug!(task=%task_name, "Journal client token expired or nearing expiry.");
        }
    }

    tracing::debug!(task=%task_name,  capability, "Fetching new task authorization for journal client.");
    metrics::counter!("dekaf_fetch_auth", "endpoint" => "/authorize/task", "task_name" => task_name.to_owned()).increment(1);
    flow_client::fetch_task_authorization(
        flow_client,
        &crate::dekaf_shard_template_id(task_name),
        data_plane_fqdn,
        data_plane_signer,
        capability,
        selector,
    )
    .await
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

// Claims returned by `/authorize/dekaf`
#[derive(Debug, Clone, serde::Deserialize)]
pub struct AccessTokenClaims {
    pub exp: u64,
}

pub enum DekafTaskAuth {
    /// Task has been migrated to a different dataplane, and the session should redirect to it.
    Redirect {
        target_dataplane_fqdn: String,
        spec: MaterializationSpec,
    },
    /// Task authorization data.
    Auth {
        token: String,
        claims: AccessTokenClaims,
        ops_logs_journal: String,
        ops_stats_journal: String,
        spec: MaterializationSpec,
    },
}

#[tracing::instrument(skip(client, data_plane_signer), err)]
async fn fetch_dekaf_task_auth(
    client: &flow_client::Client,
    shard_template_id: &str,
    data_plane_fqdn: &str,
    data_plane_signer: &jsonwebtoken::EncodingKey,
) -> anyhow::Result<DekafTaskAuth> {
    let start = std::time::Instant::now();

    let request_token = flow_client::client::build_task_authorization_request_token(
        shard_template_id,
        data_plane_fqdn,
        data_plane_signer,
        proto_flow::capability::AUTHORIZE,
        Default::default(),
    )?;
    let models::authorizations::DekafAuthResponse {
        token,
        ops_logs_journal,
        ops_stats_journal,
        task_spec,
        redirect_dataplane_fqdn,
        ..
    } = loop {
        let response: models::authorizations::DekafAuthResponse = client
            .agent_unary(
                "/authorize/dekaf",
                &models::authorizations::TaskAuthorizationRequest {
                    token: request_token.clone(),
                },
            )
            .await?;
        if response.retry_millis != 0 {
            tracing::warn!(
                secs = response.retry_millis as f64 / 1000.0,
                "authorization service tentatively rejected our request, but will retry before failing"
            );
            () = tokio::time::sleep(std::time::Duration::from_millis(response.retry_millis)).await;
            continue;
        }
        break response;
    };

    let parsed_spec = serde_json::from_str(
        task_spec
            .ok_or(anyhow::anyhow!(
                "task_spec is only None when we need to retry the auth request"
            ))?
            .get(),
    )?;

    // Check if we got a redirect response
    if let Some(redirect_fqdn) = redirect_dataplane_fqdn {
        tracing::debug!(
            redirect_target = redirect_fqdn,
            "task has been migrated to different dataplane, returning redirect"
        );
        metrics::counter!(
            "dekaf_fetch_auth",
            "endpoint" => "/authorize/dekaf",
            "redirect" => "true",
            "task_name" => shard_template_id.to_owned()
        )
        .increment(1);

        return Ok(DekafTaskAuth::Redirect {
            target_dataplane_fqdn: redirect_fqdn,
            spec: parsed_spec,
        });
    }

    let claims = flow_client::parse_jwt_claims(token.as_str())?;

    tracing::debug!(
        runtime_ms = start.elapsed().as_millis(),
        "fetched dekaf task auth",
    );

    metrics::counter!(
        "dekaf_fetch_auth",
        "endpoint" => "/authorize/dekaf",
        "redirect" => "false",
        "task_name" => shard_template_id.to_owned()
    )
    .increment(1);

    Ok(DekafTaskAuth::Auth {
        token,
        claims,
        ops_logs_journal,
        ops_stats_journal,
        spec: parsed_spec,
    })
}
