use crate::{connector, topology};
use futures::TryStreamExt;
use gazette::{broker, journal};
use models::authorizations::DekafAuthResponse;
use proto_flow::flow;
use std::{collections::HashMap, sync::Arc};

pub struct TaskBinding {
    pub backfill: u32,
    pub collection: flow::CollectionSpec,
    pub field_selection: flow::FieldSelection,
    pub journal_client: journal::Client,
    pub not_after: Option<proto_flow::Timestamp>,
    pub not_before: Option<proto_flow::Timestamp>,
    pub partition_selector: proto_gazette::LabelSelector,
    pub partitions: tokens::PendingWatch<Vec<topology::Partition>>,
    pub resource_path: Vec<String>,
}

pub struct AuthorizedTask {
    pub bindings: Vec<TaskBinding>,
    pub config_json: Vec<u8>,
    pub dekaf_config: tokens::PendingWatch<connector::DekafConfig>,
    pub name: String,
    pub ops_logs_client: journal::Client,
    pub ops_logs_journal: String,
    pub ops_stats_client: journal::Client,
    pub ops_stats_journal: String,
    pub schema_access_token: String,
    pub shard_template: proto_gazette::consumer::ShardSpec,
}

pub enum TaskState {
    Authorized(AuthorizedTask),
    Redirect { target_dataplane_fqdn: String },
}

/// Centralized manager that caches and shares task watches across sessions.
pub struct TaskManager {
    tasks: std::sync::Mutex<HashMap<String, tokens::PendingWatch<TaskState>>>,
    api_client: flow_client::rest::Client,
    data_plane_fqdn: String,
    data_plane_signing_key: jsonwebtoken::EncodingKey,
    fragment_client: reqwest::Client,
    router: gazette::Router,
}

impl TaskManager {
    pub fn new(
        api_client: flow_client::rest::Client,
        data_plane_fqdn: String,
        data_plane_signing_key: jsonwebtoken::EncodingKey,
        fragment_client: reqwest::Client,
        router: gazette::Router,
    ) -> Self {
        Self {
            tasks: std::sync::Mutex::new(HashMap::new()),
            api_client,
            data_plane_fqdn,
            data_plane_signing_key,
            fragment_client,
            router,
        }
    }

    /// Get or create a task watch for the given task name.
    /// The watch is cached and shared across sessions.
    pub fn get(&self, task_name: &str) -> tokens::PendingWatch<TaskState> {
        let mut tasks = self.tasks.lock().unwrap();

        // Check if we have an existing watch that's still valid
        if let Some(existing) = tasks.get(task_name) {
            // Clone returns a new handle to the same watch
            return existing.clone();
        }

        // Create a new task watch
        let watch = new_task_watch(
            self.api_client.clone(),
            self.data_plane_fqdn.clone(),
            self.data_plane_signing_key.clone(),
            self.fragment_client.clone(),
            self.router.clone(),
            task_name.to_string(),
        );

        tasks.insert(task_name.to_string(), watch.clone());
        watch
    }
}

fn new_task_watch(
    api_client: flow_client::rest::Client,
    data_plane_fqdn: String,
    data_plane_signing_key: jsonwebtoken::EncodingKey,
    fragment_client: reqwest::Client,
    router: gazette::Router,
    task_name: String,
) -> tokens::PendingWatch<TaskState> {
    // Create a token::Source that self-signs authorization requests for the Dekaf task.
    let signed_source = flow_client::workflows::task_dekaf_auth::new_signed_source(
        task_name,
        data_plane_fqdn.clone(),
        data_plane_signing_key.clone(),
    );
    // Map through the TaskDekafAuth workflow to obtain tokens from the authorization API.
    let task_dekaf_auth = flow_client::workflows::TaskDekafAuth {
        client: api_client.clone(),
        signed_source,
    };

    tokens::watch(task_dekaf_auth).map(move |response, prior| {
        process_task(
            &response,
            prior,
            api_client.clone(),
            router.clone(),
            fragment_client.clone(),
            data_plane_fqdn.clone(),
            data_plane_signing_key.clone(),
        )
    })
}

pub fn process_task(
    response: &DekafAuthResponse,
    prior: Option<(&DekafAuthResponse, &TaskState)>,
    api_client: flow_client::rest::Client,
    router: gazette::Router,
    fragment_client: reqwest::Client,
    data_plane_fqdn: String,
    data_plane_signing_key: jsonwebtoken::EncodingKey,
) -> tonic::Result<TaskState> {
    let DekafAuthResponse {
        ops_logs_journal,
        ops_stats_journal,
        redirect_dataplane_fqdn,
        task_spec,
        token,
        retry_millis: _,
    } = response;

    if let Some(redirect) = redirect_dataplane_fqdn.clone() {
        return Ok(TaskState::Redirect {
            target_dataplane_fqdn: redirect,
        });
    }

    let spec = task_spec.as_ref().map(|s| s.get()).unwrap_or_default();
    let spec: flow::MaterializationSpec = serde_json::from_str(spec).map_err(|e| {
        tonic::Status::internal(format!("failed to parse MaterializationSpec: {e}"))
    })?;

    let flow::MaterializationSpec {
        bindings,
        config_json,
        connector_type,
        inactive_bindings: _,
        name,
        network_ports: _,
        recovery_log_template: _,
        shard_template,
    } = spec;

    let shard_template = shard_template
        .ok_or_else(|| tonic::Status::internal("MaterializationSpec missing shard_template"))?;

    // Extract AuthorizedTask from prior TaskState for re-use, if available.
    let prior_auth = match prior {
        Some((_, TaskState::Authorized(auth))) => Some(auth),
        _ => None,
    };

    // Create or reuse the DekafConfig watch
    let config_json_vec = config_json.to_vec();
    let dekaf_config = new_dekaf_config_watch(
        config_json_vec.clone(),
        connector_type,
        prior_auth.map(|auth| (auth.config_json.as_slice(), &auth.dekaf_config)),
    );

    let bindings = bindings
        .into_iter()
        .map(|binding| {
            process_task_binding(
                binding,
                prior_auth.map(|auth| auth.bindings.as_slice()).unwrap_or(&[]),
                api_client.clone(),
                router.clone(),
                fragment_client.clone(),
                data_plane_fqdn.clone(),
                data_plane_signing_key.clone(),
                &shard_template.id,
            )
        })
        .collect::<tonic::Result<Vec<TaskBinding>>>()?;

    // Extract journal::Clients for the logs and stats journals for re-use, if available.
    // Otherwise, we must start new journal clients.
    let (ops_logs_client, ops_stats_client) =
        if let Some(auth) = prior_auth
            && &auth.ops_logs_journal == ops_logs_journal
            && &auth.ops_stats_journal == ops_stats_journal
        {
            (auth.ops_logs_client.clone(), auth.ops_stats_client.clone())
        } else {
            let slice = &[ops_logs_journal, ops_stats_journal];
            let mut it = slice.iter().map(|journal_name| {
                new_journal_client(
                    api_client.clone(),
                    proto_gazette::capability::APPEND,
                    router.clone(),
                    fragment_client.clone(),
                    data_plane_fqdn.clone(),
                    data_plane_signing_key.clone(),
                    journal_name,
                    &shard_template.id,
                )
            });
            (it.next().unwrap(), it.next().unwrap())
        };

    Ok(TaskState::Authorized(AuthorizedTask {
        bindings,
        config_json: config_json_vec,
        dekaf_config,
        name,
        ops_logs_client,
        ops_logs_journal: ops_logs_journal.clone(),
        ops_stats_client,
        ops_stats_journal: ops_stats_journal.clone(),
        schema_access_token: token.clone(),
        shard_template,
    }))
}

/// Create a PendingWatch for DekafConfig that handles async sops decryption.
/// If the config_json hasn't changed from the prior watch, reuse it to avoid re-decryption.
fn new_dekaf_config_watch(
    config_json: Vec<u8>,
    connector_type: i32,
    prior: Option<(&[u8], &tokens::PendingWatch<connector::DekafConfig>)>,
) -> tokens::PendingWatch<connector::DekafConfig> {
    // Check if config is unchanged - reuse prior watch to avoid expensive re-decryption
    if let Some((prior_config, prior_watch)) = prior {
        if prior_config == config_json {
            return prior_watch.clone();
        }
    }

    // Create manual watch for async decryption
    let (pending, update_fn) = tokens::manual();
    let update_fn = Arc::new(std::sync::Mutex::new(Some(update_fn)));

    tokio::spawn(async move {
        let result = decrypt_dekaf_config(config_json, connector_type).await;
        if let Some(update) = update_fn.lock().unwrap().take() {
            let _ = update(result);
        }
    });

    pending
}

/// Decrypt the DekafConfig from the MaterializationSpec's config_json.
async fn decrypt_dekaf_config(
    config_json: Vec<u8>,
    connector_type: i32,
) -> tonic::Result<connector::DekafConfig> {
    use models::RawValue;

    if connector_type != flow::materialization_spec::ConnectorType::Dekaf as i32 {
        return Err(tonic::Status::invalid_argument("Not a Dekaf materialization"));
    }

    let config: models::DekafConfig = serde_json::from_slice(&config_json).map_err(|e| {
        tonic::Status::internal(format!("failed to parse DekafConfig wrapper: {e}"))
    })?;

    let raw_value = RawValue::from_str(&config.config.to_string()).map_err(|e| {
        tonic::Status::internal(format!("failed to create RawValue: {e}"))
    })?;

    let decrypted = unseal::decrypt_sops(&raw_value).await.map_err(|e| {
        tonic::Status::internal(format!("failed to decrypt sops config: {e}"))
    })?;

    let dekaf_config: connector::DekafConfig =
        serde_json::from_str(decrypted.get()).map_err(|e| {
            tonic::Status::internal(format!("failed to parse decrypted DekafConfig: {e}"))
        })?;

    Ok(dekaf_config)
}

fn process_task_binding(
    binding: flow::materialization_spec::Binding,
    prior_bindings: &[TaskBinding],
    api_client: flow_client::rest::Client,
    router: gazette::Router,
    fragment_client: reqwest::Client,
    data_plane_fqdn: String,
    data_plane_signing_key: jsonwebtoken::EncodingKey,
    shard_template_id: &str,
) -> tonic::Result<TaskBinding> {
    let flow::materialization_spec::Binding {
        collection,
        resource_config_json: _,
        resource_path,
        partition_selector,
        field_selection,
        priority: _,
        delta_updates: _,
        deprecated_shuffle: _,
        journal_read_suffix: _,
        not_before,
        not_after,
        backfill,
        state_key: _,
        ser_policy: _,
    } = binding;

    let collection = some_or(collection, "MaterializationSpec.Binding missing collection")?;
    let field_selection = some_or(
        field_selection,
        "MaterializationSpec.Binding missing field_selection",
    )?;

    let partition_template = some_or(
        collection.partition_template.as_ref(),
        "MaterializationSpec.Binding.Collection missing partition template",
    )?;

    let partition_selector = some_or(
        partition_selector,
        "MaterializationSpec.Binding missing partition selector",
    )?;

    // TODO(johnny): Remove once we're consistently populating this label.
    let partition_selector = proto_gazette::LabelSelector {
        include: Some(labels::set_value(
            partition_selector.include.unwrap_or_default(),
            "name:prefix",
            &partition_template.name,
        )),
        exclude: partition_selector.exclude,
    };

    // See if we can find a prior binding having the same partition selector.
    let (journal_client, partitions) = if let Some(prior) = prior_bindings
        .iter()
        .find(|prior| prior.partition_selector == partition_selector)
    {
        // We can re-use this binding's journal client and partitions watch.
        (prior.journal_client.clone(), prior.partitions.clone())
    } else {
        new_journal_client_with_partitions(
            api_client,
            router,
            fragment_client,
            data_plane_fqdn,
            data_plane_signing_key,
            partition_template.name.clone(),
            shard_template_id,
            partition_selector.clone(),
        )
    };

    Ok(TaskBinding {
        backfill,
        collection,
        field_selection,
        journal_client,
        not_after,
        not_before,
        partition_selector,
        partitions,
        resource_path,
    })
}

fn process_list_response(
    list_response: proto_gazette::broker::ListResponse,
) -> tonic::Result<Vec<topology::Partition>> {
    fn map_journal(
        journal: proto_gazette::broker::list_response::Journal,
    ) -> tonic::Result<topology::Partition> {
        let broker::list_response::Journal {
            create_revision,
            mod_revision,
            route,
            spec,
        } = journal;

        let route = some_or(route, "ListResponse journal missing Route")?;
        let spec = some_or(spec, "ListResponse journal missing JournalSpec")?;

        Ok(topology::Partition {
            create_revision,
            mod_revision,
            route,
            spec,
        })
    }

    let mut partitions = list_response
        .journals
        .into_iter()
        .map(map_journal)
        .collect::<tonic::Result<Vec<topology::Partition>>>()?;

    // Establish stability of exposed partition indices by ordering journals
    // by their created revision, and _then_ by their name.
    partitions
        .sort_by(|l, r| (l.create_revision, &l.spec.name).cmp(&(r.create_revision, &r.spec.name)));

    Ok(partitions)
}

fn new_journal_client_with_partitions(
    api_client: flow_client::rest::Client,
    router: gazette::Router,
    fragment_client: reqwest::Client,
    data_plane_fqdn: String,
    data_plane_signing_key: jsonwebtoken::EncodingKey,
    partition_template_name: String,
    shard_template_id: &str,
    partition_selector: proto_gazette::LabelSelector,
) -> (
    journal::Client,
    tokens::PendingWatch<Vec<topology::Partition>>,
) {
    // Create a journal client with LIST and READ capabilities for this journal prefix.
    let journal_client = new_journal_client(
        api_client,
        proto_gazette::capability::LIST | proto_gazette::capability::READ,
        router,
        fragment_client,
        data_plane_fqdn,
        data_plane_signing_key,
        &partition_template_name,
        shard_template_id,
    );

    // Start a long-lived journal list watch RPC, so that brokers stream ongoing
    // partition changes as they occur.
    let list_request = proto_gazette::broker::ListRequest {
        selector: Some(partition_selector),
        ..Default::default()
    };
    let list_stream = journal_client.clone().list_watch(list_request);

    // Adapt from Stream<Item = gazette:RetryResult<..>> to Stream<Item = tonic::Result<..>>,
    // where transient errors are logged and suppressed.
    let list_stream = flow_client::adapt_gazette_retry_stream(list_stream, move |attempt, err| {
        tracing::warn!(
            "failed to list journals for prefix {} (attempt {}), will retry: {err:#}",
            partition_template_name,
            attempt
        );
        None
    });
    // Map ListResponses to Vec<topology::Partition>.
    let list_stream = list_stream
        .and_then(|list_response| std::future::ready(process_list_response(list_response)));

    // Start a watch of the partitions stream.
    let partitions = tokens::watch(tokens::StreamSource::new(list_stream));

    (journal_client, partitions)
}

fn new_journal_client(
    api_client: flow_client::rest::Client,
    capability: u32,
    router: gazette::Router,
    fragment_client: reqwest::Client,
    data_plane_fqdn: String,
    data_plane_signing_key: jsonwebtoken::EncodingKey,
    journal_name_or_prefix: &str,
    shard_template_id: &str,
) -> journal::Client {
    // Create a token::Source that self-signs authorization requests for the journal prefix.
    let signed_source = flow_client::workflows::task_collection_auth::new_signed_source(
        journal_name_or_prefix.to_string(),
        shard_template_id.to_string(),
        capability,
        data_plane_fqdn,
        data_plane_signing_key,
    );
    // Map through the TaskCollectionAuth workflow to obtain tokens from the authorization API.
    let task_collection_auth = flow_client::workflows::TaskCollectionAuth {
        client: api_client,
        signed_source,
    };
    // Wrap the TaskCollectionAuth in a journal::Client.
    flow_client::workflows::task_collection_auth::new_journal_client(
        router,
        fragment_client,
        tokens::watch(task_collection_auth),
    )
}

fn some_or<T>(opt: Option<T>, err_msg: &str) -> tonic::Result<T> {
    opt.ok_or_else(|| tonic::Status::unknown(err_msg))
}
