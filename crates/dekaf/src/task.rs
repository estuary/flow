//! Model and shared, self-refreshing state of a Dekaf task.
//!
//! [`Task`] is the model of a Dekaf materialization: what its built spec means
//! at runtime, compiled once per build into the [`Binding`]s it serves as
//! topics and the [`Source`] collections those bindings read. It follows the
//! `Task` / `Binding` / `Source` taxonomy of the `runtime-next` and `shuffle`
//! crates.
//!
//! Many sessions of one task need the same things -- its Task, its endpoint
//! config, a control-plane token, journal clients, and the current journals
//! of each binding -- so a single [`Handle`] holds them and every session
//! reads through it. Everything refreshes on its own: a session reads the
//! latest value at each access rather than being handed a snapshot which
//! quietly expires underneath it.

use crate::{connector, topology::Partition, utils};
use anyhow::Context;
use gazette::{broker, journal, uuid};
use proto_flow::flow::{self, MaterializationSpec};
use std::{collections::HashMap, sync::Arc};
use tokens::{TimeDelta, Watch};

/// How long a Handle lingers after its last lookup. A Kafka client which
/// reconnects within this window doesn't pay for a cold `/authorize/dekaf`
/// and a cold journal listing.
const LINGER: std::time::Duration = std::time::Duration::from_secs(30);

/// Task is the model of a Dekaf materialization, compiled once per build.
///
/// A Binding holds what's derived from a materialization binding, a Source
/// holds what's derived from the collection it reads, and bindings reference
/// their Source by index.
pub struct Task {
    /// Catalog name of the materialization.
    pub name: String,
    /// Build ID which produced the spec, from its shard-template labels.
    pub build: String,
    /// Log level of the task's shard template.
    pub log_level: ops::LogLevel,
    /// Deletion mode of the endpoint config, under which `Binding::extractors`
    /// were built and which reads apply when they filter deletions.
    pub deletions: connector::DeletionMode,
    /// Bindings of the materialization, sorted by topic.
    pub bindings: Vec<Binding>,
    /// Source collections read by the task's bindings.
    pub sources: Vec<Source>,
}

/// Binding is a materialization binding, served as one Kafka topic.
pub struct Binding {
    /// Kafka topic of this binding: the first component of its resource path.
    pub topic: String,
    /// Index of this binding's source collection within [`Task::sources`].
    pub source: u32,
    /// Leader epoch advertised for every partition of this topic, which is the
    /// binding's backfill counter plus one. Consumers don't run their
    /// truncation-detection logic on a 0 -> 1 transition, so the first
    /// backfill must move the epoch from 1 to 2. It's also mixed into
    /// upstream topic names, which isolates a backfilled binding's group state.
    pub leader_epoch: u32,
    /// Selector of the source journals this binding reads.
    pub partition_selector: broker::LabelSelector,
    /// Non-ACK documents with clocks before this value are not served.
    pub not_before: Option<uuid::Clock>,
    /// Non-ACK documents with clocks after this value are not served.
    pub not_after: Option<uuid::Clock>,
    /// Avro schema of this binding's selected fields, as served.
    pub value_schema: avro::Schema,
    /// Extractors of the selected fields, each paired with its Avro schema.
    pub extractors: Vec<(avro::Schema, utils::CustomizableExtractor)>,
    /// Content address of the Source's key schema and this value schema. A
    /// session compares it to notice a binding whose served schema changed,
    /// without a network round trip (unlike the registry's `avro_schema_md5`,
    /// which addresses the same schemas against the control plane).
    pub schema_hash: String,
}

/// Source is a collection read by one or more bindings of the Task.
pub struct Source {
    /// Collection name.
    pub collection_name: String,
    /// Name of the collection's partition template, which prefixes its journals.
    pub partition_template: String,
    /// Pointers of the collection key.
    pub key_ptr: Vec<json::Pointer>,
    /// Pointer at which document UUIDs are found.
    pub uuid_ptr: json::Pointer,
    /// Avro schema of the collection key, as served.
    pub key_schema: avro::Schema,
}

impl Task {
    /// Compile `spec` under `labeling`, the decoded labels of its shard
    /// template, and `deletions`, the deletion mode of its endpoint config.
    pub fn new(
        spec: &MaterializationSpec,
        labeling: &ops::ShardLabeling,
        deletions: connector::DeletionMode,
    ) -> anyhow::Result<Self> {
        let MaterializationSpec {
            bindings: spec_bindings,
            config_json: _, // Read by `sealed_config`.
            connector_type: _,
            created_at: _,
            inactive_bindings: _,
            linked_collections: _, // Walked by `resolved_bindings`.
            name,
            network_ports: _,
            recovery_log_template: _,
            secrets: _,
            shard_template: _, // Decoded into `labeling`.
            sync_schedule_json: _,
            triggers_json: _,
        } = spec;

        let mut bindings = Vec::with_capacity(spec_bindings.len());
        let mut sources = Vec::<Source>::new();
        let mut shapes = Vec::<doc::Shape>::new();
        let mut sources_by_identity = std::collections::BTreeMap::<u32, u32>::new();

        for (index, (binding, resolved)) in spec.resolved_bindings().enumerate() {
            let (collection, identity) = resolved.context("missing collection").context(index)?;

            let source = match identity.and_then(|i| sources_by_identity.get(&i)) {
                Some(&source) => source,
                None => {
                    let source = sources.len() as u32;
                    let (built, shape) = build_source(collection).context(index)?;
                    sources.push(built);
                    shapes.push(shape);

                    if let Some(identity) = identity {
                        sources_by_identity.insert(identity, source);
                    }
                    source
                }
            };
            bindings.push(
                build_binding(
                    binding,
                    collection,
                    source,
                    &sources[source as usize],
                    &shapes[source as usize],
                    deletions,
                )
                .context(index)?,
            );
        }

        // `binding()` binary-searches on topic, and `resolved_bindings()`
        // yields spec order.
        bindings.sort_by(|l, r| l.topic.cmp(&r.topic));

        // Validation requires distinct resource paths of a materialization,
        // so equal topics mean an unvalidated spec reached the runtime.
        if let Some(pair) = bindings
            .windows(2)
            .find(|pair| pair[0].topic == pair[1].topic)
        {
            anyhow::bail!("multiple bindings serve topic {}", pair[0].topic);
        }

        Ok(Self {
            name: name.clone(),
            build: labeling.build.clone(),
            log_level: labeling.log_level(),
            deletions,
            bindings,
            sources,
        })
    }

    /// Index within [`Task::bindings`] of the binding serving `topic`.
    pub fn binding(&self, topic: &str) -> Option<usize> {
        self.bindings
            .binary_search_by(|binding| binding.topic.as_str().cmp(topic))
            .ok()
    }
}

/// Build the [`Binding`] of one materialization binding, which reads
/// `collection` as `source`, whose Source and inferred Shape are given.
fn build_binding(
    binding: &flow::materialization_spec::Binding,
    collection: &flow::CollectionSpec,
    source: u32,
    Source {
        partition_template,
        key_schema,
        ..
    }: &Source,
    shape: &doc::Shape,
    deletions: connector::DeletionMode,
) -> anyhow::Result<Binding> {
    let flow::materialization_spec::Binding {
        backfill,
        collection: _,
        collection_index: _,
        delta_updates: _,
        deprecated_shuffle: _,
        field_selection,
        journal_read_suffix: _,
        not_after,
        not_before,
        partition_selector,
        priority: _,
        resource_config_json: _,
        resource_path,
        ser_policy: _,
        state_key: _,
    } = binding;

    let topic = resource_path
        .first()
        .context("missing resource path")?
        .clone();
    let field_selection = field_selection
        .as_ref()
        .context("missing field selection")?;
    let partition_selector = partition_selector
        .as_ref()
        .context("missing partition selector")?;

    // TODO(johnny): Specs built since 2026-02 carry a `name:prefix` label
    // scoping the selector to the collection's current partition template.
    // Older builds select on the collection label alone.
    // Set it here so both forms list only the current generation. Remove
    // once every Dekaf task has been confirmed re-published, and pass the
    // built selector through as `shuffle` does.
    let partition_selector = broker::LabelSelector {
        include: Some(labels::set_value(
            partition_selector.include.clone().unwrap_or_default(),
            "name:prefix",
            &format!("{partition_template}/"),
        )),
        exclude: partition_selector.exclude.clone(),
    };

    let (value_schema, extractors) =
        utils::build_field_extractors(shape, field_selection, &collection.projections, deletions)?;

    // We map into a serde_json::Value to ensure stability of property order.
    let schema_hash = {
        let key_json = serde_json::to_value(key_schema).unwrap().to_string();
        let value_json = serde_json::to_value(&value_schema).unwrap().to_string();
        format!("{:x}", md5::compute(format!("{key_json}{value_json}")))
    };

    Ok(Binding {
        topic,
        source,
        leader_epoch: backfill + 1,
        partition_selector,
        not_before: not_before
            .as_ref()
            .map(|ts| uuid::Clock::from_unix(ts.seconds as u64, ts.nanos as u32)),
        not_after: not_after
            .as_ref()
            .map(|ts| uuid::Clock::from_unix(ts.seconds as u64, ts.nanos as u32)),
        value_schema,
        extractors,
        schema_hash,
    })
}

/// Build the [`Source`] of `collection`, returning it with the inferred Shape
/// of the schema its documents are served under.
fn build_source(collection: &flow::CollectionSpec) -> anyhow::Result<(Source, doc::Shape)> {
    let partition_template = collection
        .partition_template
        .as_ref()
        .context("missing partition template")?
        .name
        .clone();

    // Documents are served under the read schema, or the write schema when no
    // read schema is defined.
    let bundle = if collection.read_schema_json.is_empty() {
        &collection.write_schema_json
    } else {
        &collection.read_schema_json
    };
    let schema = doc::validation::build_bundle(bundle)
        .with_context(|| format!("parsing schema of collection {}", collection.name))?;
    let validator = doc::Validator::new(schema)
        .with_context(|| format!("indexing schema of collection {}", collection.name))?;
    let shape = doc::Shape::infer(validator.schema(), validator.schema_index());

    let key_ptr: Vec<json::Pointer> = collection
        .key
        .iter()
        .map(|p| json::Pointer::from_str(p))
        .collect();
    let key_schema = avro::key_to_avro(&key_ptr, shape.clone());

    Ok((
        Source {
            collection_name: collection.name.clone(),
            partition_template,
            key_ptr,
            uuid_ptr: json::Pointer::from_str(&collection.uuid_ptr),
            key_schema,
        },
        shape,
    ))
}

/// Where a migrated task's sessions must go instead.
#[derive(Clone, Debug)]
pub struct Redirect {
    pub data_plane_fqdn: String,
    pub dekaf_address: Option<String>,
    pub dekaf_registry_address: Option<String>,
}

/// The fields of a Dekaf endpoint config which are read from the *sealed*
/// document, without resolving it.
///
/// Only `/token` is `secret: true` in [`connector::DekafConfig`]'s schema, so
/// these are plaintext in every well-formed config. That's what lets a task
/// which has migrated to another data-plane still serve Metadata:
/// the source plane is unable to resolve config plaintext of a moved task,
/// and must not be required to.
#[derive(Clone, Debug)]
pub struct PublicConfig {
    pub deletions: connector::DeletionMode,
    pub strict_topic_names: bool,
}

/// TaskToken is one version of a Dekaf task as this plane serves it, as of
/// one `/authorize/dekaf` response.
pub enum TaskToken {
    Authorized(Arc<Authorized>),
    /// The task now runs in a different data-plane.
    Redirect {
        public: PublicConfig,
        redirect: Redirect,
        task: Arc<Task>,
    },
}

impl TaskToken {
    pub fn public(&self) -> &PublicConfig {
        match self {
            Self::Authorized(authorized) => &authorized.public,
            Self::Redirect { public, .. } => public,
        }
    }

    pub fn task(&self) -> &Arc<Task> {
        match self {
            Self::Authorized(authorized) => &authorized.task,
            Self::Redirect { task, .. } => task,
        }
    }

    /// The Authorized version, or an error naming where a redirected task runs.
    pub fn authorized(&self) -> anyhow::Result<&Arc<Authorized>> {
        match self {
            Self::Authorized(authorized) => Ok(authorized),
            Self::Redirect { redirect, task, .. } => anyhow::bail!(
                "task {} has been redirected to {}",
                task.name,
                redirect.data_plane_fqdn
            ),
        }
    }

    /// Ask that this authorization be re-fetched now. A redirect has nothing
    /// to re-fetch sooner than its own cadence, and ignores this.
    pub fn revoke(&self) {
        if let Self::Authorized(authorized) = self {
            authorized.revoke.cancel();
        }
    }
}

/// Journal client of a Source, started on first use.
type LazyClient = Arc<std::sync::OnceLock<journal::Client>>;
/// Journals listing of a Binding, started on first use.
type LazyPartitions = Arc<std::sync::OnceLock<tokens::PendingWatch<Vec<Partition>>>>;

/// Authorized is a version of the task which this plane serves: its compiled
/// Task, its authorization, and the data-plane resources which read it.
///
/// A Source's client and a Binding's listing start on first use rather than
/// when the version is built. Each costs a refreshing `/authorize/task` or a
/// held broker stream, and a Kafka client typically subscribes to few of a
/// task's bindings. This is a deliberate difference from `shuffle`, whose
/// shards read every binding and so list eagerly. Once started, a listing is
/// carried into the next version whose binding is unchanged.
pub struct Authorized {
    /// Control-plane access token, which authorizes PostgREST queries of
    /// the Avro schema registry.
    pub access_token: String,
    pub public: PublicConfig,
    /// Cancel to ask that this authorization be re-fetched now, because
    /// we've seen something only a newer response can explain.
    pub revoke: tokens::CancellationToken,
    pub task: Arc<Task>,
    /// Password which authenticates sessions of this task: the only value
    /// taken from the resolved endpoint config.
    pub token: String,
    pub ops_logs: OpsAppender,
    pub ops_stats: OpsAppender,
    /// Builds a Source's client, on first use.
    client_factory: journal::ClientFactory,
    /// Parallel to `task.sources`.
    source_clients: Vec<LazyClient>,
    /// Parallel to `task.bindings`.
    binding_partitions: Vec<LazyPartitions>,
}

impl Authorized {
    /// Topic served by the binding named `topic`.
    /// None if `topic` is not (or is no longer) a binding.
    pub fn topic(self: &Arc<Self>, topic: &str) -> Option<Topic> {
        self.task.binding(topic).map(|binding| Topic {
            authorized: self.clone(),
            binding,
        })
    }
}

/// Topic is a `Binding` of an `Authorized`, as a Kafka client sees it:
/// with the client which reads its Source the listing of its journals
/// (which are the topic's partitions).
#[derive(Clone)]
pub struct Topic {
    authorized: Arc<Authorized>,
    binding: usize,
}

impl Topic {
    pub fn task(&self) -> &Arc<Task> {
        &self.authorized.task
    }

    pub fn binding(&self) -> &Binding {
        &self.authorized.task.bindings[self.binding]
    }

    pub fn source(&self) -> &Source {
        &self.authorized.task.sources[self.binding().source as usize]
    }

    /// Client authorized to list and read the Source's journals, started on
    /// first call. Bindings of one Source share it.
    pub fn client(&self) -> &journal::Client {
        let source = self.binding().source as usize;

        self.authorized.source_clients[source].get_or_init(|| {
            let subject = crate::dekaf_shard_template_id(&self.authorized.task.name);
            (self.authorized.client_factory)(
                subject,
                format!("{}/", self.source().partition_template),
            )
        })
    }

    /// Listing of the Binding's journals in stable Kafka partition order,
    /// started on first call.
    pub async fn partitions(&self) -> Arc<tokens::Refresh<Vec<Partition>>> {
        self.authorized.binding_partitions[self.binding]
            .get_or_init(|| {
                new_partitions_watch(
                    self.authorized.task.name.clone(),
                    self.binding().topic.clone(),
                    self.client().clone(),
                    self.binding().partition_selector.clone(),
                )
            })
            .ready()
            .await
            .token()
    }
}

/// An ops journal and the client which appends to it.
#[derive(Clone)]
pub struct OpsAppender {
    pub client: journal::Client,
    pub journal: String,
}

/// Env is the process-wide environment from which every Handle is built.
pub struct Env {
    /// Client of the control-plane agent API.
    pub api_client: flow_client_next::rest::Client,
    /// FQDN of the data-plane which Dekaf runs within.
    pub data_plane_fqdn: String,
    /// Key with which data-plane authorization requests are signed.
    pub data_plane_signer: tokens::jwt::EncodingKey,
    /// Factory of journal clients which append to a task's ops journals.
    pub ops_clients: journal::ClientFactory,
    /// Factory of journal clients which list and read a collection's journals.
    pub partition_clients: journal::ClientFactory,
    /// Upper bound on the refresh cadence of a task's authorization.
    pub max_refresh: TimeDelta,
}

/// Handle is the shared, refreshing state of one Dekaf task, through which
/// its sessions read the current TaskToken.
pub struct Handle {
    /// Catalog name of this task.
    pub name: String,
    token: tokens::PendingWatch<TaskToken>,
    /// Instant of the most recent Registry lookup, which the linger task
    /// waits out. Written only under the Registry's map lock, so that a lookup
    /// and the linger task's decision to drop are serialized; the Mutex is for
    /// the linger task's reads outside that lock.
    last_used: std::sync::Mutex<tokio::time::Instant>,
}

impl Handle {
    /// Await this Handle's first authorization. It may still be an error:
    /// callers inspect [`Handle::token`] to find out.
    pub async fn ready(&self) {
        _ = self.token.ready().await;
    }

    /// Current version of this task.
    pub fn token(&self) -> Arc<tokens::Refresh<TaskToken>> {
        self.token.watch().token()
    }

    fn last_used(&self) -> tokio::time::Instant {
        *self.last_used.lock().unwrap()
    }

    fn touch(&self) {
        *self.last_used.lock().unwrap() = tokio::time::Instant::now();
    }
}

/// Registry of the tasks which this process is currently serving.
pub struct Registry {
    env: Arc<Env>,
    handles: std::sync::Mutex<HashMap<String, std::sync::Weak<Handle>>>,
}

impl Registry {
    pub fn new(env: Env) -> Arc<Self> {
        Arc::new(Self {
            env: Arc::new(env),
            handles: std::sync::Mutex::new(HashMap::new()),
        })
    }

    /// Look up `name`, building it if this process isn't already serving it.
    pub fn get(self: &Arc<Self>, name: &str) -> Arc<Handle> {
        let mut handles = self.handles.lock().unwrap();

        if let Some(handle) = handles.get(name).and_then(std::sync::Weak::upgrade) {
            handle.touch();
            return handle;
        }
        tracing::info!(task_name = name, "starting to track task");

        let handle = Arc::new(new_handle(&self.env, name, resolve_config));
        handles.insert(name.to_string(), Arc::downgrade(&handle));

        tokio::spawn(Self::linger(self.clone(), name.to_string(), handle.clone()));

        handle
    }

    /// Hold one strong reference to `handle` until it's gone unused for LINGER.
    async fn linger(self: Arc<Self>, name: String, handle: Arc<Handle>) {
        loop {
            () = tokio::time::sleep_until(handle.last_used() + LINGER).await;

            let mut handles = self.handles.lock().unwrap();

            // `get()` touches `last_used` while holding this same lock, so a
            // lookup which raced our wake-up has already moved the deadline.
            if handle.last_used() + LINGER > tokio::time::Instant::now() {
                continue;
            }
            // A session may hold this Handle without having looked it up recently.
            // Re-arm and keep it until that session is gone, too.
            if Arc::strong_count(&handle) != 1 {
                handle.touch();
                continue;
            }
            tracing::info!(task_name = %name, "stopping tracking of unused task");
            handles.remove(&name);
            return;
        }
    }
}

/// Build the watch tree of a Handle. `resolve` turns a sealed endpoint config
/// into plaintext, and is a parameter so that tests may drive the tree
/// without running `sops`.
fn new_handle<Resolve, Fut>(env: &Arc<Env>, name: &str, resolve: Resolve) -> Handle
where
    Resolve: Fn(models::RawValue) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = anyhow::Result<connector::DekafConfig>> + Send + 'static,
{
    let auth = tokens::watch(flow_client_next::workflows::TaskDekafAuth::new(
        env.api_client.clone(),
        flow_client_next::workflows::task_dekaf_auth::new_signed_source(
            name.to_string(),
            env.data_plane_fqdn.clone(),
            env.data_plane_signer.clone(),
        ),
        env.max_refresh,
    ));

    let token = tokens::watch(tokens::StreamSource::new(resolve_stream(
        env.clone(),
        name.to_string(),
        auth.map(|t, p| parse(t, p).map_err(proto_grpc::anyhow_to_status)),
        resolve,
    )));

    Handle {
        name: name.to_string(),
        token,
        last_used: std::sync::Mutex::new(tokio::time::Instant::now()),
    }
}

/// Parsed is one `/authorize/dekaf` response with its MaterializationSpec
/// decoded and compiled into a Task.
struct Parsed {
    access_token: String,
    /// True if the build differs from the prior Parsed's. Every publication
    /// changes the build, and we use this to edge-trigger whether to re-resolve
    /// sealed configuration.
    changed: bool,
    ops_logs_journal: String,
    ops_stats_journal: String,
    /// Fields of the sealed endpoint config which need no resolution.
    public: PublicConfig,
    redirect: Option<Redirect>,
    revoke: tokens::CancellationToken,
    /// Sealed endpoint config, which resolution turns into plaintext.
    sealed: models::RawValue,
    /// Compiled from the spec when the build changed, and otherwise shared
    /// with the prior Parsed: the same build always decodes to the same spec.
    task: Arc<Task>,
}

fn parse(
    auth: &flow_client_next::workflows::DekafAuth,
    prior: Option<(&flow_client_next::workflows::DekafAuth, &Parsed)>,
) -> anyhow::Result<Parsed> {
    let models::authorizations::DekafAuthResponse {
        token,
        ops_logs_journal,
        ops_stats_journal,
        task_spec,
        redirect_dataplane_fqdn,
        redirect_dekaf_address,
        redirect_dekaf_registry_address,
        retry_millis: _,
    } = &auth.response;

    // Decode the spec and its shard labeling.
    let task_spec = task_spec
        .as_ref()
        .context("authorization response has no task spec")?;
    let spec: MaterializationSpec = serde_json::from_str(task_spec.get())?;

    let labels = spec
        .shard_template
        .as_ref()
        .context("built spec has no shard template")?
        .labels
        .as_ref()
        .context("shard template has no labels")?;
    let labeling = labels::shard::decode_labeling(labels).context("parsing shard labeling")?;

    let sealed = sealed_config(&spec).map_err(|err| tonic::Status::internal(format!("{err:#}")))?;
    let public = public_config(&sealed)?;

    let task = match prior {
        Some((_, prior)) if prior.task.build == labeling.build => prior.task.clone(),
        _ => Arc::new(Task::new(&spec, &labeling, public.deletions)?),
    };

    Ok(Parsed {
        access_token: token.clone(),
        changed: prior.map_or(true, |(_, prior)| prior.task.build != task.build),
        ops_logs_journal: ops_logs_journal.clone(),
        ops_stats_journal: ops_stats_journal.clone(),
        public,
        redirect: redirect_dataplane_fqdn
            .as_ref()
            .map(|data_plane_fqdn| Redirect {
                data_plane_fqdn: data_plane_fqdn.clone(),
                dekaf_address: redirect_dekaf_address.clone(),
                dekaf_registry_address: redirect_dekaf_registry_address.clone(),
            }),
        revoke: auth.revoke.clone(),
        sealed,
        task,
    })
}

/// Read the public fields of a sealed Dekaf endpoint config.
///
/// Each field is decoded on its own, so that a config which encrypts one of
/// them -- a `sops` `ENC[...]` string where a bool or an enum is expected --
/// says which. Everything else in the document is ignored, including the
/// `token`, which is encrypted.
fn public_config(sealed: &models::RawValue) -> anyhow::Result<PublicConfig> {
    #[derive(serde::Deserialize)]
    struct Fields<'a> {
        #[serde(default, borrow)]
        deletions: Option<&'a serde_json::value::RawValue>,
        #[serde(default, borrow)]
        strict_topic_names: Option<&'a serde_json::value::RawValue>,
    }

    let Fields {
        deletions,
        strict_topic_names,
    } = serde_json::from_str(sealed.get()).context("decoding Dekaf endpoint configuration")?;

    // An omitted field takes the serde default of `connector::DekafConfig`.
    fn field<T: Default + serde::de::DeserializeOwned>(
        name: &str,
        raw: Option<&serde_json::value::RawValue>,
    ) -> anyhow::Result<T> {
        let Some(raw) = raw else {
            return Ok(T::default());
        };
        serde_json::from_str(raw.get()).with_context(|| {
            format!("`{name}` of the Dekaf endpoint configuration must be plaintext")
        })
    }

    Ok(PublicConfig {
        deletions: field("deletions", deletions)?,
        strict_topic_names: field("strict_topic_names", strict_topic_names)?,
    })
}

/// Stream one TaskToken per version of `parsed`, resolving its endpoint
/// config whenever the build changes.
fn resolve_stream<Resolve, Fut>(
    env: Arc<Env>,
    task_name: String,
    parsed: tokens::PendingWatch<Parsed>,
    resolve: Resolve,
) -> impl futures::Stream<Item = tonic::Result<TaskToken>>
where
    Resolve: Fn(models::RawValue) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = anyhow::Result<connector::DekafConfig>> + Send + 'static,
{
    coroutines::coroutine(move |mut co| async move {
        let parsed = parsed.ready_owned().await;

        // The most recent Authorized version, re-used when its Task is unchanged.
        // This avoids re-building token refresh loops and partition watches.
        let mut prior: Option<Arc<Authorized>> = None;

        loop {
            let refresh = parsed.token();
            let next = next_token(&env, &task_name, refresh.result(), &mut prior, &resolve).await;

            () = co.yield_(next).await;
            () = refresh.expired().await;
        }
    })
}

/// Map one version of Parsed into a TaskToken, resolving its endpoint config
/// if the build changed or if no plaintext is cached.
#[tracing::instrument(
    level = "debug",
    err(Debug, level = "warn"),
    skip_all,
    fields(%task_name, prior = %prior.is_some())
)]
async fn next_token<Resolve, Fut>(
    env: &Env,
    task_name: &str,
    parsed: tonic::Result<&Parsed>,
    prior: &mut Option<Arc<Authorized>>,
    resolve: &Resolve,
) -> tonic::Result<TaskToken>
where
    Resolve: Fn(models::RawValue) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<connector::DekafConfig>>,
{
    let parsed = match parsed {
        Ok(parsed) => parsed,
        Err(status) => {
            *prior = None;
            return Err(status);
        }
    };

    if let Some(redirect) = &parsed.redirect {
        *prior = None; // Task runs elsewhere.

        tracing::info!(
            task_name,
            build = parsed.task.build,
            data_plane_fqdn = redirect.data_plane_fqdn,
            "task authorization refreshed (redirected)"
        );

        return Ok(TaskToken::Redirect {
            public: parsed.public.clone(),
            redirect: redirect.clone(),
            task: parsed.task.clone(),
        });
    }

    let token = if let Some(prior) = prior
        && !parsed.changed
    {
        prior.token.clone()
    } else {
        match resolve(parsed.sealed.clone()).await {
            Ok(config) => config.token,
            Err(err) => {
                *prior = None;
                parsed.revoke.cancel();

                tracing::warn!(?err, "failed to resolve endpoint config");

                return Err(proto_grpc::anyhow_to_status(
                    err.context("failed to resolve endpoint config"),
                ));
            }
        }
    };

    tracing::info!(
        task_name,
        build = parsed.task.build,
        "task authorization refreshed"
    );

    let authorized = Arc::new(new_authorized(env, parsed, token, prior.as_deref()));
    *prior = Some(authorized.clone());

    Ok(TaskToken::Authorized(authorized))
}

/// The sealed endpoint config of a Dekaf materialization, which is nested
/// within a `models::DekafConfig` alongside its variant.
pub fn sealed_config(spec: &MaterializationSpec) -> anyhow::Result<models::RawValue> {
    if spec.connector_type != flow::materialization_spec::ConnectorType::Dekaf as i32 {
        anyhow::bail!("not a Dekaf materialization");
    }
    let config: models::DekafConfig = serde_json::from_slice(&spec.config_json)
        .context("decoding Dekaf endpoint configuration")?;

    Ok(config.config)
}

/// Resolve a sealed Dekaf endpoint config into its plaintext.
async fn resolve_config(sealed: models::RawValue) -> anyhow::Result<connector::DekafConfig> {
    let resolved = unseal::decrypt_sops(&sealed).await?;

    Ok(serde_json::from_str(resolved.get()).context("decoding resolved Dekaf configuration")?)
}

/// Build the Authorized version of `parsed` with password `token`, re-using
/// the clients, listings, and ops appenders of `prior` where its Source,
/// Binding, or ops journal is unchanged.
fn new_authorized(
    env: &Env,
    parsed: &Parsed,
    token: String,
    prior: Option<&Authorized>,
) -> Authorized {
    let task = &parsed.task;
    let subject = crate::dekaf_shard_template_id(&task.name);

    // A Source's client is authorized to its partition template,
    // and any prior client of that template serves it.
    let source_clients = task
        .sources
        .iter()
        .map(|source| {
            prior
                .and_then(|prior| {
                    prior
                        .task
                        .sources
                        .iter()
                        .position(|p| p.partition_template == source.partition_template)
                        .map(|index| prior.source_clients[index].clone())
                })
                .unwrap_or_default()
        })
        .collect();

    // Re-use the prior listing of a Binding whose selector is unchanged: the
    // selector scopes the template, so an equal selector lists the same
    // journals, and re-listing them would churn a long-lived watch.
    let binding_partitions = task
        .bindings
        .iter()
        .map(|binding| {
            prior
                .and_then(|prior| {
                    prior
                        .task
                        .binding(&binding.topic)
                        .filter(|&index| {
                            prior.task.bindings[index].partition_selector
                                == binding.partition_selector
                        })
                        .map(|index| prior.binding_partitions[index].clone())
                })
                .unwrap_or_default()
        })
        .collect();

    let ops_appender = |journal: &String, prior: Option<&OpsAppender>| -> OpsAppender {
        if let Some(prior) = prior {
            if prior.journal == *journal {
                return prior.clone();
            }
        }
        OpsAppender {
            client: (env.ops_clients)(subject.clone(), journal.clone()),
            journal: journal.clone(),
        }
    };

    Authorized {
        access_token: parsed.access_token.clone(),
        public: parsed.public.clone(),
        revoke: parsed.revoke.clone(),
        task: task.clone(),
        token,
        ops_logs: ops_appender(&parsed.ops_logs_journal, prior.map(|p| &p.ops_logs)),
        ops_stats: ops_appender(&parsed.ops_stats_journal, prior.map(|p| &p.ops_stats)),
        client_factory: env.partition_clients.clone(),
        source_clients,
        binding_partitions,
    }
}

/// Watch the journals of one binding, in stable Kafka partition order.
/// Gazette pushes a new snapshot on every change, and the broker bounds each
/// RPC by the token's expiry -- so a token-driven client re-authorizes
/// transparently across the life of the watch.
fn new_partitions_watch(
    task_name: String,
    topic: String,
    client: journal::Client,
    selector: broker::LabelSelector,
) -> tokens::PendingWatch<Vec<Partition>> {
    let stream = client.list_watch(broker::ListRequest {
        selector: Some(selector),
        watch: true,
        watch_resume: None,
    });

    let stream = flow_client_next::adapt_gazette_retry_stream(stream, move |attempt, err| {
        tracing::warn!(
            task_name,
            topic,
            attempt,
            ?err,
            "journal listing watch failed (will retry)"
        );
        None
    });

    tokens::watch(tokens::StreamSource::new(stream)).map(|response, _prior| partitions(response))
}

/// Map a listing snapshot into stable-order partitions.
///
/// Suspended journals are kept: Dekaf serves their offsets from the journal's
/// `suspend.offset`, and dropping them would renumber the partitions which
/// follow.
fn partitions(response: &broker::ListResponse) -> tonic::Result<Vec<Partition>> {
    let mut partitions = Vec::with_capacity(response.journals.len());

    for journal in &response.journals {
        let (Some(spec), Some(route)) = (journal.spec.clone(), journal.route.clone()) else {
            return Err(tonic::Status::internal(
                "listed journal is missing its spec or route",
            ));
        };
        partitions.push(Partition {
            create_revision: journal.create_revision,
            spec,
            mod_revision: journal.mod_revision,
            route,
        });
    }

    // Establish stability of exposed partition indices by ordering journals
    // by their created revision, and _then_ by their name.
    partitions
        .sort_by(|l, r| (l.create_revision, &l.spec.name).cmp(&(r.create_revision, &r.spec.name)));

    Ok(partitions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::flow_client_next::workflows::DekafAuth;

    /// How a [`MockResolve`] fails, if it does.
    #[derive(Clone, Copy, Default, PartialEq)]
    enum Failure {
        #[default]
        None,
        /// As `resolve_config` fails when `sops` cannot reach its key service.
        Transient,
        /// As `resolve_config` fails on a task misconfiguration.
        Misconfigured,
    }

    /// A resolver which records what it was asked to resolve, and which fails
    /// in the manner of its `Failure`.
    #[derive(Clone, Default)]
    struct MockResolve(Arc<std::sync::Mutex<(Vec<String>, Failure)>>);

    impl MockResolve {
        fn calls(&self) -> Vec<String> {
            self.0.lock().unwrap().0.clone()
        }
        fn fail(&self, failure: Failure) {
            self.0.lock().unwrap().1 = failure;
        }
        async fn resolve(self, sealed: models::RawValue) -> anyhow::Result<connector::DekafConfig> {
            let mut state = self.0.lock().unwrap();
            state.0.push(sealed.get().to_string());

            match state.1 {
                Failure::None => Ok(serde_json::from_str(sealed.get())?),
                Failure::Transient => anyhow::bail!("sops is unavailable"),
                Failure::Misconfigured => Err(proto_grpc::status_to_anyhow(
                    tonic::Status::invalid_argument("endpoint configuration is malformed"),
                )),
            }
        }
    }

    fn response(build: &str, token: &str) -> DekafAuth {
        response_with_config(
            build,
            serde_json::json!({"token": token, "strict_topic_names": false}),
        )
    }

    /// As [`response`], but for a task which now runs in another data-plane.
    /// Its `token` is empty because a redirect must not resolve one.
    fn response_redirect(build: &str) -> DekafAuth {
        let mut auth = response(build, "");
        auth.response.redirect_dataplane_fqdn = Some("other.dp.estuary-data.com".to_string());
        auth
    }

    fn response_with_config(build: &str, config: serde_json::Value) -> DekafAuth {
        response_with_spec(spec_fixture(build), config)
    }

    fn response_with_spec(spec: MaterializationSpec, config: serde_json::Value) -> DekafAuth {
        let spec = MaterializationSpec {
            config_json: serde_json::json!({
                "variant": "pineapple",
                "config": config,
            })
            .to_string()
            .into(),
            ..spec
        };

        DekafAuth {
            response: models::authorizations::DekafAuthResponse {
                token: "an-access-token".to_string(),
                ops_logs_journal: "ops/logs/pivot=00".to_string(),
                ops_stats_journal: "ops/stats/pivot=00".to_string(),
                task_spec: Some(
                    models::RawValue::from_string(serde_json::to_string(&spec).unwrap()).unwrap(),
                ),
                ..Default::default()
            },
            revoke: tokens::CancellationToken::new(),
        }
    }

    /// A Dekaf materialization of `build` with no bindings.
    fn spec_fixture(build: &str) -> MaterializationSpec {
        MaterializationSpec {
            name: "acmeCo/dekaf".to_string(),
            connector_type: flow::materialization_spec::ConnectorType::Dekaf as i32,
            shard_template: Some(proto_gazette::consumer::ShardSpec {
                id: crate::dekaf_shard_template_id("acmeCo/dekaf"),
                labels: Some(labels::build_set([
                    (labels::BUILD, build),
                    (labels::LOG_LEVEL, "info"),
                    (labels::TASK_NAME, "acmeCo/dekaf"),
                    (
                        labels::TASK_TYPE,
                        proto_flow::ops::TaskType::Materialization.as_str_name(),
                    ),
                ])),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn collection_fixture(name: &str) -> flow::CollectionSpec {
        let projection = |field: &str, ptr: &str, is_primary_key, type_: &str| flow::Projection {
            field: field.to_string(),
            ptr: ptr.to_string(),
            is_primary_key,
            inference: Some(flow::Inference {
                types: vec![type_.to_string()],
                ..Default::default()
            }),
            ..Default::default()
        };
        flow::CollectionSpec {
            name: name.to_string(),
            key: vec!["/id".to_string()],
            uuid_ptr: "/_meta/uuid".to_string(),
            partition_template: Some(broker::JournalSpec {
                name: format!("{name}/0011223344556677"),
                ..Default::default()
            }),
            projections: vec![
                projection("flow_document", "", false, "object"),
                projection("id", "/id", true, "integer"),
                projection("name", "/name", false, "string"),
            ],
            write_schema_json: serde_json::json!({
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "name": {"type": "string"},
                },
                "required": ["id"],
            })
            .to_string()
            .into(),
            ..Default::default()
        }
    }

    fn binding_fixture(
        topic: &str,
        collection_index: u32,
        backfill: u32,
    ) -> flow::materialization_spec::Binding {
        flow::materialization_spec::Binding {
            backfill,
            collection_index,
            field_selection: Some(flow::FieldSelection {
                keys: vec!["id".to_string()],
                values: vec!["name".to_string()],
                document: "flow_document".to_string(),
                ..Default::default()
            }),
            partition_selector: Some(broker::LabelSelector::default()),
            resource_path: vec![topic.to_string()],
            ..Default::default()
        }
    }

    /// Drive the resolve stream from a manual `/authorize/dekaf` Watch,
    /// returning the task Watch and the closure which pushes responses.
    fn fixture(
        resolve: MockResolve,
    ) -> (
        tokens::PendingWatch<TaskToken>,
        impl Fn(tonic::Result<DekafAuth>) -> Option<tokens::WaitForCancellationFutureOwned>,
    ) {
        let (auth, replace) = tokens::manual::<DekafAuth>();

        let stream = resolve_stream(
            Arc::new(env_fixture()),
            "acmeCo/dekaf".to_string(),
            auth.map(|t, p| parse(t, p).map_err(proto_grpc::anyhow_to_status)),
            move |sealed| resolve.clone().resolve(sealed),
        );
        (tokens::watch(tokens::StreamSource::new(stream)), replace)
    }

    /// An Env whose journal clients dispatch to nowhere: no test reads or
    /// appends through them.
    fn env_fixture() -> Env {
        let clients: journal::ClientFactory = Arc::new(|_subject, _prefix| {
            journal::Client::new(
                "http://[::1]:1".to_string(),
                journal::Client::new_fragment_client(),
                proto_grpc::Metadata::new(),
                gazette::Router::new("local"),
            )
        });
        Env {
            api_client: flow_client_next::rest::Client::new(
                &url::Url::parse("http://[::1]:1").unwrap(),
                "dekaf-test",
            ),
            data_plane_fqdn: "local.dp.estuary-data.com".to_string(),
            data_plane_signer: tokens::jwt::EncodingKey::from_base64_secret("c2VjcmV0").unwrap(),
            ops_clients: clients.clone(),
            partition_clients: clients,
            max_refresh: TimeDelta::seconds(60),
        }
    }

    fn authorized_of(refresh: &tokens::Refresh<TaskToken>) -> Arc<Authorized> {
        match refresh.result() {
            Ok(TaskToken::Authorized(authorized)) => authorized.clone(),
            other => panic!("expected an authorized token, not {:?}", other.map(|_| ())),
        }
    }

    fn token_of(refresh: &tokens::Refresh<TaskToken>) -> String {
        match refresh.result() {
            Ok(TaskToken::Authorized(authorized)) => format!("Ok({})", authorized.token),
            Ok(TaskToken::Redirect { redirect, .. }) => {
                format!("Redirect({})", redirect.data_plane_fqdn)
            }
            Err(status) => format!("Err({})", status.message()),
        }
    }

    /// An indirect-form spec groups bindings onto shared Sources, and bindings
    /// are sorted by topic regardless of their spec order.
    #[test]
    fn test_task_groups_sources_and_sorts_bindings() {
        let spec = MaterializationSpec {
            bindings: vec![
                binding_fixture("users", 1, 0),
                binding_fixture("orders", 0, 2),
                binding_fixture("orders_archive", 0, 0),
            ],
            linked_collections: vec![
                collection_fixture("acmeCo/orders"),
                collection_fixture("acmeCo/users"),
            ],
            ..spec_fixture("1111111111111111")
        };
        let labeling = labels::shard::decode_labeling(
            spec.shard_template
                .as_ref()
                .unwrap()
                .labels
                .as_ref()
                .unwrap(),
        )
        .unwrap();

        let task = Task::new(&spec, &labeling, connector::DeletionMode::CDC).unwrap();

        let bindings = task
            .bindings
            .iter()
            .map(|b| {
                (
                    b.topic.as_str(),
                    b.source,
                    b.leader_epoch,
                    labels::values(b.partition_selector.include.as_ref().unwrap(), "name")
                        .iter()
                        .map(|l| (l.value.as_str(), l.prefix))
                        .collect::<Vec<_>>(),
                    b.extractors.len(),
                    b.schema_hash.as_str(),
                )
            })
            .collect::<Vec<_>>();
        let sources = task
            .sources
            .iter()
            .map(|s| (s.collection_name.as_str(), s.partition_template.as_str()))
            .collect::<Vec<_>>();

        insta::assert_debug_snapshot!((task.build.as_str(), task.log_level, bindings, sources), @r###"
        (
            "1111111111111111",
            Info,
            [
                (
                    "orders",
                    1,
                    3,
                    [
                        (
                            "acmeCo/orders/0011223344556677/",
                            true,
                        ),
                    ],
                    4,
                    "ba691bfc429678614007f95ffede1259",
                ),
                (
                    "orders_archive",
                    1,
                    1,
                    [
                        (
                            "acmeCo/orders/0011223344556677/",
                            true,
                        ),
                    ],
                    4,
                    "ba691bfc429678614007f95ffede1259",
                ),
                (
                    "users",
                    0,
                    1,
                    [
                        (
                            "acmeCo/users/0011223344556677/",
                            true,
                        ),
                    ],
                    4,
                    "ba691bfc429678614007f95ffede1259",
                ),
            ],
            [
                (
                    "acmeCo/users",
                    "acmeCo/users/0011223344556677",
                ),
                (
                    "acmeCo/orders",
                    "acmeCo/orders/0011223344556677",
                ),
            ],
        )
        "###);

        assert_eq!(task.binding("orders_archive"), Some(1));
        assert_eq!(task.binding("users"), Some(2));
        assert_eq!(task.binding("nope"), None);
    }

    /// Two bindings of one topic can't both be served, and are refused rather
    /// than one of them being silently shadowed.
    #[test]
    fn test_task_rejects_duplicate_topics() {
        let spec = MaterializationSpec {
            bindings: vec![
                binding_fixture("orders", 0, 0),
                binding_fixture("orders", 0, 1),
            ],
            linked_collections: vec![collection_fixture("acmeCo/orders")],
            ..spec_fixture("1111111111111111")
        };
        let labeling = labels::shard::decode_labeling(
            spec.shard_template
                .as_ref()
                .unwrap()
                .labels
                .as_ref()
                .unwrap(),
        )
        .unwrap();

        let err = Task::new(&spec, &labeling, connector::DeletionMode::Kafka)
            .err()
            .unwrap();
        insta::assert_snapshot!(format!("{err:#}"), @"multiple bindings serve topic orders");
    }

    /// A binding's listing cell is carried into each next version whose
    /// selector is unchanged, across a build change, and is replaced when the
    /// selector changes. An error or a redirect drops every cell.
    #[tokio::test(start_paused = true)]
    async fn test_listings_carry_across_versions() {
        let resolve = MockResolve::default();
        let (task, replace) = fixture(resolve.clone());

        let spec = |build: &str, selector: broker::LabelSelector| MaterializationSpec {
            bindings: vec![flow::materialization_spec::Binding {
                partition_selector: Some(selector),
                ..binding_fixture("orders", 0, 0)
            }],
            linked_collections: vec![collection_fixture("acmeCo/orders")],
            ..spec_fixture(build)
        };
        let config = serde_json::json!({"token": "pw"});
        let narrowed = broker::LabelSelector {
            include: Some(labels::build_set([("estuary.dev/field/region", "eu")])),
            exclude: None,
        };

        _ = replace(Ok(response_with_spec(
            spec("1111111111111111", Default::default()),
            config.clone(),
        )));
        let task = task.ready_owned().await;
        let v1 = authorized_of(&task.token());

        // An error drops every cell: the version it would carry from is gone,
        // so the task which recovers starts cold.
        let refresh = task.token();
        _ = replace(Err(tonic::Status::unavailable("agent is down")));
        () = refresh.expired().await;
        assert!(task.token().result().is_err());

        let refresh = task.token();
        _ = replace(Ok(response_with_spec(
            spec("1111111111111111", Default::default()),
            config.clone(),
        )));
        () = refresh.expired().await;
        let v2 = authorized_of(&task.token());
        assert!(!Arc::ptr_eq(
            &v1.binding_partitions[0],
            &v2.binding_partitions[0]
        ));
        assert!(!Arc::ptr_eq(&v1.source_clients[0], &v2.source_clients[0]));

        // A new build with an unchanged binding keeps the listing.
        let refresh = task.token();
        _ = replace(Ok(response_with_spec(
            spec("2222222222222222", Default::default()),
            config.clone(),
        )));
        () = refresh.expired().await;
        let v3 = authorized_of(&task.token());
        assert!(Arc::ptr_eq(
            &v2.binding_partitions[0],
            &v3.binding_partitions[0]
        ));

        // A changed selector lists afresh, but the Source's client is kept.
        let refresh = task.token();
        _ = replace(Ok(response_with_spec(
            spec("3333333333333333", narrowed.clone()),
            config.clone(),
        )));
        () = refresh.expired().await;
        let v4 = authorized_of(&task.token());
        assert!(!Arc::ptr_eq(
            &v3.binding_partitions[0],
            &v4.binding_partitions[0]
        ));
        assert!(Arc::ptr_eq(&v2.source_clients[0], &v4.source_clients[0]));

        // A redirect drops everything: a task which migrates back starts cold.
        let refresh = task.token();
        let mut redirect =
            response_with_spec(spec("3333333333333333", narrowed.clone()), config.clone());
        redirect.response.redirect_dataplane_fqdn = Some("other.dp.estuary-data.com".to_string());
        _ = replace(Ok(redirect));
        () = refresh.expired().await;
        assert!(matches!(
            task.token().result(),
            Ok(TaskToken::Redirect { .. })
        ));

        let refresh = task.token();
        _ = replace(Ok(response_with_spec(
            spec("3333333333333333", narrowed),
            config,
        )));
        () = refresh.expired().await;
        let v5 = authorized_of(&task.token());
        assert!(!Arc::ptr_eq(
            &v4.binding_partitions[0],
            &v5.binding_partitions[0]
        ));
        assert!(!Arc::ptr_eq(&v4.source_clients[0], &v5.source_clients[0]));

        // The password was resolved for the first version, again after the
        // error, on each of the two build changes, and after the redirect.
        assert_eq!(resolve.calls().len(), 5);
    }

    #[tokio::test(start_paused = true)]
    async fn test_resolve_is_edge_triggered_on_build() {
        let resolve = MockResolve::default();
        let (task, replace) = fixture(resolve.clone());

        // The first version always resolves: nothing is cached.
        _ = replace(Ok(response("1111111111111111", "first")));
        let task = task.ready_owned().await;
        let mut observed = vec![token_of(&task.token())];

        // A version of the same build re-uses the cached plaintext, even
        // though this response's sealed config says otherwise -- the sealed
        // config changes only through a publication, which moves the build,
        // so that's the only occasion to resolve again.
        let refresh = task.token();
        _ = replace(Ok(response("1111111111111111", "ignored")));
        () = refresh.expired().await;
        observed.push(token_of(&task.token()));

        // A new build resolves again.
        let refresh = task.token();
        _ = replace(Ok(response("2222222222222222", "second")));
        () = refresh.expired().await;
        observed.push(token_of(&task.token()));

        insta::assert_debug_snapshot!((observed, resolve.calls()), @r###"
        (
            [
                "Ok(first)",
                "Ok(first)",
                "Ok(second)",
            ],
            [
                "{\"strict_topic_names\":false,\"token\":\"first\"}",
                "{\"strict_topic_names\":false,\"token\":\"second\"}",
            ],
        )
        "###);
    }

    // Time is paused: recovery from an emitted `Err` costs one `tokens::watch`
    // error backoff (45-75s), which this test would otherwise sleep through.
    #[tokio::test(start_paused = true)]
    async fn test_resolve_failure_revokes_and_retries() {
        let resolve = MockResolve::default();
        let (task, replace) = fixture(resolve.clone());

        resolve.fail(Failure::Transient);
        let auth = response("1111111111111111", "first");
        let revoke = auth.revoke.clone();

        _ = replace(Ok(auth));
        let task = task.ready_owned().await;
        let mut observed = vec![token_of(&task.token())];

        // The failure asked for a prompt `/authorize/dekaf` re-fetch.
        assert!(revoke.is_cancelled());

        // The next version re-resolves even though its build is unchanged:
        // a failure leaves no plaintext to re-use.
        resolve.fail(Failure::None);
        let refresh = task.token();
        _ = replace(Ok(response("1111111111111111", "first")));
        () = refresh.expired().await;
        observed.push(token_of(&task.token()));

        // An upstream error clears the cached plaintext, too.
        let refresh = task.token();
        _ = replace(Err(tonic::Status::unavailable("agent is down")));
        () = refresh.expired().await;
        observed.push(token_of(&task.token()));

        let refresh = task.token();
        _ = replace(Ok(response("1111111111111111", "first")));
        () = refresh.expired().await;
        observed.push(token_of(&task.token()));

        insta::assert_debug_snapshot!((observed, resolve.calls().len()), @r###"
        (
            [
                "Err(failed to resolve endpoint config: sops is unavailable)",
                "Ok(first)",
                "Err(agent is down)",
                "Ok(first)",
            ],
            3,
        )
        "###);
    }

    #[tokio::test(start_paused = true)]
    async fn test_misconfiguration_revokes_and_keeps_its_code() {
        let resolve = MockResolve::default();
        let (task, replace) = fixture(resolve.clone());

        resolve.fail(Failure::Misconfigured);
        let auth = response("1111111111111111", "first");
        let revoke = auth.revoke.clone();

        _ = replace(Ok(auth));
        let task = task.ready_owned().await;

        // A misconfiguration revokes like any other resolve failure: only a
        // publication can fix the spec, and re-fetching is how we learn of one.
        // The resolver's status code survives into what sessions are told.
        let refresh = task.token();
        let status = refresh.result().err().unwrap();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            status.message(),
            "failed to resolve endpoint config: endpoint configuration is malformed"
        );
        assert!(revoke.is_cancelled());
    }

    #[tokio::test(start_paused = true)]
    async fn test_redirect_does_not_resolve() {
        let resolve = MockResolve::default();
        let (task, replace) = fixture(resolve.clone());

        // A redirected task's config is never resolved here: its password
        // authenticates sessions only in the plane where the task now runs.
        _ = replace(Ok(response_redirect("1111111111111111")));
        let task = task.ready_owned().await;
        let mut observed = vec![token_of(&task.token())];
        assert!(resolve.calls().is_empty());

        // Migrating back resolves again, even though the build is unchanged:
        // the redirect left no cached token to re-use.
        let refresh = task.token();
        _ = replace(Ok(response("1111111111111111", "first")));
        () = refresh.expired().await;
        observed.push(token_of(&task.token()));

        insta::assert_debug_snapshot!((observed, resolve.calls().len()), @r###"
        (
            [
                "Redirect(other.dp.estuary-data.com)",
                "Ok(first)",
            ],
            1,
        )
        "###);
    }

    #[test]
    fn test_encrypted_public_field_is_invalid() {
        // `strict_topic_names` isn't `secret: true`, so `sops` never encrypts it,
        // but a hand-rolled config could: it must fail here rather
        // than serving a redirect with the wrong topic encoding.
        let auth = response_with_config(
            "1111111111111111",
            serde_json::json!({"token": "ENC[AES256_GCM,data:abc]", "strict_topic_names": "ENC[AES256_GCM,data:def]"}),
        );
        let err = parse(&auth, None).err().expect("parse must fail");
        insta::assert_debug_snapshot!(err, @r#"
        Error {
            context: "`strict_topic_names` of the Dekaf endpoint configuration must be plaintext",
            source: Error("invalid type: string \"ENC[AES256_GCM,data:def]\", expected a boolean", line: 1, column: 26),
        }
        "#);
    }
}
