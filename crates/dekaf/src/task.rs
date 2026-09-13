//! Shared, self-refreshing state of a Dekaf task.
//!
//! Many sessions of one task need the same things -- its MaterializationSpec,
//! its endpoint config, a control-plane token, journal clients, and
//! the current journals of each bound collection -- so a single [`Task`] holds
//! them and every session reads through it. Everything refreshes on its own:
//! a session reads the latest value at each access rather than being handed a
//! snapshot which quietly expires underneath it.

use crate::{connector, topology::Partition};
use anyhow::Context;
use gazette::{broker, journal};
use proto_flow::flow::MaterializationSpec;
use std::{collections::HashMap, sync::Arc};
use tokens::{TimeDelta, Watch};

/// How long a Task lingers after its last lookup. A Kafka client which
/// reconnects within this window doesn't pay for a cold `/authorize/dekaf`
/// and a cold journal listing.
const LINGER: std::time::Duration = std::time::Duration::from_secs(3 * 60);

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
/// which has migrated to another data-plane still serve Metadata: the source
/// plane never resolves the config of a task which no longer lives there, and
/// must not need to.
#[derive(Clone, Debug)]
pub struct PublicConfig {
    pub deletions: connector::DeletionMode,
    pub strict_topic_names: bool,
}

/// TaskToken is the authorization and resolved specification of a Dekaf task,
/// as of one `/authorize/dekaf` response.
pub enum TaskToken {
    Authorized {
        /// Control-plane access token, which authorizes PostgREST queries of
        /// the Avro schema registry.
        access_token: String,
        ops_logs_journal: String,
        ops_stats_journal: String,
        public: PublicConfig,
        /// Cancel to ask that this authorization be re-fetched now, because
        /// we've seen something only a newer response can explain.
        revoke: tokens::CancellationToken,
        spec: MaterializationSpec,
        /// Password which authenticates sessions of this task: the only value
        /// taken from the resolved endpoint config.
        token: String,
    },
    /// The task now runs in a different data-plane.
    Redirect {
        public: PublicConfig,
        redirect: Redirect,
        spec: MaterializationSpec,
    },
}

impl TaskToken {
    pub fn public(&self) -> &PublicConfig {
        match self {
            Self::Authorized { public, .. } | Self::Redirect { public, .. } => public,
        }
    }

    pub fn spec(&self) -> &MaterializationSpec {
        match self {
            Self::Authorized { spec, .. } | Self::Redirect { spec, .. } => spec,
        }
    }

    /// Ask that this authorization be re-fetched now. A redirect has nothing
    /// to re-fetch sooner than its own cadence, and ignores this.
    pub fn revoke(&self) {
        if let Self::Authorized { revoke, .. } = self {
            revoke.cancel();
        }
    }
}

/// Listing is a bound collection's journals, and the client which reads them.
#[derive(Clone)]
pub struct Listing {
    /// Partition template of the bound collection.
    template: String,
    /// Selector which scopes the template to the journals this task reads.
    selector: broker::LabelSelector,
    /// Client authorized to list and read those journals.
    pub client: journal::Client,
    /// Watch of the journals, in stable Kafka partition order.
    pub partitions: tokens::PendingWatch<Vec<Partition>>,
}

/// An ops journal and the client which appends to it.
#[derive(Clone)]
pub struct OpsAppender {
    pub client: journal::Client,
    pub journal: String,
}

/// Resources are the data-plane clients and listings which back a TaskToken.
/// They're derived from it -- rather than built with it -- so that a spec
/// which changes without touching a binding re-uses that binding's listing.
enum Resources {
    Authorized {
        /// Sorted by partition template name.
        listings: Vec<Listing>,
        ops_logs: OpsAppender,
        ops_stats: OpsAppender,
    },
    /// A redirected task holds no data-plane authorization.
    Redirected(Redirect),
}

/// Env is the process-wide environment from which every Task is built.
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

/// Task is the shared, refreshing state of one Dekaf task.
pub struct Task {
    /// Catalog name of this task.
    pub name: String,
    token: tokens::PendingWatch<TaskToken>,
    resources: Arc<dyn Watch<Resources>>,
    /// Instant of the most recent Registry lookup, which the linger task
    /// waits out. Written only under the Registry's map lock, so that a lookup
    /// and the linger task's decision to drop are serialized; the Mutex is for
    /// the linger task's reads outside that lock.
    last_used: std::sync::Mutex<tokio::time::Instant>,
}

impl Task {
    /// Await this Task's first authorization. It may still be an error:
    /// callers inspect [`Task::token`] to find out.
    pub async fn ready(&self) {
        _ = self.token.ready().await;
    }

    /// Current authorization of this Task.
    pub fn token(&self) -> Arc<tokens::Refresh<TaskToken>> {
        self.token.watch().token()
    }

    /// Journals of `template`, a bound collection's partition template.
    /// Ok(None) if `template` is not (or is no longer) bound.
    pub fn listing(&self, template: &str) -> anyhow::Result<Option<Listing>> {
        let resources = self.resources.token();

        let Resources::Authorized { listings, .. } =
            resources.result().map_err(proto_grpc::status_to_anyhow)?
        else {
            anyhow::bail!("task {} has been redirected", self.name);
        };

        Ok(listings
            .binary_search_by(|l| l.template.as_str().cmp(template))
            .ok()
            .map(|index| listings[index].clone()))
    }

    /// Appenders of this Task's ops logs and ops stats journals.
    pub fn ops_appenders(&self) -> anyhow::Result<(OpsAppender, OpsAppender)> {
        let resources = self.resources.token();

        match resources.result().map_err(proto_grpc::status_to_anyhow)? {
            Resources::Authorized {
                ops_logs,
                ops_stats,
                ..
            } => Ok((ops_logs.clone(), ops_stats.clone())),
            Resources::Redirected(Redirect {
                data_plane_fqdn, ..
            }) => anyhow::bail!(
                "task {} has been redirected to {data_plane_fqdn}",
                self.name
            ),
        }
    }

    fn last_used(&self) -> tokio::time::Instant {
        *self.last_used.lock().unwrap()
    }

    fn touch(&self) {
        *self.last_used.lock().unwrap() = tokio::time::Instant::now();
    }
}

/// Registry of the Tasks which this process is currently serving.
pub struct Registry {
    env: Arc<Env>,
    tasks: std::sync::Mutex<HashMap<String, std::sync::Weak<Task>>>,
}

impl Registry {
    pub fn new(env: Env) -> Arc<Self> {
        Arc::new(Self {
            env: Arc::new(env),
            tasks: std::sync::Mutex::new(HashMap::new()),
        })
    }

    /// Look up `name`, building it if this process isn't already serving it.
    pub fn get(self: &Arc<Self>, name: &str) -> Arc<Task> {
        let mut tasks = self.tasks.lock().unwrap();

        if let Some(task) = tasks.get(name).and_then(std::sync::Weak::upgrade) {
            task.touch();
            return task;
        }
        tracing::info!(task_name = name, "starting to track task");

        let env = self.env.clone();
        let task = Arc::new(new_task(&env, name, resolve_config));
        tasks.insert(name.to_string(), Arc::downgrade(&task));

        tokio::spawn(Self::linger(self.clone(), name.to_string(), task.clone()));

        task
    }

    /// Hold one strong reference to `task` until it's gone unused for LINGER.
    async fn linger(self: Arc<Self>, name: String, task: Arc<Task>) {
        loop {
            () = tokio::time::sleep_until(task.last_used() + LINGER).await;

            let mut tasks = self.tasks.lock().unwrap();

            // `get()` touches `last_used` while holding this same lock, so a
            // lookup which raced our wake-up has already moved the deadline.
            if task.last_used() + LINGER > tokio::time::Instant::now() {
                continue;
            }
            // A session may hold this Task without having looked it up
            // recently. Re-arm and keep it until that session is gone, too.
            if Arc::strong_count(&task) != 1 {
                task.touch();
                continue;
            }
            tracing::info!(task_name = %name, "stopping tracking of unused task");
            tasks.remove(&name);
            return;
        }
    }
}

/// Build the watch tree of a Task. `resolve` turns a sealed endpoint config
/// into plaintext, and is a parameter so that tests may drive the tree
/// without running `sops`.
fn new_task<Resolve, Fut>(env: &Arc<Env>, name: &str, resolve: Resolve) -> Task
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
        name.to_string(),
        auth.map(parse),
        resolve,
    )));

    let (env, task_name) = (env.clone(), name.to_string());
    let resources = tokens::map(token.watch().clone(), move |token, prior| {
        Ok(new_resources(&env, &task_name, token, prior))
    });

    Task {
        name: name.to_string(),
        token,
        resources,
        last_used: std::sync::Mutex::new(tokio::time::Instant::now()),
    }
}

/// Parsed is one `/authorize/dekaf` response with its MaterializationSpec
/// decoded, and with the build which produced that spec.
struct Parsed {
    access_token: String,
    /// Build ID of `spec`, drawn from its shard-template labels.
    build: String,
    /// True if `build` differs from the prior Parsed's. The build changes on
    /// every publication, and is the edge on which the endpoint config is
    /// re-resolved: resolution is a `sops` round-trip, and its input changes
    /// only when the task is published.
    changed: bool,
    ops_logs_journal: String,
    ops_stats_journal: String,
    /// Fields of the sealed endpoint config which need no resolution.
    public: PublicConfig,
    redirect: Option<Redirect>,
    revoke: tokens::CancellationToken,
    /// Sealed endpoint config, which resolution turns into plaintext.
    sealed: models::RawValue,
    spec: MaterializationSpec,
}

fn parse(
    auth: &flow_client_next::workflows::DekafAuth,
    prior: Option<(&flow_client_next::workflows::DekafAuth, &Parsed)>,
) -> tonic::Result<Parsed> {
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

    // Decode the spec and the build which produced it, mapping any failure
    // into a Status: this closure is the fallible part of the mapping.
    let (spec, build) = (|| -> anyhow::Result<(MaterializationSpec, String)> {
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

        let build = labels::shard::decode_labeling(labels)
            .context("parsing shard labeling")?
            .build;

        Ok((spec, build))
    })()
    .map_err(|err| tonic::Status::internal(format!("{err:#}")))?;

    let sealed = sealed_config(&spec).map_err(|err| tonic::Status::internal(format!("{err:#}")))?;

    // A config which encrypts a public field is the user's error, not ours.
    // InvalidArgument makes it terminal for this build: `next_token` doesn't
    // revoke one, because only a publication can fix it.
    let public = public_config(&sealed)
        .map_err(|err| tonic::Status::invalid_argument(format!("{err:#}")))?;

    Ok(Parsed {
        access_token: token.clone(),
        changed: prior.map_or(true, |(_, prior)| prior.build != build),
        build,
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
        spec,
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
        let mut cached: Option<String> = None;

        loop {
            let refresh = parsed.token();
            let next = next_token(&task_name, refresh.result(), &mut cached, &resolve).await;

            () = co.yield_(next).await;
            () = refresh.expired().await;
        }
    })
}

/// Map one version of Parsed into a TaskToken, resolving its endpoint config
/// if the build changed or if no plaintext is cached.
async fn next_token<Resolve, Fut>(
    task_name: &str,
    parsed: tonic::Result<&Parsed>,
    cached: &mut Option<String>,
    resolve: &Resolve,
) -> tonic::Result<TaskToken>
where
    Resolve: Fn(models::RawValue) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<connector::DekafConfig>>,
{
    let parsed = match parsed {
        Ok(parsed) => parsed,
        Err(status) => {
            // Plaintext of a spec we can no longer see must not be re-used.
            *cached = None;
            return Err(status);
        }
    };

    // A redirect resolves nothing: the task runs elsewhere, and its password
    // authenticates sessions only there. The cache is cleared so that a task
    // which migrates *back* resolves afresh.
    if let Some(redirect) = &parsed.redirect {
        *cached = None;

        tracing::info!(
            task_name,
            build = parsed.build,
            data_plane_fqdn = redirect.data_plane_fqdn,
            "task authorization refreshed (redirected)"
        );

        return Ok(TaskToken::Redirect {
            public: parsed.public.clone(),
            redirect: redirect.clone(),
            spec: parsed.spec.clone(),
        });
    }

    if parsed.changed || cached.is_none() {
        match resolve(parsed.sealed.clone()).await {
            // Only the password is kept: every other field of the resolved
            // document is read from the sealed config instead.
            Ok(config) => *cached = Some(config.token),
            Err(err) => {
                *cached = None;
                let status =
                    proto_grpc::anyhow_to_status(err.context("failed to resolve endpoint config"));

                // A misconfiguration of the task (InvalidArgument) is fixed
                // only by a publication, which the refresh cadence picks up:
                // re-fetching the same spec sooner would resolve the same way.
                // Anything else may be transient, and revoking forces a prompt
                // re-fetch of `/authorize/dekaf` so that recovery costs one
                // watch backoff rather than a full refresh cadence. The
                // cool-off bounds what that can ask for.
                if status.code() != tonic::Code::InvalidArgument {
                    parsed.revoke.cancel();
                }

                tracing::error!(task_name, %status, "failed to resolve endpoint config");
                return Err(status);
            }
        }
    }
    let token = cached.clone().expect("resolved just above, or cached");

    tracing::info!(
        task_name,
        build = parsed.build,
        "task authorization refreshed"
    );

    Ok(TaskToken::Authorized {
        access_token: parsed.access_token.clone(),
        ops_logs_journal: parsed.ops_logs_journal.clone(),
        ops_stats_journal: parsed.ops_stats_journal.clone(),
        public: parsed.public.clone(),
        revoke: parsed.revoke.clone(),
        spec: parsed.spec.clone(),
        token,
    })
}

/// The sealed endpoint config of a Dekaf materialization, which is nested
/// within a `models::DekafConfig` alongside its variant.
pub fn sealed_config(spec: &MaterializationSpec) -> anyhow::Result<models::RawValue> {
    if spec.connector_type != proto_flow::flow::materialization_spec::ConnectorType::Dekaf as i32 {
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

/// Derive the data-plane clients and listings of a TaskToken, re-using those
/// of `prior` whose binding and ops journals are unchanged.
fn new_resources(
    env: &Env,
    task_name: &str,
    token: &TaskToken,
    prior: Option<(&TaskToken, &Resources)>,
) -> Resources {
    let (spec, ops_logs_journal, ops_stats_journal) = match token {
        TaskToken::Redirect { redirect, .. } => return Resources::Redirected(redirect.clone()),
        TaskToken::Authorized {
            spec,
            ops_logs_journal,
            ops_stats_journal,
            ..
        } => (spec, ops_logs_journal, ops_stats_journal),
    };
    let subject = crate::dekaf_shard_template_id(task_name);

    let (prior_listings, prior_ops) = match prior {
        Some((
            _,
            Resources::Authorized {
                listings,
                ops_logs,
                ops_stats,
            },
        )) => (listings.as_slice(), Some((ops_logs, ops_stats))),
        _ => (&[][..], None),
    };

    let mut listings: Vec<Listing> = Vec::with_capacity(spec.bindings.len());

    for (binding, resolved) in spec.resolved_bindings() {
        let Some((collection, _identity)) = resolved else {
            tracing::warn!(task_name, "binding is missing its collection spec");
            continue;
        };
        let Some(template) = collection
            .partition_template
            .as_ref()
            .map(|t| t.name.clone())
        else {
            tracing::warn!(
                task_name,
                collection = collection.name,
                "collection has no partition template"
            );
            continue;
        };
        let Some(selector) = binding.partition_selector.clone() else {
            tracing::warn!(
                task_name,
                collection = collection.name,
                "binding has no partition selector"
            );
            continue;
        };

        // The binding's selector scopes which journals of the collection this
        // task reads; `name:prefix` scopes it to the current generation.
        let selector = broker::LabelSelector {
            include: Some(labels::set_value(
                selector.include.unwrap_or_default(),
                "name:prefix",
                &template,
            )),
            exclude: selector.exclude,
        };

        // Re-use the prior listing of an unchanged binding: re-listing a
        // collection which didn't change would churn a long-lived watch.
        if let Ok(index) = prior_listings.binary_search_by(|l| l.template.cmp(&template)) {
            if prior_listings[index].selector == selector {
                listings.push(prior_listings[index].clone());
                continue;
            }
        }

        let client = (env.partition_clients)(subject.clone(), format!("{template}/"));
        let partitions = new_listing_watch(
            task_name.to_string(),
            template.clone(),
            client.clone(),
            selector.clone(),
        );

        listings.push(Listing {
            template,
            selector,
            client,
            partitions,
        });
    }

    // `listing()` binary-searches this, and `resolved_bindings()` does not
    // order by partition template.
    listings.sort_by(|l, r| l.template.cmp(&r.template));

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

    Resources::Authorized {
        listings,
        ops_logs: ops_appender(ops_logs_journal, prior_ops.map(|(logs, _)| logs)),
        ops_stats: ops_appender(ops_stats_journal, prior_ops.map(|(_, stats)| stats)),
    }
}

/// Watch the journals of one partition template, in stable Kafka partition
/// order. Gazette pushes a new snapshot on every change, and the broker
/// bounds each RPC by the token's expiry -- so a token-driven client
/// re-authorizes transparently across the life of the watch.
fn new_listing_watch(
    task_name: String,
    template: String,
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
            template,
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
        let spec = MaterializationSpec {
            name: "acmeCo/dekaf".to_string(),
            connector_type: proto_flow::flow::materialization_spec::ConnectorType::Dekaf as i32,
            config_json: serde_json::json!({
                "variant": "pineapple",
                "config": config,
            })
            .to_string()
            .into(),
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
            "acmeCo/dekaf".to_string(),
            auth.map(parse),
            move |sealed| resolve.clone().resolve(sealed),
        );
        (tokens::watch(tokens::StreamSource::new(stream)), replace)
    }

    fn token_of(refresh: &tokens::Refresh<TaskToken>) -> String {
        match refresh.result() {
            Ok(TaskToken::Authorized { token, .. }) => format!("Ok({token})"),
            Ok(TaskToken::Redirect { redirect, .. }) => {
                format!("Redirect({})", redirect.data_plane_fqdn)
            }
            Err(status) => format!("Err({})", status.message()),
        }
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
    async fn test_misconfiguration_does_not_revoke() {
        let resolve = MockResolve::default();
        let (task, replace) = fixture(resolve.clone());

        resolve.fail(Failure::Misconfigured);
        let auth = response("1111111111111111", "first");
        let revoke = auth.revoke.clone();

        _ = replace(Ok(auth));
        let task = task.ready_owned().await;

        // Only a publication can fix a misconfigured spec, so re-fetching the
        // same one sooner would be wasted: the code survives, and no revoke.
        let refresh = task.token();
        let status = refresh.result().err().unwrap();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            status.message(),
            "failed to resolve endpoint config: endpoint configuration is malformed"
        );
        assert!(!revoke.is_cancelled());
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
    fn test_encrypted_public_field_is_invalid_argument() {
        // `strict_topic_names` isn't `secret: true`, so `sops` never encrypts
        // it -- but a hand-rolled config could, and it must fail here rather
        // than serving a redirect with the wrong topic encoding.
        let auth = response_with_config(
            "1111111111111111",
            serde_json::json!({"token": "ENC[AES256_GCM,data:abc]", "strict_topic_names": "ENC[AES256_GCM,data:def]"}),
        );
        let status = parse(&auth, None).err().expect("parse must fail");

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        insta::assert_snapshot!(status.message(), @"`strict_topic_names` of the Dekaf endpoint configuration must be plaintext: invalid type: string \"ENC[AES256_GCM,data:def]\", expected a boolean at line 1 column 26");
    }

}
