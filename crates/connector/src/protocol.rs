//! Start pipeline shared by every connector protocol: locate the
//! endpoint, open a bounded connector request channel with an internal Spec request,
//! dispatch to an image / local subprocess / in-process connector, unseal the
//! endpoint configuration against the Spec response, and build `Started`.
//!
//! [`Protocol`] is a trait over what differs between captures, derivations,
//! and materializations; everything else in this module is protocol-agnostic.
//!
//! This module also owns the **connector mount**: the one host directory,
//! named by `CONNECTOR_MOUNT`, through which the runtime hands files to an
//! image or local connector. See `README.md` for its normative contract.

use crate::proto;
use anyhow::Context;
use futures::{StreamExt, future::BoxFuture, stream::BoxStream};

/// Everything a connector start needs from its [`Service`](crate::Service) and
/// the client's `Start` request.
pub(crate) struct StartContext {
    /// OCI container network to use.
    pub container_network: String,
    /// Log level of the container.
    pub log_level: ops::LogLevel,
    /// Sink for connector logs.
    pub log_sink: crate::LogSink,
    /// Type of data plane, which gates local connectors and non-Estuary images.
    pub plane: crate::Plane,
    /// Reactor process advertised in `Started.process`, or `None` in local contexts.
    pub process: Option<proto_gazette::broker::ProcessSpec>,
    /// Resolver of task secrets.
    pub secret_resolver: std::sync::Arc<dyn flow_client_next::SecretResolver>,
    /// Catalog task name, or [`crate::SPEC_TASK_NAME`] for a task-less Spec.
    pub task_name: String,
    /// Authorizes the connector to update its task configuration and secrets.
    pub task_update: Option<crate::TaskUpdate>,
}

/// Result of opening a protocol RPC on an image connector's channel.
pub(crate) type StartRpcFuture<Response> =
    BoxFuture<'static, tonic::Result<tonic::Response<tonic::Streaming<Response>>>>;

/// Constructor of an in-process connector, given its request stream.
type InProcessFn<P> = Box<
    dyn FnOnce(
            BoxStream<'static, <P as Protocol>::Request>,
        ) -> BoxStream<'static, tonic::Result<<P as Protocol>::Response>>
        + Send,
>;

/// Protocol differences between capture, derive, and materialize.
pub(crate) trait Protocol: Sized + 'static {
    type Request: prost::Message + serde::Serialize + Default + Send + Sync + Unpin + 'static;
    type Response: prost::Message
        + for<'de> serde::Deserialize<'de>
        + serde::Serialize
        + Default
        + Send
        + Sync
        + 'static;

    /// Names this protocol in errors attributed to the connector.
    const NAME: &'static str;
    /// Labels a connector container, and reports which runtime started it.
    const TASK_TYPE: ops::TaskType;

    /// Internal Spec request which opens a connector stream.
    fn spec_request(connector_type: i32) -> Self::Request;

    /// Map a connector request into a request of this protocol,
    /// or None if the request is the wrong protocol.
    fn unwrap_request(kind: proto::request::Kind) -> Option<Self::Request>;

    /// Map a protocol response into a connector response.
    fn wrap_response(response: Self::Response) -> proto::Response;

    /// Take the connector's Spec response, or return a response which isn't one.
    fn unwrap_spec(
        response: Self::Response,
    ) -> Result<proto::response::started::Spec, Self::Response>;

    /// Open this protocol's RPC over a started container's channel.
    fn open_rpc<S>(
        channel: tonic::transport::Channel,
        requests: S,
    ) -> StartRpcFuture<Self::Response>
    where
        S: futures::Stream<Item = Self::Request> + Send + 'static;

    /// Locate the endpoint configuration of a request and normalize its
    /// endpoint. `sqlite_vfs_uri` is the client's recorded recovery-log VFS,
    /// which only derive-sqlite may accept (all others reject it).
    fn extract_endpoint<'r>(
        request: &'r mut Self::Request,
        sqlite_vfs_uri: Option<String>,
    ) -> anyhow::Result<Extracted<'r, Self>>;
}

/// Endpoint of a client's initial request: how the connector runs, and where
/// its configuration lives within the request.
pub(crate) struct Extracted<'r, P: Protocol> {
    /// Connector type of the request, echoed in the internal Spec request.
    pub connector_type: i32,
    /// Normalized endpoint for launching the connector.
    pub endpoint: Endpoint<P>,
    /// Configuration slot of the initial request, to be replaced with the
    /// unwrapped or injected configuration during startup.
    pub initial_config_slot: &'r mut bytes::Bytes,
    /// Sealed configuration slot of the initial request, to be replaced with
    /// the sealed configuration during startup.
    /// Present only on `Open` of protocols which have this field.
    pub initial_sealed_config_slot: Option<&'r mut bytes::Bytes>,
    /// Build of the initial request's spec, or `None` for requests lacking one.
    pub build: Option<&'r str>,
    /// Secrets of the task. Keys are catalog names of secrets,
    /// while values are JSON pointers into the endpoint config.
    pub secrets: &'r std::collections::BTreeMap<String, String>,
}

/// Normalized endpoint: the three ways a connector is run.
pub(crate) enum Endpoint<P: Protocol> {
    Image {
        image: String,
        config: models::RawValue,
    },
    /// Development-only subprocess, permitted only in [`crate::Plane::Local`].
    /// Its `config` field is the sealed configuration.
    Local { config: models::LocalConfig },
    /// derive-sqlite or materialize-dekaf
    InProcess {
        connector: InProcessFn<P>,
        config: models::RawValue,
    },
}

/// Start a connector and complete its internal Spec exchange, returning its
/// typed request sender, response stream, resources, and prebuilt `Started`.
pub(crate) async fn start<P: Protocol>(
    ctx: StartContext,
    sqlite_vfs_uri: Option<String>,
    mut initial: P::Request,
) -> anyhow::Result<crate::Started<P>> {
    let Extracted {
        build,
        connector_type,
        endpoint,
        initial_config_slot,
        initial_sealed_config_slot,
        secrets,
    } = P::extract_endpoint(&mut initial, sqlite_vfs_uri)?;

    // TODO(johnny): This bit of ugliness is to support frozen
    // derive-typescript/python `:dev` image tags, which are required for the
    // V1 runtime. These specific images (only) are incapable of serving multiple
    // unary requests in a single session (which is how we _want_ this to work).
    // Hold them as a carve-out, while having all other connectors service
    // successive unary requests on a single session.
    // Delete all of this when the V1 runtime is retired for derivations.
    let spec_on_own_rpc = matches!(
        &endpoint,
        Endpoint::Image { image, .. } if one_request_per_invocation(image),
    );

    // Apply image policy checks that don't require inspection (and registry I/O).
    let image_policy = match &endpoint {
        Endpoint::Image { image, .. } => Some(crate::policy::Image::check(ctx.plane, image)?),
        Endpoint::Local { .. } | Endpoint::InProcess { .. } => None,
    };

    let mount = create_connector_mount()?;
    let env = connector_env(ctx.plane, ctx.log_level, mount.path())?;

    // If Some(task_update), inject and rotate credentials and metadata which
    // offer tasks a capability to update their configuration and/or secrets.
    let mut refresh = None;
    if let Some(task_update) = ctx
        .task_update
        .as_ref()
        .filter(|_| ctx.task_name != crate::SPEC_TASK_NAME)
    {
        // Written before the connector starts, so that its first read succeeds.
        let token = mint_task_update(task_update, P::TASK_TYPE, &ctx.task_name, build)?;
        write_task_update(mount.path(), task_update, &token).await?;

        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(refresh_task_update(
            task_update.clone(),
            P::TASK_TYPE,
            ctx.task_name.clone(),
            build.map(str::to_string),
            mount.path().to_owned(),
            ctx.log_sink.clone(),
            stop_rx,
        ));
        refresh = Some(stop_tx);
    }

    let (connector_tx, connector_rx) = tokio::sync::mpsc::channel(proto_grpc::CHANNEL_BUFFER);
    if !spec_on_own_rpc {
        connector_tx
            .send(P::spec_request(connector_type))
            .await
            .expect("new connector request channel is open");
    }

    // Spawn the connector, branched on its endpoint type.
    let requests = tokio_stream::wrappers::ReceiverStream::new(connector_rx).boxed();
    let crate::Transport {
        mut connector_rx,
        container,
        codec,
        process,
        sealed_config,
        spec: own_rpc_spec,
    } = match endpoint {
        Endpoint::Image {
            image,
            config: sealed_config,
        } => {
            crate::image::connect::<P>(
                &ctx,
                image,
                sealed_config,
                image_policy.as_ref().unwrap(),
                env,
                mount.path(),
                secrets,
                connector_type,
                spec_on_own_rpc,
                requests,
            )
            .await
        }
        Endpoint::InProcess {
            connector,
            config: sealed_config,
        } => Ok(crate::Transport {
            connector_rx: connector(requests),
            container: None,
            codec: connector_init::Codec::Proto,
            process: None,
            sealed_config,
            spec: None,
        }),
        Endpoint::Local { config } => connect_local::<P>(&ctx, config, secrets, env, requests),
    }?;

    let codec = match codec {
        connector_init::Codec::Proto => proto::response::started::Codec::Proto,
        connector_init::Codec::Json => proto::response::started::Codec::Json,
    };

    let spec = match own_rpc_spec {
        Some(spec) => spec,
        None => unwrap_spec_response::<P>(connector_rx.next().await)?,
    };
    let config_schema: &[u8] = match &spec {
        proto::response::started::Spec::Capture(spec) => &spec.config_schema_json,
        proto::response::started::Spec::Derive(spec) => &spec.config_schema_json,
        proto::response::started::Spec::Materialize(spec) => &spec.config_schema_json,
    };

    let inject_iam: bool;
    let mut token_restart_at = None;

    // Unseal the configuration, or inject decrypted secrets into it.
    (*initial_config_slot, inject_iam) = if ctx.task_name == crate::SPEC_TASK_NAME {
        // A Spec has no task identity under which to unseal, resolve, or inject,
        // so its configuration passes through exactly as the caller sent it.
        (
            bytes::Bytes::copy_from_slice(sealed_config.get().as_bytes()),
            false,
        )
    } else {
        // Each uniquely keyed secret is decrypted concurrently.
        let resolved = unseal::resolve(&sealed_config, secrets, config_schema, |name| {
            resolve_secret::<P>(
                &ctx,
                image_policy.as_ref().map(crate::policy::Image::repository),
                name,
            )
        })
        .await
        .map_err(|err| match err {
            // A misconfiguration of the task, and not a failure of this runtime.
            err @ unseal::Error::SopsWithSecrets => crate::invalid_argument(err.to_string()),
            err => anyhow::Error::new(err),
        })?;

        (resolved.into(), true)
    };

    // If IAM token injection is configured, fetch and inject tokens.
    if let Some(iam_config) = inject_iam
        .then(|| {
            iam_auth::extract_iam_auth_from_connector_config(initial_config_slot, config_schema)
        })
        .transpose()?
        .flatten()
    {
        let tokens = iam_config.generate_tokens(&ctx.task_name).await?;
        *initial_config_slot = tokens.inject_into(initial_config_slot)?.to_string().into();

        token_restart_at = Some(proto_flow::as_timestamp(
            crate::policy::token_restart_deadline(
                std::time::SystemTime::now(),
                tokens.expires_at(),
            ),
        ));
    }

    // Provide the original, sealed configuration for initial requests that carry it.
    // Connectors use this as a safe baseline for `configUpdate` log emissions.
    if let Some(initial_sealed_config_slot) = initial_sealed_config_slot {
        *initial_sealed_config_slot = sealed_config.into();
    }
    _ = connector_tx.try_send(initial);

    Ok(crate::Started {
        started: proto::Response {
            kind: Some(proto::response::Kind::Started(proto::response::Started {
                container,
                codec: codec as i32,
                token_restart_at,
                process: ctx.process,
                spec: Some(spec),
            })),
        },
        connector_tx,
        connector_rx,
        guard: crate::Guard {
            _process: process,
            _refresh: refresh,
            _mount: mount,
        },
    })
}

/// Build the environment contract shared by image and local connectors.
fn connector_env(
    plane: crate::Plane,
    log_level: ops::LogLevel,
    mount: &std::path::Path,
) -> anyhow::Result<std::collections::BTreeMap<String, String>> {
    let default_log_level = match plane {
        crate::Plane::Local => ops::LogLevel::Info,
        crate::Plane::Public | crate::Plane::Private => ops::LogLevel::Warn,
    };
    let mount = mount
        .to_str()
        .context("connector mount path is not valid UTF-8")?;

    Ok(std::collections::BTreeMap::from([
        ("CONNECTOR_MOUNT".to_string(), mount.to_string()),
        ("LOG_FORMAT".to_string(), "json".to_string()),
        (
            "LOG_LEVEL".to_string(),
            log_level.or(default_log_level).as_str_name().to_string(),
        ),
    ]))
}

/// Create the connector mount volume: a directory of resources shared with
/// the connector for its use. Image connectors bind-mount as read-only at
/// this same absolute path. Local connectors access it directly.
///
/// It lives under TMPDIR as a known location that a launching docker / podman
/// is capable of sharing with the container. Note Docker for Mac restricts
/// other share locations.
///
/// The root is a pure host-to-connector channel and stays read-only inside the
/// container. Writable areas (scratch space, recorded state) must arrive as
/// nested mounts under it, never as a relaxation of this root.
pub(crate) fn create_connector_mount() -> anyhow::Result<tempfile::TempDir> {
    // Traversable by all, listable by none: a connector image commonly runs as
    // an unprivileged image-defined UID, which must reach files it is told the
    // name of without being able to enumerate the mount. Scope the parent to
    // the host user so that users sharing a TMPDIR don't contend over ownership.
    #[cfg(unix)]
    let parent =
        std::env::temp_dir().join(format!("connector-mounts-{}", unsafe { libc::geteuid() }));
    #[cfg(not(unix))]
    let parent = std::env::temp_dir().join("connector-mounts");
    std::fs::create_dir_all(&parent).context("creating connector mounts directory")?;
    set_mount_mode(&parent, 0o711)?;

    let dir = tempfile::Builder::new()
        .prefix("mount-")
        .tempdir_in(&parent)
        .context("creating connector mount directory")?;
    set_mount_mode(dir.path(), 0o711)?;

    Ok(dir)
}

pub(crate) fn set_mount_mode(path: &std::path::Path, mode: u32) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
            .with_context(|| format!("setting mode of {}", path.display()))?;
    }
    #[cfg(not(unix))]
    let _ = (path, mode);

    Ok(())
}

/// Contents of `task-update.json`: everything a connector needs to call the
/// routes by which it updates its own configuration and secrets.
#[derive(serde::Serialize)]
struct TaskUpdateFile<'a> {
    token: &'a str,
    control_plane_url: &'a str,
    config_encryption_url: &'a str,
}

/// Write `task-update.json` into a connector mount via atomic rename.
async fn write_task_update(
    dir: &std::path::Path,
    task_update: &crate::TaskUpdate,
    token: &str,
) -> anyhow::Result<()> {
    let body = serde_json::to_vec(&TaskUpdateFile {
        token,
        control_plane_url: task_update.control_api.as_str(),
        config_encryption_url: task_update.config_encryption.as_str(),
    })
    .expect("task update file always serializes");

    let staged = dir.join(".task-update.json.tmp");
    match tokio::fs::remove_file(&staged).await {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err).context("removing stale staged task update file"),
    }
    tokio::fs::write(&staged, &body)
        .await
        .context("writing staged task update file")?;
    set_mount_mode(&staged, 0o444)?;

    tokio::fs::rename(&staged, dir.join("task-update.json"))
        .await
        .context("renaming staged task update file")
}

/// Re-mint the connector's `task-update.json` in place until `stop` is dropped.
async fn refresh_task_update(
    task_update: crate::TaskUpdate,
    task_type: ops::TaskType,
    task_name: String,
    build: Option<String>,
    dir: std::path::PathBuf,
    log_sink: crate::LogSink,
    mut stop: tokio::sync::oneshot::Receiver<()>,
) {
    let mut ticker = tokio::time::interval(task_update.refresh_interval);
    ticker.tick().await; // The first tick completes immediately.

    loop {
        tokio::select! {
            _ = &mut stop => return,
            _ = ticker.tick() => {}
        }

        let result = match mint_task_update(&task_update, task_type, &task_name, build.as_deref()) {
            Ok(token) => write_task_update(&dir, &task_update, &token).await,
            Err(err) => Err(err),
        };
        let Err(err) = result else { continue };

        log_sink
            .send(crate::build_log(
                ops::LogLevel::Warn,
                "failed to refresh the connector's task update credential (will retry)",
                [("error", crate::json_field(&format!("{err:#}")))],
            ))
            .await;
    }
}

/// Mint the task-scoped `TASK_UPDATE` credential handed to a connector.
fn mint_task_update(
    task_update: &crate::TaskUpdate,
    task_type: ops::TaskType,
    task_name: &str,
    build: Option<&str>,
) -> anyhow::Result<String> {
    let mut include = labels::build_set([
        (labels::TASK_NAME, task_name),
        (labels::TASK_TYPE, task_type.as_str_name()),
    ]);
    if let Some(build) = build {
        include = labels::add_value(include, labels::BUILD, build);
    }
    task_update
        .signer
        .sign(
            proto_flow::capability::TASK_UPDATE,
            task_name.to_string(),
            proto_gazette::broker::LabelSelector {
                include: Some(include),
                exclude: None,
            },
            crate::policy::TASK_UPDATE_LIFETIME,
        )
        .map_err(crate::status_to_anyhow)
}

/// Extract `estuary.dev/build` label from a task's template ShardSpec.
pub(crate) fn shard_build<'a>(
    template: &'a Option<proto_gazette::consumer::ShardSpec>,
) -> anyhow::Result<&'a str> {
    let label_set = template
        .as_ref()
        .context("missing expected template ShardSpec")?
        .labels
        .as_ref()
        .context("missing expected template ShardSpec labels")?;

    Ok(labels::expect_one(label_set, labels::BUILD)?)
}

/// Decrypt one secret of the task's `secrets` stanza under the task's identity,
/// attesting the image repository which is asking so that the control-plane may
/// admit a secret under the image rule.
async fn resolve_secret<P: Protocol>(
    ctx: &StartContext,
    image_repo: Option<&str>,
    name: &str,
) -> anyhow::Result<serde_json::Value> {
    let decrypted = ctx
        .secret_resolver
        .decrypt(
            P::TASK_TYPE,
            &ctx.task_name,
            image_repo,
            models::Secret::new(name),
        )
        .await?;

    let (Some(value), Some(secret_id)) = (decrypted.value, decrypted.secret_id) else {
        panic!("a successful secret decryption has a value and a secret id");
    };

    ctx.log_sink
        .send(crate::build_log(
            ops::LogLevel::Info,
            "resolved task secret",
            [
                ("secret", crate::json_field(&name)),
                ("secretId", crate::json_field(&secret_id)),
            ],
        ))
        .await;

    Ok(value)
}

fn connect_local<P: Protocol>(
    ctx: &StartContext,
    models::LocalConfig {
        command,
        config: sealed_config,
        env: model_env,
        protobuf,
    }: models::LocalConfig,
    secrets: &std::collections::BTreeMap<String, String>,
    runtime_env: std::collections::BTreeMap<String, String>,
    requests: BoxStream<'static, P::Request>,
) -> anyhow::Result<crate::Transport<P>> {
    if !crate::policy::local_connectors_allowed(ctx.plane) {
        return Err(tonic::Status::failed_precondition(
            "Local connectors are not permitted in this context",
        )
        .into());
    }

    crate::policy::check_secrets(
        &ctx.task_name,
        secrets
            .iter()
            .map(|(name, pointer)| (name.as_str(), pointer.as_str())),
        crate::policy::SecretIdentity::NoImage,
    )?;

    let codec = if protobuf {
        connector_init::Codec::Proto
    } else {
        connector_init::Codec::Json
    };

    let mut connector = connector_init::rpc::new_command(&command);
    connector.envs(&model_env);
    connector.envs(runtime_env);

    // Dropping the local connector's response stream kills its subprocess and
    // closes its stderr. Its `Guard` holds no process: only the mount,
    // which must outlive the subprocess reading it.
    let log_sink = ctx.log_sink.clone();
    let quoted_task_name: bytes::Bytes = format!("\"{}\"", ctx.task_name).into();
    let connector_rx = connector_init::rpc::bidi::<P::Request, P::Response, _, _, _>(
        connector,
        codec,
        requests.map(Result::Ok),
        move |log| {
            let log = crate::policy::sanitize_connector_log(&quoted_task_name, log);
            let log_sink = log_sink.clone();
            async move { log_sink.send(log).await }
        },
    )?;

    Ok(crate::Transport {
        connector_rx: connector_rx.boxed(),
        container: None,
        codec,
        process: None,
        sealed_config,
        spec: None,
    })
}

/// Take the connector's Spec from its first response, or fail with the
/// response it sent instead.
// TODO(johnny): inline with retirement of `:dev` images.
pub(super) fn unwrap_spec_response<P: Protocol>(
    response: Option<tonic::Result<P::Response>>,
) -> anyhow::Result<proto::response::started::Spec> {
    let verify = crate::verify(P::NAME, "spec response", "connector");

    match P::unwrap_spec(verify.not_eof(response)?) {
        Ok(spec) => Ok(spec),
        Err(response) => Err(verify.fail_msg(P::wrap_response(response))),
    }
}

/// Exchange a Spec on an RPC which carries nothing else, then close it.
// TODO(johnny): remove with retirement of `:dev` images.
pub(super) async fn spec_rpc<P: Protocol>(
    channel: tonic::transport::Channel,
    connector_type: i32,
) -> anyhow::Result<proto::response::started::Spec> {
    let request = P::spec_request(connector_type);
    let mut responses = P::open_rpc(channel, futures::stream::once(async move { request }))
        .await?
        .into_inner();

    unwrap_spec_response::<P>(responses.next().await)
}

/// Images which service exactly one request per connector invocation.
/// TODO(johnny): remove with retirement of `:dev` images.
fn one_request_per_invocation(image: &str) -> bool {
    let (repository, tag) = models::split_image_tag(image);

    tag == ":dev"
        && matches!(
            repository.as_str(),
            "ghcr.io/estuary/derive-typescript" | "ghcr.io/estuary/derive-python"
        )
}

#[cfg(test)]
mod test {
    #[test]
    fn connector_environment_is_transport_agnostic() {
        let mount = std::path::Path::new("/tmp/connector-mount-test");

        for (plane, expected) in [
            (crate::Plane::Public, "warn"),
            (crate::Plane::Private, "warn"),
            (crate::Plane::Local, "info"),
        ] {
            let env = super::connector_env(plane, ops::LogLevel::UndefinedLevel, mount).unwrap();

            assert_eq!(env["CONNECTOR_MOUNT"], mount.to_str().unwrap());
            assert_eq!(env["LOG_FORMAT"], "json");
            assert_eq!(env["LOG_LEVEL"], expected);
        }

        let env = super::connector_env(crate::Plane::Local, ops::LogLevel::Trace, mount).unwrap();
        assert_eq!(env["LOG_LEVEL"], "trace");
    }

    /// The claims of a minted task update token. A session's `Open` carries the
    /// build it runs, which `/task/update-config` requires and which pins the
    /// proposed configuration to a model the connector has actually seen. A
    /// unary request has no session and so no build. Both have the one
    /// lifetime, because both are refreshed in place.
    #[test]
    fn mints_a_task_update_token_scoped_to_its_task() {
        let task_update = crate::TaskUpdate::for_test();

        let outcomes = [
            ("with a build", Some("1122334455667788")),
            ("without a build", None),
        ]
        .map(|(label, build)| {
            let token = super::mint_task_update(
                &task_update,
                ops::TaskType::Capture,
                "acmeCo/source-widgets",
                build,
            )
            .unwrap();

            let claims = tokens::jwt::parse_unverified::<proto_gazette::Claims>(token.as_bytes())
                .unwrap()
                .claims()
                .clone();

            (
                label,
                claims.cap,
                claims.iss,
                claims.sub,
                claims.sel,
                format!("{}s", claims.exp - claims.iat),
            )
        });

        insta::assert_debug_snapshot!(outcomes);
    }

    /// `task-update.json` has exactly the three properties a connector reads,
    /// and lands with a mode its container's unprivileged user can read.
    #[tokio::test]
    async fn writes_the_task_update_file_into_a_mount() {
        let mount = super::create_connector_mount().unwrap();
        super::write_task_update(mount.path(), &crate::TaskUpdate::for_test(), "a.token.here")
            .await
            .unwrap();

        let path = mount.path().join("task-update.json");
        let body = std::fs::read_to_string(&path).unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = |path: &std::path::Path| {
                std::fs::metadata(path).unwrap().permissions().mode() & 0o777
            };
            assert_eq!(mode(mount.path()), 0o711);
            assert_eq!(mode(&path), 0o444);
        }

        insta::assert_snapshot!(
            body,
            @r#"{"token":"a.token.here","control_plane_url":"https://control.example.com/","config_encryption_url":"https://config-encryption.example.com/"}"#
        );
    }

    /// Pins the exhaustive list, because it gates a behavior change that is
    /// invisible until a connector fails to parse a multiplexed stream.
    #[test]
    fn one_request_per_invocation_matches_only_the_frozen_derive_images() {
        for image in [
            "ghcr.io/estuary/derive-typescript:dev",
            "ghcr.io/estuary/derive-python:dev",
        ] {
            assert!(super::one_request_per_invocation(image), "{image}");
        }

        for image in [
            // Other tags of the same connectors service a multiplexed stream.
            "ghcr.io/estuary/derive-typescript:stable",
            "ghcr.io/estuary/derive-typescript:local",
            "ghcr.io/estuary/derive-python:stable",
            // A digest pins a build we cannot identify as frozen.
            "ghcr.io/estuary/derive-python@sha256:abc",
            // Untagged, and unrelated connectors at `:dev`.
            "ghcr.io/estuary/derive-python",
            "ghcr.io/estuary/source-hello-world:dev",
            "ghcr.io/estuary/materialize-postgres:dev",
            // Not ours, however it is named.
            "example.com/derive-python:dev",
        ] {
            assert!(!super::one_request_per_invocation(image), "{image}");
        }
    }
}
