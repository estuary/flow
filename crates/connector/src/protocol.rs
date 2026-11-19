//! Start pipeline shared by every connector protocol: locate the endpoint,
//! dispatch to an image / local subprocess / in-process connector, unseal the
//! endpoint configuration against its Spec, and build `Started`.
//!
//! Every endpoint serves the `connector.Connector` contract of
//! `connector_init::serve`, so after dispatch all are handled the same way.
//!
//! [`Protocol`] is a trait over what differs between captures, derivations,
//! and materializations; everything else in this module is protocol-agnostic.
//!
//! This module also owns the **connector mount**: the one host directory,
//! named by `CONNECTOR_MOUNT`, through which the runtime hands files to an
//! image or local connector. See `README.md` for its normative contract.

use crate::proto;
use anyhow::Context;
use futures::{FutureExt, StreamExt};
use proto_flow::flow;

/// Everything a connector start needs from its [`Service`](crate::Service) and
/// the client's `Start` request.
pub(crate) struct StartContext {
    /// OCI container network to use.
    pub container_network: String,
    pub log_sink: crate::LogSink,
    /// Type of data plane, which gates local connectors and non-Estuary images.
    pub plane: crate::Plane,
    /// Reactor process advertised in `Started.process`, or `None` in local contexts.
    pub process: Option<proto_gazette::broker::ProcessSpec>,
    /// Resolver of task secrets.
    pub secret_resolver: std::sync::Arc<dyn flow_client_next::SecretResolver>,
    /// The client's `Start`, less its `sqlite_vfs_uri`.
    pub start: proto::request::Start,
    /// Catalog task name, or [`crate::SPEC_TASK_NAME`] for a task-less Spec.
    pub task_name: String,
    /// Authorizes the connector to update its task configuration and secrets.
    pub task_update: Option<crate::TaskUpdate>,
    /// VMM capability of the [`Service`](crate::Service), if any.
    pub vmm: Option<crate::Vmm>,
}

/// Constructor of an in-process connector, given its request stream.
type InProcessFn<P> = Box<
    dyn FnOnce(
            futures::stream::BoxStream<'static, <P as proto::Protocol>::Request>,
        ) -> futures::stream::BoxStream<
            'static,
            tonic::Result<<P as proto::Protocol>::Response>,
        > + Send,
>;

/// Endpoint extraction of capture, derive, and materialize, beyond the
/// message facts of [`proto::Protocol`].
pub(crate) trait Protocol: proto::Protocol {
    /// Execution of the task's built spec, if the request embeds one (Apply or
    /// Open). Previous `last_*` specs do not select the current execution.
    fn spec_execution(request: &Self::Request) -> Option<flow::ConnectorExecution>;

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
    /// Network port numbers of the initial request's spec, or empty for
    /// requests lacking one.
    pub network_ports: Vec<u32>,
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
/// request sender, response stream, resources, and prebuilt `Started`.
pub(crate) async fn start<P: Protocol>(
    ctx: StartContext,
    sqlite_vfs_uri: Option<String>,
    mut initial: P::Request,
) -> anyhow::Result<crate::Started> {
    let spec_execution = P::spec_execution(&initial);
    let Extracted {
        build,
        connector_type,
        endpoint,
        initial_config_slot,
        initial_sealed_config_slot,
        network_ports,
        secrets,
    } = P::extract_endpoint(&mut initial, sqlite_vfs_uri)?;

    let image = match &endpoint {
        Endpoint::Image { image, .. } => Some(image.as_str()),
        Endpoint::Local { .. } | Endpoint::InProcess { .. } => None,
    };
    let requested = ctx.start.execution.clone().unwrap_or_default();
    let execution = crate::vmm::vmm_for(
        ctx.vmm.as_ref(),
        ctx.plane,
        P::TASK_TYPE,
        image,
        &requested,
        spec_execution.as_ref(),
    )?;

    // Secret decryption attests the image's repository, under either execution.
    let image_repository = image.map(|image| models::split_image_tag(image).0);

    let sealed_config = match &endpoint {
        Endpoint::Image { config, .. } | Endpoint::InProcess { config, .. } => config,
        Endpoint::Local { config } => &config.config,
    }
    .clone();

    // Declared before the transport so errors release the connector before
    // its credential refresh and mount.
    let mut guard = crate::Guard {
        _process: None,
        _vmm: None,
        _refresh: None,
        _mount: None,
    };

    // The connector's log level defaults by plane. It governs the session's
    // task logs.
    let default_log_level = match ctx.plane {
        crate::Plane::Local => ops::LogLevel::Info,
        crate::Plane::Public | crate::Plane::Private => ops::LogLevel::Warn,
    };
    let mut start = ctx.start.clone();
    start.log_level = ops::LogLevel::try_from(start.log_level)
        .unwrap_or_default()
        .or(default_log_level) as i32;

    let (request_tx, request_rx) = tokio::sync::mpsc::channel(proto_grpc::CHANNEL_BUFFER);
    request_tx
        .try_send(proto::Request {
            start: Some(start),
            kind: Some(P::wrap_request(P::spec_request(
                connector_type,
                bytes::Bytes::from(sealed_config.clone()),
            ))),
            ..Default::default()
        })
        .expect("new connector request channel is empty");

    // Spawn the connector, branched on its endpoint type and execution.
    let requests = tokio_stream::wrappers::ReceiverStream::new(request_rx);
    let crate::Transport {
        mut response_rx,
        container,
        process,
        vmm,
        mut stderr_overflow,
    } = match endpoint {
        Endpoint::Image { image, .. } => match execution {
            crate::vmm::Execution::Ordinary => {
                // Apply image policy checks that don't require inspection (and registry I/O).
                let policy = crate::policy::Image::check(ctx.plane, &image)?;
                let (mount, env) = ordinary_setup::<P>(&ctx, build, &mut guard).await?;

                crate::image::connect::<P>(
                    &ctx,
                    image,
                    &policy,
                    env,
                    &mount,
                    secrets,
                    network_ports,
                    requests,
                )
                .await
            }
            crate::vmm::Execution::Vmm {
                vmm,
                eligible,
                egress,
            } => {
                let (container, channel, vmm_guard, stderr_overflow) = crate::vmm::launch::start(
                    &ctx,
                    vmm,
                    eligible,
                    egress,
                    network_ports,
                    &image,
                    secrets,
                    build,
                )
                .await?;

                Ok(crate::Transport {
                    response_rx: crate::image::open_connector(channel, requests).await?,
                    container: Some(container),
                    process: None,
                    vmm: Some(vmm_guard),
                    stderr_overflow,
                })
            }
        },
        Endpoint::InProcess { connector, .. } => {
            ordinary_setup::<P>(&ctx, build, &mut guard).await?;

            crate::policy::check_secrets(
                &ctx.task_name,
                secrets
                    .iter()
                    .map(|(name, pointer)| (name.as_str(), pointer.as_str())),
                crate::policy::SecretIdentity::NoImage,
            )
            .map(|()| crate::Transport {
                response_rx: in_process::<P>(connector, requests),
                container: None,
                process: None,
                vmm: None,
                stderr_overflow: futures::future::pending().boxed(),
            })
        }
        Endpoint::Local { config } => {
            let (_mount, env) = ordinary_setup::<P>(&ctx, build, &mut guard).await?;
            connect_local(&ctx, config, secrets, env, requests)
        }
    }?;

    let proto::response::Started { codec, spec, .. } = read_started(
        &ctx.task_name,
        &ctx.log_sink,
        &mut response_rx,
        &mut stderr_overflow,
    )
    .await?;

    let config_schema: &[u8] = match &spec {
        Some(spec) if spec.task_type() == P::TASK_TYPE => spec.config_schema_json(),
        _ => {
            return Err(
                crate::verify_connector(P::NAME, "Started with its Spec").fail_msg(
                    proto::response::Started {
                        codec,
                        spec,
                        ..Default::default()
                    },
                ),
            );
        }
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
            resolve_secret::<P>(&ctx, image_repository.as_deref(), name)
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
    _ = request_tx.try_send(proto::Request {
        start: None,
        kind: Some(P::wrap_request(initial)),
        ..Default::default()
    });

    // Of `Started`, only `codec` and `spec` come from the connector's side of
    // the contract. This crate owns every other field.
    Ok(crate::Started {
        started: proto::Response {
            kind: Some(proto::response::Kind::Started(proto::response::Started {
                container,
                codec,
                token_restart_at,
                process: ctx.process,
                spec,
                execution: Some(requested).filter(|execution| *execution != Default::default()),
            })),
            ..Default::default()
        },
        connector_tx: request_tx,
        connector_rx: response_rx,
        stderr_overflow,
        guard: crate::Guard {
            _process: process,
            _vmm: vmm,
            ..guard
        },
    })
}

/// Read a dispatched connector's responses until `Started`, sinking its
/// sanitized logs into `log_sink`. A `stderr_overflow` fails the read.
async fn read_started(
    task_name: &str,
    log_sink: &crate::LogSink,
    response_rx: &mut futures::stream::BoxStream<'static, tonic::Result<proto::Response>>,
    stderr_overflow: &mut futures::future::BoxFuture<'static, anyhow::Error>,
) -> anyhow::Result<proto::response::Started> {
    let verify = crate::verify_connector("Connector", "Started");
    let quoted_task_name = quote_task_name(task_name);

    loop {
        let response = tokio::select! {
            err = &mut *stderr_overflow => return Err(err),
            response = response_rx.next() => response,
        };
        match verify.not_eof(response)? {
            proto::Response {
                kind: Some(proto::response::Kind::Log(log)),
                ..
            } => {
                log_sink
                    .send(crate::policy::sanitize_connector_log(
                        &quoted_task_name,
                        log,
                    ))
                    .await
            }
            proto::Response {
                kind: Some(proto::response::Kind::Started(started)),
                ..
            } => return Ok(started),
            response => return Err(verify.fail_msg(response)),
        }
    }
}

/// JSON-quoted task name, against which connector logs are sanitized.
pub(crate) fn quote_task_name(task_name: &str) -> bytes::Bytes {
    format!("\"{task_name}\"").into()
}

async fn ordinary_setup<P: Protocol>(
    ctx: &StartContext,
    build: Option<&str>,
    guard: &mut crate::Guard,
) -> anyhow::Result<(
    std::path::PathBuf,
    std::collections::BTreeMap<String, String>,
)> {
    let mount = guard._mount.insert(create_connector_mount()?);
    let env = connector_env(mount.path())?;
    guard._refresh = start_task_update(
        ctx.task_update.as_ref(),
        P::TASK_TYPE,
        &ctx.task_name,
        build,
        mount.path(),
        &ctx.log_sink,
    )
    .await?;
    Ok((mount.path().to_owned(), env))
}

/// Initialize and rotate the shared task-update credential contract. A VMM
/// invokes this only after claiming its recoverable connector mount, so rotation
/// and mount release follow the same owner as its container.
pub(crate) async fn start_task_update(
    task_update: Option<&crate::TaskUpdate>,
    task_type: ops::TaskType,
    task_name: &str,
    build: Option<&str>,
    mount: &std::path::Path,
    log_sink: &crate::LogSink,
) -> anyhow::Result<Option<tokio::sync::oneshot::Sender<()>>> {
    let Some(task_update) = task_update.filter(|_| task_name != crate::SPEC_TASK_NAME) else {
        return Ok(None);
    };
    let token = mint_task_update(task_update, task_type, task_name, build)?;
    write_task_update(mount, task_update, &token).await?;

    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(refresh_task_update(
        task_update.clone(),
        task_type,
        task_name.to_string(),
        build.map(str::to_string),
        mount.to_owned(),
        log_sink.clone(),
        stop_rx,
    ));
    Ok(Some(stop_tx))
}

/// Build the environment shared by image and local connectors.
/// `connector_init::serve` adds `LOG_LEVEL` and `LOG_FORMAT`.
fn connector_env(
    mount: &std::path::Path,
) -> anyhow::Result<std::collections::BTreeMap<String, String>> {
    let mount = mount
        .to_str()
        .context("connector mount path is not valid UTF-8")?;

    Ok(std::collections::BTreeMap::from([(
        "CONNECTOR_MOUNT".to_string(),
        mount.to_string(),
    )]))
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
    let parent = connector_mounts(&std::env::temp_dir())?;

    let dir = tempfile::Builder::new()
        .prefix("mount-")
        .tempdir_in(&parent)
        .context("creating connector mount directory")?;
    set_mount_mode(dir.path(), 0o711)?;

    Ok(dir)
}

/// The host ancestor is private to the launcher. Engines bind each child
/// directory explicitly, so image-defined UIDs need no access to this ancestor.
pub(crate) fn connector_mounts(tmp: &std::path::Path) -> anyhow::Result<std::path::PathBuf> {
    #[cfg(unix)]
    let parent = tmp.join(format!(
        "connector-mounts-{}",
        rustix::process::geteuid().as_raw()
    ));
    #[cfg(not(unix))]
    let parent = tmp.join("connector-mounts");

    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    match builder.create(&parent) {
        Ok(()) => (),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            #[cfg(unix)]
            check_directory_owner(&parent, rustix::process::geteuid().as_raw())
                .with_context(|| format!("connector mounts directory {}", parent.display()))?;
        }
        Err(err) => {
            return Err(err).with_context(|| format!("creating {}", parent.display()));
        }
    }
    set_mount_mode(&parent, 0o700)?;
    Ok(parent)
}

/// Shared directories must not let another user plant launch resources.
#[cfg(unix)]
pub(crate) fn check_directory_owner(
    path: &std::path::Path,
    euid: u32,
) -> anyhow::Result<std::fs::Metadata> {
    use std::os::unix::fs::MetadataExt;

    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() {
        anyhow::bail!("is not a directory, or is a link to one");
    }
    if metadata.uid() != euid {
        anyhow::bail!(
            "is owned by uid {}, not this process's uid {euid}",
            metadata.uid()
        );
    }
    if metadata.mode() & 0o022 != 0 {
        anyhow::bail!("is writable by a user other than its owner");
    }
    Ok(metadata)
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

fn connect_local(
    ctx: &StartContext,
    models::LocalConfig {
        command,
        config: _,
        env: model_env,
        protobuf,
    }: models::LocalConfig,
    secrets: &std::collections::BTreeMap<String, String>,
    runtime_env: std::collections::BTreeMap<String, String>,
    requests: tokio_stream::wrappers::ReceiverStream<proto::Request>,
) -> anyhow::Result<crate::Transport> {
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

    // Dropping the response stream drops `serve`, which kills the subprocess.
    let response_rx = drive(move |response_tx| async move {
        let command = || {
            let mut connector = connector_init::rpc::new_command(&command);
            connector.envs(&model_env);
            connector.envs(&runtime_env);
            connector
        };
        connector_init::serve::serve(command, codec, false, None, requests.map(Ok), &response_tx)
            .await
    });

    Ok(crate::Transport {
        response_rx,
        container: None,
        process: None,
        vmm: None,
        stderr_overflow: futures::future::pending().boxed(),
    })
}

/// Serve the `connector.Connector` contract over an in-process connector.
/// `requests` begins with the internal Spec request which [`start`] wrote.
fn in_process<P: Protocol>(
    connector: InProcessFn<P>,
    mut requests: tokio_stream::wrappers::ReceiverStream<proto::Request>,
) -> futures::stream::BoxStream<'static, tonic::Result<proto::Response>> {
    drive(move |response_tx| async move {
        let spec = requests
            .next()
            .await
            .and_then(|request| request.kind)
            .and_then(P::unwrap_request)
            .expect("start() wrote the internal Spec request");

        let (connector_tx, connector_rx) = tokio::sync::mpsc::channel(proto_grpc::CHANNEL_BUFFER);
        connector_tx
            .try_send(spec)
            .expect("new connector channel is empty");
        let connector_rx =
            connector(tokio_stream::wrappers::ReceiverStream::new(connector_rx).boxed());

        let result = connector_init::serve::run::<P, _, _>(
            connector_init::Codec::Proto,
            None,
            requests.map(Ok),
            connector_tx,
            connector_rx,
            &response_tx,
        )
        .await;

        if let Err(status) = result {
            _ = response_tx.send(Err(status)).await;
        }
    })
}

/// Drive the future which `serve` builds as the producer of the returned
/// stream, which yields what it sends. Dropping the stream drops the future.
fn drive<S, F>(serve: S) -> futures::stream::BoxStream<'static, tonic::Result<proto::Response>>
where
    S: FnOnce(tokio::sync::mpsc::Sender<tonic::Result<proto::Response>>) -> F,
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let (response_tx, response_rx) = tokio::sync::mpsc::channel(proto_grpc::CHANNEL_BUFFER);

    // The stream ends once `serve` completes, dropping its sender, and its
    // sent responses are read through.
    futures::stream::select(
        tokio_stream::wrappers::ReceiverStream::new(response_rx),
        serve(response_tx)
            .into_stream()
            .filter_map(|()| std::future::ready(None)),
    )
    .boxed()
}

#[cfg(test)]
mod test {
    #[test]
    fn connector_environment_is_the_mount() {
        let mount = std::path::Path::new("/tmp/connector-mount-test");
        let env = super::connector_env(mount).unwrap();

        insta::assert_debug_snapshot!(env, @r#"
        {
            "CONNECTOR_MOUNT": "/tmp/connector-mount-test",
        }
        "#);
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
            assert_eq!(mode(mount.path().parent().unwrap()), 0o700);
            assert_eq!(mode(mount.path()), 0o711);
            assert_eq!(mode(&path), 0o444);
        }

        insta::assert_snapshot!(
            body,
            @r#"{"token":"a.token.here","control_plane_url":"https://control.example.com/","config_encryption_url":"https://config-encryption.example.com/"}"#
        );
    }
}
