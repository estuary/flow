//! Start pipeline shared by every connector protocol: locate the
//! endpoint, open a bounded connector request channel with an internal Spec request,
//! dispatch to an image / local subprocess / in-process connector, unseal the
//! endpoint configuration against the Spec response, and build `Started`.
//!
//! [`Protocol`] is a trait over what differs between captures, derivations,
//! and materializations; everything else in this module is protocol-agnostic.

use crate::proto;
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
}

/// Result of opening a protocol RPC on an image connector's channel.
pub(crate) type StartRpcFuture<Response> =
    BoxFuture<'static, tonic::Result<tonic::Response<tonic::Streaming<Response>>>>;

/// Constructor of an in-process connector, given its request stream.
pub(crate) type InProcessFn<P> = Box<
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
    /// which only an endpoint able to record into one may accept; every other
    /// endpoint rejects it with [`sqlite_vfs_uri_error`].
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
    /// Secrets of the task. Keys are JSON pointers into the endpoint config,
    /// while values are catalog names of the task's sibling secrets.
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

    let (connector_tx, connector_rx) = tokio::sync::mpsc::channel(proto_grpc::CHANNEL_BUFFER);
    if !spec_on_own_rpc {
        connector_tx
            .send(P::spec_request(connector_type))
            .await
            .expect("new connector request channel is open");
    }

    let Transport {
        mut connector_rx,
        container,
        codec,
        guard,
        sealed_config,
        spec: own_rpc_spec,
    } = connect::<P>(
        &ctx,
        endpoint,
        connector_type,
        spec_on_own_rpc,
        tokio_stream::wrappers::ReceiverStream::new(connector_rx).boxed(),
    )
    .await?;

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
    let mut iam_token_restart_at = None;

    // Unseal the configuration, or inject decrypted secrets into it.
    (*initial_config_slot, inject_iam) = if ctx.task_name == crate::SPEC_TASK_NAME {
        // A Spec has no task identity under which to unseal, resolve, or inject,
        // so its configuration passes through exactly as the caller sent it.
        (
            bytes::Bytes::copy_from_slice(sealed_config.get().as_bytes()),
            false,
        )
    } else {
        // `decrypt` is called once per distinct secret name, concurrently.
        let resolved = unseal::resolve(&sealed_config, secrets, config_schema, |name| {
            resolve_secret::<P>(&ctx, name)
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

        iam_token_restart_at = Some(proto_flow::as_timestamp(crate::token_restart_deadline(
            std::time::SystemTime::now(),
            tokens.expires_at(),
        )));
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
                token_restart_at: iam_token_restart_at,
                process: ctx.process,
                spec: Some(spec),
            })),
        },
        connector_tx,
        connector_rx,
        guard,
    })
}

/// Decrypt one secret of the task's `secrets` stanza under the task's identity.
async fn resolve_secret<P: Protocol>(
    ctx: &StartContext,
    name: &str,
) -> anyhow::Result<models::RawValue> {
    let decrypted = ctx
        .secret_resolver
        .decrypt(P::TASK_TYPE, &ctx.task_name, models::Secret::new(name))
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

/// A running connector, before its Spec response is verified.
struct Transport<P: Protocol> {
    connector_rx: BoxStream<'static, tonic::Result<P::Response>>,
    container: Option<crate::Container>,
    codec: connector_init::Codec,
    /// Ties an image connector's container to the served stream.
    guard: Option<crate::container::Guard>,
    /// Sealed endpoint configuration of the dispatched endpoint.
    sealed_config: models::RawValue,
    /// Spec already exchanged on an RPC of its own, only when required.
    // TODO(johnny): Remove with V1 derivations.
    spec: Option<proto::response::started::Spec>,
}

async fn connect<P: Protocol>(
    ctx: &StartContext,
    endpoint: Endpoint<P>,
    connector_type: i32,   // TODO(johnny): remove with V1 derivations.
    spec_on_own_rpc: bool, // TODO(johnny): remove.
    requests: BoxStream<'static, P::Request>,
) -> anyhow::Result<Transport<P>> {
    match endpoint {
        Endpoint::Image {
            image,
            config: sealed_config,
        } => {
            let (container, channel, guard, codec) =
                crate::container::start(ctx, &image, P::TASK_TYPE).await?;

            // Drive the Spec to completion on its own RPC before opening the
            // real one, so the connector sees one request per invocation.
            let spec = if spec_on_own_rpc {
                Some(spec_rpc::<P>(channel.clone(), connector_type).await?)
            } else {
                None
            };

            let connector_rx = P::open_rpc(channel, requests).await?.into_inner();

            Ok(Transport {
                connector_rx: connector_rx.boxed(),
                container: Some(container),
                codec,
                guard: Some(guard),
                sealed_config,
                spec,
            })
        }
        Endpoint::Local { .. } if !matches!(ctx.plane, crate::Plane::Local) => {
            Err(tonic::Status::failed_precondition(
                "Local connectors are not permitted in this context",
            )
            .into())
        }
        Endpoint::Local {
            config:
                models::LocalConfig {
                    command,
                    config: sealed_config,
                    env,
                    protobuf,
                },
        } => {
            let codec = if protobuf {
                connector_init::Codec::Proto
            } else {
                connector_init::Codec::Json
            };

            let mut connector = connector_init::rpc::new_command(&command);
            connector.envs(&env);
            connector.env("LOG_FORMAT", "json");
            connector.env(
                "LOG_LEVEL",
                ctx.log_level.or(ops::LogLevel::Info).as_str_name(),
            );

            // Dropping the local connector's response stream kills its subprocess
            // and closes its stderr, so only image connectors need a `Guard`.
            let log_sink = ctx.log_sink.clone();
            let connector_rx = connector_init::rpc::bidi::<P::Request, P::Response, _, _, _>(
                connector,
                codec,
                requests.map(Result::Ok),
                move |log| {
                    let log_sink = log_sink.clone();
                    async move { log_sink.send(log).await }
                },
            )?;

            Ok(Transport {
                connector_rx: connector_rx.boxed(),
                container: None,
                codec,
                guard: None,
                sealed_config,
                spec: None,
            })
        }
        Endpoint::InProcess {
            connector,
            config: sealed_config,
        } => Ok(Transport {
            connector_rx: connector(requests),
            container: None,
            codec: connector_init::Codec::Proto,
            guard: None,
            sealed_config,
            spec: None,
        }),
    }
}

/// Take the connector's Spec from its first response, or fail with the
/// response it sent instead.
// TODO(johnny): inline with retirement of `:dev` images.
fn unwrap_spec_response<P: Protocol>(
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
async fn spec_rpc<P: Protocol>(
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

/// The client sent a recovery-log VFS for a connector which cannot record into one.
pub(crate) fn sqlite_vfs_uri_error() -> anyhow::Error {
    crate::invalid_argument(
        "Start.sqlite_vfs_uri may only be set for a Sqlite derivation connector".to_string(),
    )
}
