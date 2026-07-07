use anyhow::Context;
use futures::{FutureExt, StreamExt, stream::BoxStream};
use proto_flow::{derive, flow::collection_spec::derivation::ConnectorType};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use unseal;

/// Start a derivation connector as indicated by the `initial` Request.
/// Returns a pair of streams for sending Requests and receiving Responses,
/// plus optional connector container metadata.
///
/// Unlike the materialize / capture connector starts, derivations don't perform
/// an IAM token-exchange Spec pre-dance (no derive connector uses IAM today),
/// and they support an in-process `Sqlite` connector alongside image / local.
///
/// The `plane`, `container_network`, and `task_name` arguments are the narrow
/// slice of a shard [`Service`](crate::shard::Service) this needs, passed
/// directly so the connector-proxy service — which has no `Service` — shares
/// this one implementation with shard startup.
///
/// When `remote_connectors` is set, an image connector runs remotely in the
/// task's data plane through the connector proxy instead of as a local
/// container (see [`RemoteConnectors`](crate::RemoteConnectors)): its sealed
/// config passes through undecrypted (the plane decrypts, as Validate does),
/// and container / codec are read from the first response's ext. A `Local`
/// connector cannot be offloaded and is an error; a `Sqlite` connector always
/// runs in-process (there is no container to offload), so a remote dialer is
/// ignored for it.
pub async fn start<L: crate::Logger>(
    plane: crate::Plane,
    container_network: &str,
    task_name: &str,
    remote_connectors: Option<&std::sync::Arc<dyn crate::RemoteConnectors>>,
    logger: &L,
    log_level: ops::LogLevel,
    mut initial: derive::Request,
) -> anyhow::Result<(
    mpsc::Sender<derive::Request>,
    BoxStream<'static, tonic::Result<derive::Response>>,
    Option<crate::proto::Container>,
    connector_init::Codec,
)> {
    let (connector_tx, connector_rx) = mpsc::channel(crate::CHANNEL_BUFFER);

    // Remote path: offload an image connector to the task's data plane. Decided
    // up front (immutable) so the sealed config is never decrypted locally and
    // `initial` can move into the remote start.
    if let Some(remote) = remote_connectors {
        match connector_type_of(&initial)? {
            ConnectorType::Image => {
                return start_remote(remote, task_name, connector_tx, connector_rx, initial).await;
            }
            ConnectorType::Local => {
                anyhow::bail!("Local derive connectors cannot be offloaded to a remote data plane")
            }
            // Sqlite runs in-process; Typescript/Python are a build-error the
            // local path below reports. Both fall through.
            _ => {}
        }
    }

    let (endpoint, config_json) = extract_endpoint(&mut initial)?;

    fn start_rpc(
        channel: tonic::transport::Channel,
        rx: mpsc::Receiver<derive::Request>,
    ) -> crate::image_connector::StartRpcFuture<derive::Response> {
        async move {
            proto_grpc::derive::connector_client::ConnectorClient::new(channel)
                .max_decoding_message_size(crate::MAX_MESSAGE_SIZE)
                .max_encoding_message_size(usize::MAX)
                .derive(ReceiverStream::new(rx))
                .await
        }
        .boxed()
    }

    let (connector_rx, container, codec): (
        BoxStream<'static, tonic::Result<derive::Response>>,
        _,
        connector_init::Codec,
    ) = match endpoint {
        models::DeriveUsing::Connector(models::ConnectorConfig {
            image,
            config: sealed_config,
        }) => {
            *config_json = unseal::decrypt_sops(&sealed_config).await?.into();

            let (rx, container, codec) = crate::image_connector::serve(
                image,
                logger.clone(),
                log_level,
                container_network,
                connector_rx,
                start_rpc,
                task_name,
                ops::TaskType::Derivation,
                plane,
            )
            .await?;

            (rx.boxed(), Some(container), codec)
        }
        models::DeriveUsing::Local(_) if !matches!(plane, crate::Plane::Local) => {
            return Err(tonic::Status::failed_precondition(
                "Local connectors are not permitted in this context",
            )
            .into());
        }
        models::DeriveUsing::Local(models::LocalConfig {
            command,
            config: sealed_config,
            env,
            protobuf,
        }) => {
            let codec = if protobuf {
                connector_init::Codec::Proto
            } else {
                connector_init::Codec::Json
            };
            *config_json = unseal::decrypt_sops(&sealed_config).await?.into();

            let rx = crate::local_connector::serve(
                command,
                env,
                logger.clone(),
                log_level,
                codec,
                connector_rx,
            )?
            .boxed();

            (rx, None, codec)
        }
        models::DeriveUsing::Sqlite(_) => {
            // In-process connector consuming prost requests directly; maps its
            // anyhow::Result responses to tonic::Result.
            let rx = derive_sqlite::connector(ReceiverStream::new(connector_rx))
                .map(|r| r.map_err(crate::anyhow_to_status))
                .boxed();

            (rx, None, connector_init::Codec::Proto)
        }
        models::DeriveUsing::Typescript(_) | models::DeriveUsing::Python(_) => {
            unreachable!("extract_endpoint errors on unresolved Typescript/Python connectors")
        }
    };

    _ = connector_tx.try_send(initial);

    Ok((connector_tx, connector_rx, container, codec))
}

/// Start a derive connector session against the remote connector proxy: dial
/// once, queue the initial Open, then read container / codec from the first
/// response's ext (which the proxy attaches, as V1's session start does),
/// strip them, and re-prepend the cleaned first response to the rest.
pub(crate) async fn start_remote(
    remote: &std::sync::Arc<dyn crate::RemoteConnectors>,
    task_name: &str,
    connector_tx: mpsc::Sender<derive::Request>,
    connector_rx: mpsc::Receiver<derive::Request>,
    initial: derive::Request,
) -> anyhow::Result<(
    mpsc::Sender<derive::Request>,
    BoxStream<'static, tonic::Result<derive::Response>>,
    Option<crate::proto::Container>,
    connector_init::Codec,
)> {
    // Route the dial by the derivation's own collection name, taken from the
    // Open — the caller's `task_name` may be a synthetic per-shard label (e.g.
    // the catalog-test harness's `test-derive-000`), whereas the remote-connector
    // provider keys data planes by derivation name.
    let dial_name: String = initial
        .open
        .as_ref()
        .and_then(|open| open.collection.as_ref())
        .map(|collection| collection.name.clone())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| task_name.to_string());
    let (channel, metadata) = remote.dial_derive(&dial_name).await?;

    // Queue the initial Open — sealed config intact — then start the RPC. The
    // proxy decrypts sops server-side, exactly as it does for Validate today.
    _ = connector_tx.try_send(initial);
    let mut response_rx =
        proto_grpc::derive::connector_client::ConnectorClient::with_interceptor(channel, metadata)
            .max_decoding_message_size(crate::MAX_MESSAGE_SIZE)
            .max_encoding_message_size(usize::MAX)
            .derive(ReceiverStream::new(connector_rx))
            .await
            .map_err(crate::status_to_anyhow)?
            .into_inner();

    let verify = crate::verify("Derive", "first response", "remote connector");
    let mut first = verify.not_eof(response_rx.next().await)?;
    let ext = first
        .get_internal()
        .context("decoding first response ext")?;
    let container = ext.container.clone();
    let codec = proto_codec_to_init(ext.codec);
    first.set_internal(|ext: &mut crate::proto::DeriveResponseExt| {
        ext.container = None;
        ext.codec = crate::proto::Codec::Proto as i32;
    });

    let stream = futures::stream::once(async move { Ok(first) })
        .chain(response_rx)
        .boxed();

    Ok((connector_tx, stream, container, codec))
}

/// The connector type of a derive Request's endpoint, read without the mutable
/// borrow `extract_endpoint` takes — used to route to the remote proxy before
/// any local decryption.
fn connector_type_of(request: &derive::Request) -> anyhow::Result<ConnectorType> {
    let connector_type = match request {
        derive::Request {
            spec: Some(spec), ..
        } => spec.connector_type,
        derive::Request {
            validate: Some(validate),
            ..
        } => validate.connector_type,
        derive::Request {
            open: Some(open), ..
        } => {
            open.collection
                .as_ref()
                .context("`open` missing required `collection`")?
                .derivation
                .as_ref()
                .context("`collection` missing required `derivation`")?
                .connector_type
        }
        request => {
            return Err(
                crate::verify("Derive", "valid first request", "controller").fail_msg(request)
            );
        }
    };
    ConnectorType::try_from(connector_type)
        .map_err(|_| anyhow::anyhow!("invalid derive connector type: {connector_type}"))
}

fn proto_codec_to_init(codec: i32) -> connector_init::Codec {
    match crate::proto::Codec::try_from(codec).unwrap_or_default() {
        crate::proto::Codec::Json => connector_init::Codec::Json,
        crate::proto::Codec::Proto => connector_init::Codec::Proto,
    }
}

fn extract_endpoint<'r>(
    request: &'r mut derive::Request,
) -> anyhow::Result<(models::DeriveUsing, &'r mut bytes::Bytes)> {
    let (connector_type, config_json) = match request {
        derive::Request {
            spec: Some(spec), ..
        } => (spec.connector_type, &mut spec.config_json),
        derive::Request {
            validate: Some(validate),
            ..
        } => (validate.connector_type, &mut validate.config_json),
        derive::Request {
            open: Some(open), ..
        } => {
            let inner = open
                .collection
                .as_mut()
                .context("`open` missing required `collection`")?
                .derivation
                .as_mut()
                .context("`collection` missing required `derivation`")?;

            (inner.connector_type, &mut inner.config_json)
        }
        request => {
            return Err(
                crate::verify("Derive", "valid first request", "controller").fail_msg(request)
            );
        }
    };

    if connector_type == ConnectorType::Image as i32 {
        Ok((
            models::DeriveUsing::Connector(
                serde_json::from_slice(config_json).context("parsing connector config")?,
            ),
            config_json,
        ))
    } else if connector_type == ConnectorType::Local as i32 {
        Ok((
            models::DeriveUsing::Local(
                serde_json::from_slice(config_json).context("parsing local config")?,
            ),
            config_json,
        ))
    } else if connector_type == ConnectorType::Sqlite as i32 {
        Ok((
            models::DeriveUsing::Sqlite(
                serde_json::from_slice(config_json).context("parsing sqlite config")?,
            ),
            config_json,
        ))
    } else if connector_type == ConnectorType::Typescript as i32
        || connector_type == ConnectorType::Python as i32
    {
        // The V2 runtime never resolves a built-in connector image itself: the
        // control-plane build maps TypeScript / Python derivations to a concrete
        // image (selecting the tag from the task's feature flags) so that Validate
        // and the runtime agree on the connector interface. Encountering an
        // unresolved built-in here means the spec was built without that mapping.
        anyhow::bail!(
            "derive connector type {connector_type} should have been resolved to an image at build time"
        );
    } else {
        anyhow::bail!("invalid derive connector type: {connector_type}");
    }
}
