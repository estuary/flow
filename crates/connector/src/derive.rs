//! Derivation connectors: identity extraction, endpoint unsealing, and
//! dispatch to an image, local, or in-process `derive-sqlite` connector.
use crate::Started;
use anyhow::Context;
use futures::{FutureExt, StreamExt};
use proto_flow::{
    derive::{Request, Response, request, response},
    flow::collection_spec::derivation::ConnectorType,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

/// Start a derivation connector as indicated by the `initial` Request.
///
/// Starts the connector, exchanges Spec, and then sends the client's request.
/// Derivations support an in-process `Sqlite` connector alongside image / local.
/// `sqlite_vfs_uri` is the recorded SQLite VFS an in-process shard threads to a
/// `Sqlite` connector; it is an error for any other connector type.
pub(crate) async fn start(
    plane: crate::Plane,
    container_network: &str,
    log_sink: crate::LogSink,
    log_level: ops::LogLevel,
    task_name: &str,
    sqlite_vfs_uri: String,
    mut initial: Request,
) -> anyhow::Result<Started<Request, Response>> {
    let (endpoint, config_json, connector_type) = extract_endpoint(&mut initial)?;
    let (connector_tx, connector_rx) = mpsc::channel(proto_grpc::CHANNEL_BUFFER);

    if !sqlite_vfs_uri.is_empty() && !matches!(endpoint, models::DeriveUsing::Sqlite(_)) {
        return Err(crate::invalid_argument(
            "Start.sqlite_vfs_uri may only be set for a Sqlite derivation connector".to_string(),
        ));
    }

    fn start_rpc(
        channel: tonic::transport::Channel,
        rx: mpsc::Receiver<Request>,
    ) -> crate::image::StartRpcFuture<Response> {
        async move {
            proto_grpc::derive::connector_client::ConnectorClient::new(channel)
                .max_decoding_message_size(proto_grpc::MAX_MESSAGE_SIZE)
                .max_encoding_message_size(usize::MAX)
                .derive(ReceiverStream::new(rx))
                .await
        }
        .boxed()
    }

    let (mut connector_rx, container, codec, guard) = match endpoint {
        models::DeriveUsing::Connector(models::ConnectorConfig {
            image,
            config: sealed_config,
        }) => {
            *config_json = unseal::decrypt_sops(&sealed_config).await?.into();

            let (rx, container, codec, guard) = crate::image::serve(
                image,
                log_sink,
                log_level,
                container_network,
                connector_rx,
                start_rpc,
                task_name,
                ops::TaskType::Derivation,
                plane,
            )
            .await?;

            (rx.boxed(), Some(container), codec, Some(guard))
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

            let rx = crate::local::serve(command, env, log_sink, log_level, codec, connector_rx)?
                .boxed();

            (rx, None, codec, None)
        }
        models::DeriveUsing::Sqlite(_) => {
            // In-process connector consuming prost requests directly; maps its
            // anyhow::Result responses to tonic::Result.
            let vfs_uri = (!sqlite_vfs_uri.is_empty()).then_some(sqlite_vfs_uri);
            let rx = derive_sqlite::connector(ReceiverStream::new(connector_rx), vfs_uri)
                .map(|r| r.map_err(proto_grpc::anyhow_to_status))
                .boxed();

            (rx, None, connector_init::Codec::Proto, None)
        }
        models::DeriveUsing::Typescript(_) | models::DeriveUsing::Python(_) => {
            unreachable!("extract_endpoint errors on unresolved Typescript/Python connectors")
        }
    };

    _ = connector_tx.try_send(Request {
        kind: Some(request::Kind::Spec(request::Spec {
            config_json: "{}".into(),
            connector_type,
        })),
        ..Default::default()
    });
    let verify = crate::verify("Derive", "spec response", "connector");
    let spec_response = match verify.not_eof(connector_rx.next().await)? {
        Response {
            kind: Some(response::Kind::Spec(response)),
            ..
        } => response,
        response => return Err(verify.fail_msg(response)),
    };

    _ = connector_tx.try_send(initial);

    Ok(Started {
        connector_tx,
        connector_rx,
        container,
        codec,
        token_restart_at: None,
        spec: proto_flow::connector::response::started::Spec::Derive(spec_response),
        guard,
    })
}

fn extract_endpoint<'r>(
    request: &'r mut Request,
) -> anyhow::Result<(models::DeriveUsing, &'r mut bytes::Bytes, i32)> {
    let (connector_type, config_json) = match &mut request.kind {
        Some(request::Kind::Spec(spec)) => (spec.connector_type, &mut spec.config_json),
        Some(request::Kind::Validate(validate)) => {
            (validate.connector_type, &mut validate.config_json)
        }
        Some(request::Kind::Open(open)) => {
            let inner = open
                .collection
                .as_mut()
                .expect("checked by task_name")
                .derivation
                .as_mut()
                .ok_or_else(|| {
                    crate::invalid_argument(
                        "`collection` missing required `derivation`".to_string(),
                    )
                })?;

            (inner.connector_type, &mut inner.config_json)
        }
        _ => unreachable!("checked by task_name"),
    };

    if connector_type == ConnectorType::Image as i32 {
        Ok((
            models::DeriveUsing::Connector(
                serde_json::from_slice(config_json).context("parsing connector config")?,
            ),
            config_json,
            connector_type,
        ))
    } else if connector_type == ConnectorType::Local as i32 {
        Ok((
            models::DeriveUsing::Local(
                serde_json::from_slice(config_json).context("parsing local config")?,
            ),
            config_json,
            connector_type,
        ))
    } else if connector_type == ConnectorType::Sqlite as i32 {
        Ok((
            models::DeriveUsing::Sqlite(
                serde_json::from_slice(config_json).context("parsing sqlite config")?,
            ),
            config_json,
            connector_type,
        ))
    } else if connector_type == ConnectorType::Typescript as i32
        || connector_type == ConnectorType::Python as i32
    {
        // The runtime requires a built-in connector image to be resolved by the
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
