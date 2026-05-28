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
pub async fn start<L: crate::LogHandler>(
    service: &crate::shard::Service<L>,
    log_level: ops::LogLevel,
    mut initial: derive::Request,
) -> anyhow::Result<(
    mpsc::Sender<derive::Request>,
    BoxStream<'static, tonic::Result<derive::Response>>,
    Option<crate::proto::Container>,
)> {
    let (endpoint, config_json) = extract_endpoint(&mut initial)?;
    let (connector_tx, connector_rx) = mpsc::channel(crate::CHANNEL_BUFFER);

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

    let (connector_rx, container): (BoxStream<'static, tonic::Result<derive::Response>>, _) =
        match endpoint {
            models::DeriveUsing::Connector(models::ConnectorConfig {
                image,
                config: sealed_config,
            }) => {
                *config_json = unseal::decrypt_sops(&sealed_config).await?.into();

                let (rx, container) = crate::image_connector::serve(
                    image,
                    service.log_handler.clone(),
                    log_level,
                    &service.container_network,
                    connector_rx,
                    start_rpc,
                    &service.task_name,
                    ops::TaskType::Derivation,
                    service.plane,
                )
                .await?;

                (rx.boxed(), Some(container))
            }
            models::DeriveUsing::Local(_) if !matches!(service.plane, crate::Plane::Local) => {
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
                *config_json = unseal::decrypt_sops(&sealed_config).await?.into();

                let rx = crate::local_connector::serve(
                    command,
                    env,
                    service.log_handler.clone(),
                    log_level,
                    protobuf,
                    connector_rx,
                )?
                .boxed();

                (rx, None)
            }
            models::DeriveUsing::Sqlite(_) => {
                // In-process connector: maps its anyhow::Result responses to tonic::Result.
                let rx = derive_sqlite::connector(ReceiverStream::new(connector_rx))
                    .map(|r| r.map_err(crate::anyhow_to_status))
                    .boxed();

                (rx, None)
            }
            models::DeriveUsing::Typescript(_) | models::DeriveUsing::Python(_) => {
                unreachable!("extract_endpoint errors on unresolved Typescript/Python connectors")
            }
        };

    _ = connector_tx.try_send(initial);

    Ok((connector_tx, connector_rx, container))
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
