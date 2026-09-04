use crate::{LogHandler, Runtime};
use anyhow::Context;
use futures::{FutureExt, StreamExt, channel::mpsc, stream::BoxStream};
use proto_flow::{
    derive::{Request, Response, request},
    flow::collection_spec::derivation::ConnectorType,
};
use unseal;

// Start a derivation connector as indicated by the `initial` Request.
// Returns a pair of Streams for sending Requests and receiving Responses.
pub async fn start<L: LogHandler>(
    runtime: &Runtime<L>,
    mut initial: Request,
) -> anyhow::Result<(
    mpsc::Sender<Request>,
    BoxStream<'static, anyhow::Result<Response>>,
)> {
    let log_level = initial.get_internal()?.log_level();
    let (endpoint, config_json) = extract_endpoint(&mut initial)?;
    let (mut connector_tx, connector_rx) = mpsc::channel(proto_grpc::CHANNEL_BUFFER);

    fn attach_container(response: &mut Response, container: crate::image_connector::Container) {
        response.set_internal(|internal| {
            internal.container = Some(container);
        });
    }

    fn start_rpc(
        channel: tonic::transport::Channel,
        rx: mpsc::Receiver<Request>,
    ) -> crate::image_connector::StartRpcFuture<Response> {
        async move {
            proto_grpc::derive::connector_client::ConnectorClient::new(channel)
                .max_decoding_message_size(proto_grpc::MAX_MESSAGE_SIZE)
                .max_encoding_message_size(usize::MAX)
                .derive(rx)
                .await
        }
        .boxed()
    }

    let connector_rx = match endpoint {
        models::DeriveUsing::Connector(models::ConnectorConfig {
            image,
            config: sealed_config,
        }) => {
            *config_json = unseal::decrypt_sops(&sealed_config).await?.into();
            connector_tx.try_send(initial).unwrap();

            crate::image_connector::serve(
                attach_container,
                0, // Attach container to the first response.
                image,
                runtime.log_handler.clone(),
                log_level,
                &runtime.container_network,
                connector_rx,
                start_rpc,
                &runtime.task_name,
                ops::TaskType::Derivation,
                runtime.plane,
            )
            .await?
            .boxed()
        }
        models::DeriveUsing::Local(_) if !matches!(runtime.plane, crate::Plane::Local) => {
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
            connector_tx.try_send(initial).unwrap();

            crate::local_connector::serve(
                command,
                env,
                runtime.log_handler.clone(),
                log_level,
                protobuf,
                connector_rx,
            )?
            .boxed()
        }
        models::DeriveUsing::Sqlite(_) => {
            // Open carries an internal `sqlite_vfs_uri`: extract and thread to the connector.
            // Other requests (Spec, Validate) omit it.
            let is_open = matches!(initial.kind, Some(request::Kind::Open(_)));
            let vfs_uri = if !is_open || initial.internal.is_empty() {
                None
            } else {
                let ext: proto_flow::runtime::DeriveRequestExt =
                    prost::Message::decode(initial.internal.clone())
                        .context("internal is a DeriveRequestExt")?;
                Some(
                    ext.open
                        .context("expected DeriveRequestExt.open to be set")?
                        .sqlite_vfs_uri,
                )
            };
            connector_tx.try_send(initial).unwrap();
            ::derive_sqlite::connector(connector_rx, vfs_uri).boxed()
        }
        models::DeriveUsing::Typescript(_) => unreachable!(),
        models::DeriveUsing::Python(_) => unreachable!(),
    };

    Ok((connector_tx, connector_rx))
}

fn extract_endpoint<'r>(
    request: &'r mut Request,
) -> anyhow::Result<(models::DeriveUsing, &'r mut bytes::Bytes)> {
    let verify = crate::verify("client", "valid first request");

    // The mutable borrow of `kind` lives as long as the returned references,
    // so an absent `kind` is reported before taking it, and a mis-matched
    // variant reports only itself rather than the request.
    if request.kind.is_none() {
        return verify.fail(&request);
    }
    let (connector_type, config_json) = match request.kind.as_mut().expect("checked above") {
        request::Kind::Spec(spec) => (spec.connector_type, &mut spec.config_json),
        request::Kind::Validate(validate) => (validate.connector_type, &mut validate.config_json),
        request::Kind::Open(open) => {
            let inner = open
                .collection
                .as_mut()
                .context("`open` missing required `collection`")?
                .derivation
                .as_mut()
                .context("`collection` missing required `derivation`")?;

            (inner.connector_type, &mut inner.config_json)
        }
        other => return verify.fail(other),
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
                serde_json::from_slice(config_json).context("parsing connector config")?,
            ),
            config_json,
        ))
    } else if connector_type == ConnectorType::Typescript as i32 {
        Ok((
            models::DeriveUsing::Connector(models::ConnectorConfig {
                image: "ghcr.io/estuary/derive-typescript:dev".to_string(),
                config: serde_json::from_slice::<models::RawValue>(config_json)
                    .context("parsing connector config")?,
            }),
            config_json,
        ))
    } else if connector_type == ConnectorType::Python as i32 {
        Ok((
            models::DeriveUsing::Connector(models::ConnectorConfig {
                image: "ghcr.io/estuary/derive-python:dev".to_string(),
                config: serde_json::from_slice::<models::RawValue>(config_json)
                    .context("parsing connector config")?,
            }),
            config_json,
        ))
    } else {
        anyhow::bail!("invalid connector type: {connector_type}");
    }
}
