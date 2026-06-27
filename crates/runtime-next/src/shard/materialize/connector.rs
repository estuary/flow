use anyhow::Context;
use futures::{FutureExt, StreamExt, TryStreamExt, stream::BoxStream};
use proto_flow::{flow, materialize};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use unseal;
use zeroize::Zeroize;

// Start a materialization connector as indicated by the `initial` Request.
// Returns a pair of Streams for sending Requests and receiving Responses,
// plus OpenExtras with decrypted trigger configs and connector metadata.
pub async fn start<P: crate::PublisherFactory, O: crate::ObserverFactory>(
    service: &crate::shard::Service<P, O>,
    observer: &O::Observer,
    log_level: ops::LogLevel,
    mut initial: materialize::Request,
) -> anyhow::Result<(
    mpsc::Sender<materialize::Request>,
    BoxStream<'static, tonic::Result<materialize::Response>>,
    Option<crate::proto::Container>,
    connector_init::Codec,
)> {
    let (endpoint, config_json, connector_type, catalog_name) = extract_endpoint(&mut initial)?;
    let (connector_tx, connector_rx) = mpsc::channel(crate::CHANNEL_BUFFER);

    fn start_rpc(
        channel: tonic::transport::Channel,
        rx: mpsc::Receiver<materialize::Request>,
    ) -> crate::image_connector::StartRpcFuture<materialize::Response> {
        async move {
            proto_grpc::materialize::connector_client::ConnectorClient::new(channel)
                .max_decoding_message_size(crate::MAX_MESSAGE_SIZE)
                .max_encoding_message_size(usize::MAX)
                .materialize(ReceiverStream::new(rx))
                .await
        }
        .boxed()
    }

    let (mut connector_rx, container, codec) = match endpoint {
        models::MaterializationEndpoint::Connector(models::ConnectorConfig {
            image,
            config: sealed_config,
        }) => {
            *config_json = unseal::decrypt_sops(&sealed_config).await?.into();

            let (rx, container, codec) = crate::image_connector::serve(
                image.clone(),
                observer.clone(),
                log_level,
                &service.container_network,
                connector_rx,
                start_rpc,
                &service.task_name,
                ops::TaskType::Materialization,
                service.plane,
            )
            .await?;

            (rx.boxed(), Some(container), codec)
        }
        models::MaterializationEndpoint::Local(_)
            if !matches!(service.plane, crate::Plane::Local) =>
        {
            return Err(tonic::Status::failed_precondition(
                "Local connectors are not permitted in this context",
            )
            .into());
        }
        models::MaterializationEndpoint::Local(models::LocalConfig {
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
                observer.clone(),
                log_level,
                codec,
                connector_rx,
            )?
            .boxed();

            (rx, None, codec)
        }
        models::MaterializationEndpoint::Dekaf(_) => {
            // Dekaf is in-process Rust and consumes prost requests directly.
            let rx = dekaf_connector::connector(ReceiverStream::new(connector_rx))
                .map_err(crate::anyhow_to_status)
                .boxed();

            (rx, None, connector_init::Codec::Proto)
        }
    };

    // Send an initial Spec request which may direct us to perform an IAM token exchange.
    _ = connector_tx.try_send(materialize::Request {
        spec: Some(materialize::request::Spec {
            config_json: "{}".into(),
            connector_type: connector_type,
        }),
        ..Default::default()
    });

    let verify = crate::verify("Materialize", "spec response", "connector");
    let spec_response = match verify.not_eof(connector_rx.next().await)? {
        materialize::Response { spec: Some(r), .. } => r,
        response => return Err(verify.fail_msg(response)),
    };

    if let Ok(Some(iam_config)) = iam_auth::extract_iam_auth_from_connector_config(
        config_json,
        &spec_response.config_schema_json,
    ) {
        // Only proceed with IAM auth if we have an actual catalog name
        if let Some(task_name) = catalog_name.as_deref() {
            let mut tokens = iam_config
                .generate_tokens(task_name)
                .await
                .map_err(crate::anyhow_to_status)?;

            *config_json = tokens.inject_into(config_json)?.to_string().into();
            tokens.zeroize();
        }
    }
    _ = connector_tx.try_send(initial);

    Ok((connector_tx, connector_rx, container, codec))
}

fn extract_endpoint<'r>(
    request: &'r mut materialize::Request,
) -> anyhow::Result<(
    models::MaterializationEndpoint,
    &'r mut bytes::Bytes,
    i32,
    Option<String>,
)> {
    let (connector_type, config_json, catalog_name) = match request {
        materialize::Request {
            spec: Some(spec), ..
        } => (spec.connector_type, &mut spec.config_json, None),
        materialize::Request {
            validate: Some(validate),
            ..
        } => (
            validate.connector_type,
            &mut validate.config_json,
            Some(validate.name.clone()),
        ),
        materialize::Request {
            apply: Some(apply), ..
        } => {
            let catalog_name = apply.materialization.as_ref().map(|m| m.name.clone());
            let inner = apply
                .materialization
                .as_mut()
                .context("`apply` missing required `materialization`")?;

            (inner.connector_type, &mut inner.config_json, catalog_name)
        }
        materialize::Request {
            open: Some(open), ..
        } => {
            let catalog_name = open.materialization.as_ref().map(|m| m.name.clone());
            let inner = open
                .materialization
                .as_mut()
                .context("`open` missing required `materialization`")?;

            (inner.connector_type, &mut inner.config_json, catalog_name)
        }
        request => {
            return Err(
                crate::verify("Materialize", "valid first request", "controller").fail_msg(request),
            );
        }
    };

    if connector_type == flow::materialization_spec::ConnectorType::Image as i32 {
        Ok((
            models::MaterializationEndpoint::Connector(
                serde_json::from_slice(config_json).context("parsing connector config")?,
            ),
            config_json,
            connector_type,
            catalog_name,
        ))
    } else if connector_type == flow::materialization_spec::ConnectorType::Local as i32 {
        Ok((
            models::MaterializationEndpoint::Local(
                serde_json::from_slice(config_json).context("parsing local config")?,
            ),
            config_json,
            connector_type,
            catalog_name,
        ))
    } else if connector_type == flow::materialization_spec::ConnectorType::Dekaf as i32 {
        Ok((
            models::MaterializationEndpoint::Dekaf(
                serde_json::from_slice(config_json).context("parsing local config")?,
            ),
            config_json,
            connector_type,
            catalog_name,
        ))
    } else {
        anyhow::bail!("invalid connector type: {connector_type}");
    }
}
