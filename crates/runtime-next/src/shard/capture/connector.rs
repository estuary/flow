use anyhow::Context;
use futures::{FutureExt, StreamExt, stream::BoxStream};
use proto_flow::{
    capture::{Request, Response, request},
    flow,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use unseal;
use zeroize::Zeroize;

pub async fn start<Pub: crate::PublisherFactory, Obs: crate::ObserverFactory>(
    service: &crate::shard::Service<Pub, Obs>,
    observer: &Obs::Observer,
    log_level: ops::LogLevel,
    mut initial: Request,
) -> anyhow::Result<(
    mpsc::Sender<Request>,
    BoxStream<'static, tonic::Result<Response>>,
    Option<crate::proto::Container>,
)> {
    let (endpoint, config_json, connector_type, catalog_name) = extract_endpoint(&mut initial)?;
    let (connector_tx, connector_rx) = mpsc::channel(crate::CHANNEL_BUFFER);

    fn start_rpc(
        channel: tonic::transport::Channel,
        rx: mpsc::Receiver<Request>,
    ) -> crate::image_connector::StartRpcFuture<Response> {
        async move {
            proto_grpc::capture::connector_client::ConnectorClient::new(channel)
                .max_decoding_message_size(crate::MAX_MESSAGE_SIZE)
                .max_encoding_message_size(usize::MAX)
                .capture(ReceiverStream::new(rx))
                .await
        }
        .boxed()
    }

    let (mut connector_rx, container) = match endpoint {
        models::CaptureEndpoint::Connector(models::ConnectorConfig {
            image,
            config: sealed_config,
        }) => {
            *config_json = unseal::decrypt_sops(&sealed_config).await?.into();
            // Captures don't have conditional JSON fields, so _codec is unused.
            let (rx, container, _codec) = crate::image_connector::serve(
                image,
                observer.clone(),
                log_level,
                &service.container_network,
                connector_rx,
                start_rpc,
                &service.task_name,
                ops::TaskType::Capture,
                service.plane,
            )
            .await?;
            (rx.boxed(), Some(container))
        }
        models::CaptureEndpoint::Local(_) if !matches!(service.plane, crate::Plane::Local) => {
            return Err(tonic::Status::failed_precondition(
                "Local connectors are not permitted in this context",
            )
            .into());
        }
        models::CaptureEndpoint::Local(models::LocalConfig {
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
            (rx, None)
        }
    };

    _ = connector_tx.try_send(Request {
        spec: Some(request::Spec {
            config_json: "{}".into(),
            connector_type,
        }),
        ..Default::default()
    });

    let verify = crate::verify("Capture", "spec response", "connector");
    let spec_response = match verify.not_eof(connector_rx.next().await)? {
        Response { spec: Some(r), .. } => r,
        response => return Err(verify.fail_msg(response)),
    };

    if let Ok(Some(iam_config)) = iam_auth::extract_iam_auth_from_connector_config(
        config_json,
        &spec_response.config_schema_json,
    ) {
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

    Ok((connector_tx, connector_rx, container))
}

fn extract_endpoint<'r>(
    request: &'r mut Request,
) -> anyhow::Result<(
    models::CaptureEndpoint,
    &'r mut bytes::Bytes,
    i32,
    Option<String>,
)> {
    let (connector_type, config_json, catalog_name) = match request {
        Request {
            spec: Some(spec), ..
        } => (spec.connector_type, &mut spec.config_json, None),
        Request {
            discover: Some(discover),
            ..
        } => (
            discover.connector_type,
            &mut discover.config_json,
            Some(discover.name.clone()),
        ),
        Request {
            validate: Some(validate),
            ..
        } => (
            validate.connector_type,
            &mut validate.config_json,
            Some(validate.name.clone()),
        ),
        Request {
            apply: Some(apply), ..
        } => {
            let catalog_name = apply.capture.as_ref().map(|c| c.name.clone());
            let inner = apply
                .capture
                .as_mut()
                .context("`apply` missing required `capture`")?;
            (inner.connector_type, &mut inner.config_json, catalog_name)
        }
        Request {
            open: Some(open), ..
        } => {
            let catalog_name = open.capture.as_ref().map(|c| c.name.clone());
            let inner = open
                .capture
                .as_mut()
                .context("`open` missing required `capture`")?;
            (inner.connector_type, &mut inner.config_json, catalog_name)
        }
        request => {
            return Err(
                crate::verify("Capture", "valid first request", "controller").fail_msg(request),
            );
        }
    };

    if connector_type == flow::capture_spec::ConnectorType::Image as i32 {
        Ok((
            models::CaptureEndpoint::Connector(
                serde_json::from_slice(config_json).context("parsing connector config")?,
            ),
            config_json,
            connector_type,
            catalog_name,
        ))
    } else if connector_type == flow::capture_spec::ConnectorType::Local as i32 {
        Ok((
            models::CaptureEndpoint::Local(
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
