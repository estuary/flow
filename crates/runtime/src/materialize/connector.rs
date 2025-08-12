use crate::{LogHandler, Runtime};
use anyhow::Context;
use futures::{channel::mpsc, stream::BoxStream, FutureExt, StreamExt, TryStreamExt};
use proto_flow::{
    flow::materialization_spec::ConnectorType,
    materialize::{Request, Response},
};
use unseal;
use zeroize::Zeroize;

// Start a materialization connector as indicated by the `initial` Request.
// Returns a pair of Streams for sending Requests and receiving Responses.
pub async fn start<L: LogHandler>(
    runtime: &Runtime<L>,
    mut initial: Request,
) -> anyhow::Result<(
    mpsc::Sender<Request>,
    BoxStream<'static, anyhow::Result<Response>>,
)> {
    let log_level = initial.get_internal()?.log_level();
    let (endpoint, config_json, connector_type, catalog_name) = extract_endpoint(&mut initial)?;
    let (mut connector_tx, connector_rx) = mpsc::channel(crate::CHANNEL_BUFFER);

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
            proto_grpc::materialize::connector_client::ConnectorClient::new(channel)
                .max_decoding_message_size(crate::MAX_MESSAGE_SIZE)
                .max_encoding_message_size(usize::MAX)
                .materialize(rx)
                .await
        }
        .boxed()
    }

    let mut connector_rx = match endpoint {
        models::MaterializationEndpoint::Connector(models::ConnectorConfig {
            image,
            config: sealed_config,
        }) => {
            *config_json = unseal::decrypt_sops(&sealed_config).await?.into();

            crate::image_connector::serve(
                attach_container,
                1, // Skip first (internal) Spec response.
                image,
                runtime.log_handler.clone(),
                log_level,
                &runtime.container_network,
                connector_rx,
                start_rpc,
                &runtime.task_name,
                ops::TaskType::Materialization,
                runtime.allow_local,
            )
            .await?
            .boxed()
        }
        models::MaterializationEndpoint::Local(_) if !runtime.allow_local => {
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
            *config_json = unseal::decrypt_sops(&sealed_config).await?.into();

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
        models::MaterializationEndpoint::Dekaf(_) => {
            dekaf::connector::connector(connector_rx).boxed()
        }
    };

    // Send an initial Spec request which may direct us to perform an IAM token exchange.
    connector_tx
        .try_send(Request {
            spec: Some(proto_flow::materialize::request::Spec {
                config_json: "{}".into(),
                connector_type: connector_type,
            }),
            ..Default::default()
        })
        .unwrap();

    let verify = crate::verify("connector", "spec response");
    let spec_response = match verify.not_eof(connector_rx.try_next().await?)? {
        Response { spec: Some(r), .. } => r,
        response => return verify.fail(response),
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

    connector_tx.try_send(initial).unwrap();

    Ok((connector_tx, connector_rx))
}

fn extract_endpoint<'r>(
    request: &'r mut Request,
) -> anyhow::Result<(
    models::MaterializationEndpoint,
    &'r mut bytes::Bytes,
    i32,
    Option<String>,
)> {
    let (connector_type, config_json, catalog_name) = match request {
        Request {
            spec: Some(spec), ..
        } => (spec.connector_type, &mut spec.config_json, None),
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
            let catalog_name = apply.materialization.as_ref().map(|m| m.name.clone());
            let inner = apply
                .materialization
                .as_mut()
                .context("`apply` missing required `materialization`")?;

            (inner.connector_type, &mut inner.config_json, catalog_name)
        }
        Request {
            open: Some(open), ..
        } => {
            let catalog_name = open.materialization.as_ref().map(|m| m.name.clone());
            let inner = open
                .materialization
                .as_mut()
                .context("`open` missing required `materialization`")?;

            (inner.connector_type, &mut inner.config_json, catalog_name)
        }
        request => return crate::verify("client", "valid first request").fail(request),
    };

    if connector_type == ConnectorType::Image as i32 {
        Ok((
            models::MaterializationEndpoint::Connector(
                serde_json::from_slice(config_json).context("parsing connector config")?,
            ),
            config_json,
            connector_type,
            catalog_name,
        ))
    } else if connector_type == ConnectorType::Local as i32 {
        Ok((
            models::MaterializationEndpoint::Local(
                serde_json::from_slice(config_json).context("parsing local config")?,
            ),
            config_json,
            connector_type,
            catalog_name,
        ))
    } else if connector_type == ConnectorType::Dekaf as i32 {
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
