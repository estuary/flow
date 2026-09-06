//! Materialization connectors: identity extraction, endpoint unsealing, IAM
//! injection, and dispatch to an image, local, or in-process Dekaf connector.
use crate::Started;
use anyhow::Context;
use futures::{FutureExt, StreamExt, TryStreamExt};
use proto_flow::{flow, materialize};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use zeroize::Zeroize;

/// Start a materialization connector as indicated by the `initial` Request.
pub(crate) async fn start(
    plane: crate::Plane,
    container_network: &str,
    log_sink: crate::LogSink,
    log_level: ops::LogLevel,
    task_name: &str,
    mut initial: materialize::Request,
) -> anyhow::Result<Started<materialize::Request, materialize::Response>> {
    let (endpoint, config_json, connector_type, catalog_name, sealed_config_json) =
        extract_endpoint(task_name, &mut initial)?;
    let (connector_tx, connector_rx) = mpsc::channel(proto_grpc::CHANNEL_BUFFER);

    fn start_rpc(
        channel: tonic::transport::Channel,
        rx: mpsc::Receiver<materialize::Request>,
    ) -> crate::image::StartRpcFuture<materialize::Response> {
        async move {
            proto_grpc::materialize::connector_client::ConnectorClient::new(channel)
                .max_decoding_message_size(proto_grpc::MAX_MESSAGE_SIZE)
                .max_encoding_message_size(usize::MAX)
                .materialize(ReceiverStream::new(rx))
                .await
        }
        .boxed()
    }

    // Sealed endpoint configuration, extracted from the matched endpoint and
    // decrypted later, once the connector's spec response is available. `None`
    // for Dekaf, which decrypts its own config outside this path.
    let sealed_config;
    let (mut connector_rx, container, codec, guard) = match endpoint {
        models::MaterializationEndpoint::Connector(models::ConnectorConfig { image, config }) => {
            sealed_config = Some(config);

            let (rx, container, codec, guard) = crate::image::serve(
                image,
                log_sink,
                log_level,
                container_network,
                connector_rx,
                start_rpc,
                task_name,
                ops::TaskType::Materialization,
                plane,
            )
            .await?;

            (rx.boxed(), Some(container), codec, Some(guard))
        }
        models::MaterializationEndpoint::Local(_) if !matches!(plane, crate::Plane::Local) => {
            return Err(tonic::Status::failed_precondition(
                "Local connectors are not permitted in this context",
            )
            .into());
        }
        models::MaterializationEndpoint::Local(models::LocalConfig {
            command,
            config,
            env,
            protobuf,
        }) => {
            sealed_config = Some(config);
            let codec = if protobuf {
                connector_init::Codec::Proto
            } else {
                connector_init::Codec::Json
            };

            let rx = crate::local::serve(command, env, log_sink, log_level, codec, connector_rx)?
                .boxed();

            (rx, None, codec, None)
        }
        models::MaterializationEndpoint::Dekaf(_) => {
            // Dekaf is in-process Rust and consumes prost requests directly. It
            // decrypts its own (nested) endpoint config, so there's nothing to
            // decrypt or overlay here.
            sealed_config = None;

            let rx = dekaf_connector::connector(ReceiverStream::new(connector_rx))
                .map_err(proto_grpc::anyhow_to_status)
                .boxed();

            (rx, None, connector_init::Codec::Proto, None)
        }
    };

    // Send an initial Spec request which may direct us to perform an IAM token exchange.
    _ = connector_tx.try_send(materialize::Request {
        kind: Some(materialize::request::Kind::Spec(
            materialize::request::Spec {
                config_json: "{}".into(),
                connector_type: connector_type,
            },
        )),
        ..Default::default()
    });

    let verify = crate::verify("Materialize", "spec response", "connector");
    let spec_response = match verify.not_eof(connector_rx.next().await)? {
        materialize::Response {
            kind: Some(materialize::response::Kind::Spec(r)),
            ..
        } => r,
        response => return Err(verify.fail_msg(response)),
    };

    // Decrypt the sealed endpoint configuration into the connector request, applying
    // any nonsensitive `sops.overlay` properties subject to schema validation.
    if let Some(sealed_config) = &sealed_config {
        *config_json =
            unseal::overlay::decrypt_with_overlay(sealed_config, &spec_response.config_schema_json)
                .await?
                .into();
    }

    let mut token_restart_at = None;
    if let Ok(Some(iam_config)) = iam_auth::extract_iam_auth_from_connector_config(
        config_json,
        &spec_response.config_schema_json,
    ) {
        // Only proceed with IAM auth if we have an actual catalog name
        if let Some(task_name) = catalog_name.as_deref() {
            let mut tokens = iam_config
                .generate_tokens(task_name)
                .await
                .map_err(proto_grpc::anyhow_to_status)?;

            token_restart_at = Some(crate::token_restart_deadline(
                std::time::SystemTime::now(),
                tokens.expires_at(),
            ));
            *config_json = tokens.inject_into(config_json)?.to_string().into();
            tokens.zeroize();
        }
    }

    // Provide the connector with the sealed endpoint configuration alongside the
    // decrypted `config_json`, so it may emit `configUpdate`s which adjust its own
    // `sops.overlay` without re-encrypting the configuration. Only present on Open.
    if let (Some(sealed_config_json), Some(sealed_config)) = (sealed_config_json, sealed_config) {
        *sealed_config_json = sealed_config.into();
    }

    _ = connector_tx.try_send(initial);

    Ok(Started {
        connector_tx,
        connector_rx,
        container,
        codec,
        token_restart_at,
        spec: proto_flow::connector::response::started::Spec::Materialize(spec_response),
        guard,
    })
}

fn extract_endpoint<'r>(
    task_name: &str,
    request: &'r mut materialize::Request,
) -> anyhow::Result<(
    models::MaterializationEndpoint,
    &'r mut bytes::Bytes,
    i32,
    Option<String>,
    Option<&'r mut bytes::Bytes>,
)> {
    let catalog_name = match task_name {
        crate::SPEC_TASK_NAME => None,
        name => Some(name.to_string()),
    };

    let (connector_type, config_json, sealed_config_json) = match &mut request.kind {
        Some(materialize::request::Kind::Spec(spec)) => {
            (spec.connector_type, &mut spec.config_json, None)
        }
        Some(materialize::request::Kind::Validate(validate)) => {
            (validate.connector_type, &mut validate.config_json, None)
        }
        Some(materialize::request::Kind::Apply(apply)) => {
            let inner = apply
                .materialization
                .as_mut()
                .expect("checked by task_name");
            (inner.connector_type, &mut inner.config_json, None)
        }
        Some(materialize::request::Kind::Open(open)) => {
            let sealed_config_json = &mut open.sealed_config_json;
            let inner = open.materialization.as_mut().expect("checked by task_name");
            (
                inner.connector_type,
                &mut inner.config_json,
                Some(sealed_config_json),
            )
        }
        _ => unreachable!("checked by task_name"),
    };

    if connector_type == flow::materialization_spec::ConnectorType::Image as i32 {
        Ok((
            models::MaterializationEndpoint::Connector(
                serde_json::from_slice(config_json).context("parsing connector config")?,
            ),
            config_json,
            connector_type,
            catalog_name,
            sealed_config_json,
        ))
    } else if connector_type == flow::materialization_spec::ConnectorType::Local as i32 {
        Ok((
            models::MaterializationEndpoint::Local(
                serde_json::from_slice(config_json).context("parsing local config")?,
            ),
            config_json,
            connector_type,
            catalog_name,
            sealed_config_json,
        ))
    } else if connector_type == flow::materialization_spec::ConnectorType::Dekaf as i32 {
        Ok((
            models::MaterializationEndpoint::Dekaf(
                serde_json::from_slice(config_json).context("parsing local config")?,
            ),
            config_json,
            connector_type,
            catalog_name,
            sealed_config_json,
        ))
    } else {
        anyhow::bail!("invalid connector type: {connector_type}");
    }
}
