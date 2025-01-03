use crate::{verify, LogHandler, Runtime};
use anyhow::Context;
use futures::{channel::mpsc, stream::BoxStream, FutureExt, StreamExt};
use proto_flow::{
    capture::{Request, Response},
    flow::capture_spec::ConnectorType,
};
use unseal;

// Start a capture connector as indicated by the `initial` Request.
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
            proto_grpc::capture::connector_client::ConnectorClient::new(channel)
                .max_decoding_message_size(crate::MAX_MESSAGE_SIZE)
                .max_encoding_message_size(usize::MAX)
                .capture(rx)
                .await
        }
        .boxed()
    }

    let connector_rx = match endpoint {
        models::CaptureEndpoint::Connector(models::ConnectorConfig {
            image,
            config: sealed_config,
        }) => {
            *config_json = unseal::decrypt_sops(&sealed_config).await?.to_string();
            connector_tx.try_send(initial).unwrap();

            crate::image_connector::serve(
                attach_container,
                image,
                runtime.log_handler.clone(),
                log_level,
                &runtime.container_network,
                connector_rx,
                start_rpc,
                &runtime.task_name,
                ops::TaskType::Capture,
                runtime.allow_local,
            )
            .await?
            .boxed()
        }
        models::CaptureEndpoint::Local(_) if !runtime.allow_local => {
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
            *config_json = unseal::decrypt_sops(&sealed_config).await?.to_string();
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
    };

    Ok((connector_tx, connector_rx))
}

fn extract_endpoint<'r>(
    request: &'r mut Request,
) -> anyhow::Result<(models::CaptureEndpoint, &'r mut String)> {
    let (connector_type, config_json) = match request {
        Request {
            spec: Some(spec), ..
        } => (spec.connector_type, &mut spec.config_json),
        Request {
            discover: Some(discover),
            ..
        } => (discover.connector_type, &mut discover.config_json),
        Request {
            validate: Some(validate),
            ..
        } => (validate.connector_type, &mut validate.config_json),
        Request {
            apply: Some(apply), ..
        } => {
            let inner = apply
                .capture
                .as_mut()
                .context("`apply` missing required `capture`")?;

            (inner.connector_type, &mut inner.config_json)
        }
        Request {
            open: Some(open), ..
        } => {
            let inner = open
                .capture
                .as_mut()
                .context("`open` missing required `capture`")?;

            (inner.connector_type, &mut inner.config_json)
        }
        request => return verify("client", "valid first request").fail(request),
    };

    if connector_type == ConnectorType::Image as i32 {
        Ok((
            models::CaptureEndpoint::Connector(
                serde_json::from_str(config_json).context("parsing connector config")?,
            ),
            config_json,
        ))
    } else if connector_type == ConnectorType::Local as i32 {
        Ok((
            models::CaptureEndpoint::Local(
                serde_json::from_str(config_json).context("parsing local config")?,
            ),
            config_json,
        ))
    } else {
        anyhow::bail!("invalid connector type: {connector_type}");
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_http_ingest_spec() {
        if let Err(_) = locate_bin::locate("flow-connector-init") {
            // Skip if `flow-connector-init` isn't available (yet). We're probably on CI.
            // This test is useful as a sanity check for local development
            // and we have plenty of other coverage during CI.
            return;
        }

        let spec = || {
            serde_json::from_value(json!({
                "spec": {
                    "connectorType": "IMAGE",
                    "config": {
                        "image": "ghcr.io/estuary/source-http-ingest:dev",
                        "config": {},
                    }
                }
            }))
            .unwrap()
        };

        let runtime = Runtime::new(
            true,
            "default".to_string(),
            ops::tracing_log_handler,
            None,
            "test".to_string(),
        );

        let (connector_tx, connector_rx) = start(&runtime, spec()).await.unwrap();
        std::mem::drop(connector_tx);

        let mut responses: Vec<_> = connector_rx.collect().await;
        let resp = responses.pop().unwrap().unwrap();

        assert!(resp.spec.is_some());

        let container = resp
            .get_internal()
            .expect("internal decodes")
            .container
            .expect("internal has attached container");

        assert_eq!(
            container.network_ports,
            [proto_flow::flow::NetworkPort {
                number: 8080,
                protocol: String::new(),
                public: true
            }]
        );
    }
}
