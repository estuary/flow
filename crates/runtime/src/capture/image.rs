use super::extract_endpoint;
use crate::{
    image_connector::{Connector, Container, StartRpcFuture, UnsealFuture, Unsealed},
    unseal,
};
use futures::{channel::mpsc, FutureExt, Stream};
use proto_flow::{
    capture::{Request, Response},
    runtime::CaptureRequestExt,
};

fn unseal(mut request: Request) -> Result<UnsealFuture<Request>, Request> {
    if !matches!(
        request,
        Request { spec: Some(_), .. }
            | Request {
                discover: Some(_),
                ..
            }
            | Request {
                validate: Some(_),
                ..
            }
            | Request { apply: Some(_), .. }
            | Request { open: Some(_), .. }
    ) {
        return Err(request); // Not an unseal-able request.
    };

    Ok(async move {
        let (endpoint, config_json) = extract_endpoint(&mut request)?;

        let models::CaptureEndpoint::Connector(models::ConnectorConfig {
            image,
            config: sealed_config,
        }) = endpoint else {
            anyhow::bail!("task connector type has changed and is no longer an image")
        };

        *config_json = unseal::decrypt_sops(&sealed_config).await?.to_string();

        let log_level = match request.get_internal() {
            Ok(CaptureRequestExt {
                labels: Some(labels),
                ..
            }) => Some(labels.log_level()),
            _ => None,
        };

        Ok(Unsealed {
            image,
            log_level,
            request,
        })
    }
    .boxed())
}

fn start_rpc(
    channel: tonic::transport::Channel,
    rx: mpsc::Receiver<Request>,
) -> StartRpcFuture<Response> {
    async move {
        proto_grpc::capture::connector_client::ConnectorClient::new(channel)
            .capture(rx)
            .await
    }
    .boxed()
}

fn attach_container(response: &mut Response, container: Container) {
    response.set_internal(|internal| {
        internal.container = Some(container);
    });
}

pub fn connector<L, R>(
    log_handler: L,
    network: &str,
    request_rx: R,
    task_name: &str,
) -> mpsc::Receiver<tonic::Result<Response>>
where
    L: Fn(&ops::Log) + Clone + Send + Sync + 'static,
    R: Stream<Item = tonic::Result<Request>> + Send + Unpin + 'static,
{
    let (connector, response_rx) = Connector::new(
        attach_container,
        log_handler,
        network,
        request_rx,
        start_rpc,
        task_name,
        ops::TaskType::Capture,
        unseal,
    );
    tokio::spawn(async move { connector.run().await });

    response_rx
}

#[cfg(test)]
mod test {
    use super::connector;
    use futures::StreamExt;
    use serde_json::json;

    #[tokio::test]
    async fn test_http_ingest_spec() {
        if let Err(_) = locate_bin::locate("flow-connector-init") {
            // Skip if `flow-connector-init` isn't available (yet). We're probably on CI.
            // This test is useful as a sanity check for local development
            // and we have plenty of other coverage during CI.
            return;
        }

        let request_rx = futures::stream::repeat(Ok(serde_json::from_value(json!({
            "spec": {
                "connectorType": "IMAGE",
                "config": {
                    "image": "ghcr.io/estuary/source-http-ingest:dev",
                    "config": {},
                }
            }
        }))
        .unwrap()));

        let response_rx = connector(ops::tracing_log_handler, "", request_rx.take(2), "a-task");

        let responses: Vec<_> = response_rx.collect().await;
        assert_eq!(responses.len(), 2);

        for resp in responses {
            let resp = resp.unwrap();

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
}
