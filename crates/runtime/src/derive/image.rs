use super::extract_endpoint;
use crate::{
    image_connector::{Connector, Container, StartRpcFuture, UnsealFuture, Unsealed},
    unseal,
};
use futures::{channel::mpsc, FutureExt, Stream, StreamExt};
use proto_flow::{
    derive::{Request, Response},
    runtime::DeriveRequestExt,
};

fn unseal(mut request: Request) -> Result<UnsealFuture<Request>, Request> {
    if !matches!(
        request,
        Request { spec: Some(_), .. }
            | Request {
                validate: Some(_),
                ..
            }
            | Request { open: Some(_), .. }
    ) {
        return Err(request); // Not an unseal-able request.
    };

    Ok(async move {
        let (endpoint, config_json) = extract_endpoint(&mut request)?;

        let models::DeriveUsing::Connector(models::ConnectorConfig {
            image,
            config: sealed_config,
        }) = endpoint
        else {
            anyhow::bail!("task connector type has changed and is no longer an image")
        };
        *config_json = unseal::decrypt_sops(&sealed_config).await?.to_string();

        let log_level = match request.get_internal() {
            Ok(DeriveRequestExt {
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
        proto_grpc::derive::connector_client::ConnectorClient::new(channel)
            .derive(rx)
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
) -> impl Stream<Item = anyhow::Result<Response>>
where
    L: Fn(&ops::Log) + Clone + Send + Sync + 'static,
    R: Stream<Item = anyhow::Result<Request>> + Send + 'static,
{
    let request_rx = crate::stream_error_to_status(request_rx).boxed();
    let (connector, response_rx) = Connector::new(
        attach_container,
        log_handler,
        network,
        request_rx,
        start_rpc,
        task_name,
        ops::TaskType::Derivation,
        unseal,
    );
    tokio::spawn(async move { connector.run().await });

    crate::stream_status_to_error(response_rx)
}
