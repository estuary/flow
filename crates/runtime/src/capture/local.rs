use super::extract_endpoint;
use crate::{
    local_connector::{Connector, UnsealFuture, Unsealed},
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

        let models::CaptureEndpoint::Local(models::LocalConfig {
            command,
            config: sealed_config,
            env,
            protobuf,
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
            command,
            env,
            log_level,
            protobuf,
            request,
        })
    }
    .boxed())
}

pub fn connector<L, R>(log_handler: L, request_rx: R) -> mpsc::Receiver<tonic::Result<Response>>
where
    L: Fn(&ops::Log) + Clone + Send + Sync + 'static,
    R: Stream<Item = tonic::Result<Request>> + Send + Unpin + 'static,
{
    let (connector, response_rx) = Connector::new(log_handler, request_rx, unseal);
    tokio::spawn(async move { connector.run().await });
    response_rx
}
