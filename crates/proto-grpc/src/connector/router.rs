use futures::StreamExt;
use proto_flow::connector::{Request, Response};
use tokio::sync::mpsc;

/// Opens `connector.Connector` sessions on behalf of a task.
pub trait Router: Send + Sync + 'static {
    /// Open a session for `task_name`'s connector. The first request on
    /// `request_rx` must set `start` and a protocol request of `task_type`.
    /// Authentication and connection failures are returned as the response
    /// channel's first and only item.
    fn open(
        &self,
        task_type: ops::TaskType,
        task_name: &str,
        request_rx: mpsc::Receiver<Request>,
    ) -> mpsc::Receiver<tonic::Result<Response>>;
}

/// Mint the `PROXY_CONNECTOR` metadata presented for a connector stream.
pub fn connector_bearer(
    signer: &crate::Signer,
    task_type: ops::TaskType,
    task_name: &str,
) -> tonic::Result<crate::Metadata> {
    let selector = proto_gazette::broker::LabelSelector {
        include: Some(labels::build_set([
            (labels::TASK_NAME, task_name),
            (labels::TASK_NAME, super::SPEC_TASK_NAME),
            (labels::TASK_TYPE, task_type.as_str_name()),
        ])),
        exclude: None,
    };
    let token = signer.sign(
        proto_flow::capability::PROXY_CONNECTOR,
        task_name.to_string(),
        selector,
        tokens::TimeDelta::minutes(1),
    )?;
    crate::Metadata::new().with_bearer_token(&token)
}

/// `Router` which dials one endpoint and signs every bearer with one signer.
#[derive(Clone)]
pub struct EndpointRouter {
    endpoint: String,
    signer: crate::Signer,
}

impl EndpointRouter {
    pub fn new(endpoint: String, signer: crate::Signer) -> Self {
        Self { endpoint, signer }
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

impl Router for EndpointRouter {
    fn open(
        &self,
        task_type: ops::TaskType,
        task_name: &str,
        request_rx: mpsc::Receiver<Request>,
    ) -> mpsc::Receiver<tonic::Result<Response>> {
        let (response_tx, response_rx) = mpsc::channel(crate::CHANNEL_BUFFER);

        let metadata = match connector_bearer(&self.signer, task_type, task_name) {
            Ok(metadata) => metadata,
            Err(status) => {
                response_tx
                    .try_send(Err(status))
                    .expect("new response channel is open and empty");
                return response_rx;
            }
        };
        let channel = match crate::dial_channel(&self.endpoint) {
            Ok(channel) => channel,
            Err(err) => {
                response_tx
                    .try_send(Err(tonic::Status::unavailable(err.to_string())))
                    .expect("new response channel is open and empty");
                return response_rx;
            }
        };
        let mut client =
            super::connector_client::ConnectorClient::with_interceptor(channel, metadata)
                .max_decoding_message_size(crate::MAX_MESSAGE_SIZE)
                .max_encoding_message_size(usize::MAX);

        let error_tx = response_tx.clone();

        // Keep tonic decoding and response delivery in one focused task so
        // downstream consumers receive buffered, already-decoded messages.
        tokio::spawn(async move {
            let forward = async move {
                let response = match client
                    .connector(tokio_stream::wrappers::ReceiverStream::new(request_rx))
                    .await
                {
                    Ok(response) => response,
                    Err(status) => {
                        _ = response_tx.send(Err(status)).await;
                        return Ok(());
                    }
                };
                let mut tonic_rx = response.into_inner();

                while let Some(response) = tonic_rx.next().await {
                    let terminal = response.is_err();
                    if response_tx.send(response).await.is_err() || terminal {
                        break;
                    }
                }
                Ok(())
            };

            if let Err(status) = crate::catch_panic(forward).await {
                _ = error_tx.send(Err(status)).await;
            }
        });

        response_rx
    }
}
