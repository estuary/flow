use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use proto_flow::connector::{Request, Response};

/// Opens `connector.Connector` streams on behalf of a task.
pub trait Router: Send + Sync + 'static {
    /// Open a stream for `task_name`'s connector. The first request on
    /// `request_rx` must set `start` and a protocol request of `task_type`.
    /// Authentication and connection failures are returned as the response
    /// stream's first and only item.
    fn open(
        &self,
        task_type: ops::TaskType,
        task_name: &str,
        request_rx: tokio::sync::mpsc::Receiver<Request>,
    ) -> BoxStream<'static, tonic::Result<Response>>;
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
        request_rx: tokio::sync::mpsc::Receiver<Request>,
    ) -> BoxStream<'static, tonic::Result<Response>> {
        let metadata = match connector_bearer(&self.signer, task_type, task_name) {
            Ok(metadata) => metadata,
            Err(status) => return futures::stream::once(async { Err(status) }).boxed(),
        };
        let channel = match crate::dial_channel(&self.endpoint) {
            Ok(channel) => channel,
            Err(err) => {
                return futures::stream::once(async move {
                    Err(tonic::Status::unavailable(err.to_string()))
                })
                .boxed();
            }
        };
        let mut client =
            super::connector_client::ConnectorClient::with_interceptor(channel, metadata)
                .max_decoding_message_size(crate::MAX_MESSAGE_SIZE)
                .max_encoding_message_size(usize::MAX);

        futures::stream::once(async move {
            client
                .connector(tokio_stream::wrappers::ReceiverStream::new(request_rx))
                .await
                .map(|response| response.into_inner())
        })
        .try_flatten()
        .boxed()
    }
}
