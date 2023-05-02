use super::anyhow_to_status;
use anyhow::Context;
use futures::{StreamExt, TryStreamExt};
use proto_flow::derive::{request, Request, Response};
use proto_flow::flow::collection_spec::{self, derivation::ConnectorType};
use proto_flow::ops;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Clone)]
pub struct Middleware<L>
where
    L: Fn(ops::Log) + Send + Sync + Clone + 'static,
{
    log_handler: L,
    set_log_level: Option<Arc<dyn Fn(ops::log::Level) + Send + Sync>>,
}

pub type BoxStream = std::pin::Pin<Box<dyn futures::Stream<Item = tonic::Result<Response>> + Send>>;

#[tonic::async_trait]
impl<L> proto_grpc::derive::connector_server::Connector for Middleware<L>
where
    L: Fn(ops::Log) + Send + Sync + Clone + 'static,
{
    type DeriveStream = BoxStream;

    async fn derive(
        &self,
        request: tonic::Request<tonic::Streaming<Request>>,
    ) -> tonic::Result<tonic::Response<Self::DeriveStream>> {
        let conn_info = request
            .extensions()
            .get::<tonic::transport::server::UdsConnectInfo>();
        tracing::debug!(?request, ?conn_info, "started derive request");

        let response_rx = self.clone().serve(request.into_inner()).await?;

        Ok(tonic::Response::new(response_rx))
    }
}

impl<L> Middleware<L>
where
    L: Fn(ops::Log) + Send + Sync + Clone + 'static,
{
    pub fn new(
        log_handler: L,
        set_log_level: Option<Arc<dyn Fn(ops::log::Level) + Send + Sync>>,
    ) -> Self {
        Self {
            log_handler,
            set_log_level,
        }
    }

    pub async fn serve<In>(self, request_rx: In) -> tonic::Result<BoxStream>
    where
        In: futures::Stream<Item = tonic::Result<Request>> + Send + Unpin + 'static,
    {
        let mut request_rx = request_rx.peekable();

        let peek = match Pin::new(&mut request_rx).peek().await {
            Some(Ok(peek)) => peek.clone(),
            Some(Err(status)) => return Err(status.clone()),
            None => return Ok(futures::stream::empty().boxed()),
        };

        // NOTE(johnny): To debug requests / responses at any layer of this interceptor stack, try:
        // let request_rx = request_rx.inspect_ok(|request| {
        //     eprintln!("REQUEST: {}", serde_json::to_string(request).unwrap());
        // });
        //
        // let response_rx = response_rx.inspect_ok(|response| {
        //     eprintln!("RESPONSE: {}", serde_json::to_string(response).unwrap());
        // });

        // Request interceptor which adjusts the dynamic log level with each Open.
        let request_rx = super::log_level::adapt_requests(request_rx, self.set_log_level);

        // Request interceptor which filters Request.Read of Ack documents.
        let request_rx = request_rx.try_filter(|request| {
            let keep = if let Some(request::Read {
                uuid: Some(uuid), ..
            }) = &request.read
            {
                proto_gazette::message_flags::ACK_TXN & uuid.node == 0 // Not an ACK.
            } else {
                true
            };
            futures::future::ready(keep)
        });

        // Request interceptor for combining over documents.
        let (request_rx, combine_back) =
            super::combine::adapt_requests(&peek, request_rx).map_err(anyhow_to_status)?;

        let response_rx = match temp_connector_type(&peek).map_err(anyhow_to_status)? {
            ConnectorType::InvalidConnectorType => {
                return Err(tonic::Status::invalid_argument("invalid connector type"));
            }
            ConnectorType::Sqlite => {
                // Invoke the underlying SQLite connector.
                let response_rx = ::derive_sqlite::connector(&peek, request_rx)?;

                // Response interceptor for combining over documents.
                let response_rx = combine_back.adapt_responses(response_rx);

                response_rx.boxed()
            }
            ConnectorType::Image => {
                return Err(tonic::Status::aborted("not implemented"));
            }
            ConnectorType::Typescript => {
                // Request interceptor for stateful RocksDB storage.
                let (request_rx, rocks_back) =
                    super::rocksdb::adapt_requests(&peek, request_rx).map_err(anyhow_to_status)?;

                // Invoke the underlying TypeScript connector.
                let response_rx =
                    super::connectors::typescript_connector(&peek, self.log_handler, request_rx)?;

                // Response interceptor for stateful RocksDB storage.
                let response_rx = rocks_back.adapt_responses(response_rx);
                // Response interceptor for combining over documents.
                let response_rx = combine_back.adapt_responses(response_rx);

                response_rx.boxed()
            }
        };

        Ok(response_rx)
    }

    pub async fn serve_unary(self, request: Request) -> tonic::Result<Response> {
        let request_rx = futures::stream::once(async move { Ok(request) }).boxed();
        let mut responses: Vec<Response> = self.serve(request_rx).await?.try_collect().await?;

        if responses.len() != 1 {
            return Err(tonic::Status::unknown(
                "unary request didn't return a response",
            ));
        }
        Ok(responses.pop().unwrap())
    }
}

// NOTE(johnny): This is a temporary joint to extract the ConnectorType for
// purposes of dispatching to an appropriate connector delegate invocation.
// This will definitely change when we shift `sops` unsealing from Go -> Rust,
// as we'll probably want an InvokeConfig trait or something similar that
// allows us to do the appropriate config unwrapping.
fn temp_connector_type(request: &Request) -> anyhow::Result<ConnectorType> {
    let ct = match (&request.spec, &request.validate, &request.open) {
        (Some(r), None, None) => r.connector_type,
        (None, Some(r), None) => r.connector_type,
        (None, None, Some(r)) => {
            let collection_spec::Derivation { connector_type, .. } = r
                .collection
                .as_ref()
                .context("missing collection")?
                .derivation
                .as_ref()
                .context("missing derivation")?;

            *connector_type
        }
        _ => anyhow::bail!("unexpected request (not Spec, Validate, or Open)"),
    };
    Ok(ConnectorType::from_i32(ct).unwrap_or_default())
}
