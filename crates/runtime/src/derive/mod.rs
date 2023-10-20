use crate::Runtime;
use anyhow::Context;
use futures::{Stream, StreamExt, TryStreamExt};
use proto_flow::derive::{request, Request, Response};
use proto_flow::flow::collection_spec::derivation::ConnectorType;
use proto_flow::ops;
use proto_flow::runtime::DeriveRequestExt;
use std::pin::Pin;
use std::sync::Arc;

mod combine;
mod image;
mod local;
mod rocksdb;

#[tonic::async_trait]
impl<H> proto_grpc::derive::connector_server::Connector for Runtime<H>
where
    H: Fn(&ops::Log) + Send + Sync + Clone + 'static,
{
    type DeriveStream = futures::stream::BoxStream<'static, tonic::Result<Response>>;

    async fn derive(
        &self,
        request: tonic::Request<tonic::Streaming<Request>>,
    ) -> tonic::Result<tonic::Response<Self::DeriveStream>> {
        let conn_info = request
            .extensions()
            .get::<tonic::transport::server::UdsConnectInfo>();
        tracing::debug!(?request, ?conn_info, "started derive request");

        let request_rx = crate::stream_status_to_error(request.into_inner());

        let response_rx = self
            .clone()
            .serve_derive(request_rx)
            .await
            .map_err(crate::anyhow_to_status)?;

        Ok(tonic::Response::new(
            crate::stream_error_to_status(response_rx).boxed(),
        ))
    }
}

impl<H> Runtime<H>
where
    H: Fn(&ops::Log) + Send + Sync + Clone + 'static,
{
    pub async fn serve_derive<In>(
        self,
        request_rx: In,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<Response>> + Send>
    where
        In: Stream<Item = anyhow::Result<Request>> + Send + Unpin + 'static,
    {
        let mut request_rx = request_rx.peekable();

        let mut peek_request = match Pin::new(&mut request_rx).peek().await {
            Some(Ok(peek)) => peek.clone(),
            Some(Err(_status)) => return Err(request_rx.try_next().await.unwrap_err()),
            None => return Ok(futures::stream::empty().boxed()),
        };
        let (endpoint, _) = extract_endpoint(&mut peek_request).map_err(crate::anyhow_to_status)?;

        // NOTE(johnny): To debug requests / responses at any layer of this interceptor stack, try:
        // let request_rx = request_rx.inspect_ok(|request| {
        //     eprintln!("REQUEST: {}", serde_json::to_string(request).unwrap());
        // });
        //
        // let response_rx = response_rx.inspect_ok(|response| {
        //     eprintln!("RESPONSE: {}", serde_json::to_string(response).unwrap());
        // });

        // Request interceptor which adjusts the dynamic log level with each Open.
        let request_rx = adjust_log_level(request_rx, self.set_log_level);

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
            combine::adapt_requests(&peek_request, request_rx).map_err(crate::anyhow_to_status)?;

        let response_rx = match endpoint {
            models::DeriveUsing::Connector(_) => {
                // Request interceptor for stateful RocksDB storage.
                let (request_rx, rocks_back) = rocksdb::adapt_requests(&peek_request, request_rx)
                    .map_err(crate::anyhow_to_status)?;

                // Invoke the underlying image connector.
                let response_rx = image::connector(
                    self.log_handler,
                    &self.container_network,
                    request_rx,
                    &self.task_name,
                );

                // Response interceptor for stateful RocksDB storage.
                let response_rx = rocksdb::adapt_responses(rocks_back, response_rx);
                // Response interceptor for combining over documents.
                let response_rx = combine::adapt_responses(combine_back, response_rx);

                response_rx.boxed()
            }
            models::DeriveUsing::Local(_) if !self.allow_local => {
                Err(tonic::Status::failed_precondition(
                    "Local connectors are not permitted in this context",
                ))?
            }
            models::DeriveUsing::Local(_) => {
                // Request interceptor for stateful RocksDB storage.
                let (request_rx, rocks_back) = rocksdb::adapt_requests(&peek_request, request_rx)
                    .map_err(crate::anyhow_to_status)?;

                // Invoke the underlying local connector.
                let response_rx = local::connector(self.log_handler, request_rx);

                // Response interceptor for stateful RocksDB storage.
                let response_rx = rocksdb::adapt_responses(rocks_back, response_rx);
                // Response interceptor for combining over documents.
                let response_rx = combine::adapt_responses(combine_back, response_rx);

                response_rx.boxed()
            }
            models::DeriveUsing::Sqlite(_) => {
                // Invoke the underlying SQLite connector.
                let response_rx = ::derive_sqlite::connector(&peek_request, request_rx)?;

                // Response interceptor for combining over documents.
                let response_rx = combine::adapt_responses(combine_back, response_rx);

                response_rx.boxed()
            }
            models::DeriveUsing::Typescript(_) => unreachable!(),
        };

        Ok(response_rx)
    }
}

fn adjust_log_level<R>(
    request_rx: R,
    set_log_level: Option<Arc<dyn Fn(ops::log::Level) + Send + Sync>>,
) -> impl Stream<Item = anyhow::Result<Request>>
where
    R: Stream<Item = anyhow::Result<Request>> + Send + 'static,
{
    request_rx.inspect_ok(move |request| {
        let Ok(DeriveRequestExt {
            labels: Some(ops::ShardLabeling { log_level, .. }),
            ..
        }) = request.get_internal()
        else {
            return;
        };

        if let (Some(log_level), Some(set_log_level)) =
            (ops::log::Level::from_i32(log_level), &set_log_level)
        {
            (set_log_level)(log_level);
        }
    })
}

// Returns the DeriveUsing of this Request, and a mutable reference to its inner config_json.
fn extract_endpoint<'r>(
    request: &'r mut Request,
) -> anyhow::Result<(models::DeriveUsing, &'r mut String)> {
    let (connector_type, config_json) = match request {
        Request {
            spec: Some(spec), ..
        } => (spec.connector_type, &mut spec.config_json),
        Request {
            validate: Some(validate),
            ..
        } => (validate.connector_type, &mut validate.config_json),
        Request {
            open: Some(open), ..
        } => {
            let inner = open
                .collection
                .as_mut()
                .context("`open` missing required `collection`")?
                .derivation
                .as_mut()
                .context("`collection` missing required `derivation`")?;

            (inner.connector_type, &mut inner.config_json)
        }

        _ => anyhow::bail!("request {request:?} does not contain an endpoint"),
    };

    if connector_type == ConnectorType::Image as i32 {
        Ok((
            models::DeriveUsing::Connector(
                serde_json::from_str(config_json).context("parsing connector config")?,
            ),
            config_json,
        ))
    } else if connector_type == ConnectorType::Local as i32 {
        Ok((
            models::DeriveUsing::Local(
                serde_json::from_str(config_json).context("parsing local config")?,
            ),
            config_json,
        ))
    } else if connector_type == ConnectorType::Sqlite as i32 {
        Ok((
            models::DeriveUsing::Sqlite(
                serde_json::from_str(config_json).context("parsing connector config")?,
            ),
            config_json,
        ))
    } else if connector_type == ConnectorType::Typescript as i32 {
        Ok((
            models::DeriveUsing::Connector(models::ConnectorConfig {
                image: "ghcr.io/estuary/derive-typescript:dev".to_string(),
                config: models::RawValue::from_str(config_json)
                    .context("parsing connector config")?,
            }),
            config_json,
        ))
    } else {
        anyhow::bail!("invalid connector type: {connector_type}");
    }
}
