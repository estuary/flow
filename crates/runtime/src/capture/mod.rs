use super::Runtime;
use anyhow::Context;
use futures::{Stream, StreamExt, TryStreamExt};
use proto_flow::capture::{Request, Response};
use proto_flow::flow::capture_spec::ConnectorType;
use proto_flow::ops;
use proto_flow::runtime::CaptureRequestExt;
use std::pin::Pin;
use std::sync::Arc;

// Notes on how we can structure capture middleware:

// Request loop:
//  - Spec / Discover / Validate / Apply: Unseal. Forward request.
//  - Open: Rebuild State. Unseal. Retain explicit-ack. Forward request.
//  - Acknowledge: Notify response loop. Forward iff explicit-ack.

// Response loop:
//  - Spec / Discovered / Validated / Applied: Forward response.
//  - Opened: Acquire State. Re-init combiners. Forward response.
//  - Captured: Validate & add to combiner.
//  - Checkpoint: Reduce checkpoint.
//      If "full": block until Acknowledge notification is ready.
//      If Acknowledge notification is ready:
//          Drain combiner into forwarded Captured.
//          Forward Checkpoint enriched with stats.

mod image;
mod local;

pub type BoxStream = futures::stream::BoxStream<'static, tonic::Result<Response>>;

#[tonic::async_trait]
impl<L> proto_grpc::capture::connector_server::Connector for Runtime<L>
where
    L: Fn(&ops::Log) + Send + Sync + Clone + 'static,
{
    type CaptureStream = BoxStream;

    async fn capture(
        &self,
        request: tonic::Request<tonic::Streaming<Request>>,
    ) -> tonic::Result<tonic::Response<Self::CaptureStream>> {
        let conn_info = request
            .extensions()
            .get::<tonic::transport::server::UdsConnectInfo>();
        tracing::debug!(?request, ?conn_info, "started capture request");

        let response_rx = self.clone().serve_capture(request.into_inner()).await?;

        Ok(tonic::Response::new(response_rx))
    }
}

impl<L> Runtime<L>
where
    L: Fn(&ops::Log) + Send + Sync + Clone + 'static,
{
    pub async fn serve_capture<In>(self, request_rx: In) -> tonic::Result<BoxStream>
    where
        In: Stream<Item = tonic::Result<Request>> + Send + Unpin + 'static,
    {
        let mut request_rx = request_rx.peekable();

        let mut peek_request = match Pin::new(&mut request_rx).peek().await {
            Some(Ok(peek)) => peek.clone(),
            Some(Err(status)) => return Err(status.clone()),
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

        // Request interceptor which adjusts the dynamic log level based on internal shard labels.
        let request_rx = adjust_log_level(request_rx, self.set_log_level);

        let response_rx = match endpoint {
            models::CaptureEndpoint::Connector(_) => image::connector(
                self.log_handler,
                &self.container_network,
                request_rx,
                &self.task_name,
            )
            .boxed(),
            models::CaptureEndpoint::Local(_) if !self.allow_local => {
                return Err(tonic::Status::failed_precondition(
                    "Local connectors are not permitted in this context",
                ))
            }
            models::CaptureEndpoint::Local(_) => {
                local::connector(self.log_handler, request_rx).boxed()
            }
        };

        Ok(response_rx)
    }
}

pub fn adjust_log_level<R>(
    request_rx: R,
    set_log_level: Option<Arc<dyn Fn(ops::log::Level) + Send + Sync>>,
) -> impl Stream<Item = tonic::Result<Request>>
where
    R: Stream<Item = tonic::Result<Request>> + Send + 'static,
{
    request_rx.inspect_ok(move |request| {
        let Ok(CaptureRequestExt{labels: Some(ops::ShardLabeling { log_level, .. })}) = request.get_internal() else { return };

        if let (Some(log_level), Some(set_log_level)) =
            (ops::log::Level::from_i32(log_level), &set_log_level)
        {
            (set_log_level)(log_level);
        }
    })
}

// Returns the CaptureEndpoint of this Request, and a mutable reference to its inner config_json.
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

        _ => anyhow::bail!("request {request:?} does not contain an endpoint"),
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
