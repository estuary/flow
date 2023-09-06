use super::Runtime;
use anyhow::Context;
use futures::{Stream, StreamExt, TryStreamExt};
use proto_flow::flow::materialization_spec::ConnectorType;
use proto_flow::materialize::{Request, Response};
use proto_flow::ops;
use proto_flow::runtime::MaterializeRequestExt;
use std::pin::Pin;
use std::sync::Arc;

// Notes on how we can structure materialize middleware:
//
// Request loop:
//  - Spec / Validate / Apply: Unseal. Forward.
//  - Open: Rebuild State. Unseal. Forward.
//  - Load: Acquire shared combiners & combine-right. Forward request iff key is new & not cached.
//  - Flush: Forward.
//      Block awaiting Flushed notification from response loop.
//      Acquire state combiners and drain combiners into forwarded Store requests.
//      Send Flushed stats to response loop.
//  - StartCommit: Forward.
//  - Acknowledge: Forward.
//
//  (Note that Store is never received from Go runtime).
//
// Response loop:
//  - Spec / Validated / Applied / Opened: Forward.
//  - Loaded: Acquire shared combiners & reduce-left.
//  - Flushed:
//       Send Flushed notification to request loop.
//       Block awaiting Flushed stats from request loop.
//       Forward Flushed to runtime enhanced with stats.
//  - StartedCommit: Forward.
//  - Acknowledged: Forward.

mod image;

pub type BoxStream = futures::stream::BoxStream<'static, tonic::Result<Response>>;

#[tonic::async_trait]
impl<L> proto_grpc::materialize::connector_server::Connector for Runtime<L>
where
    L: Fn(&ops::Log) + Send + Sync + Clone + 'static,
{
    type MaterializeStream = BoxStream;

    async fn materialize(
        &self,
        request: tonic::Request<tonic::Streaming<Request>>,
    ) -> tonic::Result<tonic::Response<Self::MaterializeStream>> {
        let conn_info = request
            .extensions()
            .get::<tonic::transport::server::UdsConnectInfo>();
        tracing::debug!(?request, ?conn_info, "started materialize request");

        let response_rx = self.clone().serve_materialize(request.into_inner()).await?;

        Ok(tonic::Response::new(response_rx))
    }
}

impl<L> Runtime<L>
where
    L: Fn(&ops::Log) + Send + Sync + Clone + 'static,
{
    pub async fn serve_materialize<In>(self, request_rx: In) -> tonic::Result<BoxStream>
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
            models::MaterializationEndpoint::Connector(_) => image::connector(
                self.log_handler,
                &self.container_network,
                request_rx,
                &self.task_name,
            )
            .boxed(),
            models::MaterializationEndpoint::Local(_) => todo!(),
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
        let Ok(MaterializeRequestExt{labels: Some(ops::ShardLabeling { log_level, .. })}) = request.get_internal() else { return };

        if let (Some(log_level), Some(set_log_level)) =
            (ops::log::Level::from_i32(log_level), &set_log_level)
        {
            (set_log_level)(log_level);
        }
    })
}

// Returns the MaterializationEndpoint of this Request, and a mutable reference to its inner config_json.
fn extract_endpoint<'r>(
    request: &'r mut Request,
) -> anyhow::Result<(models::MaterializationEndpoint, &'r mut String)> {
    let (connector_type, config_json) = match request {
        Request {
            spec: Some(spec), ..
        } => (spec.connector_type, &mut spec.config_json),
        Request {
            validate: Some(validate),
            ..
        } => (validate.connector_type, &mut validate.config_json),
        Request {
            apply: Some(apply), ..
        } => {
            let inner = apply
                .materialization
                .as_mut()
                .context("`apply` missing required `materialization`")?;

            (inner.connector_type, &mut inner.config_json)
        }
        Request {
            open: Some(open), ..
        } => {
            let inner = open
                .materialization
                .as_mut()
                .context("`open` missing required `materialization`")?;

            (inner.connector_type, &mut inner.config_json)
        }

        _ => anyhow::bail!("request {request:?} does not contain an endpoint"),
    };

    if connector_type == ConnectorType::Image as i32 {
        Ok((
            models::MaterializationEndpoint::Connector(
                serde_json::from_str(config_json).context("parsing connector config")?,
            ),
            config_json,
        ))
    } else if connector_type == ConnectorType::Local as i32 {
        Ok((
            models::MaterializationEndpoint::Local(
                serde_json::from_str(config_json).context("parsing local config")?,
            ),
            config_json,
        ))
    } else {
        anyhow::bail!("invalid connector type: {connector_type}");
    }
}
