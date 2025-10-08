use super::{Request, RequestStream, Response, ResponseStream, protocol::*};
use crate::{LogHandler, Runtime};
use anyhow::Context;
use futures::{StreamExt, TryStreamExt};

#[tonic::async_trait]
impl<L: LogHandler> proto_grpc::runtime::combiner_server::Combiner for Runtime<L> {
    type CombineStream = futures::stream::BoxStream<'static, tonic::Result<Response>>;

    async fn combine(
        &self,
        request: tonic::Request<tonic::Streaming<Request>>,
    ) -> tonic::Result<tonic::Response<Self::CombineStream>> {
        let conn_info = request
            .extensions()
            .get::<tonic::transport::server::UdsConnectInfo>();
        tracing::debug!(?request, ?conn_info, "started capture request");

        let request_rx = crate::stream_status_to_error(request.into_inner());
        let response_rx = crate::stream_error_to_status(self.clone().serve_combine(request_rx));

        Ok(tonic::Response::new(response_rx.boxed()))
    }
}

impl<L: LogHandler> Runtime<L> {
    pub fn serve_combine(self, mut request_rx: impl RequestStream) -> impl ResponseStream {
        coroutines::try_coroutine(move |mut co| async move {
            let Some(open) = request_rx.try_next().await? else {
                return Ok(());
            };
            let (mut accumulator, bindings) = recv_client_open(open)?;

            while let Some(add) = request_rx.try_next().await? {
                recv_client_add(&mut accumulator, add, &bindings)?;
            }

            let (mut drainer, _parser) = accumulator
                .into_drainer()
                .context("preparing to drain combiner")?;
            let mut buf = bytes::BytesMut::new();

            while let Some(drained) = drainer.drain_next()? {
                let response = send_client_response(&bindings, &mut buf, drained)?;
                () = co.yield_(response).await;
            }

            Ok(())
        })
    }
}
