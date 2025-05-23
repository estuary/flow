use super::{codec::Codec, rpc};
use futures::{StreamExt, TryStreamExt};
use proto_flow::materialize::{Request, Response};

pub struct Proxy {
    pub entrypoint: Vec<String>,
    pub codec: Codec,
}

#[tonic::async_trait]
impl proto_grpc::materialize::connector_server::Connector for Proxy {
    type MaterializeStream =
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<Response, tonic::Status>> + Send>>;

    async fn materialize(
        &self,
        request: tonic::Request<tonic::Streaming<Request>>,
    ) -> Result<tonic::Response<Self::MaterializeStream>, tonic::Status> {
        crate::inc(&crate::GRPC_SERVER_STARTED_TOTAL);
        let guard = crate::IncOnDrop(&crate::GRPC_SERVER_HANDLED_TOTAL);

        Ok(tonic::Response::new(
            rpc::bidi::<Request, Response, _, _>(
                rpc::new_command(&self.entrypoint),
                self.codec,
                request.into_inner().map_ok(|mut request| {
                    request.internal.clear();
                    request
                }),
                ops::stderr_log_handler,
            )?
            .map(move |response| {
                let _ = &guard;
                response
            })
            .boxed(),
        ))
    }
}
