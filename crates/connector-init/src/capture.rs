use super::{codec::Codec, rpc};
use futures::StreamExt;
use proto_flow::capture::{Request, Response};

pub struct Proxy {
    pub entrypoint: Vec<String>,
    pub codec: Codec,
}

#[tonic::async_trait]
impl proto_grpc::capture::connector_server::Connector for Proxy {
    type CaptureStream =
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<Response, tonic::Status>> + Send>>;

    async fn capture(
        &self,
        request: tonic::Request<tonic::Streaming<Request>>,
    ) -> Result<tonic::Response<Self::CaptureStream>, tonic::Status> {
        Ok(tonic::Response::new(
            rpc::bidi::<Request, Response, _, _>(
                rpc::new_command(&self.entrypoint),
                self.codec,
                request.into_inner(),
                ops::stderr_log_handler,
            )?
            .boxed(),
        ))
    }
}
