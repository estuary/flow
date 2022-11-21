use super::rpc;
use futures::StreamExt;
use proto_flow::capture::{
    ApplyRequest, ApplyResponse, DiscoverRequest, DiscoverResponse, PullRequest, PullResponse,
    SpecRequest, SpecResponse, ValidateRequest, ValidateResponse,
};

pub struct Driver {
    pub entrypoint: Vec<String>,
}

#[tonic::async_trait]
impl proto_grpc::capture::driver_server::Driver for Driver {
    async fn spec(
        &self,
        request: tonic::Request<SpecRequest>,
    ) -> Result<tonic::Response<SpecResponse>, tonic::Status> {
        let message = rpc::unary(&self.entrypoint, "spec", request.into_inner()).await?;
        Ok(tonic::Response::new(message))
    }

    async fn discover(
        &self,
        request: tonic::Request<DiscoverRequest>,
    ) -> Result<tonic::Response<DiscoverResponse>, tonic::Status> {
        let message = rpc::unary(&self.entrypoint, "discover", request.into_inner()).await?;
        Ok(tonic::Response::new(message))
    }

    async fn validate(
        &self,
        request: tonic::Request<ValidateRequest>,
    ) -> Result<tonic::Response<ValidateResponse>, tonic::Status> {
        let message = rpc::unary(&self.entrypoint, "validate", request.into_inner()).await?;
        Ok(tonic::Response::new(message))
    }

    async fn apply_upsert(
        &self,
        request: tonic::Request<ApplyRequest>,
    ) -> Result<tonic::Response<ApplyResponse>, tonic::Status> {
        let message = rpc::unary(&self.entrypoint, "apply-upsert", request.into_inner()).await?;
        Ok(tonic::Response::new(message))
    }

    async fn apply_delete(
        &self,
        request: tonic::Request<ApplyRequest>,
    ) -> Result<tonic::Response<ApplyResponse>, tonic::Status> {
        let message = rpc::unary(&self.entrypoint, "apply-delete", request.into_inner()).await?;
        Ok(tonic::Response::new(message))
    }

    type PullStream =
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<PullResponse, tonic::Status>> + Send>>;

    async fn pull(
        &self,
        request: tonic::Request<tonic::Streaming<PullRequest>>,
    ) -> Result<tonic::Response<Self::PullStream>, tonic::Status> {
        Ok(tonic::Response::new(
            rpc::bidi::<_, PullResponse, _>(&self.entrypoint, "pull", request.into_inner())?
                .boxed(),
        ))
    }
}
