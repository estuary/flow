use super::rpc;
use futures::StreamExt;
use proto_flow::materialize::{
    ApplyRequest, ApplyResponse, SpecRequest, SpecResponse, TransactionRequest,
    TransactionResponse, ValidateRequest, ValidateResponse,
};

pub struct Driver {
    pub entrypoint: Vec<String>,
}

#[tonic::async_trait]
impl proto_grpc::materialize::driver_server::Driver for Driver {
    async fn spec(
        &self,
        request: tonic::Request<SpecRequest>,
    ) -> Result<tonic::Response<SpecResponse>, tonic::Status> {
        let message = rpc::unary(&self.entrypoint, "spec", request.into_inner()).await?;
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

    type TransactionsStream = std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<TransactionResponse, tonic::Status>> + Send>,
    >;

    async fn transactions(
        &self,
        request: tonic::Request<tonic::Streaming<TransactionRequest>>,
    ) -> Result<tonic::Response<Self::TransactionsStream>, tonic::Status> {
        Ok(tonic::Response::new(
            rpc::bidi::<_, TransactionResponse, _>(
                &self.entrypoint,
                "transactions",
                request.into_inner(),
            )?
            .boxed(),
        ))
    }
}
