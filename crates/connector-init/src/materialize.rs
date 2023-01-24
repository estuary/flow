use super::{codec::Codec, rpc};
use futures::StreamExt;
use proto_flow::materialize::{
    ApplyRequest, ApplyResponse, SpecRequest, SpecResponse, TransactionRequest,
    TransactionResponse, ValidateRequest, ValidateResponse,
};

pub struct Driver {
    pub entrypoint: Vec<String>,
    pub codec: Codec,
}

#[tonic::async_trait]
impl proto_grpc::materialize::driver_server::Driver for Driver {
    async fn spec(
        &self,
        request: tonic::Request<SpecRequest>,
    ) -> Result<tonic::Response<SpecResponse>, tonic::Status> {
        let message =
            rpc::unary(&self.entrypoint, self.codec, "spec", request.into_inner()).await?;
        Ok(tonic::Response::new(message))
    }

    async fn validate(
        &self,
        request: tonic::Request<ValidateRequest>,
    ) -> Result<tonic::Response<ValidateResponse>, tonic::Status> {
        let message = rpc::unary(
            &self.entrypoint,
            self.codec,
            "validate",
            request.into_inner(),
        )
        .await?;
        Ok(tonic::Response::new(message))
    }

    async fn apply_upsert(
        &self,
        request: tonic::Request<ApplyRequest>,
    ) -> Result<tonic::Response<ApplyResponse>, tonic::Status> {
        let message = rpc::unary(
            &self.entrypoint,
            self.codec,
            "apply-upsert",
            request.into_inner(),
        )
        .await?;
        Ok(tonic::Response::new(message))
    }

    async fn apply_delete(
        &self,
        request: tonic::Request<ApplyRequest>,
    ) -> Result<tonic::Response<ApplyResponse>, tonic::Status> {
        // For the JSON protocol, there is no apply-delete operation.
        // Instead, a deletion is an apply with no bindings.
        let mut request = request.into_inner();
        if let Codec::Json = self.codec {
            request.materialization.as_mut().unwrap().bindings.clear();
        }

        let message = rpc::unary(&self.entrypoint, self.codec, "apply-delete", request).await?;
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
            rpc::bidi::<TransactionRequest, TransactionResponse, _>(
                &self.entrypoint,
                self.codec,
                "transactions",
                request.into_inner(),
            )?
            .boxed(),
        ))
    }
}
