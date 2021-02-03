use futures::future::{FutureExt, LocalBoxFuture};
use models::names;
use protocol::materialize;

#[derive(Debug)]
pub struct Drivers {}

impl Drivers {
    pub fn new() -> Self {
        Self {}
    }

    #[tracing::instrument(skip(self))]
    async fn validate_materialization(
        &self,
        _endpoint_type: names::EndpointType,
        _endpoint_config: serde_json::Value,
        _request: materialize::ValidateRequest,
    ) -> Result<materialize::ValidateResponse, anyhow::Error> {
        anyhow::bail!("not implemented yet")
    }
}

impl validation::Drivers for Drivers {
    fn validate_materialization<'a>(
        &'a self,
        endpoint_type: names::EndpointType,
        endpoint_config: serde_json::Value,
        request: materialize::ValidateRequest,
    ) -> LocalBoxFuture<'a, Result<materialize::ValidateResponse, anyhow::Error>> {
        self.validate_materialization(endpoint_type, endpoint_config, request)
            .boxed_local()
    }
}
