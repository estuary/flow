use futures::future::{FutureExt, LocalBoxFuture};
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
        request: materialize::ValidateRequest,
    ) -> Result<materialize::ValidateResponse, anyhow::Error> {
        anyhow::bail!("not implemented yet")
    }
}

impl validation::Drivers for Drivers {
    fn validate_materialization<'a>(
        &'a self,
        request: materialize::ValidateRequest,
    ) -> LocalBoxFuture<'a, Result<materialize::ValidateResponse, anyhow::Error>> {
        self.validate_materialization(request).boxed_local()
    }
}
