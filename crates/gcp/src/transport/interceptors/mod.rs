pub mod auth;

use anyhow::Result;
use tonic::async_trait;
use tonic::body::Body;
use tonic::codegen::http;

#[async_trait]
pub trait RequestInterceptor: Send + Sync + 'static {
    async fn intercept(&self, req: &mut http::Request<Body>) -> Result<()>;
}
