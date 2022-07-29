use std::any::Any;

use super::errors::Error;

use async_trait::async_trait;

#[async_trait]
pub trait NetworkTunnel: Send + Sync {
    // Inspect and/or modify the endpoint spec for which this network tunnel is created.
    // This takes place before the tunnel is started, and may also modify tunnel config
    // if appropriate.
    fn adjust_endpoint_spec(
        &mut self,
        endpoint_spec: serde_json::Value,
    ) -> Result<serde_json::Value, Error>;
    // Setup the network proxy server. Network proxy should be able to listen and accept requests after `prepare` is performed.
    async fn prepare(&mut self) -> Result<(), Error>;
    // Start a long-running task that serves and processes all proxy requests from clients.
    async fn start_serve(&mut self) -> Result<(), Error>;
    // Cleanup the child process. This is called in cases of failure to make sure the child process
    // is properly killed.
    async fn cleanup(&mut self) -> Result<(), Error>;

    // This is only used for testing purposes
    fn as_any(&self) -> &dyn Any;
}
