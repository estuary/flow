use super::errors::Error;

use async_trait::async_trait;

#[async_trait]
pub trait NetworkProxy: Send {
    // Setup the network proxy server. Network proxy should be able to listen and accept requests after `prepare` is performed.
    async fn prepare(&mut self) -> Result<(), Error>;
    // Start a long-running task that serves and processes all proxy requests from clients.
    async fn start_serve(&mut self) -> Result<(), Error>;
}

