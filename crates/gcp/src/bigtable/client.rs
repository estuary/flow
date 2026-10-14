use crate::bigtable::types::ReadRowsRequest;
use crate::bigtable::types::RowStream;
use crate::transport::Channel;

use anyhow::Result;
use tonic::IntoRequest;
use tonic::async_trait;

use googleapis_tonic_google_bigtable_v2::google::bigtable::v2;

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait Client: Send + Sync + 'static {
    async fn ping(&self) -> Result<()>;
    async fn read_rows(&self, req: ReadRowsRequest) -> Result<RowStream>;
}

pub fn new(ch: Channel) -> impl Client {
    let inner = v2::bigtable_client::BigtableClient::new(ch);
    ClientImpl { inner }
}

pub struct ClientImpl {
    inner: v2::bigtable_client::BigtableClient<Channel>,
}

#[async_trait]
impl Client for ClientImpl {
    async fn ping(&self) -> Result<()> {
        let mut inner = self.inner.clone();
        let req = v2::PingAndWarmRequest::default();
        let tonic_req = req.into_request();
        inner.ping_and_warm(tonic_req).await?;
        Ok(())
    }

    async fn read_rows(&self, req: ReadRowsRequest) -> Result<RowStream> {
        let mut inner = self.inner.clone();
        let tonic_req = req.into_request();
        let responses = inner.read_rows(tonic_req).await?.into_inner();
        Ok(responses.into())
    }
}
