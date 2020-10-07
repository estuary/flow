use estuary_protocol::consumer::{self, shard_client::ShardClient};
use estuary_protocol::flow::{
    self, ingester_client::IngesterClient, testing_client::TestingClient,
};
use estuary_protocol::protocol::{self, journal_client::JournalClient};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed to parse JSON")]
    Json(#[from] serde_json::Error),
    #[error("gRPC transport error")]
    TonicTransport(#[from] tonic::transport::Error),
    #[error("gRPC request error")]
    TonicStatus(#[from] tonic::Status),

    #[error("flow cluster returned !OK status {status}: {message}")]
    NotOK { status: i32, message: String },
    #[error("cluster components disagree on effective test time delta ({0}s vs {1}s)")]
    ClockDeltasDisagree(u64, u64),
}

#[derive(Clone)]
pub struct Cluster {
    pub broker_address: String,
    pub ingester_address: String,
    pub consumer_address: String,
}

// TODO(johnny): Consider this a stub implementation. I expect it to evolve
// significantly, but am just getting something working right now.
impl Cluster {
    pub async fn ingest_client(&self) -> Result<IngesterClient<tonic::transport::Channel>, Error> {
        IngesterClient::connect(self.ingester_address.clone())
            .await
            .map_err(Into::into)
    }

    pub async fn shard_client(&self) -> Result<ShardClient<tonic::transport::Channel>, Error> {
        ShardClient::connect(self.consumer_address.clone())
            .await
            .map_err(Into::into)
    }

    pub async fn advance_time(&self, req: flow::AdvanceTimeRequest) -> Result<(), Error> {
        let mut last = None;

        for uri in [&self.ingester_address, &self.consumer_address]
            .iter()
            .cloned()
        {
            let mut cli = TestingClient::connect(uri.clone())
                .await
                .map_err::<Error, _>(Into::into)?;

            let resp = cli.advance_time(req.clone()).await?.into_inner();
            log::info!(
                "got current clock delta {:?} from {:?}",
                resp.clock_delta_seconds,
                uri,
            );

            if let Some(last) = last {
                if last != resp.clock_delta_seconds {
                    return Err(Error::ClockDeltasDisagree(last, resp.clock_delta_seconds));
                }
            }
            last = Some(resp.clock_delta_seconds);
        }
        Ok(())
    }

    pub async fn stat_shard(
        &self,
        req: consumer::StatRequest,
    ) -> Result<consumer::StatResponse, Error> {
        let mut client = ShardClient::connect(self.consumer_address.clone()).await?;

        let request = tonic::Request::new(req);
        let response = client.stat(request).await?;
        let response = response.into_inner();

        if response.status != consumer::Status::Ok as i32 {
            Err(Error::NotOK {
                status: response.status,
                message: format!("{:?}", response),
            })
        } else {
            Ok(response)
        }
    }

    pub async fn list_journals(
        &self,
        selector: Option<protocol::LabelSelector>,
    ) -> Result<protocol::ListResponse, Error> {
        let mut client = JournalClient::connect(self.broker_address.clone()).await?;

        let request = tonic::Request::new(protocol::ListRequest { selector });
        let response = client.list(request).await?;
        let response = response.into_inner();

        if response.status != consumer::Status::Ok as i32 {
            Err(Error::NotOK {
                status: response.status,
                message: format!("{:?}", response),
            })
        } else {
            Ok(response)
        }
    }

    pub async fn read(
        &self,
        request: protocol::ReadRequest,
    ) -> Result<tonic::Streaming<protocol::ReadResponse>, Error> {
        let mut client = JournalClient::connect(self.broker_address.clone()).await?;

        let response = client.read(request).await?;
        Ok(response.into_inner())
    }

    pub async fn list_shards(
        &self,
        selector: Option<protocol::LabelSelector>,
    ) -> Result<consumer::ListResponse, Error> {
        let mut client = ShardClient::connect(self.consumer_address.clone()).await?;

        let request = tonic::Request::new(consumer::ListRequest {
            selector,
            ..Default::default()
        });
        let response = client.list(request).await?;
        let response = response.into_inner();

        if response.status != consumer::Status::Ok as i32 {
            Err(Error::NotOK {
                status: response.status,
                message: format!("{:?}", response),
            })
        } else {
            Ok(response)
        }
    }

    pub async fn apply_shards(
        &self,
        req: consumer::ApplyRequest,
    ) -> Result<consumer::ApplyResponse, Error> {
        let mut client = ShardClient::connect(self.consumer_address.clone()).await?;

        let request = tonic::Request::new(req);
        let response = client.apply(request).await?;
        Ok(response.into_inner())
    }

    pub async fn apply_journals(
        &self,
        req: protocol::ApplyRequest,
    ) -> Result<protocol::ApplyResponse, Error> {
        let mut client = JournalClient::connect(self.broker_address.clone()).await?;

        let request = tonic::Request::new(req);
        let response = client.apply(request).await?;
        Ok(response.into_inner())
    }
}
