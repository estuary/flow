use estuary_protocol::consumer::{self, shard_client::ShardClient};
use estuary_protocol::protocol::{self, journal_client::JournalClient};
use std::collections::BTreeMap;
use std::str::FromStr;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("http transport error")]
    Hyper(#[from] hyper::Error),
    #[error("http error")]
    Http(#[from] http::Error),
    #[error("failed to parse header")]
    HeaderToStr(#[from] http::header::ToStrError),
    #[error("failed to parse Content-Type")]
    MimeFromStr(#[from] mime::FromStrError),
    #[error("failed to parse JSON")]
    Json(#[from] serde_json::Error),
    #[error("gRPC transport error")]
    TonicTransport(#[from] tonic::transport::Error),
    #[error("gRPC request error")]
    TonicStatus(#[from] tonic::Status),

    #[error("test cluster returned {status}: {message}")]
    NotOK {
        status: hyper::StatusCode,
        message: String,
    },
    #[error("test cluster returned an unexpected Content-Type {0:?}")]
    UnexpectedContentType(Option<String>),
}

pub struct Cluster {
    client: hyper::Client<hyper_tls::HttpsConnector<hyper::client::HttpConnector>>,
    ingest_uri: String,
    consumer_uri: String,
    broker_uri: String,
}

impl Cluster {
    pub fn new() -> Cluster {
        let https = hyper_tls::HttpsConnector::new();
        let client = hyper::Client::builder().build::<_, hyper::Body>(https);

        Cluster {
            client,
            ingest_uri: "http://localhost:9010/ingest".to_owned(),
            consumer_uri: "http://localhost:9020".to_owned(),
            broker_uri: "http://localhost:8080".to_owned(),
        }
    }

    pub async fn ingest(&self, body: serde_json::Value) -> Result<BTreeMap<String, u64>, Error> {
        let body = serde_json::to_vec(&body).expect("Value to_vec cannot fail");

        let req = hyper::Request::builder()
            .method("PUT")
            .uri(&self.ingest_uri)
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(hyper::Body::from(body))?;

        let resp = self.client.request(req).await?;
        let body = check_headers(resp, &mime::APPLICATION_JSON).await?;
        let body = hyper::body::to_bytes(body).await?;

        Ok(serde_json::from_slice(&body)?)
    }

    pub async fn list_shards(&self) -> Result<consumer::ListResponse, Error> {
        let mut client = ShardClient::connect(self.consumer_uri.clone()).await?;

        let request = tonic::Request::new(consumer::ListRequest { selector: None });
        let response = client.list(request).await?;
        Ok(response.into_inner())
    }

    pub async fn apply_shards(
        &self,
        req: consumer::ApplyRequest,
    ) -> Result<consumer::ApplyResponse, Error> {
        let mut client = ShardClient::connect(self.consumer_uri.clone()).await?;

        let request = tonic::Request::new(req);
        let response = client.apply(request).await?;
        Ok(response.into_inner())
    }

    pub async fn apply_journals(
        &self,
        req: protocol::ApplyRequest,
    ) -> Result<protocol::ApplyResponse, Error> {
        let mut client = JournalClient::connect(self.broker_uri.clone()).await?;

        let request = tonic::Request::new(req);
        let response = client.apply(request).await?;
        Ok(response.into_inner())
    }
}

async fn check_headers(
    mut resp: hyper::Response<hyper::Body>,
    expect_content_type: &mime::Mime,
) -> Result<hyper::Body, Error> {
    if !resp.status().is_success() {
        let body = hyper::body::to_bytes(resp.body_mut()).await?;
        return Err(Error::NotOK {
            status: resp.status(),
            message: String::from_utf8_lossy(&body).into_owned(),
        });
    }

    let ct = match resp.headers().get(http::header::CONTENT_TYPE) {
        None => return Err(Error::UnexpectedContentType(None)),
        Some(ct) => mime::Mime::from_str(ct.to_str()?)?,
    };
    if ct != *expect_content_type {
        return Err(Error::UnexpectedContentType(Some(ct.to_string())));
    }
    Ok(resp.into_body())
}
