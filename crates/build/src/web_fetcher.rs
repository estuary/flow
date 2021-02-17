use futures::future::{FutureExt, LocalBoxFuture};
use protocol::flow;
use url::Url;

pub struct WebFetcher {
    client: hyper::Client<hyper_tls::HttpsConnector<hyper::client::HttpConnector>>,
}

impl WebFetcher {
    pub fn new() -> Self {
        let https = hyper_tls::HttpsConnector::new();
        let client = hyper::Client::builder().build::<_, hyper::Body>(https);
        Self { client }
    }

    #[tracing::instrument(skip(self, resource), fields(%resource))]
    async fn fetch(
        &self,
        resource: &Url,
        _content_type: &flow::ContentType,
    ) -> Result<bytes::Bytes, anyhow::Error> {
        tracing::info!("{} resource", resource.scheme());

        match resource.scheme() {
            "file" => {
                let path = resource
                    .to_file_path()
                    .map_err(|_| anyhow::anyhow!("{} is an invalid file URL", resource))?;
                Ok(std::fs::read(path)?.into())
            }
            "http" | "https" => {
                let request = hyper::Request::get(resource.as_str())
                    .body(hyper::Body::empty())
                    .unwrap();
                let mut response = self.client.request(request).await?;
                let body = hyper::body::to_bytes(response.body_mut()).await?;

                if response.status().is_success() {
                    Ok(body)
                } else {
                    Err(anyhow::anyhow!(
                        "request failed with status {}: {}",
                        response.status(),
                        String::from_utf8_lossy(&body)
                    ))
                }
            }
            _ => anyhow::bail!("{} has an unsupported scheme", resource),
        }
    }
}

impl sources::Fetcher for WebFetcher {
    fn fetch<'a>(
        &'a self,
        resource: &'a Url,
        content_type: &'a flow::ContentType,
    ) -> LocalBoxFuture<'a, Result<bytes::Bytes, anyhow::Error>> {
        self.fetch(resource, content_type).boxed_local()
    }
}
