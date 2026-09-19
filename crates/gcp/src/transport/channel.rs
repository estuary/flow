use anyhow::Context;
use anyhow::Result;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context as TaskContext;
use std::task::Poll;
use tonic::body::Body;
use tonic::codegen::Service;
use tonic::codegen::http;
use tonic::transport::ClientTlsConfig;
use tonic::transport::Endpoint;

use crate::transport::RequestInterceptor;

/// A wrapper around `tonic::transport::Channel` that runs a list of interceptors
/// over every outgoing request before sending it.
///
/// Generated gRPC clients accept any `Service`, so this stands in for the tonic
/// channel wherever one is expected. Tonic has its own interceptor support, but
/// `tonic::service::Interceptor::call` is synchronous, meaning it can only support
/// synchronous interceptors.
///
/// GCP clients need asynchronous interceptor support since fetching a GCP access token
/// is asynchronous (tokens may need to make http calls to refresh). This wrapper allows
/// that use-case by implementing `Service` here with async support.
#[derive(Clone)]
pub struct Channel {
    inner: tonic::transport::Channel,
    request_interceptors: Vec<Arc<dyn RequestInterceptor>>,
}

impl Channel {
    pub fn builder(url: &str) -> ChannelBuilder {
        ChannelBuilder::new(url)
    }
}

impl Service<http::Request<Body>> for Channel {
    type Response = http::Response<Body>;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, mut req: http::Request<Body>) -> Self::Future {
        // channel.call below requires a mutable reference, so we replace the channel with
        // a mutable clone to allow moving it into the async block.
        let inner = self.inner.clone();
        let mut channel = std::mem::replace(&mut self.inner, inner);
        // Need to clone these to move them into the async block.
        let request_interceptors = self.request_interceptors.clone();

        Box::pin(async move {
            for interceptor in request_interceptors {
                interceptor
                    .intercept(&mut req)
                    .await
                    .context("request interceptor failed")?;
            }

            channel.call(req).await.map_err(Into::into)
        })
    }
}

pub struct ChannelBuilder {
    url: String,
    request_interceptors: Vec<Arc<dyn RequestInterceptor>>,
}

impl ChannelBuilder {
    pub(crate) fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            request_interceptors: vec![],
        }
    }

    pub fn with_request_interceptor<T: RequestInterceptor>(mut self, interceptor: T) -> Self {
        let interceptor = Arc::new(interceptor);
        self.request_interceptors.push(interceptor);
        self
    }

    pub async fn connect(&self) -> Result<Channel> {
        let inner = get_endpoint(&self.url)?
            .connect()
            .await
            .context("could not connect to endpoint")?;

        let request_interceptors = self.request_interceptors.clone();

        Ok(Channel {
            inner,
            request_interceptors,
        })
    }

    pub fn connect_lazy(&self) -> Result<Channel> {
        let inner = get_endpoint(&self.url)?.connect_lazy();
        let request_interceptors = self.request_interceptors.clone();

        Ok(Channel {
            inner,
            request_interceptors,
        })
    }
}

fn get_endpoint(url: &str) -> Result<Endpoint> {
    let tls_config = ClientTlsConfig::new()
        .with_native_roots()
        .assume_http2(true);
    let endpoint = tonic::transport::Channel::from_shared(url.to_string())
        .context(format!("invalid endpoint {:?}", url))?
        .tls_config(tls_config)
        .context("failed to configure TLS")?;

    Ok(endpoint)
}
