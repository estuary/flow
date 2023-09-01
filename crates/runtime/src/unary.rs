use super::Runtime;
use futures::{stream::BoxStream, Future, FutureExt, StreamExt, TryStreamExt};
use proto_flow::{capture, derive, materialize};
use std::time::Duration;

impl<L> Runtime<L>
where
    L: Fn(&ops::Log) + Send + Sync + Clone + 'static,
{
    pub async fn unary_capture(
        self,
        request: capture::Request,
        timeout: Duration,
    ) -> tonic::Result<capture::Response> {
        let response = self.serve_capture(unary_in(request)).boxed();
        unary_out(response, timeout).await
    }

    pub async fn unary_derive(
        self,
        request: derive::Request,
        timeout: Duration,
    ) -> tonic::Result<derive::Response> {
        let response = self.serve_derive(unary_in(request)).boxed();
        unary_out(response, timeout).await
    }

    pub async fn unary_materialize(
        self,
        request: materialize::Request,
        timeout: Duration,
    ) -> tonic::Result<materialize::Response> {
        let response = self.serve_materialize(unary_in(request)).boxed();
        unary_out(response, timeout).await
    }
}

fn unary_in<R: Send + 'static>(request: R) -> BoxStream<'static, tonic::Result<R>> {
    futures::stream::once(async move { Ok(request) }).boxed()
}

async fn unary_out<F, R>(f: F, timeout: Duration) -> tonic::Result<R>
where
    F: Future<Output = tonic::Result<BoxStream<'static, tonic::Result<R>>>>,
{
    let response = async move {
        let mut responses: Vec<R> = f.await?.try_collect().await?;

        if responses.len() != 1 {
            return Err(tonic::Status::unknown(
                "unary request didn't return a response",
            ));
        }
        Ok(responses.pop().unwrap())
    };

    tokio::select! {
        response = response => response,
        _ = tokio::time::sleep(timeout) => {
            Err(tonic::Status::deadline_exceeded(r#"Timeout while waiting for the connector's response.
    Please verify any network configuration and retry."#))
        }
    }
}
