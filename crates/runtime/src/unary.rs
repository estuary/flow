use super::{LogHandler, Runtime};
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use proto_flow::{capture, derive, materialize};
use std::time::Duration;

impl<L: LogHandler> Runtime<L> {
    pub async fn unary_capture(
        self,
        request: capture::Request,
        timeout: Duration,
    ) -> anyhow::Result<capture::Response> {
        let response = self.serve_capture(unary_in(request));
        unary_out(response, timeout).await
    }

    pub async fn unary_derive(
        self,
        request: derive::Request,
        timeout: Duration,
    ) -> anyhow::Result<derive::Response> {
        let response = self.serve_derive(unary_in(request));
        unary_out(response, timeout).await
    }

    pub async fn unary_materialize(
        self,
        request: materialize::Request,
        timeout: Duration,
    ) -> anyhow::Result<materialize::Response> {
        let response = self.serve_materialize(unary_in(request)).boxed();
        unary_out(response, timeout).await
    }
}

fn unary_in<R: Send + 'static>(request: R) -> BoxStream<'static, anyhow::Result<R>> {
    futures::stream::once(async move { Ok(request) }).boxed()
}

async fn unary_out<S, R>(response_rx: S, timeout: Duration) -> anyhow::Result<R>
where
    S: futures::Stream<Item = anyhow::Result<R>>,
{
    let response = async move {
        let mut responses: Vec<R> = response_rx.try_collect().await?;

        if responses.len() != 1 {
            anyhow::bail!("unary request didn't return a response");
        }
        Ok(responses.pop().unwrap())
    };

    tokio::select! {
        response = response => response,
        _ = tokio::time::sleep(timeout) => {
            Err(tonic::Status::deadline_exceeded(r#"Timeout while waiting for the connector's response.
    Please verify any network configuration and retry."#))?
        }
    }
}
