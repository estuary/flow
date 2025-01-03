use super::{LogHandler, Runtime};
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use proto_flow::{capture, derive, flow::materialization_spec, materialize};

impl<L: LogHandler> Runtime<L> {
    pub async fn unary_capture(
        self,
        request: capture::Request,
    ) -> anyhow::Result<capture::Response> {
        let response = self.serve_capture(unary_in(request));
        unary_out(response).await
    }

    pub async fn unary_derive(self, request: derive::Request) -> anyhow::Result<derive::Response> {
        let response = self.serve_derive(unary_in(request));
        unary_out(response).await
    }

    pub async fn unary_materialize(
        self,
        request: materialize::Request,
    ) -> anyhow::Result<materialize::Response> {
        let is_dekaf = request.spec.as_ref().is_some_and(|spec| {
            matches!(
                spec.connector_type(),
                materialization_spec::ConnectorType::Dekaf
            )
        }) || request.validate.as_ref().is_some_and(|validate| {
            matches!(
                validate.connector_type(),
                materialization_spec::ConnectorType::Dekaf
            )
        });

        if is_dekaf {
            dekaf::connector::unary_materialize(request).await
        } else {
            let unary_resp = self.serve_materialize(unary_in(request)).boxed();
            unary_out(unary_resp).await
        }
    }
}

fn unary_in<R: Send + 'static>(request: R) -> BoxStream<'static, anyhow::Result<R>> {
    futures::stream::once(async move { Ok(request) }).boxed()
}

async fn unary_out<S, R>(response_rx: S) -> anyhow::Result<R>
where
    S: futures::Stream<Item = anyhow::Result<R>>,
{
    let mut responses: Vec<R> = response_rx.try_collect().await?;

    if responses.len() != 1 {
        anyhow::bail!("unary request didn't return a response");
    }
    Ok(responses.pop().unwrap())
}
