//! Top-level Shard service implementation.
//!
//! `Runtime` directly implements the controller-facing `Shard` trait.
//! "Controller" here is the peer that drives the shard's lifecycle: the
//! Go runtime in production, an in-process driver such as `flowctl
//! preview`, or a unit-test harness. From this crate's perspective the
//! controller is just the peer of the bidi stream that commands the
//! runtime and bounds its lifecycle.

use crate::{Runtime, materialize, new_channel, proto};
use futures::Stream;
use tokio::sync::mpsc;
use tokio_stream::wrappers;

impl<L: crate::LogHandler> Runtime<L> {
    pub fn spawn_materialize<R>(
        &self,
        controller_rx: R,
    ) -> mpsc::Receiver<tonic::Result<proto::Materialize>>
    where
        R: Stream<Item = tonic::Result<proto::Materialize>> + Send + Unpin + 'static,
    {
        let runtime = self.clone();
        let (controller_tx, response_rx) = new_channel::<tonic::Result<proto::Materialize>>();
        let error_tx = controller_tx.clone();

        tokio::spawn(async move {
            if let Err(err) =
                materialize::shard::handler::serve(runtime, controller_rx, controller_tx).await
            {
                let _ = error_tx.send(Err(crate::anyhow_to_status(err))).await;
            }
        });
        response_rx
    }
}

#[tonic::async_trait]
impl<L: crate::LogHandler> proto_grpc::runtime::shard_server::Shard for Runtime<L> {
    type MaterializeStream = wrappers::ReceiverStream<tonic::Result<proto::Materialize>>;
    type DeriveStream = wrappers::ReceiverStream<tonic::Result<proto::Derive>>;

    async fn materialize(
        &self,
        request: tonic::Request<tonic::Streaming<proto::Materialize>>,
    ) -> tonic::Result<tonic::Response<Self::MaterializeStream>> {
        Ok(tonic::Response::new(wrappers::ReceiverStream::new(
            self.spawn_materialize(request.into_inner()),
        )))
    }

    async fn derive(
        &self,
        _request: tonic::Request<tonic::Streaming<proto::Derive>>,
    ) -> tonic::Result<tonic::Response<Self::DeriveStream>> {
        Err(tonic::Status::unimplemented(
            "Shard.Derive: not in scope for the materialize phase",
        ))
    }
}
