//! The shard's client of the `connector.Connector` protocol.
//!
//! `Started` is consumed here, so callers see only their connector's protocol
//! messages and interleaved logs.

pub(crate) use ::connector::proto; // Re-export.
use futures::{StreamExt, stream::BoxStream};
use tokio::sync::mpsc;

pub(crate) use proto_grpc::connector::{
    unwrap_capture, unwrap_derive, unwrap_materialize, wrap_capture, wrap_derive, wrap_materialize,
};

/// What the connector's `Started` told us, for the session which drives it.
pub(crate) struct Started {
    pub container: Option<crate::proto::Container>,
    pub codec: connector_init::Codec,
    pub token_restart_at: Option<std::time::SystemTime>,
}

/// Route, open, and start a connector; returns once it's `Started`.
///
/// `task_name` is the caller's identity for the task; its type is that of
/// `initial`, taken through the same [`task_type`](proto_grpc::connector::task_type) the
/// serving side authorizes with. Together they route and authenticate.
///
/// A connector logs while it starts — an image pull can run for minutes — and
/// those logs stream ahead of `Started`, so they're sunk into `logger` here.
pub(crate) async fn start(
    connector_router: &dyn proto_grpc::connector::Router,
    logger: &impl crate::Logger,
    task_name: &str,
    start: proto::request::Start,
    initial: proto::request::Kind,
) -> anyhow::Result<(
    mpsc::Sender<proto::Request>,
    BoxStream<'static, tonic::Result<proto::Response>>,
    Started,
)> {
    let (connector_tx, connector_rx, started) = proto_grpc::connector::start(
        connector_router,
        &|log| logger.log(log),
        task_name,
        proto::Request {
            start: Some(start),
            kind: Some(initial),
        },
    )
    .await?;
    let proto::response::Started {
        container,
        codec,
        token_restart_at,
        process,
        spec: _,
    } = started;

    if let Some(process) = &process {
        tracing::debug!(task_name, ?process, "connector started on a remote reactor");
    }

    let codec = match proto::response::started::Codec::try_from(codec) {
        Ok(proto::response::started::Codec::Proto) => connector_init::Codec::Proto,
        Ok(proto::response::started::Codec::Json) => connector_init::Codec::Json,
        _ => anyhow::bail!("connector Started has an invalid codec {codec}"),
    };

    Ok((
        connector_tx,
        connector_rx,
        Started {
            container,
            codec,
            token_restart_at: token_restart_at.map(proto_flow::from_timestamp),
        },
    ))
}

/// Keep the logging closure here because inlining it creates a borrowed
/// temporary that requires lifetime scaffolding at each call site.
pub(crate) async fn next<S, R>(
    connector_rx: &mut S,
    logger: &impl crate::Logger,
    unwrap: fn(proto::response::Kind) -> Option<R>,
) -> Option<tonic::Result<R>>
where
    S: futures::Stream<Item = tonic::Result<proto::Response>> + Unpin,
{
    proto_grpc::connector::next(connector_rx, &|log| logger.log(log), unwrap).await
}

/// Replace a leader-stream failure with the connector's own terminal error,
/// if the connector has already failed and its error is immediately ready.
///
/// The leader fails a session when any of its shards does, and broadcasts that
/// failure to every shard — including the shard whose connector caused it. Both
/// errors are then ready at once, and while the `biased` select prefers the
/// connector arm, that only helps if the loop polls again: an error surfaced by
/// the leader arm in the meantime would report the leader's echo of this
/// shard's own failure, rather than the connector error which caused it.
///
/// A ready *response* is discarded rather than handled: the session is failing
/// either way, and the only question is which error describes why.
pub(crate) fn prefer_error<S>(
    connector_rx: &mut S,
    logger: &impl crate::Logger,
    source: &'static str,
    err: anyhow::Error,
) -> anyhow::Error
where
    S: futures::Stream<Item = tonic::Result<proto::Response>> + Unpin,
{
    // A ready log may precede a ready error, so sink logs as we look past them.
    while let Some(item) = futures::FutureExt::now_or_never(connector_rx.next()) {
        match item {
            Some(Err(status)) => {
                return crate::verify(source, "connector response", "connector")
                    .fail_status(status);
            }
            Some(Ok(proto::Response {
                kind: Some(proto::response::Kind::Log(log)),
            })) => logger.log(&log),
            _ => break,
        }
    }
    err
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    #[tokio::test]
    async fn connector_error_wins_over_the_leaders_echo() {
        let leader_err = || anyhow::anyhow!("leader session failed: some peer shard failed");

        // A connector error which is already ready replaces the leader's error:
        // the leader is echoing this shard's own failure back at it.
        let (tx, rx) = mpsc::channel(1);
        tx.send(Err(tonic::Status::unknown(
            "commit failed: refusing to commit store table",
        )))
        .await
        .unwrap();
        let mut connector_rx = ReceiverStream::new(rx);

        let err = prefer_error(
            &mut connector_rx,
            &crate::TracingLogger,
            "Materialize",
            leader_err(),
        );
        assert_eq!(
            format!("{err:#}"),
            "Materialize error (expected connector response) from connector: \
             commit failed: refusing to commit store table"
        );

        // A healthy connector leaves the leader's error in place, as does a
        // connector which has merely reached EOF.
        let (_tx, rx) = mpsc::channel::<tonic::Result<proto::Response>>(1);
        let mut connector_rx = ReceiverStream::new(rx);
        let err = prefer_error(
            &mut connector_rx,
            &crate::TracingLogger,
            "Materialize",
            leader_err(),
        );
        assert_eq!(
            format!("{err:#}"),
            "leader session failed: some peer shard failed"
        );

        let (tx, rx) = mpsc::channel::<tonic::Result<proto::Response>>(1);
        drop(tx);
        let mut connector_rx = ReceiverStream::new(rx);
        let err = prefer_error(
            &mut connector_rx,
            &crate::TracingLogger,
            "Materialize",
            leader_err(),
        );
        assert_eq!(
            format!("{err:#}"),
            "leader session failed: some peer shard failed"
        );
    }
}
