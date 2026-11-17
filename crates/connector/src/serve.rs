//! Per-stream handler of a `connector.Connector` RPC: authorize the first
//! request, start the connector it names, and pump both directions until the
//! connector is done and its logs are read through.

use crate::proto;
use futures::StreamExt;
use tokio::sync::mpsc;
use tracing::Instrument;

pub(crate) async fn serve<R>(
    service: crate::Service,
    transport: crate::service::Transport,
    verified: tokens::jwt::Verified<proto_gazette::Claims>,
    request_rx: R,
    response_tx: mpsc::Sender<tonic::Result<proto::Response>>,
) -> anyhow::Result<()>
where
    R: futures::Stream<Item = tonic::Result<proto::Request>> + Send + Unpin + 'static,
{
    let handler = service.registry.register("connector");
    let span = handler.span();

    serve_inner(
        service,
        transport,
        verified,
        request_rx,
        response_tx,
        handler,
    )
    .instrument(span)
    .await
}

async fn serve_inner<R>(
    service: crate::Service,
    transport: crate::service::Transport,
    verified: tokens::jwt::Verified<proto_gazette::Claims>,
    mut request_rx: R,
    response_tx: mpsc::Sender<tonic::Result<proto::Response>>,
    mut handler: service_kit::HandlerGuard,
) -> anyhow::Result<()>
where
    R: futures::Stream<Item = tonic::Result<proto::Request>> + Send + Unpin + 'static,
{
    handler.set_phase("authorizing");

    let first = request_rx.next().await;
    let Authorized {
        start,
        sqlite_vfs_uri,
        task_name,
        request,
    } = authorize(&handler, transport, verified, first)?;

    handler.set_phase("starting");

    let (log_sink, read_through_rx) = crate::LogSink::response(response_tx.clone());

    let result = start_and_pump(
        &service,
        log_sink,
        start,
        sqlite_vfs_uri,
        &task_name,
        request,
        request_rx,
        &response_tx,
        &mut handler,
    )
    .await;

    // Connector resources have been released, and with them every `LogSink`
    // clone. This crate's own logs precede the terminal status or EOF.
    handler.set_phase("draining");
    _ = read_through_rx.await;

    match result {
        Ok(()) => {
            handler.finish_ok();
            Ok(())
        }
        Err(err) => {
            handler.finish_err(&format!("{err:#}"));
            Err(err)
        }
    }
}

async fn start_and_pump<R>(
    service: &crate::Service,
    log_sink: crate::LogSink,
    start: proto::request::Start,
    sqlite_vfs_uri: Option<String>,
    task_name: &str,
    request: proto::request::Kind,
    request_rx: R,
    response_tx: &mpsc::Sender<tonic::Result<proto::Response>>,
    handler: &mut service_kit::HandlerGuard,
) -> anyhow::Result<()>
where
    R: futures::Stream<Item = tonic::Result<proto::Request>> + Send + Unpin + 'static,
{
    match request {
        proto::request::Kind::Capture(initial) => {
            run::<crate::capture::Capture, R>(
                service,
                log_sink,
                start,
                sqlite_vfs_uri,
                task_name,
                initial,
                request_rx,
                response_tx,
                handler,
            )
            .await
        }
        proto::request::Kind::Derive(initial) => {
            run::<crate::derive::Derive, R>(
                service,
                log_sink,
                start,
                sqlite_vfs_uri,
                task_name,
                initial,
                request_rx,
                response_tx,
                handler,
            )
            .await
        }
        proto::request::Kind::Materialize(initial) => {
            run::<crate::materialize::Materialize, R>(
                service,
                log_sink,
                start,
                sqlite_vfs_uri,
                task_name,
                initial,
                request_rx,
                response_tx,
                handler,
            )
            .await
        }
    }
}

async fn run<P, R>(
    service: &crate::Service,
    log_sink: crate::LogSink,
    start: proto::request::Start,
    sqlite_vfs_uri: Option<String>,
    task_name: &str,
    initial: P::Request,
    request_rx: R,
    response_tx: &mpsc::Sender<tonic::Result<proto::Response>>,
    handler: &mut service_kit::HandlerGuard,
) -> anyhow::Result<()>
where
    P: crate::protocol::Protocol,
    R: futures::Stream<Item = tonic::Result<proto::Request>> + Send + Unpin + 'static,
{
    let ctx = crate::protocol::StartContext {
        container_network: service.container_network.clone(),
        log_sink,
        plane: service.plane,
        process: service.process.clone(),
        secret_resolver: service.secret_resolver.clone(),
        start,
        task_name: task_name.to_string(),
        task_update: service.task_update.clone(),
    };
    let started = tokio::select! {
        () = response_tx.closed() => return Err(client_dropped()),
        started = crate::protocol::start::<P>(ctx, sqlite_vfs_uri, initial) => started?,
    };

    _ = response_tx.send(Ok(started.started.clone())).await;
    handler.set_phase("running");
    pump(request_rx, started, task_name, response_tx).await
}

/// The client is gone, and holding its connector open would serve no one.
fn client_dropped() -> anyhow::Error {
    anyhow::anyhow!("Connector client dropped its response stream")
}

/// Forward requests and responses in independent bursts. A full request
/// channel parks only its forwarding future, leaving responses free to drain.
/// Frames pass through unchanged, apart from sanitizing connector `Log`s.
///
// TODO(johnny): VMM rebase: drop the branch's check of
// `Protocol::spec_execution` on every later Apply or Open, and its tests.
// This pump no longer decodes later requests, and a stream carries one build.
async fn pump<R>(
    mut request_rx: R,
    started: crate::Started,
    task_name: &str,
    response_tx: &mpsc::Sender<tonic::Result<proto::Response>>,
) -> anyhow::Result<()>
where
    R: futures::Stream<Item = tonic::Result<proto::Request>> + Send + Unpin + 'static,
{
    let crate::Started {
        connector_tx,
        mut connector_rx,
        mut stderr_overflow,
        guard,
        ..
    } = started;
    let quoted_task_name = crate::protocol::quote_task_name(task_name);

    let mut forward = std::pin::pin!(async move {
        while let Some(request) = request_rx.next().await {
            let request = request.map_err(crate::status_to_anyhow)?;

            if connector_tx.send(request).await.is_err() {
                break;
            }
        }
        Ok(())
    });
    let mut forwarding = true;

    let result = loop {
        tokio::select! {
            biased;

            // Prefer a ready request error over connector EOF so a client
            // stream failure cannot be reported as successful completion.
            result = &mut forward, if forwarding => match result {
                Ok(()) => forwarding = false,
                Err(err) => break Err(err),
            },

            err = &mut stderr_overflow => break Err(err),

            response = connector_rx.next() => match response {
                Some(Ok(proto::Response { kind: Some(proto::response::Kind::Log(log)) })) => {
                    let log = crate::policy::sanitize_connector_log(&quoted_task_name, log);
                    let response = proto::Response {
                        kind: Some(proto::response::Kind::Log(log)),
                    };
                    _ = response_tx.send(Ok(response)).await;
                }
                Some(Ok(response)) => {
                    _ = response_tx.send(Ok(response)).await;
                }
                Some(Err(status)) => break Err(crate::status_to_anyhow(status)),
                None => break Ok(()),
            },

            () = response_tx.closed() => break Err(client_dropped()),
        }
    };

    // Drop the connector before its `Guard`. Dropping a local connector's
    // stream starts the kill of its subprocess, ahead of the `Guard` removing
    // the mount it uses. Neither drop awaits the subprocess's exit.
    std::mem::drop(connector_rx);
    std::mem::drop(guard);
    result
}

struct Authorized {
    start: proto::request::Start,
    sqlite_vfs_uri: Option<String>,
    task_name: String,
    request: proto::request::Kind,
}

fn authorize(
    handler: &service_kit::HandlerGuard,
    transport: crate::service::Transport,
    verified: tokens::jwt::Verified<proto_gazette::Claims>,
    first: Option<tonic::Result<proto::Request>>,
) -> anyhow::Result<Authorized> {
    let verify = crate::verify("Connector", "first Request", "client");
    let proto::Request { start, kind } = verify.not_eof(first)?;

    let (Some(mut start), Some(request)) = (start, kind) else {
        return Err(crate::invalid_argument(
            "the first Connector request must set `start` and exactly one protocol request"
                .to_string(),
        ));
    };
    // `sqlite_vfs_uri` is an unvalidated path which the connector opens (and
    // creates) as the reactor. It's meaningful only to the shard which recorded
    // the recovery log it names, so a remote caller may never supply one.
    crate::policy::check_remote_sqlite_vfs(
        matches!(transport, crate::service::Transport::Wire),
        !start.sqlite_vfs_uri.is_empty(),
    )
    .map_err(|err| crate::invalid_argument(err.to_string()))?;
    let sqlite_vfs_uri =
        Some(std::mem::take(&mut start.sqlite_vfs_uri)).filter(|uri| !uri.is_empty());

    let (task_type, task_name) = proto_grpc::connector::task_identity(&request)?;
    let task_name = task_name.to_string();
    let authorized = proto_grpc::Authorizer::from_verified(verified)
        .authorize(proto_grpc::connector::task_label_set(task_type, &task_name))
        .map_err(crate::status_to_anyhow)?;

    handler.set_label(&task_name);
    handler.set_field("task_type", task_type.as_str_name());
    handler.set_field(
        "token",
        serde_json::to_string(&authorized.claims()).unwrap(),
    );

    Ok(Authorized {
        start,
        sqlite_vfs_uri,
        task_name,
        request,
    })
}

#[cfg(test)]
mod test {
    use super::*;

    /// A wedged connector — one which neither responds nor exits on a closed
    /// request channel — must not outlive its client, because only `pump`'s
    /// return drops the `Guard` which kills it.
    #[tokio::test]
    async fn a_dropped_response_stream_ends_a_wedged_pump() {
        let (connector_tx, _connector_rx) = mpsc::channel(1);
        let (response_tx, response_rx) = mpsc::channel(1);

        let started = crate::Started {
            started: proto::Response::default(),
            connector_tx,
            connector_rx: futures::stream::pending().boxed(),
            stderr_overflow: Box::pin(futures::future::pending()),
            guard: crate::Guard {
                _process: None,
                _refresh: None,
                _mount: tempfile::tempdir().unwrap(),
            },
        };
        std::mem::drop(response_rx);

        let pump = pump(
            futures::stream::pending(),
            started,
            "acmeCo/derivation",
            &response_tx,
        );
        let err = tokio::time::timeout(std::time::Duration::from_secs(5), pump)
            .await
            .expect("pump must not park on the wedged connector")
            .unwrap_err();

        assert!(
            format!("{err:#}").contains("dropped its response stream"),
            "{err:#}",
        );
    }
}
