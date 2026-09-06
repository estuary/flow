//! Per-stream handler of a `connector.Connector` RPC: authorize the first
//! request, start the connector it names, and pump both directions until the
//! connector is done and its logs are read through.

use crate::proto;
use futures::{Stream, StreamExt};
use tokio::sync::mpsc;
use tracing::Instrument;

pub(crate) async fn serve<R>(
    service: crate::Service,
    verified: tokens::jwt::Verified<proto_gazette::Claims>,
    request_rx: R,
    response_tx: mpsc::Sender<tonic::Result<proto::Response>>,
) -> anyhow::Result<()>
where
    R: Stream<Item = tonic::Result<proto::Request>> + Send + Unpin + 'static,
{
    let handler = service.registry.register("connector");
    let span = handler.span();

    serve_inner(service, verified, request_rx, response_tx, handler)
        .instrument(span)
        .await
}

async fn serve_inner<R>(
    service: crate::Service,
    verified: tokens::jwt::Verified<proto_gazette::Claims>,
    mut request_rx: R,
    response_tx: mpsc::Sender<tonic::Result<proto::Response>>,
    mut handler: service_kit::HandlerGuard,
) -> anyhow::Result<()>
where
    R: Stream<Item = tonic::Result<proto::Request>> + Send + Unpin + 'static,
{
    handler.set_phase("authorizing");

    let first = request_rx.next().await;
    let Authorized {
        log_level,
        sqlite_vfs_uri,
        task_name,
        request,
    } = authorize(&handler, verified, first)?;

    handler.set_phase("starting");

    let (log_sink, read_through_rx) = crate::LogSink::response(response_tx.clone());

    let result = start_and_pump(
        &service,
        log_sink,
        log_level,
        sqlite_vfs_uri,
        &task_name,
        request,
        request_rx,
        &response_tx,
        &mut handler,
    )
    .await;

    // Connector resources have been released, allowing their log pumps to
    // finish. Logs precede the stream's terminal status or EOF.
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
    log_level: ops::LogLevel,
    sqlite_vfs_uri: String,
    task_name: &str,
    request: proto::request::Kind,
    request_rx: R,
    response_tx: &mpsc::Sender<tonic::Result<proto::Response>>,
    handler: &mut service_kit::HandlerGuard,
) -> anyhow::Result<()>
where
    R: Stream<Item = tonic::Result<proto::Request>> + Send + Unpin + 'static,
{
    let (plane, network) = (service.plane, service.container_network.as_str());

    match request {
        proto::request::Kind::Capture(initial) => {
            if !sqlite_vfs_uri.is_empty() {
                return Err(sqlite_vfs_uri_error());
            }
            let started =
                crate::capture::start(plane, network, log_sink, log_level, task_name, initial)
                    .await?;

            _ = response_tx
                .send(Ok(started_response(service, &started)))
                .await;
            handler.set_phase("running");

            pump(
                request_rx,
                started,
                response_tx,
                |r| match r {
                    proto::request::Kind::Capture(r) => Some(r),
                    _ => None,
                },
                proto::response::Kind::Capture,
            )
            .await
        }
        proto::request::Kind::Derive(initial) => {
            let started = crate::derive::start(
                plane,
                network,
                log_sink,
                log_level,
                task_name,
                sqlite_vfs_uri,
                initial,
            )
            .await?;

            _ = response_tx
                .send(Ok(started_response(service, &started)))
                .await;
            handler.set_phase("running");

            pump(
                request_rx,
                started,
                response_tx,
                |r| match r {
                    proto::request::Kind::Derive(r) => Some(r),
                    _ => None,
                },
                proto::response::Kind::Derive,
            )
            .await
        }
        proto::request::Kind::Materialize(initial) => {
            if !sqlite_vfs_uri.is_empty() {
                return Err(sqlite_vfs_uri_error());
            }
            let started =
                crate::materialize::start(plane, network, log_sink, log_level, task_name, initial)
                    .await?;

            _ = response_tx
                .send(Ok(started_response(service, &started)))
                .await;
            handler.set_phase("running");

            pump(
                request_rx,
                started,
                response_tx,
                |r| match r {
                    proto::request::Kind::Materialize(r) => Some(r),
                    _ => None,
                },
                proto::response::Kind::Materialize,
            )
            .await
        }
    }
}

fn sqlite_vfs_uri_error() -> anyhow::Error {
    crate::invalid_argument(
        "Start.sqlite_vfs_uri may only be set for a Sqlite derivation connector".to_string(),
    )
}

/// Render the `Started` a just-started connector reports. Synchronous, so the
/// borrow of `started` — whose response stream is `Send` but not `Sync` —
/// never spans the send's await.
fn started_response<Req, Resp>(
    service: &crate::Service,
    started: &crate::Started<Req, Resp>,
) -> proto::Response {
    let codec = match started.codec {
        connector_init::Codec::Proto => proto::response::started::Codec::Proto,
        connector_init::Codec::Json => proto::response::started::Codec::Json,
    };

    proto::Response {
        kind: Some(proto::response::Kind::Started(proto::response::Started {
            container: started.container.clone(),
            codec: codec as i32,
            token_restart_at: started.token_restart_at.map(proto_flow::as_timestamp),
            process: service.process.clone(),
            spec: Some(started.spec.clone()),
        })),
    }
}

/// Forward requests and responses in independent bursts. A full request
/// channel parks only its forwarding future, leaving responses free to drain.
async fn pump<Req, Resp, R>(
    mut request_rx: R,
    started: crate::Started<Req, Resp>,
    response_tx: &mpsc::Sender<tonic::Result<proto::Response>>,
    unwrap: fn(proto::request::Kind) -> Option<Req>,
    wrap: fn(Resp) -> proto::response::Kind,
) -> anyhow::Result<()>
where
    R: Stream<Item = tonic::Result<proto::Request>> + Send + Unpin + 'static,
    Req: Send + 'static,
{
    let crate::Started {
        connector_tx,
        mut connector_rx,
        guard,
        ..
    } = started;

    let mut forward = std::pin::pin!(async move {
        while let Some(result) = request_rx.next().await {
            let proto::Request { start, kind } = result.map_err(crate::status_to_anyhow)?;

            if start.is_some() {
                return Err(crate::invalid_argument(
                    "only the first Connector request may set `start`".to_string(),
                ));
            }
            let Some(request) = kind.and_then(unwrap) else {
                return Err(crate::invalid_argument(
                    "every Connector request must set exactly one protocol request, of the type \
                     established by the first request"
                        .to_string(),
                ));
            };
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

            // Prefer a ready request error over connector EOF so malformed
            // client input cannot be reported as successful completion.
            result = &mut forward, if forwarding => match result {
                Ok(()) => forwarding = false,
                Err(err) => break Err(err),
            },

            response = connector_rx.next() => match response {
                Some(Ok(response)) => {
                    let response = proto::Response { kind: Some(wrap(response)) };
                    if response_tx.send(Ok(response)).await.is_err() {
                        break Err(anyhow::anyhow!(
                            "Connector client dropped its response stream"
                        ));
                    }
                }
                Some(Err(status)) => break Err(crate::status_to_anyhow(status)),
                None => break Ok(()),
            },
        }
    };

    // Killing an image connector closes stderr, allowing log draining to finish.
    std::mem::drop(guard);
    result
}

struct Authorized {
    log_level: ops::LogLevel,
    sqlite_vfs_uri: String,
    task_name: String,
    request: proto::request::Kind,
}

fn authorize(
    handler: &service_kit::HandlerGuard,
    verified: tokens::jwt::Verified<proto_gazette::Claims>,
    first: Option<tonic::Result<proto::Request>>,
) -> anyhow::Result<Authorized> {
    let verify = crate::verify("Connector", "first Request", "client");
    let proto::Request { start, kind } = verify.not_eof(first)?;

    let (
        Some(proto::request::Start {
            log_level,
            sqlite_vfs_uri,
        }),
        Some(request),
    ) = (start, kind)
    else {
        return Err(crate::invalid_argument(
            "the first Connector request must set `start` and exactly one protocol request"
                .to_string(),
        ));
    };
    let log_level = ops::LogLevel::try_from(log_level).unwrap_or(ops::LogLevel::UndefinedLevel);

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
        log_level,
        sqlite_vfs_uri,
        task_name,
        request,
    })
}
