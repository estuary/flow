//! Per-stream handler of a `connector.Connector` RPC: authorize the first
//! request, start the connector it names, and pump both directions until the
//! connector is done and its logs are read through.

use crate::proto;
use futures::{Stream, StreamExt};
use tokio::sync::mpsc;
use tracing::Instrument;

/// Bound on how long teardown waits for the connector's log pumps to finish
/// after its container has been killed. Every pump this crate starts ends when
/// the process it reads from does, so exceeding this means either a stuck pump
/// or a client which stopped reading — neither of which may strand the RPC.
const LOG_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

pub(crate) async fn serve<R>(
    service: crate::Service,
    verified: tokens::jwt::Verified<proto_gazette::Claims>,
    request_rx: R,
    response_tx: mpsc::Sender<tonic::Result<proto::Response>>,
) -> anyhow::Result<()>
where
    R: Stream<Item = tonic::Result<proto::Request>> + Send + Unpin + 'static,
{
    // Run the whole handler inside its span so operator trace overrides (see
    // `service_kit::trace`) reach every line the connector's start emits.
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

    let verify = crate::verify("Connector", "first Request", "client");
    let proto::Request { start, kind } = verify.not_eof(request_rx.next().await)?;

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
        .authorize(proto_grpc::connector::task_label_set(task_type, &task_name))?;

    handler.set_label(&task_name);
    handler.set_field("task_type", task_type.as_str_name());
    handler.set_field(
        "token",
        serde_json::to_string(&authorized.claims()).unwrap(),
    );
    handler.set_phase("starting");

    // Sink of connector logs and container lifecycle records, which sends
    // straight onto the response channel: logs share its ordering and its
    // back-pressure with protocol responses, and are never buffered elsewhere.
    let (read_through_tx, read_through_rx) = tokio::sync::oneshot::channel::<()>();
    let log_sink = crate::LogSink::response(response_tx.clone(), read_through_tx);

    // Race the connector against the client leaving. An in-process client
    // signals hang-up only by dropping its receiver — there's no request-stream
    // error as over gRPC — and an idle connector would otherwise linger,
    // container and all, until its next response failed to send. Cancelling
    // here drops the in-flight start or the `Started` (and so its container
    // Guard), which SIGKILLs the container at once.
    let result = tokio::select! {
        result = start_and_pump(
            &service,
            log_sink,
            log_level,
            sqlite_vfs_uri,
            &task_name,
            request,
            request_rx,
            &response_tx,
            &mut handler,
        ) => result,
        _ = response_tx.closed() => Ok(()),
    };

    // Whatever happened — including a failure to start at all — the connector's
    // logs are the user-facing explanation, so they precede our last word.
    // Every clone of the sink lives inside a log pump, so awaiting its last
    // drop is awaiting the connector's stderr to have been read through.
    handler.set_phase("draining");
    if tokio::time::timeout(LOG_DRAIN_TIMEOUT, read_through_rx)
        .await
        .is_err()
    {
        tracing::warn!("timed out awaiting the connector's logs at teardown");
    }

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

/// Start the connector named by `request`, report `Started`, and pump the stream.
/// Note we may report logs before `Started`.
///
/// Sends onto `response_tx` are best-effort: a client which has gone away is
/// observed through its request stream and `serve_inner`'s hang-up arm, not
/// through a failed send.
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

    // Each arm starts its protocol's connector, sends `Started`, and then runs
    // the same pump over that protocol's request / response types.
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

/// Forward the client's later requests into the connector, and the connector's
/// responses back, until the connector's stream ends. Its logs travel
/// independently, through the sink `serve_inner` installed.
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

    // Forwarding is a concurrent future rather than a `select!` arm so that a
    // full connector channel parks *it* only, leaving the response and log arms
    // free to drain — which is what makes room in that channel.
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
                break; // Connector is gone; its response stream reports why.
            }
        }
        Ok(()) // Client EOF drops `connector_tx`.
    });

    let mut forwarding = true;

    let result = loop {
        tokio::select! {
            response = connector_rx.next() => match response {
                Some(Ok(response)) => {
                    let response = proto::Response { kind: Some(wrap(response)) };
                    if response_tx.send(Ok(response)).await.is_err() {
                        break Ok(()); // Client hung up.
                    }
                }
                Some(Err(status)) => break Err(crate::status_to_anyhow(status)),
                None => break Ok(()),
            },

            result = &mut forward, if forwarding => match result {
                Ok(()) => forwarding = false,
                Err(err) => break Err(err),
            },
        }
    };

    // Drop the container Guard before returning: it SIGKILLs the `docker run`
    // client, closing the container's stderr so its log pump finishes and
    // releases the last `LogSink`. `serve_inner` awaits that release ahead of
    // this stream's last word.
    std::mem::drop(guard);

    result
}
