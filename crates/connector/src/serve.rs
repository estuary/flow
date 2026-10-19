//! Per-stream handler of a `connector.Connector` RPC: authorize the first
//! request, start the connector it names, and pump both directions until the
//! connector is done and its logs are read through.

use crate::proto;
use futures::{Stream, StreamExt};
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
    R: Stream<Item = tonic::Result<proto::Request>> + Send + Unpin + 'static,
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
    R: Stream<Item = tonic::Result<proto::Request>> + Send + Unpin + 'static,
{
    handler.set_phase("authorizing");

    let first = request_rx.next().await;
    let Authorized {
        execution,
        log_level,
        sqlite_vfs_uri,
        task_name,
        request,
    } = authorize(&handler, transport, verified, first)?;

    handler.set_phase("starting");

    let (log_sink, read_through_rx) = crate::LogSink::response(response_tx.clone());

    let result = start_and_pump(
        &service,
        log_sink,
        execution,
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
    execution: proto_flow::flow::ConnectorExecution,
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
    let sqlite_vfs_uri = (!sqlite_vfs_uri.is_empty()).then_some(sqlite_vfs_uri);

    match request {
        proto::request::Kind::Capture(initial) => {
            run::<crate::capture::Capture, R>(
                service,
                log_sink,
                execution,
                log_level,
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
                execution,
                log_level,
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
                execution,
                log_level,
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
    execution: proto_flow::flow::ConnectorExecution,
    log_level: ops::LogLevel,
    sqlite_vfs_uri: Option<String>,
    task_name: &str,
    initial: P::Request,
    request_rx: R,
    response_tx: &mpsc::Sender<tonic::Result<proto::Response>>,
    handler: &mut service_kit::HandlerGuard,
) -> anyhow::Result<()>
where
    P: crate::protocol::Protocol,
    R: Stream<Item = tonic::Result<proto::Request>> + Send + Unpin + 'static,
{
    let ctx = crate::protocol::StartContext {
        container_network: service.container_network.clone(),
        execution,
        log_level,
        log_sink,
        plane: service.plane,
        process: service.process.clone(),
        secret_resolver: service.secret_resolver.clone(),
        task_name: task_name.to_string(),
        task_update: service.task_update.clone(),
        vmm: service.vmm.clone(),
    };
    let started = tokio::select! {
        () = response_tx.closed() => return Err(client_dropped()),
        started = crate::protocol::start::<P>(ctx, sqlite_vfs_uri, initial) => started?,
    };

    _ = response_tx.send(Ok(started.started.clone())).await;
    handler.set_phase("running");
    pump::<P, R>(request_rx, started, response_tx).await
}

/// The client is gone, and holding its connector open would serve no one.
fn client_dropped() -> anyhow::Error {
    anyhow::anyhow!("Connector client dropped its response stream")
}

/// Forward requests and responses in independent bursts. A full request
/// channel parks only its forwarding future, leaving responses free to drain.
pub(crate) async fn pump<P, R>(
    mut request_rx: R,
    started: crate::Started<P>,
    response_tx: &mpsc::Sender<tonic::Result<proto::Response>>,
) -> anyhow::Result<()>
where
    P: crate::protocol::Protocol,
    R: Stream<Item = tonic::Result<proto::Request>> + Send + Unpin + 'static,
{
    let crate::Started {
        connector_tx,
        mut connector_rx,
        guard,
        execution,
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
            let Some(request) = kind.and_then(P::unwrap_request) else {
                return Err(crate::invalid_argument(
                    "every Connector request must set exactly one protocol request, of the type \
                     established by the first request"
                        .to_string(),
                ));
            };
            crate::vmm::check_spec_execution(&execution, P::spec_execution(&request).as_ref())?;

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
                    _ = response_tx.send(Ok(P::wrap_response(response))).await;
                }
                Some(Err(status)) => break Err(crate::status_to_anyhow(status)),
                None => break Ok(()),
            },

            () = response_tx.closed() => break Err(client_dropped()),
        }
    };

    // Releases the run's host resources: killing an image connector closes
    // stderr, allowing log draining to finish.
    std::mem::drop(guard);
    result
}

struct Authorized {
    execution: proto_flow::flow::ConnectorExecution,
    log_level: ops::LogLevel,
    sqlite_vfs_uri: String,
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

    let (
        Some(proto::request::Start {
            execution,
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

    // `sqlite_vfs_uri` is an unvalidated path which the connector opens (and
    // creates) as the reactor. It's meaningful only to the shard which recorded
    // the recovery log it names, so a remote caller may never supply one.
    crate::policy::check_remote_sqlite_vfs(
        matches!(transport, crate::service::Transport::Wire),
        !sqlite_vfs_uri.is_empty(),
    )
    .map_err(|err| crate::invalid_argument(err.to_string()))?;

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
        execution: execution.unwrap_or_default(),
        log_level,
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

        let started = crate::Started::<crate::derive::Derive> {
            started: proto::Response::default(),
            connector_tx,
            connector_rx: futures::stream::pending().boxed(),
            execution: Default::default(),
            guard: crate::Guard {
                _process: None,
                _vmm: None,
                _refresh: None,
                _mount: Some(tempfile::tempdir().unwrap()),
            },
        };
        std::mem::drop(response_rx);

        let pump =
            pump::<crate::derive::Derive, _>(futures::stream::pending(), started, &response_tx);
        let err = tokio::time::timeout(std::time::Duration::from_secs(5), pump)
            .await
            .expect("pump must not park on the wedged connector")
            .unwrap_err();

        assert!(
            format!("{err:#}").contains("dropped its response stream"),
            "{err:#}",
        );
    }
    use crate::execution_fixture::*;
    use proto_flow::{capture, derive, flow, materialize};
    /// Immediate connector EOF must not hide a ready request's execution mismatch.
    async fn pump_later<P: crate::protocol::Protocol>(
        execution: flow::ConnectorExecution,
        request: proto::Request,
    ) -> (usize, anyhow::Result<()>) {
        let (connector_tx, mut connector_rx) = mpsc::channel(1);
        let (response_tx, _response_rx) = mpsc::channel(1);
        let started = crate::Started::<P> {
            started: proto::Response::default(),
            connector_tx,
            connector_rx: futures::stream::empty().boxed(),
            guard: crate::Guard {
                _process: None,
                _vmm: None,
                _refresh: None,
                _mount: None,
            },
            execution,
        };

        let requests = futures::stream::iter([Ok::<_, tonic::Status>(request)]);
        let result = crate::serve::pump(requests, started, &response_tx).await;

        let mut forwarded = 0;
        while connector_rx.try_recv().is_ok() {
            forwarded += 1;
        }
        (forwarded, result)
    }

    /// Drive `pump` directly to cover VMM sessions without launching a VMM.
    #[tokio::test]
    async fn later_requests_must_match_the_session_execution() {
        const EMBEDS_SPEC: &[&str] = &[
            "capture Apply",
            "capture Open",
            "derive Open",
            "materialize Apply",
            "materialize Open",
        ];
        let config = serde_json::json!({"image": PYTHON_IMAGE, "config": {}});
        let config: bytes::Bytes = config.to_string().into();
        let hosts = &["api.acmeco.example", "*.svc.acmeco.example"];
        let previous = egress_execution(true, &["previous.acmeco.example"]);

        let sessions = [
            ("ordinary", None),
            ("vmm", vmm_execution()),
            ("vmm egress [api, *.svc]", egress_execution(true, hosts)),
        ];
        let laters = [
            ("unset", None),
            ("ordinary", Some(flow::ConnectorExecution::default())),
            ("vmm", vmm_execution()),
            ("egress []", egress_execution(false, &[])),
            ("vmm egress []", egress_execution(true, &[])),
            ("vmm egress [api, *.svc]", egress_execution(true, hosts)),
            ("vmm egress [api]", egress_execution(true, &hosts[..1])),
        ];

        let mut rows = Vec::new();
        for (session_name, session) in &sessions {
            for (later_name, later) in &laters {
                let mut refused = Vec::new();

                for (label, task_type, _task_name, kind) in
                    every_first_request(image_connector_type, &config, later.clone())
                {
                    let request = proto::Request {
                        start: None,
                        kind: Some(with_previous_specs(kind, &previous)),
                    };
                    let session = session.clone().unwrap_or_default();

                    let (forwarded, result) = match task_type {
                        ops::TaskType::Capture => {
                            pump_later::<crate::capture::Capture>(session, request).await
                        }
                        ops::TaskType::Derivation => {
                            pump_later::<crate::derive::Derive>(session, request).await
                        }
                        ops::TaskType::Materialization => {
                            pump_later::<crate::materialize::Materialize>(session, request).await
                        }
                        _ => unreachable!(),
                    };
                    match result {
                        Ok(()) => assert_eq!(forwarded, 1, "{label}"),
                        Err(err) => {
                            assert_eq!(forwarded, 0, "{label}");
                            let status = err.downcast_ref::<proto_grpc::StatusError>().unwrap();
                            assert_eq!(status.code(), tonic::Code::InvalidArgument, "{label}");
                            refused.push(label);
                        }
                    }
                }

                assert!(
                    refused.is_empty() || refused == EMBEDS_SPEC,
                    "session {session_name} later {later_name}: {refused:?}"
                );
                rows.push(format!(
                    "session {session_name:<23} later {later_name:<23} => {}",
                    if refused.is_empty() {
                        "forwarded"
                    } else {
                        "refused"
                    },
                ));
            }
        }

        insta::assert_snapshot!(rows.join("\n"), @r"
    session ordinary                later unset                   => forwarded
    session ordinary                later ordinary                => forwarded
    session ordinary                later vmm                     => refused
    session ordinary                later egress []               => refused
    session ordinary                later vmm egress []           => refused
    session ordinary                later vmm egress [api, *.svc] => refused
    session ordinary                later vmm egress [api]        => refused
    session vmm                     later unset                   => refused
    session vmm                     later ordinary                => refused
    session vmm                     later vmm                     => forwarded
    session vmm                     later egress []               => refused
    session vmm                     later vmm egress []           => refused
    session vmm                     later vmm egress [api, *.svc] => refused
    session vmm                     later vmm egress [api]        => refused
    session vmm egress [api, *.svc] later unset                   => refused
    session vmm egress [api, *.svc] later ordinary                => refused
    session vmm egress [api, *.svc] later vmm                     => refused
    session vmm egress [api, *.svc] later egress []               => refused
    session vmm egress [api, *.svc] later vmm egress []           => refused
    session vmm egress [api, *.svc] later vmm egress [api, *.svc] => forwarded
    session vmm egress [api, *.svc] later vmm egress [api]        => refused
    ");
    }
    fn with_previous_specs(
        mut kind: proto::request::Kind,
        execution: &Option<flow::ConnectorExecution>,
    ) -> proto::request::Kind {
        use proto::request::Kind;

        let capture = || flow::CaptureSpec {
            execution: execution.clone(),
            ..Default::default()
        };
        let materialization = || flow::MaterializationSpec {
            execution: execution.clone(),
            ..Default::default()
        };

        match &mut kind {
            Kind::Capture(capture::Request {
                kind: Some(capture::request::Kind::Validate(validate)),
                ..
            }) => validate.last_capture = Some(capture()),
            Kind::Capture(capture::Request {
                kind: Some(capture::request::Kind::Apply(apply)),
                ..
            }) => apply.last_capture = Some(capture()),
            Kind::Derive(derive::Request {
                kind: Some(derive::request::Kind::Validate(validate)),
                ..
            }) => {
                validate.last_collection = Some(flow::CollectionSpec {
                    derivation: Some(Box::new(flow::collection_spec::Derivation {
                        execution: execution.clone(),
                        ..Default::default()
                    })),
                    ..Default::default()
                })
            }
            Kind::Materialize(materialize::Request {
                kind: Some(materialize::request::Kind::Validate(validate)),
                ..
            }) => validate.last_materialization = Some(materialization()),
            Kind::Materialize(materialize::Request {
                kind: Some(materialize::request::Kind::Apply(apply)),
                ..
            }) => apply.last_materialization = Some(materialization()),
            _ => {}
        }
        kind
    }
}
