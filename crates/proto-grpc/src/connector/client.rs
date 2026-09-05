use proto_flow::{capture, connector, derive, materialize, ops};
use tokio::sync::mpsc;

/// Open `first` through `router`, sink logs until the connector reports
/// `Started`, and return the sender, remaining response receiver, and `Started`.
pub async fn start(
    router: &dyn super::Router,
    logger: &(dyn Fn(&ops::Log) + Sync),
    task_name: &str,
    first: connector::Request,
) -> anyhow::Result<(
    mpsc::Sender<connector::Request>,
    mpsc::Receiver<tonic::Result<connector::Response>>,
    connector::response::Started,
)> {
    if first.start.is_none() {
        return Err(
            tonic::Status::invalid_argument("first Connector request must set `start`").into(),
        );
    }
    let kind = first.kind.as_ref().ok_or_else(|| {
        tonic::Status::invalid_argument("first Connector request must set a protocol request")
    })?;
    let task_type = super::task_type(kind);
    let (request_tx, request_rx) = mpsc::channel(crate::CHANNEL_BUFFER);
    request_tx.try_send(first).expect("channel is empty");
    let mut response_rx = router.open(task_type, task_name, request_rx);
    let verify = crate::verify("Connector", "Started", "connector");

    let started = loop {
        match response_rx.recv().await {
            Some(Err(status)) => return Err(crate::status_to_anyhow(status)),
            item => match verify.not_eof(item)? {
                connector::Response {
                    kind: Some(connector::response::Kind::Started(started)),
                } => break started,
                connector::Response {
                    kind: Some(connector::response::Kind::Log(log)),
                } => logger(&log),
                response => return Err(verify.fail_msg(response)),
            },
        }
    };
    Ok((request_tx, response_rx, started))
}

/// Drive one Connector request through `router`, consuming `Started`, one
/// protocol response, interleaved logs, and the closing EOF. Timeout errors
/// carry a `tokio::time::error::Elapsed` source so callers can add context.
pub async fn unary(
    router: &dyn super::Router,
    logger: &(dyn Fn(&ops::Log) + Sync),
    request: connector::Request,
    start_timeout: std::time::Duration,
    completion_timeout: std::time::Duration,
) -> anyhow::Result<(connector::response::Started, connector::response::Kind)> {
    let kind = request.kind.as_ref().ok_or_else(|| {
        tonic::Status::invalid_argument("Connector request must set a protocol request")
    })?;
    let (task_type, task_name) = super::task_identity(kind)?;
    let task_name = task_name.to_string();

    // Both the Started Spec and the unary response must be of the request's
    // protocol. `next` enforces the latter and also rejects a second Started
    // or an empty response, as neither is of any protocol.
    let (spec_matches, unwrap): (
        fn(&connector::response::Started) -> bool,
        fn(connector::response::Kind) -> Option<connector::response::Kind>,
    ) = match kind {
        connector::request::Kind::Capture(_) => (
            |started| {
                matches!(
                    started.spec,
                    Some(connector::response::started::Spec::Capture(_))
                )
            },
            |kind| matches!(kind, connector::response::Kind::Capture(_)).then_some(kind),
        ),
        connector::request::Kind::Derive(_) => (
            |started| {
                matches!(
                    started.spec,
                    Some(connector::response::started::Spec::Derive(_))
                )
            },
            |kind| matches!(kind, connector::response::Kind::Derive(_)).then_some(kind),
        ),
        connector::request::Kind::Materialize(_) => (
            |started| {
                matches!(
                    started.spec,
                    Some(connector::response::started::Spec::Materialize(_))
                )
            },
            |kind| matches!(kind, connector::response::Kind::Materialize(_)).then_some(kind),
        ),
    };

    let (request_tx, mut response_rx, started) =
        tokio::time::timeout(start_timeout, start(router, logger, &task_name, request))
            .await
            .map_err(|elapsed| {
                anyhow::Error::new(elapsed).context("timed out waiting for connector Started")
            })??;

    std::mem::drop(request_tx); // Send EOF after unary request.

    let mut first_error = (!spec_matches(&started)).then(|| {
        anyhow::anyhow!(
            "connector Started has no Spec matching {}",
            task_type.as_str_name()
        )
    });
    let mut unary = None;

    // Drain through EOF so that a protocol error is preferred over a timeout.
    let completed = async {
        while let Some(item) = next(&mut response_rx, logger, unwrap).await {
            let err = match item {
                Err(status) => crate::status_to_anyhow(status),
                Ok(_) if unary.is_some() => {
                    anyhow::anyhow!("connector returned more than one unary response")
                }
                Ok(kind) => {
                    unary = Some(kind);
                    continue;
                }
            };
            first_error.get_or_insert(err);
        }
    };
    let completed = tokio::time::timeout(completion_timeout, completed).await;

    if let Some(err) = first_error {
        return Err(err);
    }
    if let Err(elapsed) = completed {
        return Err(
            anyhow::Error::new(elapsed).context("timed out completing unary connector request")
        );
    }
    let response =
        unary.ok_or_else(|| anyhow::anyhow!("connector closed without a unary response"))?;

    Ok((started, response))
}

/// Read the next protocol response while sinking interleaved logs. Returns
/// `None` at EOF and rejects a response of another protocol. Cancel-safe.
pub async fn next<R>(
    connector_rx: &mut mpsc::Receiver<tonic::Result<connector::Response>>,
    logger: &(dyn Fn(&ops::Log) + Sync),
    unwrap: fn(connector::response::Kind) -> Option<R>,
) -> Option<tonic::Result<R>> {
    loop {
        match connector_rx.recv().await {
            None => return None,
            Some(Err(status)) => return Some(Err(status)),
            Some(Ok(connector::Response {
                kind: Some(connector::response::Kind::Log(log)),
            })) => logger(&log),
            Some(Ok(response)) => {
                return Some(response.kind.and_then(unwrap).ok_or_else(|| {
                    crate::bounded_unknown_status(
                        "connector response is not of this stream's protocol".to_string(),
                    )
                }));
            }
        }
    }
}

pub fn wrap_capture(request: capture::Request) -> connector::Request {
    connector::Request {
        start: None,
        kind: Some(connector::request::Kind::Capture(request)),
    }
}
pub fn wrap_derive(request: derive::Request) -> connector::Request {
    connector::Request {
        start: None,
        kind: Some(connector::request::Kind::Derive(request)),
    }
}
pub fn wrap_materialize(request: materialize::Request) -> connector::Request {
    connector::Request {
        start: None,
        kind: Some(connector::request::Kind::Materialize(request)),
    }
}
pub fn unwrap_capture(response: connector::response::Kind) -> Option<capture::Response> {
    match response {
        connector::response::Kind::Capture(response) => Some(response),
        _ => None,
    }
}
pub fn unwrap_derive(response: connector::response::Kind) -> Option<derive::Response> {
    match response {
        connector::response::Kind::Derive(response) => Some(response),
        _ => None,
    }
}
pub fn unwrap_materialize(response: connector::response::Kind) -> Option<materialize::Response> {
    match response {
        connector::response::Kind::Materialize(response) => Some(response),
        _ => None,
    }
}

#[cfg(test)]
mod test {
    use proto_flow::{capture, connector, ops};
    use tokio::sync::mpsc;

    struct TestRouter {
        response_rx: std::sync::Mutex<Option<mpsc::Receiver<tonic::Result<connector::Response>>>>,
        _response_tx: Option<mpsc::Sender<tonic::Result<connector::Response>>>,
    }

    impl TestRouter {
        fn new(
            responses: impl IntoIterator<Item = tonic::Result<connector::Response>>,
            remain_open: bool,
        ) -> Self {
            let (response_tx, response_rx) = mpsc::channel(crate::CHANNEL_BUFFER);
            for response in responses {
                response_tx
                    .try_send(response)
                    .expect("test responses fit the channel");
            }
            Self {
                response_rx: std::sync::Mutex::new(Some(response_rx)),
                _response_tx: remain_open.then_some(response_tx),
            }
        }
    }

    impl super::super::Router for TestRouter {
        fn open(
            &self,
            _task_type: ops::TaskType,
            _task_name: &str,
            _request_rx: mpsc::Receiver<connector::Request>,
        ) -> mpsc::Receiver<tonic::Result<connector::Response>> {
            self.response_rx.lock().unwrap().take().unwrap()
        }
    }

    fn request() -> connector::Request {
        connector::Request {
            start: Some(Default::default()),
            kind: Some(connector::request::Kind::Capture(capture::Request {
                kind: Some(capture::request::Kind::Validate(Box::new(
                    capture::request::Validate {
                        name: "acmeCo/capture".to_string(),
                        ..Default::default()
                    },
                ))),
                ..Default::default()
            })),
        }
    }

    fn started() -> connector::Response {
        connector::Response {
            kind: Some(connector::response::Kind::Started(
                connector::response::Started {
                    spec: Some(connector::response::started::Spec::Capture(Box::new(
                        Default::default(),
                    ))),
                    ..Default::default()
                },
            )),
        }
    }

    fn validated() -> connector::Response {
        connector::Response {
            kind: Some(connector::response::Kind::Capture(capture::Response {
                kind: Some(capture::response::Kind::Validated(Default::default())),
                ..Default::default()
            })),
        }
    }

    #[tokio::test]
    async fn unary_sinks_logs_and_returns_started_and_response() {
        let router = TestRouter::new(
            [
                Ok(connector::Response {
                    kind: Some(connector::response::Kind::Log(Default::default())),
                }),
                Ok(started()),
                Ok(connector::Response {
                    kind: Some(connector::response::Kind::Log(Default::default())),
                }),
                Ok(validated()),
            ],
            false,
        );
        let logs = std::sync::atomic::AtomicUsize::new(0);
        let logger = |_log: &ops::Log| {
            _ = logs.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        };

        let (started, response) = super::unary(
            &router,
            &logger,
            request(),
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap();

        assert!(started.spec.is_some());
        assert!(matches!(response, connector::response::Kind::Capture(_)));
        assert_eq!(logs.load(std::sync::atomic::Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn unary_preserves_a_protocol_error_when_drain_times_out() {
        let router = TestRouter::new([Ok(started()), Ok(validated()), Ok(validated())], true);

        let err = super::unary(
            &router,
            &|_| {},
            request(),
            std::time::Duration::from_secs(1),
            std::time::Duration::from_millis(1),
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("more than one unary response"));
    }

    #[tokio::test]
    async fn unary_timeouts_expose_elapsed_for_caller_context() {
        let router = TestRouter::new([], true);

        let err = super::unary(
            &router,
            &|_| {},
            request(),
            std::time::Duration::from_millis(1),
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("timed out waiting for connector Started")
        );
        assert!(
            err.downcast_ref::<tokio::time::error::Elapsed>().is_some(),
            "callers add data-plane context only to timeout errors"
        );
    }
}
