use futures::{Stream, StreamExt, stream::BoxStream};
use proto_flow::{capture, connector, derive, materialize, ops};

/// Open `first` through `router`, sink logs until the connector reports
/// `Started`, and return the sender, remaining response stream, and `Started`.
pub async fn start(
    router: &dyn super::Router,
    logger: &(dyn Fn(&ops::Log) + Sync),
    task_name: &str,
    first: connector::Request,
) -> anyhow::Result<(
    tokio::sync::mpsc::Sender<connector::Request>,
    BoxStream<'static, tonic::Result<connector::Response>>,
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
    let (request_tx, request_rx) = tokio::sync::mpsc::channel(crate::CHANNEL_BUFFER);
    request_tx.try_send(first).expect("channel is empty");
    let mut response_rx = router.open(task_type, task_name, request_rx);
    let verify = crate::verify("Connector", "Started", "connector");

    let started = loop {
        match response_rx.next().await {
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

/// Read the next protocol response while sinking interleaved logs. Returns
/// `None` at EOF and rejects a response of another protocol. Cancel-safe.
pub async fn next<S, R>(
    connector_rx: &mut S,
    logger: &(dyn Fn(&ops::Log) + Sync),
    unwrap: fn(connector::response::Kind) -> Option<R>,
) -> Option<tonic::Result<R>>
where
    S: Stream<Item = tonic::Result<connector::Response>> + Unpin,
{
    loop {
        match connector_rx.next().await {
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
