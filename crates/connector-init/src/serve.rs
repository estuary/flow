//! Serve one `connector.Connector` stream over a connector child process.
//!
//! The first request sets `start` and a Spec request. The connector's Spec
//! response is returned as `Started`. Every later request is a protocol
//! request of the first request's protocol, which passes to the connector.
//! Connector stdout is returned as protocol responses, and stderr as `Log`s.
//!
//! [`serve`] touches no process-global state, as it also runs in-process
//! within the reactor and `flowctl`. Its own task-relevant events are `Log`
//! responses, filtered by `start.log_level`. `tracing` is for application
//! logging only.

use crate::{Codec, rpc};
use futures::StreamExt;
use proto_flow::connector::{self, Capture, Derive, Materialize, Protocol};
use tokio::sync::mpsc;

/// Serve one `connector.Connector` stream of `requests` into `response_tx`,
/// over connector processes which `command` builds. It returns once the
/// stream is done and its connector has exited or is killed. A failure is the
/// stream's last item.
///
/// When `spec_alone`, the connector serves one request per process. Its Spec
/// then runs in a process of its own, and a second process serves the stream.
///
/// Each of a VMM's `refusals` is written as a `Log` until the connector is done.
pub async fn serve<C, R>(
    command: C,
    codec: Codec,
    spec_alone: bool,
    refusals: Option<crate::refusals::Reader>,
    requests: R,
    response_tx: &mpsc::Sender<tonic::Result<connector::Response>>,
) where
    C: Fn() -> async_process::Command,
    R: futures::Stream<Item = tonic::Result<connector::Request>> + Send + Unpin + 'static,
{
    if let Err(status) =
        serve_inner(command, codec, spec_alone, refusals, requests, response_tx).await
    {
        _ = response_tx.send(Err(status)).await;
    }
}

async fn serve_inner<C, R>(
    command: C,
    codec: Codec,
    spec_alone: bool,
    refusals: Option<crate::refusals::Reader>,
    mut requests: R,
    response_tx: &mpsc::Sender<tonic::Result<connector::Response>>,
) -> tonic::Result<()>
where
    C: Fn() -> async_process::Command,
    R: futures::Stream<Item = tonic::Result<connector::Request>> + Send + Unpin + 'static,
{
    let (start, kind) = match requests.next().await {
        Some(Ok(connector::Request {
            start: Some(start),
            kind: Some(kind),
        })) => (start, kind),
        Some(Err(status)) => return Err(status),
        _ => {
            return Err(tonic::Status::invalid_argument(
                "the first Connector request must set `start` and exactly one protocol request",
            ));
        }
    };
    let log_level = ops::LogLevel::try_from(start.log_level)
        .unwrap_or_default()
        .or(ops::LogLevel::Info);

    let command = || {
        let mut command = command();
        command.env("LOG_LEVEL", log_level.as_str_name());
        command.env("LOG_FORMAT", "json");
        command
    };
    let logs = Logs {
        log_level,
        response_tx: response_tx.clone(),
    };

    // Refusals are read only while the connector runs, and so precede the
    // stream's EOF or terminal status.
    let refusals = {
        let logs = logs.clone();
        async move {
            if let Some(reader) = refusals {
                pump_refusals(reader, logs).await;
            }
            std::future::pending::<()>().await
        }
    };
    let run = async move {
        match kind {
            connector::request::Kind::Capture(first) => {
                run_processes::<Capture, _, _>(command, codec, spec_alone, first, requests, logs)
                    .await
            }
            connector::request::Kind::Derive(first) => {
                run_processes::<Derive, _, _>(command, codec, spec_alone, first, requests, logs)
                    .await
            }
            connector::request::Kind::Materialize(first) => {
                run_processes::<Materialize, _, _>(
                    command, codec, spec_alone, first, requests, logs,
                )
                .await
            }
        }
    };
    tokio::select! {
        result = run => result,
        () = refusals => unreachable!("the refusals pump never completes"),
    }
}

/// Write each of a VMM's `refusals` to `logs`, until its reader ends.
async fn pump_refusals(mut refusals: crate::refusals::Reader, logs: Logs) {
    use tokio::io::AsyncBufReadExt;

    let mut line = String::new();
    loop {
        line.clear();
        match refusals.read_line(&mut line).await {
            Ok(0) => return,
            Ok(_) => (),
            Err(error) => {
                tracing::warn!(%error, "failed to read the resolver's refusals");
                return;
            }
        }
        match crate::refusals::render(&line) {
            Ok((message, field)) => {
                logs.write_fields(ops::LogLevel::Warn, &message, [field])
                    .await
            }
            Err(error) => tracing::warn!(%error, line, "invalid refusal from the resolver"),
        }
    }
}

/// [`run`] over connector processes which `command` builds.
async fn run_processes<P, C, R>(
    command: C,
    codec: Codec,
    spec_alone: bool,
    first: P::Request,
    requests: R,
    logs: Logs,
) -> tonic::Result<()>
where
    P: Protocol,
    C: Fn() -> async_process::Command,
    R: futures::Stream<Item = tonic::Result<connector::Request>> + Send + Unpin + 'static,
{
    if !P::is_spec(&first) {
        return Err(tonic::Status::invalid_argument(format!(
            "the first {} request must be a Spec",
            P::NAME
        )));
    }
    let response_tx = logs.response_tx.clone();
    let (connector_tx, connector_rx) = mpsc::channel(proto_grpc::CHANNEL_BUFFER);

    let spec = if spec_alone {
        logs.write(
            ops::LogLevel::Debug,
            "running Spec in a process of its own, as this image serves one request per process",
        )
        .await;
        let spec = rpc::spawn_unary::<P::Request, P::Response, _, _, _, _>(
            command(),
            codec,
            first,
            log_handler(&response_tx),
            exit_handler(logs.clone()),
        );
        Some(tokio::select! {
            () = response_tx.closed() => return Err(client_dropped()),
            spec = spec => spec?,
        })
    } else {
        connector_tx
            .try_send(first)
            .expect("new connector channel is empty");
        None
    };

    let connector_rx = rpc::spawn::<P::Request, P::Response, _, _, _, _, _>(
        command(),
        codec,
        tokio_stream::wrappers::ReceiverStream::new(connector_rx).map(Ok),
        log_handler(&response_tx),
        exit_handler(logs),
    )?
    .boxed();

    run::<P, _, _>(
        codec,
        spec,
        requests,
        connector_tx,
        connector_rx,
        &response_tx,
    )
    .await
}

/// Serve a connector which `connector_tx` and `connector_rx` reach. Its
/// response to the Spec is `spec`, or else its first response. That response
/// becomes `Started`, and then requests and responses are pumped until the
/// connector is done.
pub async fn run<P, R, S>(
    codec: Codec,
    spec: Option<P::Response>,
    requests: R,
    connector_tx: mpsc::Sender<P::Request>,
    mut connector_rx: S,
    response_tx: &mpsc::Sender<tonic::Result<connector::Response>>,
) -> tonic::Result<()>
where
    P: Protocol,
    R: futures::Stream<Item = tonic::Result<connector::Request>> + Unpin,
    S: futures::Stream<Item = tonic::Result<P::Response>> + Unpin,
{
    let spec = match spec {
        Some(spec) => spec,
        None => tokio::select! {
            () = response_tx.closed() => return Err(client_dropped()),
            response = connector_rx.next() => match response {
                Some(response) => response?,
                None => {
                    return Err(tonic::Status::internal(
                        "connector closed its output without responding to Spec",
                    ));
                }
            },
        },
    };
    _ = response_tx.send(Ok(started::<P>(codec, spec)?)).await;

    pump::<P, _, _>(requests, connector_tx, connector_rx, response_tx).await
}

/// Build `Started` from the connector's response to the Spec, after checking
/// its protocol version.
fn started<P: Protocol>(codec: Codec, response: P::Response) -> tonic::Result<connector::Response> {
    let spec = P::unwrap_spec(response).map_err(|response| {
        tonic::Status::internal(format!(
            "{} connector responded to Spec with {}",
            P::NAME,
            serde_json::to_string(&response).unwrap_or_default(),
        ))
    })?;

    let protocol = spec.protocol();
    if protocol != crate::EXPECT_PROTOCOL {
        return Err(tonic::Status::internal(format!(
            "connector returned an unexpected protocol version {protocol} (expected {})",
            crate::EXPECT_PROTOCOL,
        )));
    }

    Ok(connector::Response {
        kind: Some(connector::response::Kind::Started(
            connector::response::Started {
                codec: connector::response::started::Codec::from(codec) as i32,
                spec: Some(spec),
                ..Default::default()
            },
        )),
    })
}

/// Forward requests and responses in independent bursts, until the connector
/// is done. A full connector channel parks only the forwarding of requests,
/// leaving responses free to drain.
async fn pump<P, R, S>(
    mut requests: R,
    connector_tx: mpsc::Sender<P::Request>,
    mut connector_rx: S,
    response_tx: &mpsc::Sender<tonic::Result<connector::Response>>,
) -> tonic::Result<()>
where
    P: Protocol,
    R: futures::Stream<Item = tonic::Result<connector::Request>> + Unpin,
    S: futures::Stream<Item = tonic::Result<P::Response>> + Unpin,
{
    let mut forward = std::pin::pin!(async move {
        while let Some(request) = requests.next().await {
            let connector::Request { start, kind } = request?;

            if start.is_some() {
                return Err(tonic::Status::invalid_argument(
                    "only the first Connector request may set `start`",
                ));
            }
            let Some(request) = kind.and_then(P::unwrap_request) else {
                return Err(tonic::Status::invalid_argument(
                    "every Connector request must set exactly one protocol request, of the type \
                     established by the first request",
                ));
            };
            if connector_tx.send(request).await.is_err() {
                break;
            }
        }
        Ok(())
    });
    let mut forwarding = true;

    loop {
        tokio::select! {
            biased;

            // Prefer a ready request error over connector EOF, so that
            // malformed client input can't be reported as a clean completion.
            result = &mut forward, if forwarding => match result {
                Ok(()) => forwarding = false,
                Err(status) => return Err(status),
            },

            response = connector_rx.next() => match response {
                Some(Ok(response)) => {
                    let response = connector::Response {
                        kind: Some(P::wrap_response(response)),
                    };
                    _ = response_tx.send(Ok(response)).await;
                }
                Some(Err(status)) => return Err(status),
                None => return Ok(()),
            },

            () = response_tx.closed() => return Err(client_dropped()),
        }
    }
}

fn client_dropped() -> tonic::Status {
    tonic::Status::cancelled("Connector client dropped its response stream")
}

/// Send connector stderr logs as `Log` responses. Sends are awaited, so a
/// client which doesn't read back-pressures the connector on its stderr.
fn log_handler(
    response_tx: &mpsc::Sender<tonic::Result<connector::Response>>,
) -> impl Fn(ops::Log) -> futures::future::BoxFuture<'static, ()> + Send + Sync + 'static {
    let response_tx = response_tx.clone();

    move |log| {
        let response_tx = response_tx.clone();
        Box::pin(async move {
            let response = connector::Response {
                kind: Some(connector::response::Kind::Log(log)),
            };
            _ = response_tx.send(Ok(response)).await;
        })
    }
}

/// Write the connector's exit status as a `Log` response.
fn exit_handler(
    logs: Logs,
) -> impl FnOnce(std::process::ExitStatus) -> futures::future::BoxFuture<'static, ()> + Send + 'static
{
    move |status| {
        Box::pin(async move {
            let (level, message) = if status.success() {
                (ops::LogLevel::Debug, "connector exited")
            } else {
                (ops::LogLevel::Error, "connector failed")
            };
            logs.write_fields(level, message, [("status", status.to_string())])
                .await;
        })
    }
}

/// Writer of this module's own task-relevant `Log` responses.
#[derive(Clone)]
struct Logs {
    log_level: ops::LogLevel,
    response_tx: mpsc::Sender<tonic::Result<connector::Response>>,
}

impl Logs {
    async fn write(&self, level: ops::LogLevel, message: &str) {
        self.write_fields(level, message, []).await
    }

    async fn write_fields<const N: usize>(
        &self,
        level: ops::LogLevel,
        message: &str,
        fields: [(&str, String); N],
    ) {
        // Lesser levels are more severe.
        if level as i32 > self.log_level as i32 {
            return;
        }
        let log = ops::Log {
            meta: None,
            shard: None,
            timestamp: Some(proto_flow::as_timestamp(std::time::SystemTime::now())),
            level: level as i32,
            message: message.to_string(),
            fields_json_map: fields
                .into_iter()
                .map(|(key, value)| {
                    let value = serde_json::to_vec(&value).expect("a string serializes");
                    (key.to_string(), value.into())
                })
                .collect(),
            spans: Vec::new(),
        };
        let response = connector::Response {
            kind: Some(connector::response::Kind::Log(log)),
        };
        _ = self.response_tx.send(Ok(response)).await;
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::{Codec, serve};
    use proto_flow::connector;
    use serde_json::json;

    pub(crate) const SPEC: &str =
        r#"{"spec":{"protocol":3032023,"configSchema":{},"resourceConfigSchema":{}}}"#;

    /// Serve `requests` over a `/bin/sh` connector running `script` with the
    /// JSON codec, and render its responses one per line.
    async fn drive(script: &str, spec_alone: bool, requests: Vec<connector::Request>) -> String {
        drive_with(script, spec_alone, None, requests).await
    }

    /// [`drive`], with a VMM's `refusals`.
    async fn drive_with(
        script: &str,
        spec_alone: bool,
        refusals: Option<crate::refusals::Reader>,
        requests: Vec<connector::Request>,
    ) -> String {
        let requests: Vec<tonic::Result<connector::Request>> =
            requests.into_iter().map(Ok).collect();
        let (response_tx, mut response_rx) = tokio::sync::mpsc::channel(64);

        serve(
            || crate::rpc::new_command(&["/bin/sh", "-c", script]),
            Codec::Json,
            spec_alone,
            refusals,
            futures::stream::iter(requests),
            &response_tx,
        )
        .await;
        std::mem::drop(response_tx);

        let mut lines = Vec::new();
        while let Some(response) = response_rx.recv().await {
            lines.push(match response {
                Err(status) => format!("Status({:?}): {}", status.code(), status.message()),
                Ok(connector::Response {
                    kind: Some(connector::response::Kind::Log(log)),
                }) => {
                    let fields: std::collections::BTreeMap<_, serde_json::Value> = log
                        .fields_json_map
                        .iter()
                        .map(|(key, value)| (key, serde_json::from_slice(value).unwrap()))
                        .collect();
                    format!(
                        "Log({}): {} {fields:?}",
                        log.level().as_str_name(),
                        log.message
                    )
                }
                Ok(connector::Response {
                    kind: Some(connector::response::Kind::Started(started)),
                }) => {
                    let spec = connector::response::Started {
                        spec: started.spec.clone(),
                        ..Default::default()
                    };
                    format!(
                        "Started({:?}): {}",
                        started.codec(),
                        serde_json::to_string(&spec).unwrap()
                    )
                }
                Ok(response @ connector::Response { kind: Some(_) }) => {
                    serde_json::to_string(&response).unwrap()
                }
                Ok(connector::Response { kind: None }) => "empty".to_string(),
            });
        }
        lines.join("\n")
    }

    /// Build a request of `start` and a JSON protocol request.
    fn request(
        start: Option<serde_json::Value>,
        mut request: serde_json::Value,
    ) -> connector::Request {
        if let Some(start) = start {
            request["start"] = start;
        }
        serde_json::from_value(request).unwrap()
    }

    pub(crate) fn first(log_level: &str) -> connector::Request {
        request(
            Some(json!({"logLevel": log_level})),
            json!({"derive": {"spec": {"connectorType": "IMAGE"}}}),
        )
    }

    pub(crate) fn validate() -> connector::Request {
        request(
            None,
            json!({"derive": {"validate": {"connectorType": "IMAGE"}}}),
        )
    }

    #[tokio::test]
    async fn serves_started_responses_and_its_own_logs() {
        // The Spec reports the connector's environment.
        let script = r#"
            read spec
            echo "{\"spec\":{\"protocol\":3032023,\"documentationUrl\":\"$LOG_LEVEL $LOG_FORMAT\"}}"
            read validate
            echo '{"validated":{}}'
            "#;

        insta::assert_snapshot!(drive(script, false, vec![first("debug"), validate()]).await, @r#"
        Started(Json): {"derive":{"protocol":3032023,"documentationUrl":"debug json"}}
        {"derive":{"validated":{}}}
        Log(debug): connector exited {"status": String("exit status: 0")}
        "#);

        // At `info`, the exit is filtered out.
        insta::assert_snapshot!(drive(script, false, vec![first("info"), validate()]).await, @r#"
        Started(Json): {"derive":{"protocol":3032023,"documentationUrl":"info json"}}
        {"derive":{"validated":{}}}
        "#);
    }

    #[tokio::test]
    async fn runs_spec_alone_for_one_request_per_process() {
        // Answers exactly one request, as a frozen `:dev` image does.
        let script = format!(
            r#"
            read request
            case "$request" in
              *'"spec"'*) echo '{SPEC}' ;;
              *) echo '{{"validated":{{}}}}' ;;
            esac
            "#
        );

        insta::assert_snapshot!(drive(&script, true, vec![first("debug"), validate()]).await, @r#"
        Log(debug): running Spec in a process of its own, as this image serves one request per process {}
        Log(debug): connector exited {"status": String("exit status: 0")}
        Started(Json): {"derive":{"protocol":3032023,"configSchema":{},"resourceConfigSchema":{}}}
        {"derive":{"validated":{}}}
        Log(debug): connector exited {"status": String("exit status: 0")}
        "#);

        // Without it, the Validate goes unanswered.
        insta::assert_snapshot!(drive(&script, false, vec![first("info"), validate()]).await, @r#"Started(Json): {"derive":{"protocol":3032023,"configSchema":{},"resourceConfigSchema":{}}}"#);
    }

    #[tokio::test]
    async fn rejects_invalid_requests() {
        let script = format!("read spec; echo '{SPEC}'; cat >/dev/null");

        let outcomes = [
            vec![validate()],
            vec![request(
                Some(json!({})),
                json!({"derive": {"validate": {}}}),
            )],
            vec![first("info"), first("info")],
            vec![
                first("info"),
                request(None, json!({"capture": {"spec": {}}})),
            ],
        ];
        let mut rendered = Vec::new();
        for requests in outcomes {
            rendered.push(drive(&script, false, requests).await);
        }

        insta::assert_snapshot!(rendered.join("\n"), @r#"
        Status(InvalidArgument): the first Connector request must set `start` and exactly one protocol request
        Status(InvalidArgument): the first Derive request must be a Spec
        Started(Json): {"derive":{"protocol":3032023,"configSchema":{},"resourceConfigSchema":{}}}
        Status(InvalidArgument): only the first Connector request may set `start`
        Started(Json): {"derive":{"protocol":3032023,"configSchema":{},"resourceConfigSchema":{}}}
        Status(InvalidArgument): every Connector request must set exactly one protocol request, of the type established by the first request
        "#);
    }

    #[tokio::test]
    async fn fails_with_the_connectors_last_log() {
        let outcomes = [
            // Fails before responding to Spec.
            "echo 'a failed walrus appears' >&2; exit 3".to_string(),
            // Responds to Spec with another protocol version.
            r#"read spec; echo '{"spec":{"protocol":42}}'; cat >/dev/null"#.to_string(),
            // Responds to Spec with something else.
            r#"read spec; echo '{"validated":{}}'; cat >/dev/null"#.to_string(),
            // Fails after Started.
            format!("read spec; echo '{SPEC}'; read v; echo 'a later walrus' >&2; exit 4"),
        ];
        let mut rendered = Vec::new();
        for script in outcomes {
            rendered.push(drive(&script, false, vec![first("info"), validate()]).await);
        }

        insta::assert_snapshot!(rendered.join("\n"), @r#"
        Log(error): a failed walrus appears {}
        Log(error): connector failed {"status": String("exit status: 3")}
        Status(Unknown): a failed walrus appears
        Status(Internal): connector returned an unexpected protocol version 42 (expected 3032023)
        Status(Internal): Derive connector responded to Spec with {"validated":{}}
        Started(Json): {"derive":{"protocol":3032023,"configSchema":{},"resourceConfigSchema":{}}}
        Log(warn): a later walrus {}
        Log(error): connector failed {"status": String("exit status: 4")}
        Status(Unknown): a later walrus
        "#);
    }

    #[tokio::test]
    async fn writes_a_vmms_refusals_while_its_connector_runs() {
        let script = format!(r#"read spec; echo '{SPEC}'; read v; echo '{{"validated":{{}}}}'"#);
        let refusals: &'static [u8] = b"{\"refusal\":\"name\",\"name\":\"pypi.org\"}\n\
            not a refusal\n\
            {\"refusal\":\"stopped\",\"reported\":32}\n";
        let output = drive_with(
            &script,
            false,
            Some(Box::pin(tokio::io::BufReader::new(refusals))),
            vec![first("info"), validate()],
        )
        .await;

        // The pump runs beside the connector, so its logs may land anywhere
        // ahead of the stream's end. An invalid line is only traced.
        let (logs, responses): (Vec<&str>, Vec<&str>) =
            output.lines().partition(|line| line.starts_with("Log("));
        assert_eq!(responses.len(), 2, "{output}");
        insta::assert_snapshot!(logs.join("\n"), @r#"
        Log(warn): refused DNS name pypi.org, which this connector's egress does not permit; a task may permit it in egress.hosts; installing packages from PyPI typically requires both pypi.org and files.pythonhosted.org in egress.hosts {"name": String("pypi.org")}
        Log(warn): further refused DNS names are not reported {"reported": String("32")}
        "#);
    }
}
