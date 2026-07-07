//! Connector-proxy backend for runtime-next.
//!
//! Serves the raw `capture` / `derive` / `materialize` `Connector` gRPC
//! services on the task-service UDS so the data-plane Go connector-proxy
//! (`go/runtime/connector_proxy.go`) can forward control-plane connector RPCs
//! into runtime-next, replacing the V1 runtime backend. It is a hard cutover:
//! the control plane must not be able to tell V1 from V2 for its existing
//! Validate / Discover / Spec traffic.
//!
//! Two request shapes select two behaviors per stream, on protocol surface that
//! previously had no proxy clients:
//!
//! - **Unary requests** (per protocol: derive `Spec`/`Validate`; capture
//!   `Spec`/`Discover`/`Validate`/`Apply`; materialize `Spec`/`Validate`/`Apply`)
//!   start a *fresh* connector each, forward the single request, yield the
//!   single verified response with the connector [`Container`](proto::Container)
//!   ext attached, then drain the connector to EOF and await the next request.
//!   This matches V1's `serve_unary`, minus the RocksDB-mediated state
//!   injection (that path — `serve_session` — has no proxy clients and is not
//!   ported).
//!
//! - A first-request **`Open`** starts one connector via the same startup used
//!   by V2 shards and pipes both directions verbatim (`Reset` included) until
//!   client EOF stops it. Exactly one connector per stream; a subsequent `Open`
//!   is a protocol error.
//!
//! Unlike the mediated V1 session path, raw sessions do no RocksDB state
//! injection, combining, or checkpoint rewriting: the connector's IO is the
//! client's IO. Sops config is decrypted server-side by connector startup, as
//! Validate does today.

use crate::proto;
use futures::{StreamExt, stream::BoxStream};
use proto_flow::{capture, derive, materialize};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

/// Hosts the three raw connector services. It holds only the narrow slice of a
/// shard [`Service`](crate::shard::Service) that connector startup needs — the
/// data plane, container network, and a [`LoggerFactory`](crate::LoggerFactory)
/// for per-stream connector logs — with no publisher, RocksDB, or leader.
#[derive(Clone)]
pub struct ConnectorProxy<L: crate::LoggerFactory> {
    plane: crate::Plane,
    container_network: String,
    /// Proxy-runtime task name (`connector-proxy-<nanos>`), used to label unary
    /// requests' logs and containers (V1 parity). Raw sessions instead take the
    /// task name from the `Open`'s embedded spec.
    task_name: String,
    logger_factory: L,
}

impl<L: crate::LoggerFactory> ConnectorProxy<L> {
    pub fn new(
        plane: crate::Plane,
        container_network: String,
        task_name: String,
        logger_factory: L,
    ) -> Self {
        Self {
            plane,
            container_network,
            task_name,
            logger_factory,
        }
    }

    pub fn into_capture_service(
        self,
    ) -> proto_grpc::capture::connector_server::ConnectorServer<Self> {
        proto_grpc::capture::connector_server::ConnectorServer::new(self)
            .max_decoding_message_size(crate::MAX_MESSAGE_SIZE)
            .max_encoding_message_size(usize::MAX)
    }

    pub fn into_derive_service(
        self,
    ) -> proto_grpc::derive::connector_server::ConnectorServer<Self> {
        proto_grpc::derive::connector_server::ConnectorServer::new(self)
            .max_decoding_message_size(crate::MAX_MESSAGE_SIZE)
            .max_encoding_message_size(usize::MAX)
    }

    pub fn into_materialize_service(
        self,
    ) -> proto_grpc::materialize::connector_server::ConnectorServer<Self> {
        proto_grpc::materialize::connector_server::ConnectorServer::new(self)
            .max_decoding_message_size(crate::MAX_MESSAGE_SIZE)
            .max_encoding_message_size(usize::MAX)
    }
}

#[tonic::async_trait]
impl<L: crate::LoggerFactory> proto_grpc::derive::connector_server::Connector
    for ConnectorProxy<L>
{
    type DeriveStream = UnboundedReceiverStream<tonic::Result<derive::Response>>;

    async fn derive(
        &self,
        request: tonic::Request<tonic::Streaming<derive::Request>>,
    ) -> tonic::Result<tonic::Response<Self::DeriveStream>> {
        let proxy = self.clone();
        let (tx, rx) = mpsc::unbounded_channel();
        let request_rx = request.into_inner();
        let error_tx = tx.clone();

        tokio::spawn(async move {
            if let Err(err) = serve_derive(proxy, request_rx, tx).await {
                let _ = error_tx.send(Err(crate::anyhow_to_status(err)));
            }
        });
        Ok(tonic::Response::new(UnboundedReceiverStream::new(rx)))
    }
}

#[tonic::async_trait]
impl<L: crate::LoggerFactory> proto_grpc::capture::connector_server::Connector
    for ConnectorProxy<L>
{
    type CaptureStream = UnboundedReceiverStream<tonic::Result<capture::Response>>;

    async fn capture(
        &self,
        request: tonic::Request<tonic::Streaming<capture::Request>>,
    ) -> tonic::Result<tonic::Response<Self::CaptureStream>> {
        let proxy = self.clone();
        let (tx, rx) = mpsc::unbounded_channel();
        let request_rx = request.into_inner();
        let error_tx = tx.clone();

        tokio::spawn(async move {
            if let Err(err) = serve_capture(proxy, request_rx, tx).await {
                let _ = error_tx.send(Err(crate::anyhow_to_status(err)));
            }
        });
        Ok(tonic::Response::new(UnboundedReceiverStream::new(rx)))
    }
}

#[tonic::async_trait]
impl<L: crate::LoggerFactory> proto_grpc::materialize::connector_server::Connector
    for ConnectorProxy<L>
{
    type MaterializeStream = UnboundedReceiverStream<tonic::Result<materialize::Response>>;

    async fn materialize(
        &self,
        request: tonic::Request<tonic::Streaming<materialize::Request>>,
    ) -> tonic::Result<tonic::Response<Self::MaterializeStream>> {
        let proxy = self.clone();
        let (tx, rx) = mpsc::unbounded_channel();
        let request_rx = request.into_inner();
        let error_tx = tx.clone();

        tokio::spawn(async move {
            if let Err(err) = serve_materialize(proxy, request_rx, tx).await {
                let _ = error_tx.send(Err(crate::anyhow_to_status(err)));
            }
        });
        Ok(tonic::Response::new(UnboundedReceiverStream::new(rx)))
    }
}

async fn serve_derive<L, S>(
    proxy: ConnectorProxy<L>,
    mut request_rx: S,
    client_tx: mpsc::UnboundedSender<tonic::Result<derive::Response>>,
) -> anyhow::Result<()>
where
    L: crate::LoggerFactory,
    S: futures::Stream<Item = tonic::Result<derive::Request>> + Send + Unpin + 'static,
{
    let Some(first) = next_request("Derive", &mut request_rx).await? else {
        return Ok(()); // Empty stream: clean EOF.
    };
    let log_level = first.get_internal()?.log_level();

    if first.open.is_some() {
        let task_name = first
            .open
            .as_ref()
            .and_then(|o| o.collection.as_ref())
            .map(|c| c.name.as_str())
            .filter(|n| !n.is_empty())
            .unwrap_or(&proxy.task_name)
            .to_string();
        let logger = proxy.logger_factory.open(&task_name);

        let (connector_tx, connector_rx, container, codec) =
            crate::shard::derive::connector::start(
                proxy.plane,
                &proxy.container_network,
                &task_name,
                // The proxy *is* the connector's data plane: it always starts
                // the connector locally, never re-dialing another proxy.
                None,
                &logger,
                log_level,
                first,
            )
            .await?;

        let codec = codec_to_proto(codec) as i32;
        pipe_session(
            request_rx,
            connector_tx,
            connector_rx,
            client_tx,
            |r| r.open.is_some(),
            move |resp| {
                resp.set_internal(|ext: &mut proto::DeriveResponseExt| {
                    ext.container = container.clone();
                    ext.codec = codec;
                });
            },
        )
        .await
    } else {
        let mut next = Some(first);
        loop {
            let request = match next.take() {
                Some(request) => request,
                None => match next_request("Derive", &mut request_rx).await? {
                    Some(request) => request,
                    None => return Ok(()),
                },
            };
            if request.open.is_some() {
                anyhow::bail!("Derive protocol error: Open may only be a stream's first request");
            }
            let is_spec = request.spec.is_some();
            let is_validate = request.validate.is_some();
            let log_level = request.get_internal()?.log_level();

            let logger = proxy.logger_factory.open(&proxy.task_name);
            let (connector_tx, connector_rx, container, _codec) =
                crate::shard::derive::connector::start(
                    proxy.plane,
                    &proxy.container_network,
                    &proxy.task_name,
                    None,
                    &logger,
                    log_level,
                    request,
                )
                .await?;
            std::mem::drop(connector_tx); // Send EOF.

            let mut response = unary_exchange("Derive", connector_rx, |r| {
                (is_spec && r.spec.is_some()) || (is_validate && r.validated.is_some())
            })
            .await?;
            response.set_internal(|ext: &mut proto::DeriveResponseExt| ext.container = container);

            if client_tx.send(Ok(response)).is_err() {
                return Ok(()); // Client hung up.
            }
        }
    }
}

async fn serve_capture<L, S>(
    proxy: ConnectorProxy<L>,
    mut request_rx: S,
    client_tx: mpsc::UnboundedSender<tonic::Result<capture::Response>>,
) -> anyhow::Result<()>
where
    L: crate::LoggerFactory,
    S: futures::Stream<Item = tonic::Result<capture::Request>> + Send + Unpin + 'static,
{
    let Some(first) = next_request("Capture", &mut request_rx).await? else {
        return Ok(());
    };
    let log_level = first.get_internal()?.log_level();

    if first.open.is_some() {
        let task_name = first
            .open
            .as_ref()
            .and_then(|o| o.capture.as_ref())
            .map(|c| c.name.as_str())
            .filter(|n| !n.is_empty())
            .unwrap_or(&proxy.task_name)
            .to_string();
        let logger = proxy.logger_factory.open(&task_name);

        let (connector_tx, connector_rx, container, _token_restart_at) =
            crate::shard::capture::connector::start(
                proxy.plane,
                &proxy.container_network,
                &task_name,
                &logger,
                log_level,
                first,
            )
            .await?;

        pipe_session(
            request_rx,
            connector_tx,
            connector_rx,
            client_tx,
            |r| r.open.is_some(),
            move |resp| {
                resp.set_internal(|ext: &mut proto::CaptureResponseExt| {
                    ext.container = container.clone();
                });
            },
        )
        .await
    } else {
        let mut next = Some(first);
        loop {
            let request = match next.take() {
                Some(request) => request,
                None => match next_request("Capture", &mut request_rx).await? {
                    Some(request) => request,
                    None => return Ok(()),
                },
            };
            if request.open.is_some() {
                anyhow::bail!("Capture protocol error: Open may only be a stream's first request");
            }
            let is_spec = request.spec.is_some();
            let is_discover = request.discover.is_some();
            let is_validate = request.validate.is_some();
            let is_apply = request.apply.is_some();
            let log_level = request.get_internal()?.log_level();

            let logger = proxy.logger_factory.open(&proxy.task_name);
            let (connector_tx, connector_rx, container, _token_restart_at) =
                crate::shard::capture::connector::start(
                    proxy.plane,
                    &proxy.container_network,
                    &proxy.task_name,
                    &logger,
                    log_level,
                    request,
                )
                .await?;
            std::mem::drop(connector_tx);

            let mut response = unary_exchange("Capture", connector_rx, |r| {
                (is_spec && r.spec.is_some())
                    || (is_discover && r.discovered.is_some())
                    || (is_validate && r.validated.is_some())
                    || (is_apply && r.applied.is_some())
            })
            .await?;
            response.set_internal(|ext: &mut proto::CaptureResponseExt| ext.container = container);

            if client_tx.send(Ok(response)).is_err() {
                return Ok(());
            }
        }
    }
}

async fn serve_materialize<L, S>(
    proxy: ConnectorProxy<L>,
    mut request_rx: S,
    client_tx: mpsc::UnboundedSender<tonic::Result<materialize::Response>>,
) -> anyhow::Result<()>
where
    L: crate::LoggerFactory,
    S: futures::Stream<Item = tonic::Result<materialize::Request>> + Send + Unpin + 'static,
{
    let Some(first) = next_request("Materialize", &mut request_rx).await? else {
        return Ok(());
    };
    let log_level = first.get_internal()?.log_level();

    if first.open.is_some() {
        let task_name = first
            .open
            .as_ref()
            .and_then(|o| o.materialization.as_ref())
            .map(|m| m.name.as_str())
            .filter(|n| !n.is_empty())
            .unwrap_or(&proxy.task_name)
            .to_string();
        let logger = proxy.logger_factory.open(&task_name);

        let (connector_tx, connector_rx, container, _codec, _token_restart_at) =
            crate::shard::materialize::connector::start(
                proxy.plane,
                &proxy.container_network,
                &task_name,
                &logger,
                log_level,
                first,
            )
            .await?;

        pipe_session(
            request_rx,
            connector_tx,
            connector_rx,
            client_tx,
            |r| r.open.is_some(),
            move |resp| {
                resp.set_internal(|ext: &mut proto::MaterializeResponseExt| {
                    ext.container = container.clone();
                });
            },
        )
        .await
    } else {
        let mut next = Some(first);
        loop {
            let request = match next.take() {
                Some(request) => request,
                None => match next_request("Materialize", &mut request_rx).await? {
                    Some(request) => request,
                    None => return Ok(()),
                },
            };
            if request.open.is_some() {
                anyhow::bail!(
                    "Materialize protocol error: Open may only be a stream's first request"
                );
            }
            let is_spec = request.spec.is_some();
            let is_validate = request.validate.is_some();
            let is_apply = request.apply.is_some();
            let log_level = request.get_internal()?.log_level();

            let logger = proxy.logger_factory.open(&proxy.task_name);
            let (connector_tx, connector_rx, container, _codec, _token_restart_at) =
                crate::shard::materialize::connector::start(
                    proxy.plane,
                    &proxy.container_network,
                    &proxy.task_name,
                    &logger,
                    log_level,
                    request,
                )
                .await?;
            std::mem::drop(connector_tx);

            let mut response = unary_exchange("Materialize", connector_rx, |r| {
                (is_spec && r.spec.is_some())
                    || (is_validate && r.validated.is_some())
                    || (is_apply && r.applied.is_some())
            })
            .await?;
            response
                .set_internal(|ext: &mut proto::MaterializeResponseExt| ext.container = container);

            if client_tx.send(Ok(response)).is_err() {
                return Ok(());
            }
        }
    }
}

/// Read the next request, mapping a stream `Status` into an `anyhow` error.
async fn next_request<Req>(
    source: &'static str,
    request_rx: &mut (impl futures::Stream<Item = tonic::Result<Req>> + Unpin),
) -> anyhow::Result<Option<Req>> {
    match request_rx.next().await {
        None => Ok(None),
        Some(Ok(request)) => Ok(Some(request)),
        Some(Err(status)) => {
            Err(crate::verify(source, "client request", "controller").fail_status(status))
        }
    }
}

/// Drive one unary request/response exchange over a started connector: read its
/// single response, confirm it matches the request kind via `matches`, then
/// confirm the connector sends EOF. The caller has already queued the request
/// and dropped the sender (connector EOF), so this only reads.
async fn unary_exchange<Resp>(
    source: &'static str,
    mut connector_rx: BoxStream<'static, tonic::Result<Resp>>,
    matches: impl FnOnce(&Resp) -> bool,
) -> anyhow::Result<Resp>
where
    Resp: serde::Serialize + Send + 'static,
{
    let verify = crate::verify(source, "unary response", "connector");
    let response = verify.not_eof(connector_rx.next().await)?;
    if !matches(&response) {
        return Err(verify.fail_msg(response));
    }
    verify.eof(connector_rx.next().await)?;
    Ok(response)
}

/// Pipe a raw connector session verbatim: forward client requests to the
/// connector and connector responses to the client until the client sends EOF
/// (which stops the connector), then drain remaining connector responses to
/// EOF. `on_first` mutates the first connector response so the caller can
/// attach container / codec metadata, as V1's session start does.
///
/// The connector was already started with the client's first `Open`; a later
/// client `Open` (per `is_open`) is a protocol error — one connector per stream.
async fn pipe_session<Req, Resp, S>(
    mut client_rx: S,
    connector_tx: mpsc::Sender<Req>,
    mut connector_rx: BoxStream<'static, tonic::Result<Resp>>,
    client_tx: mpsc::UnboundedSender<tonic::Result<Resp>>,
    is_open: impl Fn(&Req) -> bool + Send + 'static,
    mut on_first: impl FnMut(&mut Resp) + Send,
) -> anyhow::Result<()>
where
    Req: Send + 'static,
    Resp: Send + 'static,
    S: futures::Stream<Item = tonic::Result<Req>> + Send + Unpin + 'static,
{
    // Forward client → connector. Dropping `connector_tx` on client EOF (or a
    // send failure) delivers the connector its EOF.
    let forward = tokio::spawn(async move {
        while let Some(request) = client_rx.next().await {
            let request = request.map_err(crate::status_to_anyhow)?;
            if is_open(&request) {
                anyhow::bail!(
                    "connector proxy protocol error: Open may only be a session's first request"
                );
            }
            if connector_tx.send(request).await.is_err() {
                break; // Connector stopped; its EOF/error surfaces on the reverse path.
            }
        }
        Ok::<(), anyhow::Error>(())
    });

    // Reverse: connector → client, attaching metadata to the first response.
    let mut is_first = true;
    let reverse = async {
        while let Some(response) = connector_rx.next().await {
            let mut response = response.map_err(crate::status_to_anyhow)?;
            if std::mem::take(&mut is_first) {
                on_first(&mut response);
            }
            if client_tx.send(Ok(response)).is_err() {
                break; // Client hung up.
            }
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;

    // The reverse path has ended (connector EOF or error, or client gone). If
    // the client hasn't yet closed its request stream, the forward task is
    // still parked on it: abort so we don't wait on a client that will send
    // nothing more. A forward task that already finished (the common client-EOF
    // path) is unaffected, and its result — including a second-Open protocol
    // error — is still observed below.
    forward.abort();
    let forward = match forward.await {
        Ok(result) => result,
        Err(join) if join.is_cancelled() => Ok(()),
        Err(join) => Err(anyhow::anyhow!(
            "connector proxy forward task panicked: {join}"
        )),
    };

    reverse.and(forward)
}

fn codec_to_proto(codec: connector_init::Codec) -> proto::Codec {
    match codec {
        connector_init::Codec::Proto => proto::Codec::Proto,
        connector_init::Codec::Json => proto::Codec::Json,
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use proto_flow::derive::{self, request};
    use proto_flow::flow::collection_spec::derivation::ConnectorType;

    // A single SQLite derivation reading one source: enough to exercise both the
    // unary surface (Spec/Validate — SQLite has no container) and a raw Open
    // session, all in-process with no connector containers.
    const CATALOG: &str = r#"
collections:
  test/ints:
    schema:
      type: object
      properties:
        Key: { type: string }
        Int: { type: integer }
      required: [Key, Int]
    key: [/Key]
  test/sums:
    schema:
      type: object
      properties:
        Key: { type: string }
        Sum: { type: integer }
      required: [Key, Sum]
    key: [/Key]
    derive:
      using:
        sqlite:
          migrations:
            - CREATE TABLE s (k TEXT PRIMARY KEY, v INTEGER);
      transforms:
        - name: fromInts
          source: { name: test/ints }
          shuffle: { key: [/Key] }
          lambda: |
            INSERT INTO s (k, v) VALUES ($Key, $Int)
              ON CONFLICT DO UPDATE SET v = v + $Int;
            SELECT JSON_OBJECT('Key', k, 'Sum', v) FROM s WHERE k = $Key;
"#;

    fn test_proxy() -> ConnectorProxy<crate::TracingLoggerFactory> {
        ConnectorProxy::new(
            crate::Plane::Local,
            String::new(),
            "connector-proxy-test".to_string(),
            crate::TracingLoggerFactory,
        )
    }

    async fn built_derivation() -> proto_flow::flow::CollectionSpec {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catalog.flow.yaml");
        std::fs::write(&path, CATALOG).unwrap();
        let url = build::arg_source_to_url(path.to_str().unwrap(), false).unwrap();
        let output = build::for_local_test(&url, false)
            .await
            .into_result()
            .expect("catalog build should succeed");

        output
            .built
            .built_collections
            .iter()
            .find_map(|bc| bc.spec.as_ref().filter(|s| s.name == "test/sums").cloned())
            .expect("built test/sums derivation")
    }

    /// Drive `serve_derive` over a fixed request sequence to completion (as the
    /// spawned trait handler does), returning its result and every yielded
    /// response.
    async fn run_derive(
        requests: Vec<derive::Request>,
    ) -> (anyhow::Result<()>, Vec<derive::Response>) {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let result = serve_derive(
            test_proxy(),
            futures::stream::iter(requests.into_iter().map(Ok)),
            tx,
        )
        .await;

        let mut responses = Vec::new();
        while let Some(resp) = rx.recv().await {
            responses.push(resp.expect("proxy did not yield a Status mid-stream"));
        }
        (result, responses)
    }

    /// Compact label of which response variant is set, for readable walk-throughs.
    fn kind(resp: &derive::Response) -> &'static str {
        if resp.spec.is_some() {
            "spec"
        } else if resp.validated.is_some() {
            "validated"
        } else if resp.opened.is_some() {
            "opened"
        } else if resp.published.is_some() {
            "published"
        } else if resp.flushed.is_some() {
            "flushed"
        } else if resp.started_commit.is_some() {
            "startedCommit"
        } else {
            "other"
        }
    }

    fn spec_request() -> derive::Request {
        derive::Request {
            spec: Some(request::Spec {
                connector_type: ConnectorType::Sqlite as i32,
                config_json: "{}".into(),
            }),
            ..Default::default()
        }
    }

    /// A single unary Spec starts a fresh connector, yields one verified Spec
    /// response, and closes. SQLite has no container, so none is attached.
    #[tokio::test]
    async fn unary_spec() {
        let (result, responses) = run_derive(vec![spec_request()]).await;
        result.expect("serve_derive");

        assert_eq!(responses.iter().map(kind).collect::<Vec<_>>(), vec!["spec"]);
        assert!(
            responses[0].get_internal().unwrap().container.is_none(),
            "SQLite connector has no container"
        );
    }

    /// Two unary requests on one stream each start their own fresh connector and
    /// yield their own verified response — the parity behavior the control plane's
    /// `[Spec, Discover]` sequence relies on (derive has no Discover, so `[Spec,
    /// Spec]` stands in).
    #[tokio::test]
    async fn unary_sequence_starts_a_fresh_connector_each() {
        let (result, responses) = run_derive(vec![spec_request(), spec_request()]).await;
        result.expect("serve_derive");
        assert_eq!(
            responses.iter().map(kind).collect::<Vec<_>>(),
            vec!["spec", "spec"]
        );
    }

    /// Once a stream is in unary mode, an `Open` is a protocol error (mode is
    /// selected by the first request's shape). The first Spec still succeeds; the
    /// stream then fails.
    #[tokio::test]
    async fn open_after_unary_is_a_protocol_error() {
        let open = derive::Request {
            open: Some(request::Open::default()),
            ..Default::default()
        };
        let (result, responses) = run_derive(vec![spec_request(), open]).await;

        assert_eq!(responses.iter().map(kind).collect::<Vec<_>>(), vec!["spec"]);
        let err = result.expect_err("Open after a unary request must error");
        assert!(
            format!("{err:#}").contains("Open may only be a stream's first request"),
            "unexpected error: {err:#}"
        );
    }

    /// A first-request `Open` starts one connector and pipes both directions
    /// verbatim until client EOF: Open→Opened (container attached — here None for
    /// SQLite), a forwarded `Reset` (no response), then a Flush / StartCommit
    /// transaction whose Flushed / StartedCommit flow back through the pipe.
    #[tokio::test]
    async fn raw_session_pipes_reset_and_transaction() {
        let spec = built_derivation().await;
        let open = derive::Request {
            open: Some(request::Open {
                collection: Some(spec),
                version: "v1".to_string(),
                range: Some(Default::default()),
                state_json: Default::default(),
            }),
            ..Default::default()
        };
        let reset = derive::Request {
            reset: Some(request::Reset {}),
            ..Default::default()
        };
        let flush = derive::Request {
            flush: Some(request::Flush::default()),
            ..Default::default()
        };
        let start_commit = derive::Request {
            start_commit: Some(request::StartCommit {
                runtime_checkpoint: None,
            }),
            ..Default::default()
        };

        let (result, responses) = run_derive(vec![open, reset, flush, start_commit]).await;
        result.expect("serve_derive");
        assert_eq!(
            responses.iter().map(kind).collect::<Vec<_>>(),
            vec!["opened", "flushed", "startedCommit"],
        );
    }

    // ---- Phase 8: the client (shard) side of the remote seam ----

    use crate::LoggerFactory as _;
    use proto_flow::flow;

    /// A [`RemoteConnectors`](crate::RemoteConnectors) that always fails to dial:
    /// used to prove a code path never reaches the network.
    struct FailRemote;
    #[tonic::async_trait]
    impl crate::RemoteConnectors for FailRemote {
        async fn dial_derive(
            &self,
            _task_name: &str,
        ) -> anyhow::Result<(tonic::transport::Channel, proto_grpc::Metadata)> {
            anyhow::bail!("dial_derive must not be called")
        }
    }

    fn sqlite_open(spec: flow::CollectionSpec) -> derive::Request {
        derive::Request {
            open: Some(request::Open {
                collection: Some(spec),
                version: "v1".to_string(),
                range: Some(Default::default()),
                state_json: Default::default(),
            }),
            ..Default::default()
        }
    }

    /// A SQLite derivation runs in-process even when the shard is configured with
    /// a remote dialer — it has no container to offload, and a run of only SQLite
    /// derivations must never touch the network (the dialer here would error).
    #[tokio::test]
    async fn sqlite_ignores_a_remote_dialer() {
        let spec = built_derivation().await;
        let remote: std::sync::Arc<dyn crate::RemoteConnectors> = std::sync::Arc::new(FailRemote);
        let logger = crate::TracingLoggerFactory.open("test/sums");

        let (_tx, _rx, container, codec) = crate::shard::derive::connector::start(
            crate::Plane::Local,
            "",
            "test/sums",
            Some(&remote),
            &logger,
            ops::LogLevel::UndefinedLevel,
            sqlite_open(spec),
        )
        .await
        .expect("SQLite runs in-process without dialing");

        assert!(container.is_none());
        assert_eq!(codec, connector_init::Codec::Proto);
    }

    /// A Local connector cannot be offloaded to a remote data plane: routing
    /// errors before any dial or local start.
    #[tokio::test]
    async fn local_connector_with_remote_dialer_is_an_error() {
        let local = derive::Request {
            open: Some(request::Open {
                collection: Some(flow::CollectionSpec {
                    name: "test/local".to_string(),
                    derivation: Some(flow::collection_spec::Derivation {
                        connector_type: flow::collection_spec::derivation::ConnectorType::Local
                            as i32,
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let remote: std::sync::Arc<dyn crate::RemoteConnectors> = std::sync::Arc::new(FailRemote);
        let logger = crate::TracingLoggerFactory.open("test/local");

        let result = crate::shard::derive::connector::start(
            crate::Plane::Local,
            "",
            "test/local",
            Some(&remote),
            &logger,
            ops::LogLevel::UndefinedLevel,
            local,
        )
        .await;
        let Err(err) = result else {
            panic!("Local + remote dialer must error");
        };
        assert!(
            format!("{err:#}").contains("cannot be offloaded to a remote data plane"),
            "unexpected error: {err:#}"
        );
    }

    /// A [`RemoteConnectors`](crate::RemoteConnectors) that hands out a fixed
    /// channel to an in-process proxy, with empty metadata (no proxy-id needed:
    /// the runtime-next connector server doesn't verify it — the Go layer does).
    struct TestRemote {
        channel: tonic::transport::Channel,
    }
    #[tonic::async_trait]
    impl crate::RemoteConnectors for TestRemote {
        async fn dial_derive(
            &self,
            _task_name: &str,
        ) -> anyhow::Result<(tonic::transport::Channel, proto_grpc::Metadata)> {
            Ok((self.channel.clone(), proto_grpc::Metadata::new()))
        }
    }

    /// End-to-end remote path, no Docker and no Go: host the real Phase-7
    /// [`ConnectorProxy`] in-process (running derive-sqlite locally), then drive
    /// the shard-side [`start_remote`](crate::shard::derive::connector::start_remote)
    /// against it. The first response's container / codec are read and stripped,
    /// and a `Reset` between two transactions is piped verbatim — the
    /// reset-between-cases the harness relies on.
    #[tokio::test]
    async fn remote_round_trip_through_proxy() {
        // Host the proxy's derive service on an ephemeral loopback port.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = futures::stream::unfold(listener, |listener| async move {
            Some((listener.accept().await.map(|(conn, _)| conn), listener))
        });
        tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(test_proxy().into_derive_service())
                .serve_with_incoming(incoming)
                .await;
        });

        let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let remote: std::sync::Arc<dyn crate::RemoteConnectors> =
            std::sync::Arc::new(TestRemote { channel });

        let open = sqlite_open(built_derivation().await);
        let (request_tx, request_rx) = mpsc::channel(crate::CHANNEL_BUFFER);
        let (request_tx, mut stream, container, codec) =
            crate::shard::derive::connector::start_remote(
                &remote,
                "test/sums",
                request_tx,
                request_rx,
                open,
            )
            .await
            .expect("start_remote");

        // Container / codec were read from the first response and returned.
        assert!(container.is_none(), "sqlite has no container");
        assert_eq!(codec, connector_init::Codec::Proto);

        // The first response is Opened, with the ext stripped of container/codec.
        let opened = stream.next().await.unwrap().unwrap();
        assert_eq!(kind(&opened), "opened");
        let ext = opened.get_internal().unwrap();
        assert!(
            ext.container.is_none() && ext.codec == 0,
            "ext was stripped"
        );

        // Two transactions with a Reset between them, all piped through the proxy.
        for _ in 0..2 {
            request_tx
                .send(derive::Request {
                    flush: Some(request::Flush::default()),
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(kind(&stream.next().await.unwrap().unwrap()), "flushed");

            request_tx
                .send(derive::Request {
                    start_commit: Some(request::StartCommit {
                        runtime_checkpoint: None,
                    }),
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(
                kind(&stream.next().await.unwrap().unwrap()),
                "startedCommit"
            );

            // Reset is forwarded verbatim and yields no response.
            request_tx
                .send(derive::Request {
                    reset: Some(request::Reset {}),
                    ..Default::default()
                })
                .await
                .unwrap();
        }

        // Client EOF stops the connector; the response stream then ends.
        drop(request_tx);
        assert!(
            stream.next().await.is_none(),
            "stream ends at connector EOF"
        );
    }
}
