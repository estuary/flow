use crate::logs;
use anyhow::Context;
use futures::{TryFutureExt, TryStreamExt};
use proto_flow::{capture, derive, materialize};
use std::future::Future;
use uuid::Uuid;

/// Trait for performing Discover operations from the control plane, which handles logging.
pub trait DiscoverConnectors: Clone + Send + Sync + 'static {
    fn discover<'a>(
        &'a self,
        data_plane: &'a tables::DataPlane,
        task: &'a models::Capture,
        logs_token: Uuid,
        request: capture::Request,
    ) -> impl Future<
        Output = anyhow::Result<(capture::response::Spec, capture::response::Discovered)>,
    >
    + 'a
    + Send;
}

pub trait MakeConnectors: std::fmt::Debug + Sync + Send + 'static {
    type Connectors: validation::Connectors;

    fn make_connectors(&self, logs_token: Uuid) -> Self::Connectors;
}

#[derive(Debug, Clone)]
pub struct DataPlaneConnectors {
    logs_tx: logs::Tx,
}
impl DataPlaneConnectors {
    pub fn new(logs_tx: logs::Tx) -> DataPlaneConnectors {
        Self { logs_tx }
    }
}

impl MakeConnectors for DataPlaneConnectors {
    type Connectors = ProxyConnectors<logs::OpsHandler>;
    fn make_connectors(&self, logs_token: uuid::Uuid) -> Self::Connectors {
        let log_handler = logs::ops_handler(self.logs_tx.clone(), "build".to_string(), logs_token);
        ProxyConnectors::new(log_handler)
    }
}

impl DiscoverConnectors for DataPlaneConnectors {
    async fn discover<'a>(
        &'a self,
        data_plane: &'a tables::DataPlane,
        task: &'a models::Capture,
        logs_token: Uuid,
        request: capture::Request,
    ) -> anyhow::Result<(capture::response::Spec, capture::response::Discovered)> {
        let discover = request
            .discover
            .as_ref()
            .expect("expected a discover request");

        // Start an RPC which requests a Spec followed by a Discover.
        let request_rx = futures::stream::iter([
            capture::Request {
                spec: Some(capture::request::Spec {
                    connector_type: discover.connector_type,
                    config_json: discover.config_json.clone(),
                }),
                internal: request.internal.clone(), // Use same log level.
                ..Default::default()
            },
            request,
        ]);

        let proxy = ProxyConnectors::new(logs::ops_handler(
            self.logs_tx.clone(),
            "discover".to_string(),
            logs_token,
        ));
        let response_rx = <ProxyConnectors<_> as validation::Connectors>::capture(
            &proxy, data_plane, task, request_rx,
        );
        futures::pin_mut!(response_rx);

        let spec = match response_rx.try_next().await? {
            Some(capture::Response {
                spec: Some(spec), ..
            }) => spec,
            response => anyhow::bail!(
                "expected connector to send a Response.Spec, but got {}",
                serde_json::to_string(&response).unwrap()
            ),
        };
        let discovered = match response_rx.try_next().await? {
            Some(capture::Response {
                discovered: Some(discovered),
                ..
            }) => discovered,
            response => anyhow::bail!(
                "expected connector to send a Response.Discovered, but got {}",
                serde_json::to_string(&response).unwrap()
            ),
        };
        match response_rx.try_next().await? {
            None => (), // Expected EOF.
            Some(response) => {
                anyhow::bail!(
                    "expected connector to send closing EOF, but got {}",
                    serde_json::to_string(&response).unwrap()
                );
            }
        }

        Ok((spec, discovered))
    }
}

pub struct ProxyConnectors<L: runtime::LogHandler> {
    log_handler: L,
}

impl<L: runtime::LogHandler> validation::Connectors for ProxyConnectors<L> {
    fn capture<'a, R>(
        &'a self,
        data_plane: &'a tables::DataPlane,
        task: &'a models::Capture,
        request_rx: R,
    ) -> impl futures::Stream<Item = anyhow::Result<capture::Response>> + Send + 'a
    where
        R: futures::Stream<Item = capture::Request> + Send + Unpin + 'static,
    {
        coroutines::try_coroutine(|co| async move {
            let (channel, metadata, logs) = crate::timeout(
                DIAL_PROXY_TIMEOUT,
                self.dial_proxy(data_plane, task.as_str()),
                || dial_proxy_timeout_msg(data_plane),
            )
            .await?;

            let mut client =
                proto_grpc::capture::connector_client::ConnectorClient::with_interceptor(
                    channel, metadata,
                )
                .max_decoding_message_size(runtime::MAX_MESSAGE_SIZE);

            Self::drive_proxy_rpc(co, logs, client.capture(request_rx).await).await
        })
    }

    fn derive<'a, R>(
        &'a self,
        data_plane: &'a tables::DataPlane,
        task: &'a models::Collection,
        request_rx: R,
    ) -> impl futures::Stream<Item = anyhow::Result<derive::Response>> + Send + 'a
    where
        R: futures::Stream<Item = derive::Request> + Send + Unpin + 'static,
    {
        coroutines::try_coroutine(|co| async move {
            let (channel, metadata, logs) = crate::timeout(
                DIAL_PROXY_TIMEOUT,
                self.dial_proxy(data_plane, task.as_str()),
                || dial_proxy_timeout_msg(data_plane),
            )
            .await?;

            let mut client =
                proto_grpc::derive::connector_client::ConnectorClient::with_interceptor(
                    channel, metadata,
                )
                .max_decoding_message_size(runtime::MAX_MESSAGE_SIZE);

            Self::drive_proxy_rpc(co, logs, client.derive(request_rx).await).await
        })
    }

    fn materialize<'a, R>(
        &'a self,
        data_plane: &'a tables::DataPlane,
        task: &'a models::Materialization,
        request_rx: R,
    ) -> impl futures::Stream<Item = anyhow::Result<materialize::Response>> + Send + 'a
    where
        R: futures::Stream<Item = materialize::Request> + Send + Unpin + 'static,
    {
        coroutines::try_coroutine(|co| async move {
            let (channel, metadata, logs) = crate::timeout(
                DIAL_PROXY_TIMEOUT,
                self.dial_proxy(data_plane, task.as_str()),
                || dial_proxy_timeout_msg(data_plane),
            )
            .await?;

            let mut client =
                proto_grpc::materialize::connector_client::ConnectorClient::with_interceptor(
                    channel, metadata,
                )
                .max_decoding_message_size(runtime::MAX_MESSAGE_SIZE);

            Self::drive_proxy_rpc(co, logs, client.materialize(request_rx).await).await
        })
    }
}

impl<L: runtime::LogHandler> ProxyConnectors<L> {
    pub(crate) fn new(log_handler: L) -> Self {
        Self { log_handler }
    }

    async fn dial_proxy<'a>(
        &'a self,
        data_plane: &tables::DataPlane,
        task: &str,
    ) -> anyhow::Result<(
        tonic::transport::Channel,
        proto_grpc::Metadata,
        (
            futures::channel::oneshot::Sender<()>,
            impl Future<Output = anyhow::Result<()>> + 'a,
        ),
    )> {
        // A unary Validate / Discover / Spec proxy is short-lived; its token
        // need only outlive the connector RPC (as V1 sized it).
        let (channel, metadata, _features, cancel_tx, mut proxy_responses) =
            proxy_handshake(data_plane, task, *CONNECTOR_TIMEOUT * 2).await?;

        let log_loop = async move {
            while let Some(log) = proxy_responses
                .try_next()
                .map_err(runtime::status_to_anyhow)
                .await
                .context("failed to read connector proxy log response stream")?
            {
                if let Some(log) = log.log.as_ref() {
                    self.log_handler.log(log);
                }
            }
            Result::<(), anyhow::Error>::Ok(())
        };

        Ok((channel, metadata, (cancel_tx, log_loop)))
    }

    async fn drive_proxy_rpc<Response>(
        mut co: coroutines::Suspend<Response, ()>,
        (cancel_tx, log_loop): (
            futures::channel::oneshot::Sender<()>,
            impl Future<Output = anyhow::Result<()>>,
        ),
        response: Result<tonic::Response<tonic::Streaming<Response>>, tonic::Status>,
    ) -> anyhow::Result<()> {
        // Loop which processes all stream responses and errors.
        let response_loop = async move {
            // Drop on EOF to gracefully stop the proxy runtime and finish reading logs.
            let _guard = cancel_tx;

            let mut response_rx = response.map_err(runtime::status_to_anyhow)?.into_inner();

            while let Some(response) = crate::timeout(
                *CONNECTOR_TIMEOUT,
                response_rx.try_next().map_err(runtime::status_to_anyhow),
                || CONNECTOR_TIMEOUT_MSG,
            )
            .await?
            {
                () = co.yield_(response).await;
            }
            Ok::<_, anyhow::Error>(())
        };

        // Drive the response and log loops to completion:
        // * The response loop drops `cancel_tx` on EOF or Error
        // * This closes the tx side of the connector proxy RPC
        // * The log loop reads the rx side of the connector proxy RPC until EOF,
        //   ensuring late-arriving logs (after a response error) are processed.
        let (response_loop, log_loop) = futures::join!(response_loop, log_loop);

        match (response_loop, log_loop) {
            (Err(response), Err(log)) => {
                tracing::error!(?log, "failed to read connector proxy response stream");
                Err(response)
            }
            (Ok(()), Err(err)) | (Err(err), Ok(())) => Err(err),
            (Ok(()), Ok(())) => Ok(()),
        }
    }
}

/// Perform the `ProxyConnectors` handshake with a data plane: sign a
/// `PROXY_CONNECTOR` token (subject `sub`, living `token_ttl`), open the control
/// stream, and read the first `ConnectorProxyResponse`. Returns a channel dialed
/// to the connector address, the metadata (bearer + `proxy-id`) to attach to
/// connector RPCs, the advertised feature bitmask, a keep-alive sender (dropping
/// it gracefully stops the proxy runtime), and the remaining response stream —
/// which carries connector logs.
///
/// Shared by the unary `dial_proxy` (Validate / Discover / Spec) and the
/// remote-session dialer, which differ only in token lifetime and how they
/// consume the log stream.
async fn proxy_handshake(
    data_plane: &tables::DataPlane,
    sub: &str,
    token_ttl: std::time::Duration,
) -> anyhow::Result<(
    tonic::transport::Channel,
    proto_grpc::Metadata,
    u32,
    futures::channel::oneshot::Sender<()>,
    tonic::Streaming<proto_flow::runtime::ConnectorProxyResponse>,
)> {
    let tables::DataPlane {
        reactor_address,
        hmac_keys,
        data_plane_fqdn,
        ..
    } = data_plane;

    // Parse first data-plane HMAC key (used for signing tokens).
    let (encode_key, _decode) = tokens::jwt::parse_base64_hmac_keys(hmac_keys.iter().take(1))
        .context("invalid data-plane HMAC key")?;

    let iat = tokens::now();
    let claims = proto_gazette::Claims {
        cap: proto_flow::capability::PROXY_CONNECTOR,
        exp: (iat + token_ttl).timestamp() as u64,
        iat: iat.timestamp() as u64,
        iss: data_plane_fqdn.clone(),
        sel: Default::default(),
        sub: sub.to_string(),
    };
    let token = tokens::jwt::sign(&claims, &encode_key)
        .context("failed to sign claims for connector proxy")?;

    let mut metadata = proto_grpc::Metadata::new()
        .with_bearer_token(&token)
        .expect("token is valid");

    // Start an RPC against the base reactor service to start a connector proxy.
    // Use a request stream which blocks until cancelled, and then sends EOF.
    let (cancel_tx, cancel_rx) = futures::channel::oneshot::channel::<()>();

    let mut proxy_client =
        proto_grpc::runtime::connector_proxy_client::ConnectorProxyClient::with_interceptor(
            gazette::dial_channel(&reactor_address)?,
            metadata.clone(),
        );
    let mut proxy_responses = proxy_client
        .proxy_connectors(futures::stream::once(async move {
            _ = cancel_rx.await;
            proto_flow::runtime::ConnectorProxyRequest {}
        }))
        .await?
        .into_inner();

    let Some(proto_flow::runtime::ConnectorProxyResponse {
        address,
        proxy_id,
        features,
        ..
    }) = proxy_responses.try_next().await?
    else {
        anyhow::bail!("unexpected EOF starting connector proxy");
    };

    // `proxy-id` is attached to RPCs issued to this proxy.
    metadata.0.insert("proxy-id", proxy_id.parse()?);

    tracing::debug!(address, proxy_id, features, sub, "started proxy runtime");

    Ok((
        gazette::dial_channel(&address)?,
        metadata,
        features,
        cancel_tx,
        proxy_responses,
    ))
}

/// Upper bound on a catalog-test run, sizing the session proxy token's lifetime.
/// Per-RPC auth is verified when each derive stream opens and Reset is in-band,
/// so no re-dial happens mid-run; the token need only outlive the whole run.
const SESSION_PROXY_TOKEN_TTL: std::time::Duration = std::time::Duration::from_secs(60 * 60);

/// Runs derivation connector sessions remotely for a catalog-test run: one
/// multiplexed proxy runtime per data plane, shared by every task assigned to
/// that plane (tasks in a plane share one `ProxyConnectors` handshake /
/// `proxy-id`). Implements the runtime-next remote-connector seam the harness
/// dials once per session.
pub struct RemoteSessionConnectors<L: runtime::LogHandler> {
    // Derivation task name -> its assigned data plane.
    task_planes: std::collections::HashMap<String, tables::DataPlane>,
    // Connector logs (returned on each plane's control stream) sink here.
    log_handler: L,
    // Lazily-opened proxy runtime per data plane, keyed by control_id.
    runtimes:
        tokio::sync::Mutex<std::collections::HashMap<models::Id, std::sync::Arc<ProxyRuntime>>>,
}

/// A live proxy runtime in one data plane, kept alive for the whole run.
struct ProxyRuntime {
    channel: tonic::transport::Channel,
    metadata: proto_grpc::Metadata,
    // Dropping this gracefully stops the remote proxy runtime at run's end,
    // which then EOFs its response stream and ends the log task below.
    _cancel: futures::channel::oneshot::Sender<()>,
    _logs: tokio::task::JoinHandle<()>,
}

impl<L: runtime::LogHandler> RemoteSessionConnectors<L> {
    /// Build a provider from a completed build: resolve each derivation's
    /// assigned data plane (a missing plane row is a publication error). No
    /// network IO happens until the first `dial_derive`, so a SQLite-only run —
    /// which never dials — never contacts a data plane.
    pub fn new(catalog: &build::Output, log_handler: L) -> anyhow::Result<Self> {
        let mut task_planes = std::collections::HashMap::new();
        for built in catalog.built.built_collections.iter() {
            let Some(spec) = &built.spec else { continue };
            if spec.derivation.is_none() {
                continue;
            }
            let plane = catalog
                .live
                .data_planes
                .get_key(&built.data_plane_id)
                .with_context(|| {
                    format!(
                        "derivation {} is assigned to data-plane {} which is missing from the build",
                        built.collection, built.data_plane_id
                    )
                })?;
            task_planes.insert(spec.name.clone(), plane.clone());
        }
        Ok(Self {
            task_planes,
            log_handler,
            runtimes: Default::default(),
        })
    }
}

#[tonic::async_trait]
impl<L: runtime::LogHandler> runtime_harness::RemoteConnectors for RemoteSessionConnectors<L> {
    async fn dial_derive(
        &self,
        task_name: &str,
    ) -> anyhow::Result<(tonic::transport::Channel, proto_grpc::Metadata)> {
        let plane = self
            .task_planes
            .get(task_name)
            .with_context(|| format!("no data plane resolved for derivation {task_name}"))?;

        // Holding the lock across the handshake serializes dials, but they're
        // rare (once per plane) and this is how one runtime per plane is
        // guaranteed even under concurrent first-dials.
        let mut runtimes = self.runtimes.lock().await;
        if let Some(existing) = runtimes.get(&plane.control_id) {
            return Ok((existing.channel.clone(), existing.metadata.clone()));
        }

        let sub = format!("catalog-test/{}", plane.data_plane_name);
        let (channel, metadata, features, cancel, log_stream) = crate::timeout(
            DIAL_PROXY_TIMEOUT,
            proxy_handshake(plane, &sub, SESSION_PROXY_TOKEN_TTL),
            || dial_proxy_timeout_msg(plane),
        )
        .await?;

        // Fail loudly against an old reactor whose proxy can't serve raw
        // sessions, rather than silently landing on V1 mediated `Open`
        // semantics.
        if features & (proto_flow::runtime::ProxyFeature::RawSessions as u32) == 0 {
            anyhow::bail!(
                "data-plane {} requires an upgrade: its connector proxy does not support remote connector sessions",
                plane.data_plane_name
            );
        }

        // Return connector logs to the publication's job logs for the whole run.
        let log_handler = self.log_handler.clone();
        let log_task = tokio::spawn(async move {
            let mut log_stream = log_stream;
            while let Ok(Some(response)) = log_stream.try_next().await {
                if let Some(log) = response.log.as_ref() {
                    log_handler.log(log);
                }
            }
        });

        let proxy_rt = std::sync::Arc::new(ProxyRuntime {
            channel: channel.clone(),
            metadata: metadata.clone(),
            _cancel: cancel,
            _logs: log_task,
        });
        runtimes.insert(plane.control_id, proxy_rt);
        Ok((channel, metadata))
    }
}

const DIAL_PROXY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

fn dial_proxy_timeout_msg(data_plane: &tables::DataPlane) -> String {
    format!(
        "Timeout starting remote proxy for connector in data-plane {}",
        data_plane.data_plane_name
    )
}

static CONNECTOR_TIMEOUT: std::sync::LazyLock<std::time::Duration> =
    std::sync::LazyLock::new(|| {
        std::env::var("FLOW_CONNECTOR_TIMEOUT")
            .map(|timeout| {
                tracing::info!(%timeout, "using FLOW_CONNECTOR_TIMEOUT from env");
                humantime::parse_duration(&timeout).expect("invalid FLOW_CONNECTOR_TIMEOUT value")
            })
            .unwrap_or(std::time::Duration::from_secs(300)) // Five minutes.
    });
const CONNECTOR_TIMEOUT_MSG: &'static str = "Timeout while waiting for the connector's response. Please verify any network configuration and retry.";

#[cfg(test)]
mod test {
    use super::RemoteSessionConnectors;
    use runtime_harness::RemoteConnectors as _; // brings `dial_derive` into scope

    // A single SQLite derivation: enough to exercise the provider's mapping of
    // each derivation to its assigned data plane, without any connector image.
    const CATALOG: &str = r#"
collections:
  test/ints:
    schema: { type: object, properties: { Key: { type: string } }, required: [Key] }
    key: [/Key]
  test/sums:
    schema: { type: object, properties: { Key: { type: string } }, required: [Key] }
    key: [/Key]
    derive:
      using:
        sqlite: {}
      transforms:
        - name: fromInts
          source: { name: test/ints }
          shuffle: { key: [/Key] }
          lambda: "SELECT $Key AS Key;"
"#;

    async fn build_catalog() -> build::Output {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catalog.flow.yaml");
        std::fs::write(&path, CATALOG).unwrap();
        let url = build::arg_source_to_url(path.to_str().unwrap(), false).unwrap();
        build::for_local_test(&url, false)
            .await
            .into_result()
            .expect("catalog build should succeed")
    }

    /// With the assigned data plane present (`for_local_test` synthesizes one),
    /// the provider resolves each derivation to it. (Dialing / capability checks
    /// are exercised end-to-end against the in-process proxy in the runtime-next
    /// tests, and against a live data plane in the local-stack e2e.)
    #[tokio::test]
    async fn resolves_each_derivations_data_plane() {
        let catalog = build_catalog().await;
        let provider = RemoteSessionConnectors::new(&catalog, |_: &ops::Log| {})
            .expect("resolution should succeed");
        assert!(
            provider.task_planes.contains_key("test/sums"),
            "the SQLite derivation should map to its data plane"
        );
    }

    /// A derivation whose assigned data plane is absent from the build is a
    /// publication error (naming the derivation and its plane), caught before
    /// any run begins.
    #[tokio::test]
    async fn missing_data_plane_is_a_publication_error() {
        let mut catalog = build_catalog().await;
        catalog.live.data_planes = Default::default(); // Drop the synthesized plane.

        let result = RemoteSessionConnectors::new(&catalog, |_: &ops::Log| {});
        let Err(err) = result else {
            panic!("expected a missing-data-plane error");
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("test/sums") && msg.contains("missing from the build"),
            "unexpected error: {msg}"
        );
    }

    // A base64-encoded HMAC key (of "secret"); the in-process handshake mock
    // below does not verify the signed token, so any valid-format key that lets
    // `proxy_handshake` sign a JWT suffices.
    const HMAC_KEY_B64: &str = "c2VjcmV0";

    /// In-process `ConnectorProxy` handshake mock: replies to `ProxyConnectors`
    /// with one `(address, proxy_id, features)` response, then EOFs. It does not
    /// verify the token — we're exercising the client's handshake, capability
    /// check, and per-plane caching, not data-plane auth.
    struct MockProxyHandshake {
        address: String,
        features: u32,
    }

    #[tonic::async_trait]
    impl proto_grpc::runtime::connector_proxy_server::ConnectorProxy for MockProxyHandshake {
        type ProxyConnectorsStream = futures::stream::BoxStream<
            'static,
            Result<proto_flow::runtime::ConnectorProxyResponse, tonic::Status>,
        >;

        async fn proxy_connectors(
            &self,
            _request: tonic::Request<tonic::Streaming<proto_flow::runtime::ConnectorProxyRequest>>,
        ) -> Result<tonic::Response<Self::ProxyConnectorsStream>, tonic::Status> {
            use futures::StreamExt;
            let first = proto_flow::runtime::ConnectorProxyResponse {
                address: self.address.clone(),
                proxy_id: "test-proxy-1".to_string(),
                features: self.features,
                log: None,
            };
            let stream = futures::stream::once(async move { Ok(first) }).boxed();
            Ok(tonic::Response::new(stream))
        }
    }

    /// Host the mock handshake on an ephemeral loopback port; return a catalog
    /// with a data plane whose `reactor_address` points at it, plus the provider.
    async fn provider_against_mock(
        features: u32,
    ) -> RemoteSessionConnectors<impl runtime::LogHandler> {
        // gazette::dial_channel configures a rustls client; install a provider.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = futures::stream::unfold(listener, |listener| async move {
            Some((listener.accept().await.map(|(conn, _)| conn), listener))
        });
        let mock = MockProxyHandshake {
            // The connector-RPC address is dialed lazily and never connected in
            // these tests, so a placeholder is fine.
            address: "http://127.0.0.1:1".to_string(),
            features,
        };
        tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(
                    proto_grpc::runtime::connector_proxy_server::ConnectorProxyServer::new(mock),
                )
                .serve_with_incoming(incoming)
                .await;
        });

        let mut catalog = build_catalog().await;
        let plane_id = catalog
            .built
            .built_collections
            .iter()
            .find(|bc| bc.spec.as_ref().is_some_and(|s| s.derivation.is_some()))
            .expect("a derivation")
            .data_plane_id;
        let mut plane = fake_data_plane(plane_id);
        plane.reactor_address = format!("http://{addr}");
        plane.hmac_keys = vec![HMAC_KEY_B64.to_string()];
        catalog.live.data_planes.insert(plane);

        RemoteSessionConnectors::new(&catalog, |_: &ops::Log| {}).expect("provider")
    }

    fn fake_data_plane(control_id: models::Id) -> tables::DataPlane {
        tables::DataPlane {
            control_id,
            data_plane_name: "ops/dp/test".to_string(),
            data_plane_fqdn: "test.dp.estuary".to_string(),
            hmac_keys: vec![HMAC_KEY_B64.to_string()],
            encrypted_hmac_keys: models::RawValue::from_string("{}".to_string()).unwrap(),
            ops_logs_name: models::Collection::new("ops/logs"),
            ops_stats_name: models::Collection::new("ops/stats"),
            broker_address: "broker.test".to_string(),
            reactor_address: "reactor.test".to_string(),
            dekaf_address: None,
            dekaf_registry_address: None,
        }
    }

    /// A backend advertising `RAW_SESSIONS` is dialed successfully: the returned
    /// metadata carries the handshake's `proxy-id`, and a second dial for the
    /// same plane reuses the cached runtime (one proxy runtime per plane).
    #[tokio::test]
    async fn dials_and_caches_a_capable_backend() {
        let provider =
            provider_against_mock(proto_flow::runtime::ProxyFeature::RawSessions as u32).await;

        let (_c1, meta1) = provider.dial_derive("test/sums").await.expect("first dial");
        assert_eq!(
            meta1.0.get("proxy-id").map(|v| v.to_str().unwrap()),
            Some("test-proxy-1"),
            "metadata should carry the handshake proxy-id"
        );

        let (_c2, _meta2) = provider
            .dial_derive("test/sums")
            .await
            .expect("second dial");
        assert_eq!(
            provider.runtimes.lock().await.len(),
            1,
            "both dials should share one cached proxy runtime for the plane"
        );
    }

    /// A backend that does not advertise raw sessions (an old reactor) fails the
    /// dial loudly, before any connector session opens.
    #[tokio::test]
    async fn old_backend_without_raw_sessions_fails_loudly() {
        let provider = provider_against_mock(0).await;

        let result = provider.dial_derive("test/sums").await;
        let Err(err) = result else {
            panic!("expected a capability error");
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("requires an upgrade"),
            "unexpected error: {msg}"
        );
    }
}

/*

7 year old Abby sneaks in while Johnny's working on this code, surprising him enough to jump out of his chair.
We have a little talk about sneaking up on people ...

Abby's reading a library book "Lunch Lady: 2-for-1 special".
And she does a dance routine on Johnny's standing mat.
And Johnny adores Abby.
And Abby is missing her two front teeth.

Abby shakes daddy's arm, and it's kind of easy, like it's a wiggly worm.

FIN
*/
