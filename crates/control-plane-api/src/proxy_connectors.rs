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
            exp: (iat + (*CONNECTOR_TIMEOUT * 2)).timestamp() as u64,
            iat: iat.timestamp() as u64,
            iss: data_plane_fqdn.clone(),
            sel: Default::default(),
            sub: task.to_string(),
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
            address, proxy_id, ..
        }) = proxy_responses.try_next().await?
        else {
            anyhow::bail!("unexpected EOF starting connector proxy");
        };

        // `proxy-id` is attached to RPCs issued to this proxy.
        metadata.0.insert("proxy-id", proxy_id.parse()?);

        tracing::debug!(address, proxy_id, task=?ops::DebugJson(&task), "started proxy runtime");

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

        Ok((
            gazette::dial_channel(&address)?,
            metadata,
            (cancel_tx, log_loop),
        ))
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
