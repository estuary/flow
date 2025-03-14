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
    ) -> impl Future<Output = anyhow::Result<(capture::response::Spec, capture::response::Discovered)>>
           + 'a
           + Send;
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
        gazette::Metadata,
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

        let mut metadata = gazette::Metadata::default();

        metadata
            .signed_claims(
                proto_flow::capability::PROXY_CONNECTOR,
                data_plane_fqdn,
                *CONNECTOR_TIMEOUT * 2,
                hmac_keys,
                Default::default(),
                task,
            )
            .context("failed to sign claims for connector proxy")?;

        // Start an RPC against the base reactor service to start a connector proxy.
        // Use a request stream which blocks until cancelled, and then sends EOF.
        let (cancel_tx, cancel_rx) = futures::channel::oneshot::channel::<()>();

        let mut proxy_client =
            proto_grpc::runtime::connector_proxy_client::ConnectorProxyClient::with_interceptor(
                gazette::dial_channel(reactor_address)?,
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
        metadata.insert("proxy-id", proxy_id.parse()?);

        tracing::debug!(address, proxy_id, task=?ops::DebugJson(&task), "started proxy runtime");

        let log_loop = async move {
            while let Some(log) = proxy_responses
                .try_next()
                .await
                .context("failed to read proxy response stream")?
            {
                if let Some(log) = log.log.as_ref() {
                    (self.log_handler)(log);
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
        let mut response_rx = response.map_err(runtime::status_to_anyhow)?.into_inner();

        let response_loop = async move {
            // Drop on EOF to gracefully stop the proxy runtime and finish reading logs.
            let _guard = cancel_tx;

            while let Some(response) = crate::timeout(
                *CONNECTOR_TIMEOUT,
                response_rx.try_next().map_err(runtime::status_to_anyhow),
                || CONNECTOR_TIMEOUT_MSG,
            )
            .await?
            {
                () = co.yield_(response).await;
            }
            Ok(())
        };

        ((), ()) = futures::try_join!(response_loop, log_loop)?;
        Ok(())
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
