use anyhow::Context;
use futures::{FutureExt, TryFutureExt, TryStreamExt};
use proto_flow::{capture, derive, flow::materialization_spec, materialize};
use std::future::Future;

pub struct ProxyConnectors<L: runtime::LogHandler> {
    log_handler: L,
}

impl<L: runtime::LogHandler> validation::Connectors for ProxyConnectors<L> {
    fn validate_capture<'a>(
        &'a self,
        request: proto_flow::capture::Request,
        data_plane: &'a tables::DataPlane,
    ) -> futures::future::BoxFuture<'a, anyhow::Result<capture::Response>> {
        let task = ops::ShardRef {
            name: request.validate.as_ref().unwrap().name.clone(),
            kind: ops::TaskType::Capture as i32,
            ..Default::default()
        };
        self.unary_capture(data_plane, task, request).boxed()
    }

    fn validate_derivation<'a>(
        &'a self,
        request: derive::Request,
        data_plane: &'a tables::DataPlane,
    ) -> futures::future::BoxFuture<'a, anyhow::Result<derive::Response>> {
        let collection = &request.validate.as_ref().unwrap().collection;
        let task = ops::ShardRef {
            name: collection.as_ref().unwrap().name.clone(),
            kind: ops::TaskType::Derivation as i32,
            ..Default::default()
        };
        self.unary_derive(data_plane, task, request).boxed()
    }

    fn validate_materialization<'a>(
        &'a self,
        request: materialize::Request,
        data_plane: &'a tables::DataPlane,
    ) -> futures::future::BoxFuture<'a, anyhow::Result<materialize::Response>> {
        match materialization_spec::ConnectorType::try_from(
            request.validate.as_ref().unwrap().connector_type,
        ) {
            Ok(materialization_spec::ConnectorType::Dekaf) => {
                dekaf::connector::unary_materialize(request).boxed()
            }
            _ => {
                let task = ops::ShardRef {
                    name: request.validate.as_ref().unwrap().name.clone(),
                    kind: ops::TaskType::Materialization as i32,
                    ..Default::default()
                };
                self.unary_materialize(data_plane, task, request).boxed()
            }
        }
    }
}

impl<L: runtime::LogHandler> ProxyConnectors<L> {
    pub(crate) fn new(log_handler: L) -> Self {
        Self { log_handler }
    }

    #[tracing::instrument(
        skip(self, data_plane, request),
        fields(data_plane_fqdn = %data_plane.data_plane_fqdn)
    )]
    pub(crate) async fn unary_capture(
        &self,
        data_plane: &tables::DataPlane,
        task: ops::ShardRef,
        request: capture::Request,
    ) -> anyhow::Result<capture::Response> {
        let (channel, metadata, logs) = crate::timeout(
            DIAL_PROXY_TIMEOUT,
            self.dial_proxy(data_plane, task),
            || dial_proxy_timeout_msg(data_plane),
        )
        .await?;

        let mut client = proto_grpc::capture::connector_client::ConnectorClient::with_interceptor(
            channel, metadata,
        )
        .max_decoding_message_size(runtime::MAX_MESSAGE_SIZE);

        crate::timeout(
            CONNECTOR_TIMEOUT,
            Self::drive_unary_response(
                client.capture(futures::stream::once(async move { request })),
                logs,
            ),
            || CONNECTOR_TIMEOUT_MSG,
        )
        .await
    }

    #[tracing::instrument(
        skip(self, data_plane, request),
        fields(data_plane_fqdn = %data_plane.data_plane_fqdn)
    )]
    pub(crate) async fn unary_derive(
        &self,
        data_plane: &tables::DataPlane,
        task: ops::ShardRef,
        request: derive::Request,
    ) -> anyhow::Result<derive::Response> {
        let (channel, metadata, logs) = crate::timeout(
            DIAL_PROXY_TIMEOUT,
            self.dial_proxy(data_plane, task),
            || dial_proxy_timeout_msg(data_plane),
        )
        .await?;

        let mut client = proto_grpc::derive::connector_client::ConnectorClient::with_interceptor(
            channel, metadata,
        )
        .max_decoding_message_size(runtime::MAX_MESSAGE_SIZE);

        crate::timeout(
            CONNECTOR_TIMEOUT,
            Self::drive_unary_response(
                client.derive(futures::stream::once(async move { request })),
                logs,
            ),
            || CONNECTOR_TIMEOUT_MSG,
        )
        .await
    }

    #[tracing::instrument(
        skip(self, data_plane, request),
        fields(data_plane_fqdn = %data_plane.data_plane_fqdn)
    )]
    pub(crate) async fn unary_materialize(
        &self,
        data_plane: &tables::DataPlane,
        task: ops::ShardRef,
        request: materialize::Request,
    ) -> anyhow::Result<materialize::Response> {
        let (channel, metadata, logs) = crate::timeout(
            DIAL_PROXY_TIMEOUT,
            self.dial_proxy(data_plane, task),
            || dial_proxy_timeout_msg(data_plane),
        )
        .await?;

        let mut client =
            proto_grpc::materialize::connector_client::ConnectorClient::with_interceptor(
                channel, metadata,
            )
            .max_decoding_message_size(runtime::MAX_MESSAGE_SIZE);

        crate::timeout(
            CONNECTOR_TIMEOUT,
            Self::drive_unary_response(
                client.materialize(futures::stream::once(async move { request })),
                logs,
            ),
            || CONNECTOR_TIMEOUT_MSG,
        )
        .await
    }

    async fn dial_proxy<'a>(
        &'a self,
        data_plane: &tables::DataPlane,
        task: ops::ShardRef,
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
                CONNECTOR_TIMEOUT * 2,
                hmac_keys,
                Default::default(),
                &task.name,
            )
            .context("failed to sign claims for connector proxy")?;

        // Start an RPC against the base reactor service to start a connector proxy.
        // Use a request stream which blocks until cancelled, and then sends EOF.
        let (cancel_tx, cancel_rx) = futures::channel::oneshot::channel::<()>();

        let mut proxy_client =
            proto_grpc::runtime::connector_proxy_client::ConnectorProxyClient::with_interceptor(
                gazette::dial_channel(reactor_address).await?,
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
            gazette::dial_channel(&address).await?,
            metadata,
            (cancel_tx, log_loop),
        ))
    }

    async fn drive_unary_response<Response>(
        response: impl Future<Output = tonic::Result<tonic::Response<tonic::Streaming<Response>>>>,
        (cancel_tx, log_loop): (
            futures::channel::oneshot::Sender<()>,
            impl Future<Output = anyhow::Result<()>>,
        ),
    ) -> anyhow::Result<Response> {
        let response = async move {
            // Drop on exit to gracefully stop the proxy runtime and finish reading logs.
            let _cancel_tx = cancel_tx;

            let mut responses: Vec<Response> = response.await?.into_inner().try_collect().await?;
            if responses.len() != 1 {
                return Err(tonic::Status::internal(format!(
                    "Expected connector to return a single response, but got {}",
                    responses.len()
                )));
            }
            Ok(responses.pop().unwrap())
        }
        .map_err(runtime::status_to_anyhow);

        futures::try_join!(response, log_loop).map(|(response, ())| response)
    }
}

const DIAL_PROXY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

fn dial_proxy_timeout_msg(data_plane: &tables::DataPlane) -> String {
    format!(
        "Timeout starting remote proxy for connector in data-plane {}",
        data_plane.data_plane_name
    )
}

const CONNECTOR_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300); // Five minutes.
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
