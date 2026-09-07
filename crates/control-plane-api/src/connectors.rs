use crate::logs;
use futures::FutureExt;
use proto_flow::connector;
use runtime::LogHandler;
use uuid::Uuid;

pub use validation::Connectors;

/// Creates operation-scoped connector callables bound to control-plane resources.
pub trait ConnectorFactory: std::fmt::Debug + Sync + Send + 'static {
    fn make_connectors<'a>(
        &'a self,
        log_task: &'static str,
        logs_token: Uuid,
    ) -> Box<Connectors<'a>>;
}

/// The control-plane connector factory, backed by the platform log sink.
#[derive(Debug)]
pub struct ControlPlaneConnectorFactory {
    logs_tx: logs::Tx,
}

impl ControlPlaneConnectorFactory {
    pub fn new(logs_tx: logs::Tx) -> Self {
        Self { logs_tx }
    }

    fn connect(
        &self,
        log_task: &'static str,
        logs_token: Uuid,
        data_plane: &tables::DataPlane,
        request: connector::Request,
    ) -> futures::future::BoxFuture<
        'static,
        anyhow::Result<(connector::response::Started, connector::response::Kind)>,
    > {
        let route = crate::connector_router::data_plane_route(data_plane);
        let data_plane_name = data_plane.data_plane_name.clone();
        let log_handler = logs::ops_handler(self.logs_tx.clone(), log_task.to_string(), logs_token);

        async move {
            let route = route?;
            let logger = move |log: &proto_flow::ops::Log| log_handler.log(log);

            proto_grpc::connector::unary(
                &route,
                &logger,
                request,
                *CONNECTOR_TIMEOUT,
                *CONNECTOR_TIMEOUT,
            )
            .await
            .map_err(|err| {
                if err.downcast_ref::<tokio::time::error::Elapsed>().is_none() {
                    return err;
                }
                err.context(format!(
                    "timeout awaiting connector in data-plane {data_plane_name}"
                ))
            })
        }
        .boxed()
    }
}

impl ConnectorFactory for ControlPlaneConnectorFactory {
    fn make_connectors<'a>(
        &'a self,
        log_task: &'static str,
        logs_token: Uuid,
    ) -> Box<Connectors<'a>> {
        Box::new(move |data_plane, request| self.connect(log_task, logs_token, data_plane, request))
    }
}

static CONNECTOR_TIMEOUT: std::sync::LazyLock<std::time::Duration> =
    std::sync::LazyLock::new(|| {
        std::env::var("FLOW_CONNECTOR_TIMEOUT")
            .map(|timeout| {
                tracing::info!(%timeout, "using FLOW_CONNECTOR_TIMEOUT from env");
                humantime::parse_duration(&timeout).expect("invalid FLOW_CONNECTOR_TIMEOUT value")
            })
            .unwrap_or(std::time::Duration::from_secs(300))
    });
