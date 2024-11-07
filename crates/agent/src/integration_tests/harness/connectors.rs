use crate::proxy_connectors::Connectors;
use proto_flow::capture;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

pub trait MockCall<Req, Resp>: Send + Sync + 'static {
    fn call(
        &self,
        req: Req,
        logs_token: uuid::Uuid,
        task: ops::ShardRef,
        data_plane: &tables::DataPlane,
    ) -> anyhow::Result<Resp>;
}

impl<Req, Resp> MockCall<Req, Resp> for Result<Resp, String>
where
    Resp: Clone + Send + Sync + 'static,
{
    fn call(
        &self,
        _req: Req,
        _logs_token: uuid::Uuid,
        _task: ops::ShardRef,
        _data_plane: &tables::DataPlane,
    ) -> anyhow::Result<Resp> {
        self.clone().map_err(anyhow::Error::msg)
    }
}

struct DefaultFail;
impl<Req, Resp> MockCall<Req, Resp> for DefaultFail
where
    Req: Debug,
{
    fn call(
        &self,
        req: Req,
        _logs_token: uuid::Uuid,
        _task: ops::ShardRef,
        _data_plane: &tables::DataPlane,
    ) -> anyhow::Result<Resp> {
        Err(anyhow::anyhow!("default mock failure for request: {req:?}"))
    }
}

pub type MockDiscover =
    Box<dyn MockCall<capture::request::Discover, capture::response::Discovered>>;

#[derive(Clone)]
pub struct MockConnectors {
    discover: Arc<Mutex<MockDiscover>>,
}

impl Default for MockConnectors {
    fn default() -> Self {
        MockConnectors {
            discover: Arc::new(Mutex::new(Box::new(DefaultFail))),
        }
    }
}

impl MockConnectors {
    pub fn mock_discover(&mut self, respond: MockDiscover) {
        let mut lock = self.discover.lock().unwrap();
        *lock = respond;
    }
}

/// Currently, `MockConnectors` only supports capture Discover RPCs.
/// Publications do not yet use this for validate RPCs, but the plan is to do
/// that at some point, so that we can more easily test the publication logic.
impl Connectors for MockConnectors {
    async fn unary_capture<'a>(
        &'a self,
        mut req: capture::Request,
        logs_token: uuid::Uuid,
        task: ops::ShardRef,
        data_plane: &'a tables::DataPlane,
    ) -> anyhow::Result<capture::Response> {
        if let Some(discover) = req.discover.take() {
            let locked = self.discover.lock().unwrap();
            return locked
                .call(discover, logs_token, task, data_plane)
                .map(|resp| capture::Response {
                    discovered: Some(resp),
                    ..Default::default()
                });
        }
        Err(anyhow::anyhow!("unhandled capture request type: {req:?}"))
    }

    async fn unary_derive<'a>(
        &'a self,
        _req: proto_flow::derive::Request,
        _logs_token: uuid::Uuid,
        _task: ops::ShardRef,
        _data_plane: &'a tables::DataPlane,
    ) -> anyhow::Result<proto_flow::derive::Response> {
        unimplemented!("mock connectors do not yet handle unary_derive calls");
    }

    async fn unary_materialize<'a>(
        &'a self,
        _req: proto_flow::materialize::Request,
        _logs_token: uuid::Uuid,
        _task: ops::ShardRef,
        _data_plane: &'a tables::DataPlane,
    ) -> anyhow::Result<proto_flow::materialize::Response> {
        unimplemented!("mock connectors do not yet handle unary_materialize calls");
    }
}
