use crate::proxy_connectors::DiscoverConnectors;
use proto_flow::capture;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub type MockDiscover = Result<capture::response::Discovered, String>;

#[derive(Clone)]
pub struct MockDiscoverConnectors {
    mocks: Arc<Mutex<HashMap<models::Capture, MockDiscover>>>,
}

impl Default for MockDiscoverConnectors {
    fn default() -> Self {
        MockDiscoverConnectors {
            mocks: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl MockDiscoverConnectors {
    pub fn mock_discover(&mut self, capture_name: &str, respond: MockDiscover) {
        let mut lock = self.mocks.lock().unwrap();
        lock.insert(models::Capture::new(capture_name), respond);
    }
}

impl DiscoverConnectors for MockDiscoverConnectors {
    async fn discover<'a>(
        &'a self,
        mut req: capture::Request,
        _logs_token: uuid::Uuid,
        task: ops::ShardRef,
        _data_plane: &'a tables::DataPlane,
    ) -> anyhow::Result<capture::Response> {
        let Some(discover) = req.discover.take() else {
            anyhow::bail!("unexpected capture request type: {req:?}")
        };

        let locked = self.mocks.lock().unwrap();
        let capture = models::Capture::new(&task.name);
        let Some(mock) = locked.get(&capture) else {
            anyhow::bail!("no mock for capture: {capture}");
        };

        tracing::debug!(req = ?discover, resp = ?mock, "responding with mock discovered response");
        mock.clone()
            .map_err(|err_str| anyhow::anyhow!("{err_str}"))
            .map(|dr| capture::Response {
                discovered: Some(dr),
                ..Default::default()
            })
    }
}
