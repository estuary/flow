use crate::proxy_connectors::{DiscoverConnectors, MakeConnectors};
use futures::stream::StreamExt;
use proto_flow::capture;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use validation::Connectors;

pub type MockDiscover = Result<(capture::response::Spec, capture::response::Discovered), String>;

#[derive(Debug, Clone)]
pub struct MockDiscoverConnectors {
    discover_mocks: Arc<Mutex<HashMap<models::Capture, MockDiscover>>>,
}

impl Default for MockDiscoverConnectors {
    fn default() -> Self {
        MockDiscoverConnectors {
            discover_mocks: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl MockDiscoverConnectors {
    pub fn mock_discover(&mut self, capture_name: &str, respond: MockDiscover) {
        let mut lock = self.discover_mocks.lock().unwrap();
        lock.insert(models::Capture::new(capture_name), respond);
    }
}

impl DiscoverConnectors for MockDiscoverConnectors {
    async fn discover<'a>(
        &'a self,
        _data_plane: &'a tables::DataPlane,
        task: &'a models::Capture,
        _logs_token: uuid::Uuid,
        mut request: capture::Request,
    ) -> anyhow::Result<(capture::response::Spec, capture::response::Discovered)> {
        let Some(discover) = request.discover.take() else {
            anyhow::bail!("unexpected capture request type: {request:?}")
        };

        let locked = self.discover_mocks.lock().unwrap();
        let Some(mock) = locked.get(task) else {
            anyhow::bail!("no mock for capture: {task}");
        };

        tracing::debug!(req = ?discover, resp = ?mock, "responding with mock discovered response");
        mock.clone().map_err(|err_str| anyhow::anyhow!("{err_str}"))
    }
}

impl MakeConnectors for MockDiscoverConnectors {
    type Connectors = TestConnectors;
    fn make_connectors(&self, _logs_token: uuid::Uuid) -> Self::Connectors {
        TestConnectors
    }
}

#[derive(Debug, Clone)]
pub struct TestConnectors;

impl Connectors for TestConnectors {
    fn capture<'a, R>(
        &'a self,
        _data_plane: &'a tables::DataPlane,
        _task: &'a models::Capture,
        mut request_rx: R,
    ) -> impl futures::Stream<Item = anyhow::Result<proto_flow::capture::Response>> + Send + 'a
    where
        R: futures::Stream<Item = proto_flow::capture::Request> + Send + Unpin + 'static,
    {
        use proto_flow::capture::{
            response::{self, Validated},
            Response,
        };

        coroutines::try_coroutine(|mut co| async move {
            while let Some(request) = request_rx.next().await {
                if request.spec.is_some() {
                    let response = Response {
                        spec: Some(response::Spec {
                            protocol: 3032023,
                            config_schema_json: r#"{"type": "object", "properties": {}}"#.into(),
                            resource_config_schema_json: r#"true"#.into(),
                            documentation_url: "http://test.test/test-docs".to_string(),
                            oauth2: None,
                            resource_path_pointers: vec!["/id".to_string()],
                        }),
                        ..Default::default()
                    };
                    co.yield_(response).await;
                } else if let Some(validate) = request.validate.as_ref() {
                    let bindings = validate
                        .bindings
                        .iter()
                        .map(|binding| {
                            let resource_config =
                                serde_json::from_slice(&binding.resource_config_json).unwrap();
                            let resource_path = mock_resource_path(&resource_config);
                            response::validated::Binding { resource_path }
                        })
                        .collect();
                    let response = Response {
                        validated: Some(Validated { bindings }),
                        ..Default::default()
                    };
                    co.yield_(response).await;
                } else {
                    anyhow::bail!("unexpected request: {request:?}");
                }
            }
            Ok(())
        })
    }

    fn derive<'a, R>(
        &'a self,
        _data_plane: &'a tables::DataPlane,
        _task: &'a models::Collection,
        mut request_rx: R,
    ) -> impl futures::Stream<Item = anyhow::Result<proto_flow::derive::Response>> + Send + 'a
    where
        R: futures::Stream<Item = proto_flow::derive::Request> + Send + Unpin + 'static,
    {
        use proto_flow::derive::{
            response::{self, Spec, Validated},
            Response,
        };
        coroutines::try_coroutine(|mut co| async move {
            while let Some(request) = request_rx.next().await {
                if request.spec.is_some() {
                    let response = Response {
                        spec: Some(Spec {
                            protocol: 3032023,
                            config_schema_json: "{}".into(),
                            resource_config_schema_json: "{}".into(),
                            documentation_url: "http://test.test/test-docs".to_string(),
                            oauth2: None,
                        }),
                        ..Default::default()
                    };
                    () = co.yield_(response).await;
                } else if let Some(validate) = request.validate.as_ref() {
                    let transforms = validate
                        .transforms
                        .iter()
                        .map(|_| response::validated::Transform { read_only: false })
                        .collect();
                    let response = Response {
                        validated: Some(Validated {
                            transforms,
                            generated_files: Default::default(),
                        }),
                        ..Default::default()
                    };
                    () = co.yield_(response).await;
                } else {
                    anyhow::bail!("unexpected derive request: {request:?}");
                }
            }
            Ok(())
        })
    }

    fn materialize<'a, R>(
        &'a self,
        _data_plane: &'a tables::DataPlane,
        _task: &'a models::Materialization,
        mut request_rx: R,
    ) -> impl futures::Stream<Item = anyhow::Result<proto_flow::materialize::Response>> + Send + 'a
    where
        R: futures::Stream<Item = proto_flow::materialize::Request> + Send + Unpin + 'static,
    {
        use proto_flow::materialize;
        coroutines::try_coroutine(|mut co| async move {
            while let Some(request) = request_rx.next().await {
                if let Some(_spec) = request.spec {
                    () = co
                        .yield_(materialize::Response {
                            spec: Some(materialize::response::Spec {
                                config_schema_json: "true".into(),
                                resource_config_schema_json: r#"{
                                    "type": "object",
                                    "properties": {
                                      "table": {
                                        "type": "string",
                                        "x-collection-name": true
                                      },
                                      "deltaUpdates": {
                                        "type": "boolean",
                                        "x-delta-updates": true
                                      }
                                    }
                                }"#
                                .into(),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .await;
                    continue;
                } else if let Some(validate) = request.validate {
                    let v_bindings = validate
                        .bindings
                        .iter()
                        .map(|binding| {
                            let collection = binding.collection.as_ref().unwrap();
                            let resource_config: serde_json::Value =
                                serde_json::from_slice(&binding.resource_config_json).unwrap();
                            let delta_updates = resource_config
                                .get("deltaUpdates")
                                .and_then(|d| d.as_bool())
                                .unwrap_or(false);
                            let constraints = collection
                                .projections
                                .iter()
                                .map(|p| {
                                    (
                                        p.field.clone(),
                                        materialize::response::validated::Constraint {
                                            r#type: 3,
                                            reason: "all fields are recommended in tests"
                                                .to_string(),
                                            folded_field: String::new(),
                                        },
                                    )
                                })
                                .collect();
                            let resource_path = mock_resource_path(&resource_config);
                            materialize::response::validated::Binding {
                                case_insensitive_fields: false,
                                constraints,
                                resource_path,
                                delta_updates,
                                ser_policy: None,
                            }
                        })
                        .collect();
                    () = co
                        .yield_(materialize::Response {
                            validated: Some(materialize::response::Validated {
                                bindings: v_bindings,
                            }),
                            ..Default::default()
                        })
                        .await;
                } else {
                    anyhow::bail!("expected Spec or Validate")
                }
            }
            Ok(())
        })
    }
}

fn mock_resource_path(resource_config: &serde_json::Value) -> Vec<String> {
    ["id", "table", "name"]
        .iter()
        .flat_map(|key| resource_config.get(*key).and_then(|v| v.as_str()))
        .map(|v| v.to_owned())
        .collect()
}
