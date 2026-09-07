use control_plane_api::connectors::{ConnectorFactory, Connectors};
use control_plane_api::proxy_connectors::DiscoverConnectors;
use futures::FutureExt;
use proto_flow::{capture, connector, derive, materialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub type MockDiscover = Result<(capture::response::Spec, capture::response::Discovered), String>;
pub type MockConnectorResponse =
    Result<(connector::response::Started, connector::response::Kind), String>;

#[derive(Clone, Debug, Default)]
pub struct MockConnectors {
    discover_mocks: Arc<Mutex<HashMap<models::Capture, MockConnectorResponse>>>,
    discover_requests: Arc<Mutex<HashMap<models::Capture, capture::request::Discover>>>,
}

impl MockConnectors {
    pub fn mock_discover(&self, capture_name: &str, respond: MockDiscover) {
        self.mock_raw_discover(
            capture_name,
            respond.map(|(spec, discovered)| {
                (
                    connector::response::Started {
                        spec: Some(connector::response::started::Spec::Capture(Box::new(spec))),
                        ..Default::default()
                    },
                    connector::response::Kind::Capture(capture::Response {
                        kind: Some(capture::response::Kind::Discovered(discovered)),
                        ..Default::default()
                    }),
                )
            }),
        );
    }

    pub fn mock_raw_discover(&self, capture_name: &str, respond: MockConnectorResponse) {
        self.discover_mocks
            .lock()
            .unwrap()
            .insert(models::Capture::new(capture_name), respond);
    }

    /// Returns the most recent Discover request received for the given
    /// capture, so that tests can assert on request fields.
    pub fn last_discover_request(&self, capture_name: &str) -> Option<capture::request::Discover> {
        self.discover_requests
            .lock()
            .unwrap()
            .get(&models::Capture::new(capture_name))
            .cloned()
    }

    fn connect(
        &self,
        request: connector::Request,
    ) -> futures::future::BoxFuture<
        '_,
        anyhow::Result<(connector::response::Started, connector::response::Kind)>,
    > {
        async move {
            match request.kind {
                Some(connector::request::Kind::Capture(capture::Request {
                    kind: Some(capture::request::Kind::Discover(discover)),
                    ..
                })) => self.discover(*discover),
                Some(connector::request::Kind::Capture(capture::Request {
                    kind: Some(capture::request::Kind::Validate(validate)),
                    ..
                })) => Ok(validate_capture(*validate)),
                Some(connector::request::Kind::Derive(derive::Request {
                    kind: Some(derive::request::Kind::Validate(validate)),
                    ..
                })) => Ok(validate_derivation(*validate)),
                Some(connector::request::Kind::Materialize(materialize::Request {
                    kind: Some(materialize::request::Kind::Validate(validate)),
                    ..
                })) => Ok(validate_materialization(*validate)),
                _ => anyhow::bail!("unexpected connector request: {request:?}"),
            }
        }
        .boxed()
    }

    fn discover(
        &self,
        discover: capture::request::Discover,
    ) -> anyhow::Result<(connector::response::Started, connector::response::Kind)> {
        let capture_name = models::Capture::new(&discover.name);
        self.discover_requests
            .lock()
            .unwrap()
            .insert(capture_name.clone(), discover.clone());

        let response = self
            .discover_mocks
            .lock()
            .unwrap()
            .get(&capture_name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("no mock for capture: {capture_name}"))?;

        tracing::debug!(req = ?discover, resp = ?response, "responding with mock connector response");
        response.map_err(|err| anyhow::anyhow!(err))
    }
}

impl ConnectorFactory for MockConnectors {
    fn make_connectors<'a>(
        &'a self,
        _log_task: &'static str,
        _logs_token: uuid::Uuid,
    ) -> Box<Connectors<'a>> {
        Box::new(move |_data_plane, request| self.connect(request))
    }
}

impl DiscoverConnectors for MockConnectors {
    async fn discover<'a>(
        &'a self,
        _data_plane: &'a tables::DataPlane,
        _task: &'a models::Capture,
        _logs_token: uuid::Uuid,
        request: capture::Request,
    ) -> anyhow::Result<(capture::response::Spec, capture::response::Discovered)> {
        let Some(capture::request::Kind::Discover(discover)) = request.kind else {
            anyhow::bail!("unexpected capture request type: {request:?}")
        };
        let (started, response) = self.discover(*discover)?;

        match (started.spec, response) {
            (
                Some(connector::response::started::Spec::Capture(spec)),
                connector::response::Kind::Capture(capture::Response {
                    kind: Some(capture::response::Kind::Discovered(discovered)),
                    ..
                }),
            ) => Ok((*spec, discovered)),
            _ => anyhow::bail!("mock connector did not return capture Discovered"),
        }
    }
}

fn validate_capture(
    validate: capture::request::Validate,
) -> (connector::response::Started, connector::response::Kind) {
    let spec = capture::response::Spec {
        protocol: 3032023,
        config_schema_json: r#"{"type": "object", "properties": {}}"#.into(),
        resource_config_schema_json: r#"true"#.into(),
        documentation_url: "http://test.test/test-docs".to_string(),
        oauth2: None,
        resource_path_pointers: vec!["/id".to_string()],
    };
    let bindings = validate
        .bindings
        .iter()
        .map(|binding| {
            let resource_config = serde_json::from_slice(&binding.resource_config_json).unwrap();
            let resource_path = mock_resource_path(&resource_config);
            capture::response::validated::Binding { resource_path }
        })
        .collect();

    (
        connector::response::Started {
            spec: Some(connector::response::started::Spec::Capture(Box::new(spec))),
            ..Default::default()
        },
        connector::response::Kind::Capture(capture::Response {
            kind: Some(capture::response::Kind::Validated(
                capture::response::Validated { bindings },
            )),
            ..Default::default()
        }),
    )
}

fn validate_derivation(
    validate: derive::request::Validate,
) -> (connector::response::Started, connector::response::Kind) {
    let spec = derive::response::Spec {
        protocol: 3032023,
        config_schema_json: "{}".into(),
        resource_config_schema_json: "{}".into(),
        documentation_url: "http://test.test/test-docs".to_string(),
        oauth2: None,
    };
    let transforms = validate
        .transforms
        .iter()
        .map(|_| derive::response::validated::Transform { read_only: false })
        .collect();

    (
        connector::response::Started {
            spec: Some(connector::response::started::Spec::Derive(Box::new(spec))),
            ..Default::default()
        },
        connector::response::Kind::Derive(derive::Response {
            kind: Some(derive::response::Kind::Validated(
                derive::response::Validated {
                    transforms,
                    generated_files: Default::default(),
                },
            )),
            ..Default::default()
        }),
    )
}

fn validate_materialization(
    validate: materialize::request::Validate,
) -> (connector::response::Started, connector::response::Kind) {
    let spec = materialize::response::Spec {
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
    };
    let bindings = validate
        .resolved_bindings()
        .map(|(binding, resolved)| {
            let (collection, _identity) = resolved.unwrap();
            let resource_config: serde_json::Value =
                serde_json::from_slice(&binding.resource_config_json).unwrap();
            let delta_updates = resource_config
                .get("deltaUpdates")
                .and_then(|d| d.as_bool())
                .unwrap_or(false);
            let projection_constraints = collection
                .projections
                .iter()
                .map(|p| materialize::response::validated::ProjectionConstraint {
                    field: p.field.clone(),
                    constraint: Some(materialize::response::validated::Constraint {
                        r#type: 3,
                        reason: "all fields are recommended in tests".to_string(),
                        folded_field: String::new(),
                    }),
                })
                .collect();
            let resource_path = mock_resource_path(&resource_config);
            materialize::response::validated::Binding {
                case_insensitive_fields: false,
                projection_constraints,
                resource_path,
                delta_updates,
                ser_policy: None,
            }
        })
        .collect();

    (
        connector::response::Started {
            spec: Some(connector::response::started::Spec::Materialize(Box::new(
                spec,
            ))),
            ..Default::default()
        },
        connector::response::Kind::Materialize(materialize::Response {
            kind: Some(materialize::response::Kind::Validated(
                materialize::response::Validated { bindings },
            )),
            ..Default::default()
        }),
    )
}

fn mock_resource_path(resource_config: &serde_json::Value) -> Vec<String> {
    ["id", "table", "name"]
        .iter()
        .flat_map(|key| resource_config.get(*key).and_then(|v| v.as_str()))
        .map(|v| v.to_owned())
        .collect()
}
