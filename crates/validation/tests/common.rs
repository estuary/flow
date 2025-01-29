use futures::{future::BoxFuture, FutureExt};
use proto_flow::{capture, derive, flow, materialize, runtime::Container};
use std::collections::BTreeMap;

/// Outcome is a snapshot-able test outcome.
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct Outcome {
    pub built_captures: tables::BuiltCaptures,
    pub built_collections: tables::BuiltCollections,
    pub built_materializations: tables::BuiltMaterializations,
    pub built_tests: tables::BuiltTests,
    pub captures: tables::DraftCaptures,
    pub collections: tables::DraftCollections,
    pub errors: tables::Errors,
    pub errors_draft: tables::Errors,
    pub fetches: tables::Fetches,
    pub imports: tables::Imports,
    pub materializations: tables::DraftMaterializations,
    pub resources: tables::Resources,
    pub storage_mappings: tables::StorageMappings,
    pub tests: tables::DraftTests,
}

impl Outcome {
    #[allow(dead_code)]
    pub fn as_tables(&self) -> Vec<&dyn tables::SqlTableObj> {
        let Self {
            built_captures,
            built_collections,
            built_materializations,
            built_tests,
            captures,
            collections,
            errors,
            errors_draft,
            fetches,
            imports,
            materializations,
            resources,
            storage_mappings,
            tests,
        } = self;

        vec![
            built_captures,
            built_collections,
            built_materializations,
            built_tests,
            captures,
            collections,
            errors,
            errors_draft,
            fetches,
            imports,
            materializations,
            resources,
            storage_mappings,
            tests,
        ]
    }

    #[allow(dead_code)]
    pub fn as_tables_mut(&mut self) -> Vec<&mut dyn tables::SqlTableObj> {
        let Self {
            built_captures,
            built_collections,
            built_materializations,
            built_tests,
            captures,
            collections,
            errors,
            errors_draft,
            fetches,
            imports,
            materializations,
            resources,
            storage_mappings,
            tests,
        } = self;

        vec![
            built_captures,
            built_collections,
            built_materializations,
            built_tests,
            captures,
            collections,
            errors,
            errors_draft,
            fetches,
            imports,
            materializations,
            resources,
            storage_mappings,
            tests,
        ]
    }
}

pub fn run(fixture_yaml: &str, patch_yaml: &str) -> Outcome {
    let mut fixture: serde_json::Value = serde_yaml::from_str(fixture_yaml).unwrap();
    let patch: serde_json::Value = serde_yaml::from_str(patch_yaml).unwrap();

    () = json_patch::merge(&mut fixture, &patch);

    // Extract out driver mock call fixtures.
    let mock_calls: MockDriverCalls = fixture
        .get_mut("driver")
        .map(|d| serde_json::from_value(d.take()).unwrap())
        .unwrap_or_default();

    let mut draft = sources::scenarios::evaluate_fixtures(Default::default(), &fixture);
    sources::inline_draft_catalog(&mut draft);

    let mut live = tables::LiveCatalog::default();

    let live_connector_fixture = models::ConnectorConfig {
        image: "live/image".to_string(),
        config: models::RawValue::from_str("{\"live\":\"config\"}").unwrap(),
    };

    for (control_id, mock) in &mock_calls.data_planes {
        live.data_planes.insert_row(
            control_id,
            "ops/dp/public/test".to_string(),
            "the-data-plane.dp.estuary-data.com".to_string(),
            mock.default,
            vec!["hmac-key".to_string()],
            models::Collection::new("ops/logs"),
            models::Collection::new("ops/stats"),
            "broker:address".to_string(),
            "reactor:address".to_string(),
        );
    }

    // Load into LiveCatalog::live_captures.
    for (capture, mock) in &mock_calls.live_captures {
        let model = models::CaptureDef {
            auto_discover: None,
            bindings: Vec::new(),
            endpoint: models::CaptureEndpoint::Connector(live_connector_fixture.clone()),
            expect_pub_id: None,
            interval: std::time::Duration::from_secs(32),
            shards: models::ShardTemplate::default(),
            delete: false,
        };
        let shard_template = proto_gazette::consumer::ShardSpec {
            id: format!("{capture}/pass-through/shard_id_prefix"),
            ..Default::default()
        };
        let recovery_template = proto_gazette::broker::JournalSpec {
            name: format!("{capture}/pass-through/recovery_name_prefix"),
            ..Default::default()
        };
        let built_spec = flow::CaptureSpec {
            name: capture.to_string(),
            connector_type: flow::capture_spec::ConnectorType::Image as i32,
            interval_seconds: 100,
            network_ports: Vec::new(),
            recovery_log_template: Some(recovery_template),
            bindings: Vec::new(),
            shard_template: Some(shard_template),
            config_json: String::new(),
        };
        live.captures.insert_row(
            capture,
            mock.control_id,
            mock.data_plane_id,
            mock.last_pub_id,
            mock.last_build_id.unwrap_or(mock.last_pub_id),
            model,
            built_spec,
            None,
        );
    }
    // Load into LiveCatalog::live_collections.
    for (collection, mock) in &mock_calls.live_collections {
        let schema =
            mock.schema
                .clone()
                .unwrap_or(models::Schema::new(models::RawValue::from_value(
                    &serde_json::json!({
                        "x-live": "schema",
                    }),
                )));

        let model = models::CollectionDef {
            delete: false,
            derive: None,
            expect_pub_id: None,
            journals: Default::default(),
            key: mock.key.clone(),
            projections: mock.projections.clone(),
            read_schema: None,
            schema: Some(schema.clone()),
            write_schema: None,
        };
        let partition_template = proto_gazette::broker::JournalSpec {
            name: format!("{collection}/pass-through/partition_name_prefix"),
            ..Default::default()
        };
        let shard_template = proto_gazette::consumer::ShardSpec {
            id: format!("{collection}/pass-through/shard_id_prefix"),
            ..Default::default()
        };
        let recovery_template = proto_gazette::broker::JournalSpec {
            name: format!("{collection}/pass-through/recovery_name_prefix"),
            ..Default::default()
        };
        let derivation = if mock.derivation {
            Some(flow::collection_spec::Derivation {
                config_json: String::new(),
                connector_type: flow::collection_spec::derivation::ConnectorType::Sqlite as i32,
                network_ports: Vec::new(),
                recovery_log_template: Some(recovery_template),
                shard_template: Some(shard_template),
                shuffle_key_types: Vec::new(),
                transforms: Vec::new(),
            })
        } else {
            None
        };

        let built_spec = flow::CollectionSpec {
            name: collection.to_string(),
            ack_template_json: String::new(),
            derivation,
            key: model.key.iter().map(|k| k.to_string()).collect(),
            partition_fields: Vec::new(),
            partition_template: Some(partition_template),
            projections: Vec::new(),
            write_schema_json: schema.to_string(),
            read_schema_json: String::new(),
            uuid_ptr: "/_meta/uuid".to_string(),
        };
        live.collections.insert_row(
            collection,
            mock.control_id,
            mock.data_plane_id,
            mock.last_pub_id,
            mock.last_build_id.unwrap_or(mock.last_pub_id),
            model,
            built_spec,
            None,
        );
    }
    // Load into LiveCatalog::live_materializations.
    for (materialization, mock) in &mock_calls.live_materializations {
        let model = models::MaterializationDef {
            bindings: mock.bindings.clone(),
            endpoint: models::MaterializationEndpoint::Connector(live_connector_fixture.clone()),
            expect_pub_id: None,
            shards: models::ShardTemplate::default(),
            source_capture: None,
            delete: false,
            on_incompatible_schema_change: Default::default(),
        };
        let shard_template = proto_gazette::consumer::ShardSpec {
            id: format!("{materialization}/pass-through/shard_id_prefix"),
            ..Default::default()
        };
        let recovery_template = proto_gazette::broker::JournalSpec {
            name: format!("{materialization}/pass-through/recovery_name_prefix"),
            ..Default::default()
        };
        let built_spec = flow::MaterializationSpec {
            name: materialization.to_string(),
            connector_type: flow::materialization_spec::ConnectorType::Image as i32,
            network_ports: Vec::new(),
            recovery_log_template: Some(recovery_template),
            bindings: Vec::new(),
            shard_template: Some(shard_template),
            config_json: String::new(),
        };
        live.materializations.insert_row(
            materialization,
            mock.control_id,
            mock.data_plane_id,
            mock.last_pub_id,
            mock.last_build_id.unwrap_or(mock.last_pub_id),
            model,
            built_spec,
            None,
        );
    }
    // Load into LiveCatalog::live_tests.
    for (test, mock) in &mock_calls.live_tests {
        let model = models::TestDef {
            description: "live test".to_string(),
            steps: Vec::new(),
            expect_pub_id: None,
            delete: false,
        };
        let built_spec = flow::TestSpec {
            name: test.to_string(),
            steps: Vec::new(),
        };
        live.tests.insert_row(
            test,
            mock.control_id,
            mock.last_pub_id,
            mock.last_build_id.unwrap_or(mock.last_pub_id),
            model,
            built_spec,
            None,
        );
    }
    // Load into LiveCatalog::storage_mappings.
    for (prefix, storage) in &mock_calls.storage_mappings {
        live.storage_mappings
            .insert_row(prefix, models::Id::zero(), &storage.stores);
    }
    // Allow fixtures to omit a storage mapping by providing a default.
    if mock_calls.storage_mappings.is_empty() {
        let store = models::Store::S3(models::S3StorageConfig {
            bucket: "a-bucket".to_string(),
            prefix: None,
            region: None,
        });
        live.storage_mappings
            .insert_row(models::Prefix::new(""), models::Id::zero(), vec![store]);
    }

    let validations = futures::executor::block_on(validation::validate(
        models::Id::new([32; 8]),
        models::Id::new([33; 8]),
        &url::Url::parse("file:///project/root").unwrap(),
        &mock_calls,
        &draft,
        &live,
        false, // Don't fail-fast.
    ));

    let tables::DraftCatalog {
        captures,
        collections,
        errors: errors_draft,
        fetches,
        imports,
        materializations,
        resources,
        tests,
    } = draft;

    let tables::LiveCatalog {
        storage_mappings, ..
    } = live;

    let tables::Validations {
        built_captures,
        built_collections,
        built_materializations,
        built_tests,
        errors,
    } = validations;

    Outcome {
        built_captures,
        built_collections,
        built_materializations,
        built_tests,
        captures,
        collections,
        errors,
        errors_draft,
        fetches,
        imports,
        materializations,
        resources,
        storage_mappings,
        tests,
    }
}

pub fn run_errors(fixture_yaml: &str, patch_yaml: &str) -> tables::Errors {
    let outcome = run(fixture_yaml, patch_yaml);
    outcome.errors
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MockLiveCapture {
    control_id: models::Id,
    data_plane_id: models::Id,
    last_pub_id: models::Id,
    #[serde(default)]
    last_build_id: Option<models::Id>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MockLiveCollection {
    control_id: models::Id,
    data_plane_id: models::Id,
    last_pub_id: models::Id,
    #[serde(default)]
    last_build_id: Option<models::Id>,
    key: models::CompositeKey,
    #[serde(default)]
    derivation: bool,
    #[serde(default)]
    schema: Option<models::Schema>,
    #[serde(default)]
    projections: BTreeMap<models::Field, models::Projection>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MockLiveMaterialization {
    control_id: models::Id,
    data_plane_id: models::Id,
    last_pub_id: models::Id,
    #[serde(default)]
    last_build_id: Option<models::Id>,
    #[serde(default)]
    bindings: Vec<models::MaterializationBinding>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MockLiveTest {
    control_id: models::Id,
    last_pub_id: models::Id,
    #[serde(default)]
    last_build_id: Option<models::Id>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MockDataPlane {
    #[serde(default)]
    default: bool,
}

#[derive(Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MockDriverCalls {
    // Connector validations mocks:
    #[serde(default)]
    captures: BTreeMap<String, MockCaptureValidateCall>,
    #[serde(default)]
    derivations: BTreeMap<String, MockDeriveValidateCall>,
    #[serde(default)]
    materializations: BTreeMap<String, MockMaterializationValidateCall>,

    // Live catalog mocks:
    #[serde(default)]
    data_planes: BTreeMap<models::Id, MockDataPlane>,
    #[serde(default)]
    live_captures: BTreeMap<models::Capture, MockLiveCapture>,
    #[serde(default)]
    live_collections: BTreeMap<models::Collection, MockLiveCollection>,
    #[serde(default)]
    live_materializations: BTreeMap<models::Materialization, MockLiveMaterialization>,
    #[serde(default)]
    live_tests: BTreeMap<models::Test, MockLiveTest>,
    #[serde(default)]
    storage_mappings: BTreeMap<models::Prefix, models::StorageDef>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct MockCaptureValidateCall {
    connector_type: flow::capture_spec::ConnectorType,
    config: serde_json::Value,
    bindings: Vec<MockDriverBinding>,
    #[serde(default)]
    network_ports: Vec<flow::NetworkPort>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct MockDeriveValidateCall {
    connector_type: flow::collection_spec::derivation::ConnectorType,
    config: serde_json::Value,
    shuffle_key_types: Vec<flow::collection_spec::derivation::ShuffleType>,
    transforms: Vec<MockDeriveTransform>,
    #[serde(default)]
    generated_files: BTreeMap<String, String>,
    #[serde(default)]
    network_ports: Vec<flow::NetworkPort>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct MockDeriveTransform {
    read_only: bool,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct MockMaterializationValidateCall {
    connector_type: flow::materialization_spec::ConnectorType,
    config: serde_json::Value,
    bindings: Vec<MockDriverBinding>,
    #[serde(default)]
    delta_updates: bool,
    #[serde(default)]
    network_ports: Vec<flow::NetworkPort>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct MockDriverBinding {
    resource_path: Vec<String>,
    #[serde(default)]
    constraints: BTreeMap<String, materialize::response::validated::Constraint>,
    // type_override overrides the parsed constraints[].type for
    // each constraint. It supports test cases which want to deliberately
    // use type values which are invalid, and can't be parsed as YAML
    // (because of serde deserialization checks by the pbjson crate).
    #[serde(default)]
    type_override: i32,
}

impl validation::Connectors for MockDriverCalls {
    fn validate_capture<'a>(
        &'a self,
        request: capture::Request,
        _data_plane: &tables::DataPlane,
    ) -> BoxFuture<'a, anyhow::Result<capture::Response>> {
        let capture::Request {
            validate: Some(request),
            ..
        } = request
        else {
            unreachable!()
        };

        async move {
            let call = match self.captures.get(&request.name) {
                Some(call) => call,
                None => {
                    return Err(anyhow::anyhow!(
                        "driver fixture not found: {}",
                        request.name
                    ));
                }
            };

            let config: serde_json::Value = serde_json::from_str(&request.config_json)?;

            if call.connector_type as i32 != request.connector_type {
                return Err(anyhow::anyhow!(
                    "connector type mismatch: {} vs {}",
                    call.connector_type as i32,
                    request.connector_type
                ));
            }
            if &call.config != &config {
                return Err(anyhow::anyhow!(
                    "connector config mismatch: {} vs {}",
                    call.config.to_string(),
                    &request.config_json,
                ));
            }
            if let Some(err) = &call.error {
                return Err(anyhow::anyhow!("{err}"));
            }

            let bindings = call
                .bindings
                .iter()
                .map(|b| capture::response::validated::Binding {
                    resource_path: b.resource_path.clone(),
                })
                .collect();

            Ok(capture::Response {
                validated: Some(capture::response::Validated { bindings }),
                ..Default::default()
            }
            .with_internal(|internal| {
                internal.container = Some(Container {
                    ip_addr: "1.2.3.4".to_string(),
                    network_ports: call.network_ports.clone(),
                    mapped_host_ports: Default::default(),
                    usage_rate: 1.0,
                });
            }))
        }
        .boxed()
    }

    fn validate_derivation<'a>(
        &'a self,
        request: derive::Request,
        _data_plane: &tables::DataPlane,
    ) -> BoxFuture<'a, anyhow::Result<derive::Response>> {
        let derive::Request {
            validate: Some(request),
            ..
        } = request
        else {
            unreachable!()
        };

        async move {
            let name = &request.collection.as_ref().unwrap().name;

            let call = match self.derivations.get(name) {
                Some(call) => call,
                None => {
                    return Err(anyhow::anyhow!("driver fixture not found: {}", name));
                }
            };

            let config: serde_json::Value = serde_json::from_str(&request.config_json)?;

            if call.connector_type as i32 != request.connector_type {
                return Err(anyhow::anyhow!(
                    "connector type mismatch: {} vs {}",
                    call.connector_type as i32,
                    request.connector_type
                ));
            }
            if &call.config != &config {
                return Err(anyhow::anyhow!(
                    "connector config mismatch: {} vs {}",
                    call.config.to_string(),
                    &request.config_json,
                ));
            }
            if call
                .shuffle_key_types
                .iter()
                .map(|t| *t as i32)
                .collect::<Vec<_>>()
                != request.shuffle_key_types
            {
                return Err(anyhow::anyhow!(
                    "shuffle types mismatch: {:?} vs {:?}",
                    call.shuffle_key_types,
                    request.shuffle_key_types,
                ));
            }

            if let Some(err) = &call.error {
                return Err(anyhow::anyhow!("{err}"));
            }

            let transforms = call
                .transforms
                .iter()
                .map(|b| derive::response::validated::Transform {
                    read_only: b.read_only,
                })
                .collect();

            Ok(derive::Response {
                validated: Some(derive::response::Validated {
                    transforms,
                    generated_files: call.generated_files.clone(),
                }),
                ..Default::default()
            }
            .with_internal(|internal| {
                internal.container = Some(Container {
                    ip_addr: "1.2.3.4".to_string(),
                    network_ports: call.network_ports.clone(),
                    mapped_host_ports: Default::default(),
                    usage_rate: 0.0,
                });
            }))
        }
        .boxed()
    }

    fn validate_materialization<'a>(
        &'a self,
        request: materialize::Request,
        _data_plane: &tables::DataPlane,
    ) -> BoxFuture<'a, anyhow::Result<materialize::Response>> {
        let materialize::Request {
            validate: Some(request),
            ..
        } = request
        else {
            unreachable!()
        };

        async move {
            let call = match self.materializations.get(&request.name) {
                Some(call) => call,
                None => {
                    return Err(anyhow::anyhow!(
                        "driver fixture not found: {}",
                        request.name
                    ));
                }
            };

            let config: serde_json::Value = serde_json::from_str(&request.config_json)?;

            if call.connector_type as i32 != request.connector_type {
                return Err(anyhow::anyhow!(
                    "connector type mismatch: {} vs {}",
                    call.connector_type as i32,
                    request.connector_type
                ));
            }
            if &call.config != &config {
                return Err(anyhow::anyhow!(
                    "connector config mismatch: {} vs {}",
                    call.config.to_string(),
                    &request.config_json,
                ));
            }
            if let Some(err) = &call.error {
                return Err(anyhow::anyhow!("{err}"));
            }

            let bindings = call
                .bindings
                .iter()
                .map(|b| {
                    let mut out = materialize::response::validated::Binding {
                        constraints: b.constraints.clone(),
                        delta_updates: call.delta_updates,
                        resource_path: b.resource_path.clone(),
                    };

                    // NOTE(johnny): clunky support for test_materialization_driver_unknown_constraints,
                    // to work around serde deser not allowing parsing of invalid enum values.
                    for c in out.constraints.iter_mut() {
                        if c.1.r#type == 0 && b.type_override != 0 {
                            c.1.r#type = b.type_override;
                        }
                    }

                    out
                })
                .collect();

            Ok(materialize::Response {
                validated: Some(materialize::response::Validated { bindings }),
                ..Default::default()
            }
            .with_internal(|internal| {
                internal.container = Some(Container {
                    ip_addr: "1.2.3.4".to_string(),
                    network_ports: call.network_ports.clone(),
                    mapped_host_ports: Default::default(),
                    usage_rate: 1.25,
                });
            }))
        }
        .boxed()
    }
}
