use futures::StreamExt;
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
            bindings: mock.bindings.clone(),
            endpoint: models::CaptureEndpoint::Connector(live_connector_fixture.clone()),
            expect_pub_id: None,
            interval: std::time::Duration::from_secs(32),
            shards: models::ShardTemplate::default(),
            delete: false,
        };
        let shard_template = proto_gazette::consumer::ShardSpec {
            id: format!("capture/{capture}/0000000000000001"),
            ..Default::default()
        };
        let recovery_template = proto_gazette::broker::JournalSpec {
            name: format!("recovery/capture/{capture}/0000000000000001"),
            ..Default::default()
        };

        let bindings: Vec<flow::capture_spec::Binding> = mock
            .bindings
            .iter()
            .map(|binding| flow::capture_spec::Binding {
                collection: Some(flow::CollectionSpec {
                    name: binding.target.to_string(),
                    partition_template: Some(proto_gazette::broker::JournalSpec {
                        name: format!("{}/0000000000000001", binding.target),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                resource_path: validation::load_resource_meta_path(binding.resource.get()),
                backfill: binding.backfill,
                ..Default::default()
            })
            .collect();

        let built_spec = flow::CaptureSpec {
            name: capture.to_string(),
            connector_type: flow::capture_spec::ConnectorType::Image as i32,
            interval_seconds: 100,
            network_ports: Vec::new(),
            recovery_log_template: Some(recovery_template),
            bindings,
            shard_template: Some(shard_template),
            config_json: String::new(),
            inactive_bindings: Vec::new(),
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
            derive: mock.derive.clone(),
            expect_pub_id: None,
            journals: Default::default(),
            key: mock.key.clone(),
            projections: mock.projections.clone(),
            read_schema: None,
            schema: Some(schema.clone()),
            write_schema: None,
            reset: false,
        };
        let partition_template = proto_gazette::broker::JournalSpec {
            name: format!("{collection}/0000000000000001"),
            ..Default::default()
        };
        let shard_template = proto_gazette::consumer::ShardSpec {
            id: format!("derivation/{collection}/0000000000000001"),
            ..Default::default()
        };
        let recovery_template = proto_gazette::broker::JournalSpec {
            name: format!("recovery/derivation/{collection}/0000000000000001"),
            ..Default::default()
        };
        let derivation = if let Some(derive) = &mock.derive {
            let transforms: Vec<flow::collection_spec::derivation::Transform> = derive
                .transforms
                .iter()
                .map(|transform| flow::collection_spec::derivation::Transform {
                    name: transform.name.to_string(),
                    collection: Some(flow::CollectionSpec {
                        name: transform.source.collection().to_string(),
                        partition_template: Some(proto_gazette::broker::JournalSpec {
                            name: format!("{}/0000000000000001", transform.source.collection()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    backfill: transform.backfill,
                    ..Default::default()
                })
                .collect();

            Some(flow::collection_spec::Derivation {
                config_json: String::new(),
                connector_type: flow::collection_spec::derivation::ConnectorType::Sqlite as i32,
                network_ports: Vec::new(),
                recovery_log_template: Some(recovery_template),
                shard_template: Some(shard_template),
                shuffle_key_types: Vec::new(),
                transforms,
                inactive_transforms: Vec::new(),
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
            source: None,
            delete: false,
            on_incompatible_schema_change: Default::default(),
        };
        let shard_template = proto_gazette::consumer::ShardSpec {
            id: format!("materialize/{materialization}/0000000000000001"),
            ..Default::default()
        };
        let recovery_template = proto_gazette::broker::JournalSpec {
            name: format!("recovery/materialize/{materialization}/0000000000000001"),
            ..Default::default()
        };
        let bindings: Vec<flow::materialization_spec::Binding> = mock
            .bindings
            .iter()
            .map(|binding| flow::materialization_spec::Binding {
                collection: Some(flow::CollectionSpec {
                    name: binding.source.collection().to_string(),
                    partition_template: Some(proto_gazette::broker::JournalSpec {
                        name: format!("{}/0000000000000001", binding.source.collection()),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                resource_path: validation::load_resource_meta_path(binding.resource.get()),
                backfill: binding.backfill,
                ..Default::default()
            })
            .collect();

        let built_spec = flow::MaterializationSpec {
            name: materialization.to_string(),
            connector_type: flow::materialization_spec::ConnectorType::Image as i32,
            network_ports: Vec::new(),
            recovery_log_template: Some(recovery_template),
            bindings,
            shard_template: Some(shard_template),
            config_json: String::new(),
            inactive_bindings: Vec::new(),
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
            steps: mock.steps.clone(),
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
    // Load into LiveCatalog::inferred_schemas.
    for (collection, schema) in &mock_calls.live_inferred_schemas {
        live.inferred_schemas
            .insert_row(collection, schema, "an-md5".to_string());
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
        false, // Don't no-op captures.
        false, // Don't no-op derivations.
        false, // Don't no-op materializations.
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

#[allow(dead_code)]
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
    #[serde(default)]
    bindings: Vec<models::CaptureBinding>,
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
    schema: Option<models::Schema>,
    #[serde(default)]
    projections: BTreeMap<models::Field, models::Projection>,
    #[serde(default)]
    derive: Option<models::Derivation>,
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
    #[serde(default)]
    steps: Vec<models::TestStep>,
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
    live_inferred_schemas: BTreeMap<models::Collection, models::Schema>,
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

impl std::fmt::Debug for MockDriverCalls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MockDriverCalls")
    }
}

impl validation::Connectors for MockDriverCalls {
    fn capture<'a, R>(
        &'a self,
        _data_plane: &'a tables::DataPlane,
        _task: &'a models::Capture,
        mut request_rx: R,
    ) -> impl futures::Stream<Item = anyhow::Result<capture::Response>> + Send + 'a
    where
        R: futures::Stream<Item = capture::Request> + Send + Unpin + 'static,
    {
        coroutines::try_coroutine(|mut co| async move {
            while let Some(request) = request_rx.next().await {
                if let Some(_spec) = request.spec {
                    () = co
                        .yield_(capture::Response {
                            spec: Some(capture::response::Spec {
                                config_schema_json: serde_json::json!({
                                    "type": "object",
                                })
                                .to_string(),
                                resource_config_schema_json: serde_json::json!({
                                    "type": "object",
                                    "properties": {
                                        "schema": {"type": "string"},
                                        "source": {"type": "string"},
                                    },
                                    "required": ["source"]
                                })
                                .to_string(),
                                resource_path_pointers: Vec::new(),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .await;
                    continue;
                }

                let Some(validate) = request.validate else {
                    anyhow::bail!("expected Spec or Validate")
                };

                let call = match self.captures.get(&validate.name) {
                    Some(call) => call,
                    None => {
                        return Err(anyhow::anyhow!(
                            "driver fixture not found: {}",
                            validate.name
                        ));
                    }
                };

                let config: serde_json::Value = serde_json::from_str(&validate.config_json)?;

                if call.connector_type as i32 != validate.connector_type {
                    return Err(anyhow::anyhow!(
                        "connector type mismatch: {} vs {}",
                        call.connector_type as i32,
                        validate.connector_type
                    ));
                }
                if &call.config != &config {
                    return Err(anyhow::anyhow!(
                        "connector config mismatch: {} vs {}",
                        call.config.to_string(),
                        &validate.config_json,
                    ));
                }
                if let Some(err) = &call.error {
                    return Err(anyhow::anyhow!("{err}"));
                }

                let bindings = call
                    .bindings
                    .iter()
                    .take(validate.bindings.len())
                    .map(|b| capture::response::validated::Binding {
                        resource_path: b.resource_path.clone(),
                    })
                    .collect();

                () = co
                    .yield_(
                        capture::Response {
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
                        }),
                    )
                    .await;
            }
            Ok(())
        })
    }

    fn derive<'a, R>(
        &'a self,
        _data_plane: &'a tables::DataPlane,
        _task: &'a models::Collection,
        mut request_rx: R,
    ) -> impl futures::Stream<Item = anyhow::Result<derive::Response>> + Send + 'a
    where
        R: futures::Stream<Item = derive::Request> + Send + Unpin + 'static,
    {
        coroutines::try_coroutine(|mut co| async move {
            while let Some(request) = request_rx.next().await {
                if let Some(_spec) = request.spec {
                    () = co
                        .yield_(derive::Response {
                            spec: Some(derive::response::Spec {
                                config_schema_json: "true".to_string(),
                                resource_config_schema_json: "true".to_string(),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .await;
                    continue;
                }

                let Some(validate) = request.validate else {
                    anyhow::bail!("expected Spec or Validate")
                };

                let name = &validate.collection.as_ref().unwrap().name;

                let call = match self.derivations.get(name) {
                    Some(call) => call,
                    None => {
                        return Err(anyhow::anyhow!("driver fixture not found: {}", name));
                    }
                };

                let config: serde_json::Value = serde_json::from_str(&validate.config_json)?;

                if call.connector_type as i32 != validate.connector_type {
                    return Err(anyhow::anyhow!(
                        "connector type mismatch: {} vs {}",
                        call.connector_type as i32,
                        validate.connector_type
                    ));
                }
                if &call.config != &config {
                    return Err(anyhow::anyhow!(
                        "connector config mismatch: {} vs {}",
                        call.config.to_string(),
                        &validate.config_json,
                    ));
                }
                if call
                    .shuffle_key_types
                    .iter()
                    .map(|t| *t as i32)
                    .collect::<Vec<_>>()
                    != validate.shuffle_key_types
                {
                    return Err(anyhow::anyhow!(
                        "shuffle types mismatch: {:?} vs {:?}",
                        call.shuffle_key_types,
                        validate.shuffle_key_types,
                    ));
                }

                if let Some(err) = &call.error {
                    return Err(anyhow::anyhow!("{err}"));
                }

                let transforms = call
                    .transforms
                    .iter()
                    .take(validate.transforms.len())
                    .map(|b| derive::response::validated::Transform {
                        read_only: b.read_only,
                    })
                    .collect();

                () = co
                    .yield_(
                        derive::Response {
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
                        }),
                    )
                    .await;
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
        coroutines::try_coroutine(|mut co| async move {
            while let Some(request) = request_rx.next().await {
                if let Some(_spec) = request.spec {
                    () = co
                        .yield_(materialize::Response {
                            spec: Some(materialize::response::Spec {
                                config_schema_json: serde_json::json!({
                                    "type": "object",
                                })
                                .to_string(),
                                resource_config_schema_json: serde_json::json!({
                                    "type": "object",
                                    "properties": {
                                        "schema": {"type": "string", "x-schema-name": true},
                                        "target": {"type": "string", "x-collection-name": true},
                                    },
                                    "required": ["target"]
                                })
                                .to_string(),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .await;
                    continue;
                }

                let Some(validate) = request.validate else {
                    anyhow::bail!("expected Spec or Validate")
                };

                let call = match self.materializations.get(&validate.name) {
                    Some(call) => call,
                    None => {
                        return Err(anyhow::anyhow!(
                            "driver fixture not found: {}",
                            validate.name
                        ));
                    }
                };

                let config: serde_json::Value = serde_json::from_str(&validate.config_json)?;

                if call.connector_type as i32 != validate.connector_type {
                    return Err(anyhow::anyhow!(
                        "connector type mismatch: {} vs {}",
                        call.connector_type as i32,
                        validate.connector_type
                    ));
                }
                if &call.config != &config {
                    return Err(anyhow::anyhow!(
                        "connector config mismatch: {} vs {}",
                        call.config.to_string(),
                        &validate.config_json,
                    ));
                }
                if let Some(err) = &call.error {
                    return Err(anyhow::anyhow!("{err}"));
                }

                let bindings = call
                    .bindings
                    .iter()
                    .take(validate.bindings.len())
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

                () = co
                    .yield_(
                        materialize::Response {
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
                        }),
                    )
                    .await;
            }
            Ok(())
        })
    }
}
