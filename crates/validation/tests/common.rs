use futures::FutureExt;
use proto_flow::{capture, connector, derive, flow, materialize, runtime::Container};
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
            tests,
        ]
    }
}

/// Define `$name`, which rewrites the bindings of a live built spec into indirect
/// form: collections move into a `linked_collections` table which bindings then
/// name by index. It mimics a task last published under the `indirect-specs` flag.
///
/// Entries are ordered by first use rather than by name -- unlike the builder
/// under test -- so that a spec which copied a live index instead of resolving
/// through it is caught.
macro_rules! indirect {
    ($name:ident, $msg:ty) => {
        fn $name(spec: &mut $msg) {
            let mut table: Vec<flow::CollectionSpec> = Vec::new();

            for binding in spec.bindings.iter_mut() {
                let collection = *binding.collection.take().unwrap();

                binding.collection_index = match table.iter().position(|c| *c == collection) {
                    Some(index) => index as u32,
                    None => {
                        table.push(collection);
                        table.len() as u32 - 1
                    }
                };
            }
            spec.linked_collections = table;
        }
    };
}

indirect!(indirect_capture, flow::CaptureSpec);
indirect!(indirect_materialization, flow::MaterializationSpec);

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

    // Load into LiveCatalog::live_captures.
    for (capture, mock) in &mock_calls.live_captures {
        let model = models::CaptureDef {
            auto_discover: None,
            bindings: mock.bindings.clone(),
            endpoint: models::CaptureEndpoint::Connector(live_connector_fixture.clone()),
            expect_pub_id: None,
            interval: std::time::Duration::from_secs(32),
            secrets: Default::default(),
            shards: models::ShardTemplate::default(),
            delete: false,
            reset: false,
            redact_salt: None,
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
                collection: Some(Box::new(flow::CollectionSpec {
                    name: binding.target.to_string(),
                    partition_template: Some(proto_gazette::broker::JournalSpec {
                        name: format!("{}/0000000000000001", binding.target),
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
                resource_path: validation::load_resource_meta_path(
                    binding.resource.get().as_bytes(),
                ),
                backfill: binding.backfill,
                ..Default::default()
            })
            .collect();

        let mut built_spec = flow::CaptureSpec {
            name: capture.to_string(),
            connector_type: flow::capture_spec::ConnectorType::Image as i32,
            interval_seconds: 100,
            network_ports: Vec::new(),
            recovery_log_template: Some(recovery_template),
            bindings,
            shard_template: Some(shard_template),
            config_json: bytes::Bytes::new(),
            inactive_bindings: Vec::new(),
            redact_salt: b"pass-through-capture-salt".as_slice().into(),
            created_at: String::new(),
            linked_collections: Vec::new(),
            secrets: Default::default(),
        };
        if mock.indirect_specs {
            indirect_capture(&mut built_spec);
        }
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
                    collection: Some(Box::new(flow::CollectionSpec {
                        name: transform.source.collection().to_string(),
                        partition_template: Some(proto_gazette::broker::JournalSpec {
                            name: format!("{}/0000000000000001", transform.source.collection()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                    backfill: transform.backfill,
                    ..Default::default()
                })
                .collect();

            Some(Box::new(flow::collection_spec::Derivation {
                config_json: bytes::Bytes::new(),
                connector_type: flow::collection_spec::derivation::ConnectorType::Sqlite as i32,
                network_ports: Vec::new(),
                recovery_log_template: Some(recovery_template),
                shard_template: Some(shard_template),
                shuffle_key_types: Vec::new(),
                transforms,
                inactive_transforms: Vec::new(),
                redact_salt: b"pass-through-derivation-salt".as_slice().into(),
                linked_collections: Vec::new(),
                secrets: Default::default(),
            }))
        } else {
            None
        };

        let built_spec = flow::CollectionSpec {
            name: collection.to_string(),
            ack_template_json: bytes::Bytes::new(),
            derivation,
            key: model.key.iter().map(|k| k.to_string()).collect(),
            partition_fields: Vec::new(),
            partition_template: Some(partition_template),
            projections: Vec::new(),
            write_schema_json: schema.to_string().into(),
            read_schema_json: bytes::Bytes::new(),
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
            secrets: Default::default(),
            shards: models::ShardTemplate::default(),
            source: None,
            target_naming: None,
            triggers: None,
            sync_schedule: None,
            delete: false,
            reset: false,
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
            .enumerate()
            .map(|(index, binding)| flow::materialization_spec::Binding {
                collection: Some(Box::new(flow::CollectionSpec {
                    name: binding.source.collection().to_string(),
                    partition_template: Some(proto_gazette::broker::JournalSpec {
                        name: format!("{}/0000000000000001", binding.source.collection()),
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
                resource_path: validation::load_resource_meta_path(
                    binding.resource.get().as_bytes(),
                ),
                backfill: binding.backfill,
                field_selection: mock.last_fields.get(index).cloned(),
                delta_updates: mock
                    .last_fields
                    .get(index)
                    .map(|l| l.keys.is_empty())
                    .unwrap_or_default(),
                ..Default::default()
            })
            .collect();

        let mut built_spec = flow::MaterializationSpec {
            name: materialization.to_string(),
            connector_type: flow::materialization_spec::ConnectorType::Image as i32,
            network_ports: Vec::new(),
            recovery_log_template: Some(recovery_template),
            bindings,
            shard_template: Some(shard_template),
            config_json: bytes::Bytes::new(),
            inactive_bindings: Vec::new(),
            triggers_json: bytes::Bytes::new(),
            created_at: String::new(),
            sync_schedule_json: bytes::Bytes::new(),
            linked_collections: Vec::new(),
            secrets: Default::default(),
        };
        if mock.indirect_specs {
            indirect_materialization(&mut built_spec);
        }
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
        let data_plane_ids = storage
            .data_planes
            .iter()
            .map(|name| {
                mock_calls
                    .data_planes
                    .keys()
                    .find(|id| name == &format!("ops/dp/public/test-{id}"))
                    .copied()
                    .unwrap_or_else(models::Id::zero)
            })
            .collect::<Vec<_>>();
        live.storage_mappings.insert_row(
            prefix,
            models::Id::zero(),
            &storage.stores,
            data_plane_ids,
        );
    }
    // Allow fixtures to omit a storage mapping by providing a default.
    if mock_calls.storage_mappings.is_empty() {
        let store = models::Store::S3(models::S3StorageConfig {
            bucket: "a-bucket".to_string(),
            prefix: None,
            region: None,
        });
        let data_plane_ids = mock_calls.data_planes.keys().copied().collect::<Vec<_>>();
        live.storage_mappings.insert_row(
            models::Prefix::new(""),
            models::Id::zero(),
            vec![store],
            data_plane_ids,
        );
    }

    // Use a constant initialization vector for deterministic test output.
    const TEST_INIT_VECTOR: &[u8] = b"test-init-vector";

    let connectors = |_data_plane_id: models::Id, request| mock_calls.connect(request);
    let validations = futures::executor::block_on(validation::validate(
        models::Id::new([32; 8]),
        models::Id::new([33; 8]),
        &url::Url::parse("file:///project/root").unwrap(),
        &connectors,
        None, // No default data plane name.
        &draft,
        &live,
        false, // Don't fail-fast.
        false, // Don't no-op captures.
        false, // Don't no-op derivations.
        false, // Don't no-op materializations.
        TEST_INIT_VECTOR,
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
        tests,
    }
}

#[allow(dead_code)]
pub fn run_errors(fixture_yaml: &str, patch_yaml: &str) -> tables::Errors {
    let outcome = run(fixture_yaml, patch_yaml);
    outcome.errors
}

#[allow(dead_code)]
pub fn run_selection(
    fixture_yaml: &str,
    patch_yaml: &str,
) -> (
    Vec<(
        Vec<flow::FieldSelection>,
        Option<models::MaterializationDef>,
        Vec<String>,
    )>,
    tables::Errors,
) {
    let outcome = run(fixture_yaml, patch_yaml);

    let mut errors = outcome.errors_draft;
    errors.extend(outcome.errors.into_iter());

    let selections: Vec<_> = outcome
        .built_materializations
        .into_iter()
        .map(
            |tables::BuiltMaterialization {
                 spec,
                 model,
                 model_fixes: fixes,
                 ..
             }| {
                (
                    std::mem::take(&mut spec.unwrap().bindings)
                        .into_iter()
                        .filter_map(|b| b.field_selection)
                        .collect::<Vec<_>>(),
                    model,
                    fixes,
                )
            },
        )
        .collect();

    (selections, errors)
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
    /// Build this live spec in indirect form, as if last published under the
    /// `indirect-specs` flag.
    #[serde(default)]
    indirect_specs: bool,
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
    #[serde(default)]
    last_fields: Vec<flow::FieldSelection>,
    /// Build this live spec in indirect form, as if last published under the
    /// `indirect-specs` flag.
    #[serde(default)]
    indirect_specs: bool,
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
struct MockDataPlane {}

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
    case_insensitive_fields: bool,
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

impl MockDriverCalls {
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
                    kind: Some(capture::request::Kind::Validate(validate)),
                    ..
                })) => self.validate_capture(*validate),
                Some(connector::request::Kind::Derive(derive::Request {
                    kind: Some(derive::request::Kind::Validate(validate)),
                    ..
                })) => self.validate_derivation(*validate),
                Some(connector::request::Kind::Materialize(materialize::Request {
                    kind: Some(materialize::request::Kind::Validate(validate)),
                    ..
                })) => self.validate_materialization(*validate),
                _ => anyhow::bail!("expected a connector Validate request"),
            }
        }
        .boxed()
    }

    fn validate_capture(
        &self,
        validate: capture::request::Validate,
    ) -> anyhow::Result<(connector::response::Started, connector::response::Kind)> {
        let call = self
            .captures
            .get(&validate.name)
            .ok_or_else(|| anyhow::anyhow!("driver fixture not found: {}", validate.name))?;
        let config: serde_json::Value = serde_json::from_slice(&validate.config_json)?;

        if call.connector_type as i32 != validate.connector_type {
            anyhow::bail!(
                "connector type mismatch: {} vs {}",
                call.connector_type as i32,
                validate.connector_type
            );
        }
        if call.config != config {
            anyhow::bail!("connector config mismatch: {} vs {}", call.config, config,);
        }
        if let Some(err) = &call.error {
            anyhow::bail!("{err}");
        }

        let bindings = call
            .bindings
            .iter()
            .take(validate.bindings.len())
            .map(|binding| capture::response::validated::Binding {
                resource_path: binding.resource_path.clone(),
            })
            .collect();

        Ok(mock_response(
            connector::response::started::Spec::Capture(Box::new(capture::response::Spec {
                config_schema_json: serde_json::json!({"type": "object"}).to_string().into(),
                resource_config_schema_json: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "schema": {"type": "string"},
                        "source": {"type": "string"},
                    },
                    "required": ["source"]
                })
                .to_string()
                .into(),
                resource_path_pointers: Vec::new(),
                ..Default::default()
            })),
            connector::response::Kind::Capture(capture::Response {
                kind: Some(capture::response::Kind::Validated(
                    capture::response::Validated { bindings },
                )),
                ..Default::default()
            }),
            call.network_ports.clone(),
            1.0,
        ))
    }

    fn validate_derivation(
        &self,
        validate: derive::request::Validate,
    ) -> anyhow::Result<(connector::response::Started, connector::response::Kind)> {
        let name = &validate.collection.as_ref().unwrap().name;
        let call = self
            .derivations
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("driver fixture not found: {name}"))?;
        let config: serde_json::Value = serde_json::from_slice(&validate.config_json)?;

        if call.connector_type as i32 != validate.connector_type {
            anyhow::bail!(
                "connector type mismatch: {} vs {}",
                call.connector_type as i32,
                validate.connector_type
            );
        }
        if call.config != config {
            anyhow::bail!("connector config mismatch: {} vs {}", call.config, config,);
        }
        if call
            .shuffle_key_types
            .iter()
            .map(|kind| *kind as i32)
            .collect::<Vec<_>>()
            != validate.shuffle_key_types
        {
            anyhow::bail!(
                "shuffle types mismatch: {:?} vs {:?}",
                call.shuffle_key_types,
                validate.shuffle_key_types,
            );
        }
        if let Some(err) = &call.error {
            anyhow::bail!("{err}");
        }

        let transforms = call
            .transforms
            .iter()
            .take(validate.transforms.len())
            .map(|transform| derive::response::validated::Transform {
                read_only: transform.read_only,
            })
            .collect();

        Ok(mock_response(
            connector::response::started::Spec::Derive(Box::new(derive::response::Spec {
                config_schema_json: "true".into(),
                resource_config_schema_json: "true".into(),
                ..Default::default()
            })),
            connector::response::Kind::Derive(derive::Response {
                kind: Some(derive::response::Kind::Validated(
                    derive::response::Validated {
                        transforms,
                        generated_files: call.generated_files.clone(),
                    },
                )),
                ..Default::default()
            }),
            call.network_ports.clone(),
            0.0,
        ))
    }

    fn validate_materialization(
        &self,
        validate: materialize::request::Validate,
    ) -> anyhow::Result<(connector::response::Started, connector::response::Kind)> {
        let call = self
            .materializations
            .get(&validate.name)
            .ok_or_else(|| anyhow::anyhow!("driver fixture not found: {}", validate.name))?;
        let config: serde_json::Value = serde_json::from_slice(&validate.config_json)?;

        if call.connector_type as i32 != validate.connector_type {
            anyhow::bail!(
                "connector type mismatch: {} vs {}",
                call.connector_type as i32,
                validate.connector_type
            );
        }
        if call.config != config {
            anyhow::bail!("connector config mismatch: {} vs {}", call.config, config,);
        }
        if let Some(err) = &call.error {
            anyhow::bail!("{err}");
        }

        let bindings = call
            .bindings
            .iter()
            .take(validate.bindings.len())
            .map(|binding| {
                let projection_constraints = binding
                    .constraints
                    .iter()
                    .map(|(field, constraint)| {
                        let mut constraint = constraint.clone();
                        // Invalid enum values cannot be expressed through pbjson fixtures.
                        if constraint.r#type == 0 && binding.type_override != 0 {
                            constraint.r#type = binding.type_override;
                        }
                        materialize::response::validated::ProjectionConstraint {
                            field: field.clone(),
                            constraint: Some(constraint),
                        }
                    })
                    .collect();

                materialize::response::validated::Binding {
                    case_insensitive_fields: binding.case_insensitive_fields,
                    projection_constraints,
                    delta_updates: call.delta_updates,
                    resource_path: binding.resource_path.clone(),
                    ser_policy: None,
                }
            })
            .collect();

        Ok(mock_response(
            connector::response::started::Spec::Materialize(Box::new(
                materialize::response::Spec {
                    config_schema_json: serde_json::json!({"type": "object"}).to_string().into(),
                    resource_config_schema_json: serde_json::json!({
                        "type": "object",
                        "properties": {
                            "schema": {"type": "string", "x-schema-name": true},
                            "target": {"type": "string", "x-collection-name": true},
                        },
                        "required": ["target"]
                    })
                    .to_string()
                    .into(),
                    ..Default::default()
                },
            )),
            connector::response::Kind::Materialize(materialize::Response {
                kind: Some(materialize::response::Kind::Validated(
                    materialize::response::Validated { bindings },
                )),
                ..Default::default()
            }),
            call.network_ports.clone(),
            1.25,
        ))
    }
}

fn mock_response(
    spec: connector::response::started::Spec,
    response: connector::response::Kind,
    network_ports: Vec<flow::NetworkPort>,
    usage_rate: f32,
) -> (connector::response::Started, connector::response::Kind) {
    (
        connector::response::Started {
            spec: Some(spec),
            container: Some(Container {
                ip_addr: "1.2.3.4".to_string(),
                network_ports,
                mapped_host_ports: Default::default(),
                usage_rate,
            }),
            ..Default::default()
        },
        response,
    )
}
