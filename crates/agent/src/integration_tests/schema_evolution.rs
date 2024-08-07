use std::collections::{BTreeMap, BTreeSet};

use super::harness::{draft_catalog, mock_inferred_schema, FailBuild, TestHarness};
use crate::{
    controllers::ControllerState,
    publications::{JobStatus, UncommittedBuild},
    ControlPlane,
};
use models::CatalogType;
use proto_flow::materialize::response::validated::constraint::Type as ConstraintType;
use tables::BuiltRow;

#[tokio::test]
#[serial_test::serial]
async fn test_schema_evolution() {
    let mut harness = TestHarness::init("test_dependencies_and_controllers").await;

    let user_id = harness.setup_tenant("goats").await;
    let draft = draft_catalog(serde_json::json!({
        "collections": {
            "goats/pasture": {
                "writeSchema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    }
                },
                "readSchema": {
                    "allOf": [
                        {"$ref": "flow://write-schema"},
                        {"$ref": "flow://inferred-schema"}
                    ]
                },
                "key": ["/id"]
            },
            "goats/totes": {
                "writeSchema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    }
                },
                "readSchema": {
                    "$defs": {
                        "flow://inferred-schema": {
                            "$id": "flow://inferred-schema",
                            "type": "object",
                            "properties": {
                                "pre-existing": { "type": "integer" }
                            }
                        },
                        // Include write schema to start, and expect that this is removed
                        "flow://write-schema": {
                            "$id": "flow://write-schema",
                            "type": "object",
                            "properties": {
                                "id": { "type": "string" }
                            }
                        }
                    },
                    "allOf": [
                        {"$ref": "flow://write-schema"},
                        {"$ref": "flow://inferred-schema"}
                    ]
                },
                "key": ["/id"],
                "derive": {
                    "using": {
                        "sqlite": { "migrations": [] }
                    },
                    "transforms": [
                        {
                            "name": "fromPature",
                            "source": "goats/pasture",
                            "lambda": "select $id;",
                            "shuffle": "any"
                        }
                    ]
                }
            }
        },
        "materializations": {
            "goats/materializeBackfill": {
                "endpoint": {
                    "connector": {
                        "image": "ghcr.io/estuary/materialize-postgres:dev",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": { "table": "pasture" },
                        "source": "goats/pasture"
                    },
                    {
                        "resource": { "table": "totes" },
                        "source": "goats/totes"
                    }
                ]
            },
            "goats/materializeDisableBinding": {
                "onIncompatibleSchemaChange": "disableBinding",
                "endpoint": {
                    "connector": {
                        "image": "ghcr.io/estuary/materialize-postgres:dev",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": { "table": "pasture" },
                        "source": "goats/pasture"
                    },
                    {
                        "resource": { "table": "totes" },
                        "source": "goats/totes"
                    }
                ]
            },
            "goats/materializeMixed": {
                "onIncompatibleSchemaChange": "disableTask",
                "endpoint": {
                    "connector": {
                        "image": "ghcr.io/estuary/materialize-postgres:dev",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": { "table": "pasture" },
                        "source": "goats/pasture",
                        "onIncompatibleSchemaChange": "disableBinding"
                    },
                    {
                        "resource": { "table": "totes" },
                        "source": "goats/totes",
                        "onIncompatibleSchemaChange": "abort",
                    }
                ]
            }
        }
    }));

    let initial_result = harness
        .user_publication(user_id, "initial publication", draft)
        .await;
    assert!(initial_result.status.is_success());
    harness.run_pending_controllers(None).await;

    // Assert that the pasture collection has had the inferred schema placeholder added
    let pasture_state = harness.get_controller_state("goats/pasture").await;
    let pasture_spec = unwrap_built_collection(&pasture_state);
    assert!(pasture_spec
        .read_schema_json
        .contains("inferredSchemaIsNotAvailable"));
    // A collection that uses schema inferrence should always have a next_run scheduled
    assert!(pasture_state.next_run.is_some());

    // Assert that the totes collection has _not_ had the inferred schema placeholder added
    let totes_state = harness.get_controller_state("goats/totes").await;
    let totes_spec = unwrap_built_collection(&totes_state);
    assert!(totes_spec
        .read_schema_json
        .contains("inferredSchemaIsNotAvailable"));
    // Assert that the bundled write schema has been removed. We expect one reference to
    // the write schema url, down from 3 originally.
    // TODO: we can remove these assertions (and the bundled write schema in the setup) once
    // all the collections have been updated.
    let totes_model = totes_state
        .live_spec
        .as_ref()
        .unwrap()
        .as_collection()
        .unwrap();
    assert_eq!(
        1,
        totes_model
            .read_schema
            .as_ref()
            .unwrap()
            .get()
            .matches(models::Schema::REF_WRITE_SCHEMA_URL)
            .count()
    );
    // Assert that the schema in the built spec _does_ contain the bundled write schema
    assert_eq!(
        3,
        totes_spec
            .read_schema_json
            .matches(models::Schema::REF_WRITE_SCHEMA_URL)
            .count()
    );
    assert!(totes_state.next_run.is_some());

    harness.control_plane().reset_activations();
    harness
        .upsert_inferred_schema(mock_inferred_schema("goats/pasture", 1))
        .await;

    harness.run_pending_controller("goats/pasture").await;
    let pasture_state = harness.get_controller_state("goats/pasture").await;
    let pasture_spec = unwrap_built_collection(&pasture_state);
    // Assert the placeholder schema is no longer present
    assert!(!pasture_spec
        .read_schema_json
        .contains("inferredSchemaIsNotAvailable"));

    harness.run_pending_controllers(None).await;
    harness.control_plane().assert_activations(
        "after pasture inferred schema updated",
        vec![
            ("goats/pasture", Some(CatalogType::Collection)),
            ("goats/totes", Some(CatalogType::Collection)),
            (
                "goats/materializeBackfill",
                Some(CatalogType::Materialization),
            ),
            (
                "goats/materializeDisableBinding",
                Some(CatalogType::Materialization),
            ),
            ("goats/materializeMixed", Some(CatalogType::Materialization)),
        ],
    );

    // Update the inferred schema again and trigger the controller to publish
    harness
        .upsert_inferred_schema(mock_inferred_schema("goats/pasture", 2))
        .await;
    harness.run_pending_controller("goats/pasture").await;

    // Simulate an unsatisfiable constraint on the next publications of the materializations
    harness.control_plane().fail_next_build(
        "goats/materializeBackfill",
        UnsatisfiableConstraints {
            binding: 0,
            field: "p1",
        },
    );
    harness.control_plane().fail_next_build(
        "goats/materializeDisableBinding",
        UnsatisfiableConstraints {
            binding: 0,
            field: "p0",
        },
    );
    harness.control_plane().fail_next_build(
        "goats/materializeMixed",
        UnsatisfiableConstraints {
            binding: 0,
            field: "p0",
        },
    );

    harness.run_pending_controllers(None).await;
    // All consumers should have been published
    harness.control_plane().assert_activations(
        "after breaking change to pasture schema",
        vec![
            ("goats/pasture", Some(CatalogType::Collection)),
            ("goats/totes", Some(CatalogType::Collection)),
            (
                "goats/materializeBackfill",
                Some(CatalogType::Materialization),
            ),
            (
                "goats/materializeDisableBinding",
                Some(CatalogType::Materialization),
            ),
            ("goats/materializeMixed", Some(CatalogType::Materialization)),
        ],
    );

    let all_materializations = &[
        "goats/materializeBackfill",
        "goats/materializeDisableBinding",
        "goats/materializeMixed",
    ];
    let specs = materialization_specs(all_materializations, &mut harness).await;
    insta::assert_yaml_snapshot!("after-pasture-breaking-change", specs);

    // Next simulate an update the totes inferred schema, and expect that all materializations are
    // published successfully.
    harness
        .upsert_inferred_schema(mock_inferred_schema("goats/totes", 1))
        .await;
    harness.run_pending_controller("goats/totes").await;
    harness.run_pending_controllers(None).await;
    harness.control_plane().assert_activations(
        "after initial update of totes schema",
        vec![
            ("goats/totes", Some(CatalogType::Collection)),
            (
                "goats/materializeBackfill",
                Some(CatalogType::Materialization),
            ),
            (
                "goats/materializeDisableBinding",
                Some(CatalogType::Materialization),
            ),
            ("goats/materializeMixed", Some(CatalogType::Materialization)),
        ],
    );
    let specs = materialization_specs(all_materializations, &mut harness).await;
    insta::assert_yaml_snapshot!("after-totes-initial-schema", specs);

    // Simulate an unsatisfiable constraint on the next publications of the materializations
    harness.control_plane().fail_next_build(
        "goats/materializeBackfill",
        UnsatisfiableConstraints {
            binding: 1,
            field: "p1",
        },
    );
    // Binding is 0 here because the first binding in each of these materializations was disabled
    // and disabled bindings are not sent as part of the validate request.
    harness.control_plane().fail_next_build(
        "goats/materializeDisableBinding",
        UnsatisfiableConstraints {
            binding: 0,
            field: "p0",
        },
    );
    harness.control_plane().fail_next_build(
        "goats/materializeMixed",
        UnsatisfiableConstraints {
            binding: 0,
            field: "p0",
        },
    );
    harness
        .upsert_inferred_schema(mock_inferred_schema("goats/totes", 2))
        .await;
    harness.run_pending_controller("goats/totes").await;
    harness.run_pending_controllers(None).await;
    harness.control_plane().assert_activations(
        "after breaking changes to totes schema",
        vec![
            ("goats/totes", Some(CatalogType::Collection)),
            (
                "goats/materializeBackfill",
                Some(CatalogType::Materialization),
            ),
            (
                "goats/materializeDisableBinding",
                Some(CatalogType::Materialization),
            ),
            // note that materializeMixed should not have been activated due to
            // onIncompatibleSchemaChange: abort
        ],
    );
    let specs = materialization_specs(all_materializations, &mut harness).await;
    insta::assert_yaml_snapshot!("after-totes-breaking-change", specs);

    // expect that the materializeMixed controller has an error due to the binding with 'abort'
    let mixed_state = harness.get_controller_state("goats/materializeMixed").await;
    assert_eq!(1, mixed_state.failures);
    let error = mixed_state
        .error
        .as_ref()
        .expect("expected error to be Some");
    assert!(error.contains("incompatible schema changes observed for binding [binding-0] and onIncompatibleSchemaChange is 'abort'"));
    assert!(mixed_state.next_run.is_some()); // should have scheduled a re-try

    // Now re-try materializeMixed, and this time there is no unsatisfiable constraint. IRL, this
    // might happen if someone was manually updating the destination table.
    harness
        .run_pending_controller("goats/materializeMixed")
        .await;
    let mixed_state = harness.get_controller_state("goats/materializeMixed").await;
    // Expect that the error has been cleared, and the unchanged materialization spec was published
    assert_eq!(0, mixed_state.failures);
    assert!(mixed_state.error.is_none());

    insta::assert_yaml_snapshot!("after-materializeMixed-retry", mixed_state.live_spec);
}

#[tokio::test]
#[serial_test::serial]
async fn test_collection_key_changes() {
    let mut harness = TestHarness::init("test_dependencies_and_controllers").await;

    let user_id = harness.setup_tenant("camels").await;
    let draft = draft_catalog(serde_json::json!({
        "collections": {
            "camels/water": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "id1": { "type": "string" },
                        "id2": { "type": "string" }
                    },
                    "required": ["id1", "id2"]
                },
                "key": ["/id1"]
            },
        },
        // A capture is necessary, otherwise the collection would get pruned
        "captures": {
            "camels/capture": {
                "endpoint": {
                    "connector": {
                        "image": "source/test:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": { "thing": "water" },
                        "target": "camels/water"
                    },
                ]
            },
        }
    }));

    let initial_result = harness
        .user_publication(user_id, "initial publication", draft)
        .await;
    assert!(initial_result.status.is_success());
    harness.run_pending_controllers(None).await;

    // Simulate a user-initiated change to partitions, and expect a job status with
    // incompatible_collections that need re-created.
    let partition_change_draft = draft_catalog(serde_json::json!({
        "collections": {
            "camels/water": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "id1": { "type": "string" },
                        "id2": { "type": "string" }
                    },
                    "required": ["id1", "id2"]
                },
                "key": ["/id1"],
                "projections": {
                    "naughty": {
                        "location": "/id2",
                        "partition": true
                    }
                }
            },
        },
    }));
    let update_result = harness
        .user_publication(user_id, "update partitions", partition_change_draft)
        .await;
    insta::assert_debug_snapshot!(update_result.status, @r###"
    BuildFailed {
        incompatible_collections: [
            IncompatibleCollection {
                collection: "camels/water",
                requires_recreation: [
                    PartitionChange,
                ],
                affected_materializations: [],
            },
        ],
        evolution_id: None,
    }
    "###);

    // Simulate an auto-discover publication that changes the key, and expect that
    // there's an evolution created. This scenario could continue, but is being cut
    // short because of imminent changes to auto-discovers.
    let key_change_draft = draft_catalog(serde_json::json!({
        "collections": {
            "camels/water": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "id1": { "type": "string" },
                        "id2": { "type": "string" }
                    },
                    "required": ["id1", "id2"]
                },
                "key": ["/id2"]
            },
        },
    }));
    let update_result = harness
        .auto_discover_publication(key_change_draft, true)
        .await;
    let JobStatus::BuildFailed {
        incompatible_collections,
        evolution_id,
    } = &update_result.status
    else {
        panic!("expected buildFailed, got: {:?}", update_result.status);
    };

    insta::assert_debug_snapshot!(incompatible_collections, @r###"
    [
        IncompatibleCollection {
            collection: "camels/water",
            requires_recreation: [
                KeyChange,
            ],
            affected_materializations: [],
        },
    ]
    "###);
    let Some(evolution_id) = evolution_id else {
        panic!("expected an evolution was created, but no id present");
    };
    let evo = sqlx::query!(
        r##"select auto_publish, background from evolutions where id = $1"##,
        agent_sql::Id::from(*evolution_id) as agent_sql::Id
    )
    .fetch_one(&harness.pool)
    .await
    .unwrap();

    assert!(evo.auto_publish);
    assert!(evo.background);
}

#[derive(Debug)]
struct UnsatisfiableConstraints {
    binding: usize,
    field: &'static str,
}
impl FailBuild for UnsatisfiableConstraints {
    fn modify(&mut self, result: &mut UncommittedBuild) {
        let Some(mat) = result
            .output
            .built
            .built_materializations
            .iter_mut()
            .filter(|m| !m.is_unchanged())
            .next()
        else {
            panic!("no materialization in build");
        };
        let Some(validated) = mat.validated.as_mut() else {
            panic!("validated must be Some");
        };
        tracing::warn!(materialization = %mat.materialization, binding = %self.binding, "setting binding field to unsatisfiable");
        let binding = validated
            .bindings
            .get_mut(self.binding)
            .expect("binding does not exist");

        let Some(constraint) = binding.constraints.get_mut(self.field) else {
            panic!(
                "no such field '{}' in mat '{}' bindings: {:?}",
                self.field, mat.materialization, validated.bindings
            )
        };
        constraint.r#type = ConstraintType::Unsatisfiable as i32;
        constraint.reason = "mock unsatisfiable field".to_string();

        result.output.built.errors.insert(tables::Error {
            scope: tables::synthetic_scope(
                CatalogType::Materialization,
                mat.materialization.as_str(),
            ),
            error: anyhow::anyhow!("omg an unsatisfiable constraint"),
        });
    }
}

fn unwrap_built_collection(state: &ControllerState) -> &proto_flow::flow::CollectionSpec {
    match state.built_spec.as_ref().expect("missing built spec") {
        proto_flow::AnyBuiltSpec::Collection(cs) => cs,
        other => panic!("expected built CollectionSpec, got: {other:?}"),
    }
}

async fn materialization_specs(
    names: &[&str],
    harness: &mut TestHarness,
) -> BTreeMap<models::Materialization, models::MaterializationDef> {
    let name_set = names.iter().map(|n| n.to_string()).collect::<BTreeSet<_>>();
    let live = harness
        .control_plane()
        .get_live_specs(name_set)
        .await
        .unwrap();
    // The results are already sorted in the table.
    live.materializations
        .into_iter()
        .map(|m| (m.materialization, m.model))
        .collect()
}
