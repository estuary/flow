use std::collections::{BTreeMap, BTreeSet};

use super::harness::{draft_catalog, mock_inferred_schema, FailBuild, TestHarness};
use crate::{controllers::ControllerState, publications::UncommittedBuild, ControlPlane};
use models::CatalogType;
use proto_flow::materialize::response::validated::{
    constraint::Type as ConstraintType, Constraint,
};
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

    // Assert that the pasture collection has had the inferred schema placeholder added in both
    // the model and the built spec.
    let pasture_state = harness.get_controller_state("goats/pasture").await;
    let pasture_model = pasture_state
        .live_spec
        .as_ref()
        .unwrap()
        .as_collection()
        .unwrap();
    assert!(pasture_model
        .read_schema
        .as_ref()
        .unwrap()
        .get()
        .contains("inferredSchemaIsNotAvailable"));
    let pasture_spec = unwrap_built_collection(&pasture_state);
    assert!(pasture_spec
        .read_schema_json
        .contains("inferredSchemaIsNotAvailable"));
    // A collection that uses schema inferrence should always have a wake_at scheduled
    harness.assert_controller_pending("goats/pasture").await;

    // Assert that the totes collection has _not_ had the inferred schema placeholder added
    let totes_state = harness.get_controller_state("goats/totes").await;
    harness.assert_controller_pending("goats/totes").await;
    let totes_spec = unwrap_built_collection(&totes_state);
    assert!(totes_spec
        .read_schema_json
        .contains("inferredSchemaIsNotAvailable"));
    // Assert that the schema in the built spec _does_ contain the bundled write schema
    assert_eq!(
        3,
        totes_spec
            .read_schema_json
            .matches(models::Schema::REF_WRITE_SCHEMA_URL)
            .count()
    );

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
    assert!(error.contains("incompatible schema changes observed for binding [totes] and onIncompatibleSchemaChange is 'abort'"), "unexpected error: {error}");
    // should have scheduled a re-try
    harness
        .assert_controller_pending("goats/materializeMixed")
        .await;

    // Now re-try materializeMixed, and this time there is no unsatisfiable constraint. IRL, this
    // might happen if someone was manually updating the destination table.
    // We need to first skip past the backoff from the failed publication.
    let new_last_attempt = harness.control_plane().current_time() - chrono::Duration::minutes(2);
    harness
        .push_back_last_pub_history_ts("goats/materializeMixed", new_last_attempt)
        .await;
    harness
        .run_pending_controller("goats/materializeMixed")
        .await;
    let mixed_state = harness.get_controller_state("goats/materializeMixed").await;
    // Expect that the error has been cleared, and the unchanged materialization spec was published
    assert_eq!(0, mixed_state.failures);
    assert!(mixed_state.error.is_none());

    insta::assert_yaml_snapshot!("after-materializeMixed-retry", mixed_state.live_spec);
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
            .filter(|m| !m.is_passthrough())
            .next()
        else {
            panic!("no materialization in build");
        };
        let Some(validated) = mat.validated.as_mut() else {
            panic!("validated must be Some");
        };
        tracing::warn!(materialization = %mat.materialization, binding = %self.binding, "setting binding field to unsatisfiable");

        validated
            .bindings
            .resize(self.binding + 1, Default::default());

        validated.bindings[self.binding].constraints = [(
            self.field.to_string(),
            Constraint {
                r#type: ConstraintType::Unsatisfiable as i32,
                reason: "mock unsatisfiable field".to_string(),
            },
        )]
        .into_iter()
        .collect();

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
