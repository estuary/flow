use std::collections::BTreeSet;

use crate::{
    controllers::ControllerState,
    integration_tests::harness::{
        draft_catalog, get_collection_generation_id, mock_inferred_schema, InjectBuildError,
        TestHarness,
    },
    ControlPlane,
};
use models::{
    status::{publications::PublicationStatus, ShardRef},
    CatalogType,
};
use uuid::Uuid;

#[tokio::test]
#[serial_test::serial]
async fn test_activations_performed_after_publication_failure() {
    let mut harness = TestHarness::init("test_activations_take_precedence_over_publications").await;
    let _user_id = harness.setup_tenant("muskrats").await;

    let draft = draft_catalog(serde_json::json!({
        "collections": {
            "muskrats/water": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    }
                },
                "key": ["/id"]
            },
            "muskrats/sedges": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    }
                },
                "key": ["/id"],
                "derive": {
                    "using": { "sqlite": { "migrations": [] } },
                    "transforms": [
                        {
                            "name": "fromWater",
                            "source": "muskrats/water",
                            "lambda": "select $id;",
                            "shuffle": "any"
                        }
                    ]
                }
            },
        },
        "captures": {
            "muskrats/capture": {
                "endpoint": {
                    "connector": { "image": "source/test:test", "config": {} }
                },
                "bindings": [
                    { "resource": { "table": "water" }, "target": "muskrats/water" }
                ]
            }
        },
        "materializations": {
            "muskrats/materialize": {
                "endpoint": {
                    "connector": { "image": "materialize/test:test", "config": {} }
                },
                "bindings": [
                    { "resource": { "table": "sedges" }, "source": "muskrats/sedges" },
                    { "resource": { "table": "water" }, "source": "muskrats/water" },
                ]
            }
        },
    }));

    let result = harness
        .control_plane()
        .publish(
            Some(format!("initial publication")),
            Uuid::new_v4(),
            draft,
            Some("ops/dp/public/test".to_string()),
        )
        .await
        .expect("initial publish failed");
    assert!(
        result.status.is_success(),
        "publication failed with: {:?}",
        result.draft_errors()
    );

    harness.run_pending_controllers(None).await;
    harness.control_plane().assert_activations(
        "initial activations",
        vec![
            ("muskrats/capture", Some(CatalogType::Capture)),
            ("muskrats/materialize", Some(CatalogType::Materialization)),
            ("muskrats/sedges", Some(CatalogType::Collection)),
            ("muskrats/water", Some(CatalogType::Collection)),
        ],
    );

    // publish water, so that all other specs will need to publish in response
    let draft = draft_catalog(serde_json::json!({
        "collections": {
            "muskrats/water": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    }
                },
                "key": ["/id"]
            },
        },
    }));
    let result = harness
        .control_plane()
        .publish(None, Uuid::new_v4(), draft, None)
        .await
        .expect("publish failed");
    assert!(result.status.is_success());

    // We'll follow the same pattern for the capture, derivation, and
    // materialization. Setup a publication failure, and fail the task shard,
    // setting up a scenario where controllers will need to both publish and
    // activate. Expect that the activation still completes successfully, and
    // that the publication failure is recorded as expected.
    // Capture
    harness.control_plane().fail_next_build(
        "muskrats/capture",
        InjectBuildError::new(
            tables::synthetic_scope("capture", "muskrats/capture"),
            anyhow::anyhow!("simulated build failure"),
        ),
    );
    let capture_state = harness.get_controller_state("muskrats/capture").await;
    harness
        .fail_shard(&ShardRef {
            name: "muskrats/capture".to_string(),
            build: capture_state.last_build_id,
            key_begin: "00000000".to_string(),
            r_clock_begin: "00000000".to_string(),
        })
        .await;
    let capture_state = harness.run_pending_controller("muskrats/capture").await;
    assert!(capture_state.error.is_some());
    assert_last_publication_failed(&capture_state.current_status.unwrap_capture().publications);
    harness.control_plane().assert_activations(
        "after capture shard failure",
        vec![("muskrats/capture", Some(CatalogType::Capture))],
    );

    // Materialization
    harness.control_plane().fail_next_build(
        "muskrats/materialize",
        InjectBuildError::new(
            tables::synthetic_scope("materialize", "muskrats/materialize"),
            anyhow::anyhow!("simulated build failure"),
        ),
    );
    let materialize_state = harness.get_controller_state("muskrats/materialize").await;
    harness
        .fail_shard(&ShardRef {
            name: "muskrats/materialize".to_string(),
            build: materialize_state.last_build_id,
            key_begin: "00000000".to_string(),
            r_clock_begin: "00000000".to_string(),
        })
        .await;
    let materialize_state = harness.run_pending_controller("muskrats/materialize").await;
    assert!(materialize_state.error.is_some());
    assert_last_publication_failed(
        &materialize_state
            .current_status
            .unwrap_materialization()
            .publications,
    );
    harness.control_plane().assert_activations(
        "after materialize shard failure",
        vec![("muskrats/materialize", Some(CatalogType::Materialization))],
    );

    // Derivation
    harness.control_plane().fail_next_build(
        "muskrats/sedges",
        InjectBuildError::new(
            tables::synthetic_scope("collection", "muskrats/sedges"),
            anyhow::anyhow!("simulated build failure"),
        ),
    );
    let sedges_state = harness.get_controller_state("muskrats/sedges").await;
    harness
        .fail_shard(&ShardRef {
            name: "muskrats/sedges".to_string(),
            build: sedges_state.last_build_id,
            key_begin: "00000000".to_string(),
            r_clock_begin: "00000000".to_string(),
        })
        .await;
    let sedges_state = harness.run_pending_controller("muskrats/sedges").await;
    assert!(sedges_state.error.is_some());
    assert_last_publication_failed(&sedges_state.current_status.unwrap_collection().publications);
    harness.control_plane().assert_activations(
        "after derivation shard failure",
        vec![("muskrats/sedges", Some(CatalogType::Collection))],
    );
}

fn assert_last_publication_failed(ps: &PublicationStatus) {
    let last = ps
        .history
        .front()
        .expect("expected at least 1 publication in history, got 0");
    assert!(
        !last.errors.is_empty(),
        "expected at least 1 publication error, got 0"
    );
    let status = last
        .result
        .as_ref()
        .expect("missing publication result status");
    assert!(!status.is_success());
}

#[tokio::test]
#[serial_test::serial]
async fn test_dependencies_and_controllers() {
    let mut harness = TestHarness::init("test_dependencies_and_controllers").await;

    // This test is focusing on controller-initiated publications, and how
    // controllers respond to changes in their dependencies. The user id is
    // irrelevant because controllers always publish as the "system user".
    let _user_id = harness.setup_tenant("owls").await;

    let draft = draft_catalog(serde_json::json!({
        "collections": {
            "owls/hoots": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    }
                },
                "key": ["/id"]
            },
            "owls/nests": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    }
                },
                "key": ["/id"],
                "derive": {
                    "using": {
                        "sqlite": { "migrations": [] }
                    },
                    "transforms": [
                        {
                            "name": "fromHoots",
                            "source": "owls/hoots",
                            "lambda": "select $id;",
                            "shuffle": "any"
                        }
                    ]
                }
            }
        },
        "captures": {
            "owls/capture": {
                "endpoint": {
                    "connector": {
                        "image": "ghcr.io/estuary/source-hello-world:dev",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": {
                            "name": "greetings",
                            "prefix": "Hello {}!"
                        },
                        "target": "owls/hoots"
                    }
                ]
            }
        },
        "materializations": {
            "owls/materialize": {
                "sourceCapture": "owls/capture",
                "endpoint": {
                    "connector": {
                        "image": "materialize/test:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": { "table": "hoots" },
                        "source": "owls/hoots"
                    },
                    {
                        "resource": { "table": "nests" },
                        "source": "owls/nests"
                    }
                ]
            }
        },
        "tests": {
            "owls/test-test": {
                "description": "a test of testing",
                "steps": [
                    {"ingest": {
                        "collection": "owls/hoots",
                        "documents": [{"id": "hooty hoot!"}]
                    }},
                    {"verify": {
                        "collection": "owls/nests",
                        "documents": [{"id": "hooty hoot!"}]
                    }}
                ]
            }
        }
    }));

    let result = harness
        .control_plane()
        .publish(
            Some(format!("initial publication")),
            Uuid::new_v4(),
            draft,
            Some("ops/dp/public/test".to_string()),
        )
        .await
        .expect("initial publish failed");
    assert!(
        result.status.is_success(),
        "publication failed with: {:?}",
        result.draft_errors()
    );
    assert_eq!(5, result.draft.spec_count());

    let all_names = [
        "owls/hoots",
        "owls/capture",
        "owls/materialize",
        "owls/nests",
        "owls/test-test",
    ]
    .iter()
    .map(|n| n.to_string())
    .collect::<BTreeSet<_>>();

    // Controller runs should have been immediately enqueued for all published specs.
    let due_controllers = harness
        .get_enqueued_controllers(chrono::Duration::seconds(1))
        .await;
    assert_eq!(
        all_names.iter().cloned().collect::<Vec<_>>(),
        due_controllers
    );

    let runs = harness.run_pending_controllers(None).await;
    let run_names = runs
        .into_iter()
        .map(|s| s.catalog_name)
        .collect::<BTreeSet<_>>();
    assert_eq!(all_names, run_names);
    harness.control_plane().assert_activations(
        "initial activations",
        vec![
            ("owls/hoots", Some(CatalogType::Collection)),
            ("owls/capture", Some(CatalogType::Capture)),
            ("owls/materialize", Some(CatalogType::Materialization)),
            ("owls/nests", Some(CatalogType::Collection)),
        ],
    );
    harness.control_plane().reset_activations();

    // Fetch and re-publish just the hoots collection. This should trigger controller updates of
    // all the other specs.
    let live_hoots = harness
        .control_plane()
        .get_collection(models::Collection::new("owls/hoots"))
        .await
        .unwrap()
        .expect("hoots spec must be Some");
    let hoots_last_activated = live_hoots.last_build_id;
    let mut draft = tables::DraftCatalog::default();
    draft.collections.insert(tables::DraftCollection {
        collection: models::Collection::new("owls/hoots"),
        scope: tables::synthetic_scope(models::CatalogType::Collection, "owls/hoots"),
        expect_pub_id: None,
        model: Some(live_hoots.model.clone()),
        is_touch: false,
    });

    let result = harness
        .control_plane()
        .publish(
            Some("test publication of owls/hoots".to_string()),
            Uuid::new_v4(),
            draft,
            Some("ops/dp/public/test".to_string()),
        )
        .await
        .expect("publication failed");
    assert_eq!(1, result.draft.spec_count());
    assert!(result.status.is_success());

    // Simulate a failed call to activate the collection in the data plane
    harness.control_plane().fail_next_activation("owls/hoots");
    // Fetch the other specs, so we can assert that they get touched
    let mut all_except_hoots = all_names.clone();
    all_except_hoots.remove("owls/hoots");
    let starting_specs = harness
        .control_plane()
        .get_live_specs(all_except_hoots.clone())
        .await
        .unwrap();

    let runs = harness.run_pending_controllers(None).await;
    assert_controllers_ran(
        &[
            "owls/capture",
            "owls/materialize",
            "owls/hoots",
            "owls/nests",
            "owls/test-test",
        ],
        runs,
    );

    // Assert that the hoots controller_jobs row recorded the failure.
    let hoots_state = harness.get_controller_state("owls/hoots").await;
    // The hoots controller may have been run multiple times, due to its
    // dependencies being published, but we'd expect each run to result
    // in the same activation error.
    assert!(hoots_state.failures > 0);
    assert!(hoots_state
        .error
        .as_ref()
        .unwrap()
        .contains("data_plane_delete simulated failure"));
    assert_eq!(
        hoots_last_activated,
        hoots_state
            .current_status
            .unwrap_collection()
            .activation
            .last_activated,
        "expect hoots last_activated to be unchanged"
    );

    // Assert that other specs where touched and activated
    harness.assert_specs_touched_since(&starting_specs).await;
    harness.control_plane().assert_activations(
        "subsequent activations",
        vec![
            ("owls/capture", Some(CatalogType::Capture)),
            ("owls/materialize", Some(CatalogType::Materialization)),
            ("owls/nests", Some(CatalogType::Collection)),
            // tests do not get activated
        ],
    );

    // Now re-try the hoots controller, and expect it to have recovered from the error
    harness.control_plane().reset_activations();
    let run = harness.run_pending_controller("owls/hoots").await;
    assert_eq!("owls/hoots", &run.catalog_name);
    harness.control_plane().assert_activations(
        "hoots publish",
        vec![("owls/hoots", Some(CatalogType::Collection))],
    );
    let hoots_state = harness.get_controller_state("owls/hoots").await;
    assert_eq!(0, hoots_state.failures);
    assert!(hoots_state.error.is_none());
    assert_eq!(
        hoots_state.last_build_id,
        hoots_state
            .current_status
            .unwrap_collection()
            .activation
            .last_activated
    );
    assert!(
        hoots_state.last_build_id > hoots_last_activated,
        "sanity check that the last_build_id increased"
    );

    // Publish hoots again and expect dependents to be touched again in response.
    // This time, we'll assert that the dependent's publication histories have collapsed both
    // touch publications into one history entry.
    let starting_specs = harness
        .control_plane()
        .get_live_specs(all_except_hoots)
        .await
        .unwrap();

    let mut draft = tables::DraftCatalog::default();
    draft.collections.insert(tables::DraftCollection {
        collection: models::Collection::new("owls/hoots"),
        scope: tables::synthetic_scope(models::CatalogType::Collection, "owls/hoots"),
        expect_pub_id: None,
        model: Some(live_hoots.model.clone()),
        is_touch: false,
    });
    harness
        .control_plane()
        .publish(
            Some("3rd pub of hoots".to_string()),
            Uuid::new_v4(),
            draft,
            Some("ops/dp/public/test".to_string()),
        )
        .await
        .expect("publication must succeed");
    let runs = harness.run_pending_controllers(None).await;
    assert_controllers_ran(
        &[
            "owls/capture",
            "owls/materialize",
            "owls/hoots",
            "owls/nests",
            "owls/test-test",
        ],
        runs,
    );
    harness.assert_specs_touched_since(&starting_specs).await;
    harness.control_plane().assert_activations(
        "3rd activations",
        vec![
            ("owls/capture", Some(CatalogType::Capture)),
            ("owls/materialize", Some(CatalogType::Materialization)),
            ("owls/nests", Some(CatalogType::Collection)),
            ("owls/hoots", Some(CatalogType::Collection)),
            // tests do not get activated
        ],
    );

    let mat_state = harness.get_controller_state("owls/materialize").await;
    let mat_history = &mat_state
        .current_status
        .unwrap_materialization()
        .publications
        .history;
    assert_eq!(
        1,
        mat_history.len(),
        "unexpected entry count in materialize pub history: {:?}",
        mat_history
    );

    // Insert an inferred schema so that we can assert it gets deleted along with the collection
    let hoots_generation_id = get_collection_generation_id(&hoots_state);
    harness
        .upsert_inferred_schema(mock_inferred_schema("owls/hoots", hoots_generation_id, 3))
        .await;

    // Publish a deletion of the collection, and then assert that the dependents can still be
    // notified after the deletion
    let mut draft = tables::DraftCatalog::default();
    draft.delete("owls/hoots", CatalogType::Collection, None);
    let del_result = harness
        .control_plane()
        .publish(
            Some("delete owls/hoots".to_string()),
            Uuid::new_v4(),
            draft,
            Some("ops/dp/public/test".to_string()),
        )
        .await
        .expect("failed to publish collection deletion");
    assert!(del_result.status.is_success());
    harness
        .assert_live_spec_soft_deleted("owls/hoots", del_result.pub_id)
        .await;

    // All the controllers ought to run now. The collection controller should
    // run first and notfiy the others. Note that `run_pending_controllers`
    // cannot return anything when the spec is deleted since the rows will have
    // been deleted from the database.
    harness.run_pending_controllers(None).await;
    harness.assert_live_spec_hard_deleted("owls/hoots").await;

    let _ = harness.run_pending_controllers(None).await;
    harness.control_plane().assert_activations(
        "after hoots deleted",
        vec![
            ("owls/hoots", None),
            ("owls/capture", Some(CatalogType::Capture)),
            ("owls/materialize", Some(CatalogType::Materialization)),
            ("owls/nests", Some(CatalogType::Collection)),
        ],
    );

    // The capture binding ought to have been disabled.
    let capture_state = harness.get_controller_state("owls/capture").await;
    let capture_model = capture_state
        .live_spec
        .as_ref()
        .unwrap()
        .as_capture()
        .unwrap();
    assert!(capture_model.bindings[0].disable);
    assert_eq!(0, capture_state.failures);
    assert_eq!(
        Some("in response to deletion one or more depencencies, disabled 1 binding(s) in response to deleted collections: [owls/hoots]"),
        capture_state
            .current_status
            .unwrap_capture()
            .publications
            .history[0]
            .detail
            .as_deref()
    );

    // The derivation transform should have been disabled
    let derivation_state = harness.get_controller_state("owls/nests").await;

    let derivation_model = derivation_state
        .live_spec
        .as_ref()
        .unwrap()
        .as_collection()
        .unwrap()
        .derive
        .as_ref()
        .unwrap();
    assert!(derivation_model.transforms[0].disable);
    assert_eq!(
        Some("in response to deletion one or more depencencies, disabled 1 transform(s) in response to deleted collections: [owls/hoots]"),
        derivation_state
            .current_status
            .unwrap_collection()
            .publications
            .history[0]
            .detail
            .as_deref()
    );

    // The materialization binding should have been disabled
    let materialization_state = harness.get_controller_state("owls/materialize").await;
    let materialization_model = materialization_state
        .live_spec
        .as_ref()
        .unwrap()
        .as_materialization()
        .unwrap();
    assert!(materialization_model.bindings[0].disable);
    assert!(!materialization_model.bindings[1].disable); // nests binding should still be enabled

    // The materialization controller should have run 3 times. Once in response to hoots being
    // deleted, again in response to the publication of the source capture, and again in response
    // to the publication of the derivation (both of which also published in response to the hoots
    // deletion).
    let expected = "in response to deletion one or more depencencies, \ndisabled binding of deleted collection owls/hoots";
    let history = &materialization_state
        .current_status
        .unwrap_materialization()
        .publications
        .history;
    assert!(
        history
            .iter()
            .any(|item| item.detail.as_deref() == Some(expected)),
        "missing expected detail in publication history: {history:?}"
    );

    // The test spec should not have been updated at all, but it should now be showing as failing
    let test_state = harness.get_controller_state("owls/test-test").await;
    let test_status = test_state.current_status.unwrap_test();
    assert!(!test_status.passing);

    let actual_error = test_state
        .error
        .expect("expected controller error to be Some");
    assert_eq!(
        "updating model in response to deleted dependencies: test failed because 1 of the collection(s) it depends on have been deleted",
        &actual_error
    );

    // Delete the capture, and expect the materialization to respond by removing the `sourceCapture`
    let mut draft = tables::DraftCatalog::default();
    draft.delete("owls/capture", CatalogType::Capture, None);
    let result = harness
        .control_plane()
        .publish(
            Some("deleting capture".to_string()),
            Uuid::new_v4(),
            draft,
            Some("ops/dp/public/test".to_string()),
        )
        .await
        .expect("failed to publish");
    assert!(result.status.is_success());
    harness
        .assert_live_spec_soft_deleted("owls/capture", result.pub_id)
        .await;

    harness.control_plane().fail_next_build(
        "owls/materialize",
        InjectBuildError::new(
            tables::synthetic_scope("materialization", "owls/materialize"),
            anyhow::anyhow!("simulated build failure"),
        ),
    );
    harness.control_plane().reset_activations();
    let runs = harness.run_pending_controllers(None).await;
    assert_controllers_ran(&["owls/materialize"], runs);

    harness.assert_live_spec_hard_deleted("owls/capture").await;
    harness
        .control_plane()
        .assert_activations("after capture deleted", vec![("owls/capture", None)]);
    // Assert that the materialization recorded the build error and has a retry scheduled
    let materialization_state = harness.get_controller_state("owls/materialize").await;
    let failed_pub = &materialization_state
        .current_status
        .unwrap_materialization()
        .publications
        .history[0];
    assert_eq!("simulated build failure", &failed_pub.errors[0].detail);
    harness.assert_controller_pending("owls/materialize").await;

    // Assert that the controller backs off
    let materialization_state = harness.run_pending_controller("owls/materialize").await;
    let error = materialization_state
        .error
        .as_deref()
        .expect("expected controller error, got None");
    assert!(
        error.contains("backing off dependency update"),
        "unexpected error, got: '{error}'"
    );
    let last_pub = &materialization_state
        .current_status
        .unwrap_materialization()
        .publications
        .history[0];
    assert!(!last_pub.is_success());
    assert_eq!(1, last_pub.count, "expect a single attempted publication");
    harness
        .push_back_last_pub_history_ts(
            "owls/materialize",
            last_pub.completed.unwrap() - chrono::Duration::minutes(2),
        )
        .await;

    // The materialization should now successfully retry and then activate
    harness.run_pending_controller("owls/materialize").await;
    let materialization_state = harness.get_controller_state("owls/materialize").await;
    let success_pub = &materialization_state
        .current_status
        .unwrap_materialization()
        .publications
        .history[0];
    assert!(success_pub.is_success());
    // The sourceCapture should have been removed
    let materialization_model = materialization_state
        .live_spec
        .as_ref()
        .unwrap()
        .as_materialization()
        .unwrap();
    assert_eq!(
        Some(&models::SourceType::Configured(models::SourceDef::default())),
        materialization_model.source.as_ref()
    );

    harness.run_pending_controller("owls/materialize").await;
    harness.control_plane().assert_activations(
        "after capture deleted",
        vec![("owls/materialize", Some(CatalogType::Materialization))],
    );

    // Publish deletions of all the remaining tasks
    let mut draft = tables::DraftCatalog::default();
    draft.delete("owls/materialize", CatalogType::Materialization, None);
    draft.delete("owls/nests", CatalogType::Collection, None);
    draft.delete("owls/test-test", CatalogType::Test, None);

    let del_result = harness
        .control_plane()
        .publish(
            Some("delete owls/ stuff".to_string()),
            Uuid::new_v4(),
            draft,
            Some("ops/dp/public/test".to_string()),
        )
        .await
        .expect("failed to publish deletions");
    assert!(del_result.status.is_success());
    harness
        .assert_live_spec_soft_deleted("owls/materialize", del_result.pub_id)
        .await;
    harness
        .assert_live_spec_soft_deleted("owls/nests", del_result.pub_id)
        .await;
    harness
        .assert_live_spec_soft_deleted("owls/test-test", del_result.pub_id)
        .await;

    harness.run_pending_controllers(None).await;
    harness
        .assert_live_spec_hard_deleted("owls/materialize")
        .await;
    harness.assert_live_spec_hard_deleted("owls/nests").await;
    harness
        .assert_live_spec_hard_deleted("owls/test-test")
        .await;
}

fn assert_controllers_ran(expected: &[&str], actual: Vec<ControllerState>) {
    let actual_names = actual
        .iter()
        .map(|s| s.catalog_name.as_str())
        .collect::<BTreeSet<_>>();
    let expected_names = expected.into_iter().map(|n| *n).collect::<BTreeSet<_>>();
    assert_eq!(
        expected_names, actual_names,
        "mismatched controller runs, expected"
    );
}
