use std::collections::BTreeSet;

use super::harness::{self, draft_catalog, TestHarness};
use crate::{
    controllers::ControllerState, integration_tests::harness::mock_inferred_schema, ControlPlane,
};
use models::CatalogType;
use uuid::Uuid;

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
                        "image": "ghcr.io/estuary/materialize-postgres:dev",
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

    let first_pub_id = harness.control_plane().next_pub_id();
    let result = harness
        .control_plane()
        .publish(
            first_pub_id,
            Some(format!("initial publication")),
            Uuid::new_v4(),
            draft,
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
    let live = harness
        .control_plane()
        .get_live_specs(std::iter::once("owls/hoots".to_string()).collect())
        .await
        .expect("failed to fetch hoots");

    let next_pub = harness.control_plane().next_pub_id();
    let result = harness
        .control_plane()
        .publish(
            next_pub,
            Some("test publication of owls/hoots".to_string()),
            Uuid::new_v4(),
            tables::DraftCatalog::from(live),
        )
        .await
        .expect("publication failed");
    assert_eq!(1, result.draft.spec_count());
    assert!(result.status.is_success());

    // Simulate a failed call to activate the collection in the data plane
    harness.control_plane().fail_next_activation("owls/hoots");
    // Only the hoots controller should have run, because it should not have
    // notified dependents yet due to the activation failure.
    let runs = harness.run_pending_controllers(None).await;
    assert_eq!(1, runs.len());
    assert_eq!("owls/hoots", &runs[0].catalog_name);
    // Assert that the controller_jobs row recorded the failure.
    let hoots_state = harness.get_controller_state("owls/hoots").await;
    assert_eq!(1, hoots_state.failures);
    assert!(hoots_state
        .error
        .as_ref()
        .unwrap()
        .contains("data_plane_delete simulated failure"));
    assert_eq!(
        first_pub_id,
        hoots_state
            .current_status
            .unwrap_collection()
            .activation
            .last_activated
    );
    harness.control_plane().reset_activations();

    // Now re-try the controller, and expect it to have recovered from the error
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
        next_pub,
        hoots_state
            .current_status
            .unwrap_collection()
            .activation
            .last_activated
    );

    // Other controllers should run now that the activation of hoots was successful.
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
    harness.control_plane().assert_activations(
        "subsequent activations",
        vec![
            ("owls/capture", Some(CatalogType::Capture)),
            ("owls/materialize", Some(CatalogType::Materialization)),
            ("owls/nests", Some(CatalogType::Collection)),
            // tests do not get activated
        ],
    );
    // Insert an inferred schema so that we can assert it gets deleted along with the collection
    harness
        .upsert_inferred_schema(mock_inferred_schema("owls/hoots", 3))
        .await;

    // Publish a deletion of the collection, and then assert that the dependents can still be
    // notified after the deletion
    let mut draft = tables::DraftCatalog::default();
    draft.delete("owls/hoots", CatalogType::Collection, None);
    let del_pub_id = harness.control_plane().next_pub_id();
    let del_result = harness
        .control_plane()
        .publish(
            del_pub_id,
            Some("delete owls/hoots".to_string()),
            Uuid::new_v4(),
            draft,
        )
        .await
        .expect("failed to publish collection deletion");
    assert!(del_result.status.is_success());
    harness
        .assert_live_spec_soft_deleted("owls/hoots", del_pub_id)
        .await;

    // All the controllers ought to run now. The collection controller should run first and notfiy
    // the others.
    let runs = harness.run_pending_controllers(Some(1)).await;
    assert_eq!("owls/hoots", &runs[0].catalog_name);
    harness
        .control_plane()
        .assert_activations("hoots deletion", vec![("owls/hoots", None)]);
    harness.assert_live_spec_hard_deleted("owls/hoots").await;

    let _ = harness.run_pending_controllers(None).await;
    harness.control_plane().assert_activations(
        "after hoots deleted",
        vec![
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
        Some("in response to publication of one or more depencencies, disabled 1 binding(s) in response to deleted collections: [owls/hoots]"),
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
        Some("in response to publication of one or more depencencies, disabled 1 transform(s) in response to deleted collections: [owls/hoots]"),
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
    let expected = "in response to publication of one or more depencencies, disabled 1 binding(s) in response to deleted collections: [owls/hoots]";
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
    assert!(!test_status.publications.history[0].is_success());
    let err = &test_status.publications.history[0].errors[0];
    assert_eq!("collection owls/hoots, referenced by this test step, is not defined; did you mean owls/nests defined at flow://collection/owls/nests ?", err.detail);

    // Delete the capture, and expect the materialization to respond by removing the `sourceCapture`
    let mut draft = tables::DraftCatalog::default();
    draft.delete("owls/capture", CatalogType::Capture, None);
    let del_pub_id = harness.control_plane().next_pub_id();
    let result = harness
        .control_plane()
        .publish(
            del_pub_id,
            Some("deleting capture".to_string()),
            Uuid::new_v4(),
            draft,
        )
        .await
        .expect("failed to publish");
    assert!(result.status.is_success());
    harness
        .assert_live_spec_soft_deleted("owls/capture", del_pub_id)
        .await;

    harness.control_plane().fail_next_build(
        "owls/materialize",
        BuildFailure {
            catalog_name: "owls/materialize",
            catalog_type: CatalogType::Materialization,
        },
    );

    let runs = harness.run_pending_controllers(None).await;
    assert_controllers_ran(&["owls/capture", "owls/materialize"], runs);

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
    assert!(materialization_state.next_run.is_some());

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
    assert!(materialization_model.source_capture.is_none());

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

    let del_pub_id = harness.control_plane().next_pub_id();
    let del_result = harness
        .control_plane()
        .publish(
            del_pub_id,
            Some("delete owls/ stuff".to_string()),
            Uuid::new_v4(),
            draft,
        )
        .await
        .expect("failed to publish deletions");
    assert!(del_result.status.is_success());
    harness
        .assert_live_spec_soft_deleted("owls/materialize", del_pub_id)
        .await;
    harness
        .assert_live_spec_soft_deleted("owls/nests", del_pub_id)
        .await;
    harness
        .assert_live_spec_soft_deleted("owls/test-test", del_pub_id)
        .await;

    let runs = harness.run_pending_controllers(None).await;
    assert_eq!(
        3,
        runs.len(),
        "expected one run of each controller, got: {runs:?}"
    );
    harness
        .assert_live_spec_hard_deleted("owls/materialize")
        .await;
    harness.assert_live_spec_hard_deleted("owls/nests").await;
    harness
        .assert_live_spec_hard_deleted("owls/test-test")
        .await;
}

#[derive(Debug)]
struct BuildFailure {
    catalog_name: &'static str,
    catalog_type: CatalogType,
}
impl harness::FailBuild for BuildFailure {
    fn modify(&mut self, result: &mut crate::publications::UncommittedBuild) {
        result.output.built.errors.insert(tables::Error {
            scope: tables::synthetic_scope(self.catalog_type, self.catalog_name),
            error: anyhow::anyhow!("simulated build failure"),
        });
    }
}

fn assert_controllers_ran(expected: &[&str], actual: Vec<ControllerState>) {
    let actual_names = actual
        .iter()
        .map(|s| s.catalog_name.as_str())
        .collect::<BTreeSet<_>>();
    let expected_names = expected.into_iter().map(|n| *n).collect::<BTreeSet<_>>();
    assert_eq!(
        expected_names, actual_names,
        "mismatched controller runs, expected:\n{expected_names:?}\nactual:\n{actual:?}"
    );
}
