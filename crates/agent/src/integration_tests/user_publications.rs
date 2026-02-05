use super::harness::{
    TestHarness, draft_catalog, get_collection_generation_id, mock_inferred_schema, set_of,
};
use crate::{
    ControlPlane, controllers::ControllerState, integration_tests::harness::InjectBuildError,
};
use models::{Capability, CatalogType, Id, status::AlertType};

#[tokio::test]
async fn test_user_publications() {
    let mut harness = TestHarness::init("test_publications").await;

    let cats_user = harness.setup_tenant("cats").await;
    let draft = draft_catalog(serde_json::json!({
        "collections": {
            "cats/noms": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    }
                },
                "key": ["/id"]
            }
        },
        "captures": {
            "cats/capture": {
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
                        "target": "cats/noms"
                    }
                ]
            }
        },
        "materializations": {
            "cats/materialize": {
                "sourceCapture": "cats/capture",
                "endpoint": {
                    "connector": {
                        "image": "materialize/test:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": { "table": "noms" },
                        "source": "cats/noms"
                    }
                ]
            }
        }
    }));
    let first_pub_result = harness
        .user_publication(cats_user, format!("initial publication"), draft)
        .await;
    assert!(
        first_pub_result.status.is_success(),
        "pub failed: {:?}",
        first_pub_result.errors
    );

    // Verify that reads_from and writes_to are set appropriately
    let capture = first_pub_result
        .live_specs
        .iter()
        .find(|s| s.catalog_name == "cats/capture")
        .unwrap();
    assert_eq!(&Some(vec!["cats/noms".to_string()]), &capture.writes_to);
    assert!(capture.reads_from.is_none());

    let noms = first_pub_result
        .live_specs
        .iter()
        .find(|s| s.catalog_name == "cats/noms")
        .unwrap();
    assert!(noms.reads_from.is_none());
    assert!(noms.writes_to.is_none());
    let materialize = first_pub_result
        .live_specs
        .iter()
        .find(|s| s.catalog_name == "cats/materialize")
        .unwrap();
    assert!(materialize.writes_to.is_none());
    assert_eq!(
        &Some(vec!["cats/noms".to_string()]),
        &materialize.reads_from
    );

    harness.run_pending_controllers(None).await;
    harness.control_plane().assert_activations(
        "after initial publication",
        vec![
            ("cats/capture", Some(CatalogType::Capture)),
            ("cats/noms", Some(CatalogType::Collection)),
            ("cats/materialize", Some(CatalogType::Materialization)),
        ],
    );

    // Setup a dogs tenant so we can test how spec expansion and controllers interact with the
    // authorization system.
    let dogs_user = harness.setup_tenant("dogs").await;

    let dog_draft = serde_json::json!({
        "materializations": {
            "dogs/materialize": {
                "endpoint": {
                    "connector": {
                        "image": "ghcr.io/estuary/materialize-postgres:dev",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": { "table": "dog_noms" },
                        "source": "cats/noms"
                    }
                ]
            }
        }
    });

    // First we'll do a couple of quick tests of authorization failures.
    // Dog tries to materialize noms and gets rejected
    let dog_result = harness
        .user_publication(
            dogs_user,
            "expect fail no auth",
            draft_catalog(dog_draft.clone()),
        )
        .await;
    assert!(!dog_result.status.is_success());
    insta::assert_debug_snapshot!(dog_result.errors, @r###"
    [
        (
            "flow://unauthorized/cats/noms",
            "User is not authorized to read this catalog name",
        ),
        (
            "flow://materialization/dogs/materialize",
            "Specification 'dogs/materialize' is not read-authorized to 'cats/noms'.\nAvailable grants are: [\n  {\n    \"subject_role\": \"dogs/\",\n    \"object_role\": \"dogs/\",\n    \"capability\": \"write\"\n  },\n  {\n    \"subject_role\": \"dogs/\",\n    \"object_role\": \"ops/dp/public/\",\n    \"capability\": \"read\"\n  }\n]",
        ),
    ]
    "###);

    // Add a user_grant for dogs and assert that a subsequent publication still fails for lack of a role_grant.
    harness
        .add_user_grant(dogs_user, "cats/", Capability::Read)
        .await;
    let dog_result = harness
        .user_publication(
            dogs_user,
            "expect fail no role_grant",
            draft_catalog(dog_draft.clone()),
        )
        .await;
    assert!(!dog_result.status.is_success());
    insta::assert_debug_snapshot!(dog_result.errors, @r###"
    [
        (
            "flow://materialization/dogs/materialize",
            "Specification 'dogs/materialize' is not read-authorized to 'cats/noms'.\nAvailable grants are: [\n  {\n    \"subject_role\": \"dogs/\",\n    \"object_role\": \"dogs/\",\n    \"capability\": \"write\"\n  },\n  {\n    \"subject_role\": \"dogs/\",\n    \"object_role\": \"ops/dp/public/\",\n    \"capability\": \"read\"\n  }\n]",
        ),
    ]
    "###);

    // Add the role grant, and now dogs can materialize cats/noms
    harness
        .add_role_grant("dogs/", "cats/", Capability::Read)
        .await;
    let dog_result = harness
        .user_publication(
            dogs_user,
            "expect success",
            draft_catalog(dog_draft.clone()),
        )
        .await;
    assert!(dog_result.status.is_success());
    assert_publication_excluded(
        dog_result.pub_id.unwrap(),
        &["cats/noms", "cats/capture", "cats/materialize"],
        &mut harness,
    )
    .await;
    harness.run_pending_controllers(None).await;
    harness.control_plane().assert_activations(
        "after dogs pub",
        vec![("dogs/materialize", Some(CatalogType::Materialization))],
    );

    // Now publish cats and assert that spec expansion and controllers behave as expected.
    let tables::LiveCollection {
        collection: noms_collection,
        last_pub_id: noms_last_pub_id,
        model: noms_model,
        ..
    } = harness
        .control_plane()
        .get_collection(models::Collection::new("cats/noms"))
        .await
        .unwrap()
        .unwrap();
    let mut draft = tables::DraftCatalog::default();
    draft.collections.insert(tables::DraftCollection {
        scope: tables::synthetic_scope(
            models::CatalogType::Collection.to_string(),
            &noms_collection.as_ref(),
        ),
        collection: noms_collection,
        expect_pub_id: Some(noms_last_pub_id),
        model: Some(noms_model),
        is_touch: false,
    });

    // Snapshot the current state of the capture and materialization, so that we can assert they
    // get touched by the publication of noms.
    let starting_expanded_specs = harness
        .control_plane()
        .get_live_specs(set_of(&["cats/capture", "cats/materialize"]))
        .await
        .unwrap();

    let result = harness
        .user_publication(
            cats_user,
            "publish noms after inferred schema updated",
            draft,
        )
        .await;
    assert!(result.status.is_success());
    // only noms should have been modified by the publication
    assert_publication_included(result.pub_id.unwrap(), &["cats/noms"], &mut harness).await;
    // Assert that the drafted specs were properly expanded, and that the expanded specs
    // were only touched.
    harness
        .assert_specs_touched_since(&starting_expanded_specs)
        .await;
    assert_publication_excluded(result.pub_id.unwrap(), &["dogs/materialize"], &mut harness).await;

    harness.run_pending_controllers(None).await;
    harness.control_plane().assert_activations(
        "after noms update",
        vec![
            ("dogs/materialize", Some(CatalogType::Materialization)),
            ("cats/capture", Some(CatalogType::Capture)),
            ("cats/noms", Some(CatalogType::Collection)),
            ("cats/materialize", Some(CatalogType::Materialization)),
        ],
    );

    // Delete cats/* and assert that dogs/materialize later responds by disabling the noms binding
    let mut draft = tables::DraftCatalog::default();
    draft.delete("cats/capture", CatalogType::Capture, None);
    draft.delete("cats/noms", CatalogType::Collection, None);
    draft.delete("cats/materialize", CatalogType::Materialization, None);
    let del_result = harness
        .user_publication(cats_user, "deleting cats/*", draft)
        .await;
    assert!(del_result.status.is_success());
    assert_publication_excluded(
        del_result.pub_id.unwrap(),
        &["dogs/materialize"],
        &mut harness,
    )
    .await;

    harness.run_pending_controllers(None).await;
    harness.control_plane().assert_activations(
        "after cats/* deleted",
        vec![
            ("dogs/materialize", Some(CatalogType::Materialization)),
            ("cats/capture", None),
            ("cats/noms", None),
            ("cats/materialize", None),
        ],
    );

    let dog_mat = harness
        .control_plane()
        .get_materialization(models::Materialization::new("dogs/materialize"))
        .await
        .unwrap()
        .expect("dogs/materialize must exist");
    assert!(dog_mat.model.bindings[0].disable);
}

#[tokio::test]
async fn successful_user_publication_clears_background_publication_failed_alert() {
    let mut harness =
        TestHarness::init("successful_user_publication_clears_background_publication_failed_alert")
            .await;

    let cats_user = harness.setup_tenant("cats").await;
    let draft = draft_catalog(serde_json::json!({
        "collections": {
            "cats/noms": {
                "writeSchema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    },
                },
                "readSchema": {
                    "allOf": [
                        {"$ref": "flow://write-schema"},
                        {"$ref": "flow://inferred-schema"}
                    ]
                },
                "key": ["/id"]
            }
        },
        "captures": {
            "cats/capture": {
                "endpoint": {
                    "connector": {
                        "image": "source/test:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": {
                            "id": "noms",
                        },
                        "target": "cats/noms"
                    }
                ]
            }
        },
    }));
    let setup_result = harness
        .user_publication(cats_user, format!("initial publication"), draft)
        .await;
    assert!(
        setup_result.status.is_success(),
        "setup errors: {:?}",
        setup_result.errors
    );
    harness.run_pending_controllers(None).await;

    // Trigger an inferred schema update to noms, and simulate a publication failure of the capture.
    let noms_state = harness.get_controller_state("cats/noms").await;
    harness
        .upsert_inferred_schema(mock_inferred_schema(
            "cats/noms",
            get_collection_generation_id(&noms_state),
            1,
        ))
        .await;
    harness.run_pending_controller("cats/noms").await;

    for i in 0..3 {
        if i > 0 {
            // Simulate the passage of time to allow the publication to be re-attempted
            let fake_time = harness.control_plane().current_time() - chrono::Duration::minutes(20);
            harness
                .push_back_last_pub_history_ts("cats/capture", fake_time)
                .await;
        }

        harness.control_plane().fail_next_build(
            "cats/capture",
            InjectBuildError::new(
                tables::synthetic_scope("capture", "cats/capture"),
                anyhow::anyhow!("simulated failure i={i}"),
            ),
        );
        let result = harness.run_pending_controller("cats/capture").await;
        assert!(
            result
                .error
                .as_ref()
                .is_some_and(|e| e.contains("publication failed")),
            "unexpected error: {:?}",
            result.error
        );
    }

    let fired_alert = harness
        .assert_alert_firing("cats/capture", AlertType::BackgroundPublicationFailed)
        .await;
    let _alerting_capture_state = harness.get_controller_state("cats/capture").await;

    let user_draft = draft_catalog(serde_json::json!({
        "captures": {
            "cats/capture": {
                "endpoint": {
                    "connector": {
                        "image": "source/test:test",
                        "config": { "updated": "this is totally gonna work, probably" }
                    }
                },
                "bindings": [
                    {
                        "resource": {
                            "id": "noms",
                        },
                        "target": "cats/noms"
                    }
                ]
            }
        }
    }));
    let result = harness
        .user_publication(cats_user, "after alerting", user_draft)
        .await;
    assert!(result.status.is_success());

    let after_user_pub_state = harness.run_pending_controller("cats/capture").await;
    assert!(after_user_pub_state.error.is_none());

    harness.control_plane().assert_activations(
        "after user publication",
        vec![
            ("cats/capture", Some(CatalogType::Capture)),
            ("cats/noms", Some(CatalogType::Collection)),
        ],
    );
    harness.assert_alert_resolved(fired_alert.alert.id).await;
}

async fn assert_publication_included(
    publication_id: Id,
    catalog_names: &[&str],
    harness: &mut TestHarness,
) -> Vec<ControllerState> {
    let mut states = Vec::new();
    for name in catalog_names {
        let state = harness.get_controller_state(name).await;
        if state.last_pub_id != publication_id {
            panic!(
                "expected publication {publication_id} to include '{name}', but the last_pub_id of {name} is {}",
                state.last_pub_id
            );
        }
        states.push(state);
    }
    states
}

async fn assert_publication_excluded(
    publication_id: Id,
    catalog_names: &[&str],
    harness: &mut TestHarness,
) {
    for name in catalog_names {
        let state = harness.get_controller_state(name).await;
        // Techincally, `==` would be correct here, but `>=` provides an extra sanity check
        if state.last_pub_id >= publication_id {
            panic!(
                "expected publication {publication_id} to not include '{name}', but the last_pub_id of {name} is {}",
                state.last_pub_id
            );
        }
    }
}
