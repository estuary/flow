use super::harness::{draft_catalog, TestHarness};
use crate::{controllers::ControllerState, ControlPlane};
use agent_sql::Capability;
use models::{CatalogType, Id};

#[tokio::test]
#[serial_test::serial]
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
                        "image": "ghcr.io/estuary/materialize-postgres:dev",
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
    assert!(first_pub_result.status.is_success());

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
        dog_result.publication_id,
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
    });

    let result = harness
        .user_publication(
            cats_user,
            "publish noms after inferred schema updated",
            draft,
        )
        .await;
    assert!(result.status.is_success());
    // Assert that the drafted specs were properly expanded
    assert_publication_included(
        result.publication_id,
        &["cats/noms", "cats/capture", "cats/materialize"],
        &mut harness,
    )
    .await;
    assert_publication_excluded(result.publication_id, &["dogs/materialize"], &mut harness).await;

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
        del_result.publication_id,
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

async fn assert_publication_included(
    publication_id: Id,
    catalog_names: &[&str],
    harness: &mut TestHarness,
) -> Vec<ControllerState> {
    let mut states = Vec::new();
    for name in catalog_names {
        let state = harness.get_controller_state(name).await;
        if state.last_pub_id != publication_id {
            panic!("expected publication {publication_id} to include '{name}', but the last_pub_id of {name} is {}", state.last_pub_id);
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
            panic!("expected publication {publication_id} to not include '{name}', but the last_pub_id of {name} is {}", state.last_pub_id);
        }
    }
}
