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
    insta::assert_debug_snapshot!(dog_result.errors, @r#"
    [
        (
            "flow://unauthorized/cats/noms",
            "User is not authorized to read this catalog name",
        ),
        (
            "flow://materialization/dogs/materialize",
            "Specification 'dogs/materialize' is not read-authorized to 'cats/noms'.\nAvailable grants are: [\n  {\n    \"subject_role\": \"dogs/\",\n    \"object_role\": \"dogs/\",\n    \"capability\": \"write\",\n    \"bundles\": []\n  },\n  {\n    \"subject_role\": \"dogs/\",\n    \"object_role\": \"ops/dp/public/\",\n    \"capability\": \"read\",\n    \"bundles\": []\n  }\n]",
        ),
    ]
    "#);

    // The denied read of cats/noms is Snapshot-evaluated, so it requests an
    // early background refresh.
    let snapshot = harness.snapshot_watch.token();
    assert!(
        snapshot.result().unwrap().revoke.is_cancelled(),
        "expected the denied referenced name to cancel the Snapshot's revoke token"
    );

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
    insta::assert_debug_snapshot!(dog_result.errors, @r#"
    [
        (
            "flow://materialization/dogs/materialize",
            "Specification 'dogs/materialize' is not read-authorized to 'cats/noms'.\nAvailable grants are: [\n  {\n    \"subject_role\": \"dogs/\",\n    \"object_role\": \"dogs/\",\n    \"capability\": \"write\",\n    \"bundles\": []\n  },\n  {\n    \"subject_role\": \"dogs/\",\n    \"object_role\": \"ops/dp/public/\",\n    \"capability\": \"read\",\n    \"bundles\": []\n  }\n]",
        ),
    ]
    "#);

    // The remaining error is spec-to-spec, which is not Snapshot-evaluated
    // and must not request a refresh.
    let snapshot = harness.snapshot_watch.token();
    assert!(
        !snapshot.result().unwrap().revoke.is_cancelled(),
        "expected the spec-to-spec denial to leave the Snapshot's revoke token alone"
    );

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

    // Filtering dogs/materialize from the expansion merely narrowed the
    // draft, and must not request a Snapshot refresh.
    let snapshot = harness.snapshot_watch.token();
    assert!(
        !snapshot.result().unwrap().revoke.is_cancelled(),
        "expected the filtered expansion to leave the Snapshot's revoke token alone"
    );

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

/// The runtime-v2 capture rollout (`RuntimeV2Rollout` initializer) stamps
/// `enable-runtime-v2: true` into the model of a *newly-created* capture when
/// enabled. Covers: a capture created while it's off is untouched; a new capture
/// created while it's on is enabled in both the committed model and the
/// built-spec shard label; an explicit flag is preserved; and an existing
/// capture is never retroactively enabled on republish.
#[tokio::test]
async fn test_runtime_v2_new_captures() {
    let mut harness = TestHarness::init("test_runtime_v2_new_captures").await;
    let user = harness.setup_tenant("cats").await;

    let collection = || {
        serde_json::json!({
            "schema": { "type": "object", "properties": { "id": { "type": "string" } }, "required": ["id"] },
            "key": ["/id"]
        })
    };
    let capture = |target: &str, prefix: &str| {
        serde_json::json!({
            "endpoint": { "connector": {
                "image": "ghcr.io/estuary/source-hello-world:dev",
                "config": {}
            }},
            "bindings": [ {
                "resource": { "name": "greetings", "prefix": prefix },
                "target": target
            } ]
        })
    };
    // The `enable-runtime-v2` value in a capture's committed model, if any.
    async fn model_flag(harness: &mut TestHarness, name: &str) -> Option<String> {
        let state = harness.get_controller_state(name).await;
        let models::AnySpec::Capture(model) = state.live_spec.as_ref().unwrap() else {
            panic!("expected a capture model");
        };
        model
            .shards
            .flags
            .get(&models::Token::new(models::ENABLE_RUNTIME_V2))
            .map(|v| v.as_str().to_string())
    }
    // The `enable-runtime-v2` value on a built capture's shard template, if any.
    fn built_capture_v2_label(spec: &proto_flow::AnyBuiltSpec) -> Option<String> {
        let proto_flow::AnyBuiltSpec::Capture(capture) = spec else {
            return None;
        };
        let set = capture.shard_template.as_ref()?.labels.as_ref()?;
        labels::values(set, labels::RUNTIME_V2_FLAG)
            .first()
            .map(|l| l.value.clone())
    }

    // Rollout disabled: a capture created now is left on v1.
    harness.runtime_v2_new_captures = false;
    let draft = draft_catalog(serde_json::json!({
        "collections": { "cats/early-out": collection() },
        "captures": { "cats/early": capture("cats/early-out", "Hello {}!") },
    }));
    let result = harness
        .user_publication(user, "rollout disabled", draft)
        .await;
    assert!(
        result.status.is_success(),
        "publication failed: {:?}",
        result.errors
    );
    assert_eq!(
        model_flag(&mut harness, "cats/early").await,
        None,
        "a capture created while the rollout is off must be unflagged"
    );

    // Rollout enabled from here on.
    harness.runtime_v2_new_captures = true;

    // A newly-created capture is enabled onto v2; one that pins itself to v1 is
    // left alone.
    let mut pinned = capture("cats/pinned-out", "Hello {}!");
    pinned["shards"] = serde_json::json!({ "flags": { "enable-runtime-v2": "false" } });
    let draft = draft_catalog(serde_json::json!({
        "collections": { "cats/auto-out": collection(), "cats/pinned-out": collection() },
        "captures": { "cats/auto": capture("cats/auto-out", "Hello {}!"), "cats/pinned": pinned },
    }));
    let result = harness
        .user_publication(user, "rollout enabled", draft)
        .await;
    assert!(
        result.status.is_success(),
        "publication failed: {:?}",
        result.errors
    );

    // cats/auto: enabled in the committed model AND emitted as the built-spec label.
    assert_eq!(
        model_flag(&mut harness, "cats/auto").await.as_deref(),
        Some("true"),
        "a new capture is enabled in the model"
    );
    let state = harness.get_controller_state("cats/auto").await;
    assert_eq!(
        built_capture_v2_label(state.built_spec.as_ref().unwrap()).as_deref(),
        Some("true"),
        "the flag is emitted as the built-spec shard label"
    );

    // cats/pinned: an explicit flag is never changed.
    assert_eq!(
        model_flag(&mut harness, "cats/pinned").await.as_deref(),
        Some("false"),
        "an explicit `false` is preserved"
    );

    // Republishing `cats/early` (created while the rollout was off) with a real
    // edit does NOT retroactively enable it: only new captures are stamped.
    let draft = draft_catalog(serde_json::json!({
        "collections": { "cats/early-out": collection() },
        "captures": { "cats/early": capture("cats/early-out", "Hola {}!") },
    }));
    let result = harness
        .user_publication(user, "republish existing", draft)
        .await;
    assert!(
        result.status.is_success(),
        "publication failed: {:?}",
        result.errors
    );
    assert_eq!(
        model_flag(&mut harness, "cats/early").await,
        None,
        "an existing capture must stay unflagged on republish"
    );
}

/// The runtime-v2 materialization rollout (`RuntimeV2Rollout` initializer) stamps
/// `enable-runtime-v2: true` into the model of a *newly-created* materialization
/// when enabled. Covers: a materialization created while it's off is untouched; a
/// new materialization created while it's on is enabled in both the committed
/// model and the built-spec shard label; an explicit flag is preserved; and an
/// existing materialization is never retroactively enabled on republish.
#[tokio::test]
async fn test_runtime_v2_new_materializations() {
    let mut harness = TestHarness::init("test_runtime_v2_new_materializations").await;
    let user = harness.setup_tenant("cats").await;

    let collection = || {
        serde_json::json!({
            "schema": { "type": "object", "properties": { "id": { "type": "string" } }, "required": ["id"] },
            "key": ["/id"]
        })
    };
    let materialization = |source: &str, table: &str| {
        serde_json::json!({
            "endpoint": { "connector": {
                "image": "materialize/test:test",
                "config": {}
            }},
            "bindings": [ {
                "resource": { "table": table },
                "source": source
            } ]
        })
    };
    // The `enable-runtime-v2` value in a materialization's committed model, if any.
    async fn model_flag(harness: &mut TestHarness, name: &str) -> Option<String> {
        let state = harness.get_controller_state(name).await;
        let models::AnySpec::Materialization(model) = state.live_spec.as_ref().unwrap() else {
            panic!("expected a materialization model");
        };
        model
            .shards
            .flags
            .get(&models::Token::new(models::ENABLE_RUNTIME_V2))
            .map(|v| v.as_str().to_string())
    }
    // The `enable-runtime-v2` value on a built materialization's shard template, if any.
    fn built_materialization_v2_label(spec: &proto_flow::AnyBuiltSpec) -> Option<String> {
        let proto_flow::AnyBuiltSpec::Materialization(materialization) = spec else {
            return None;
        };
        let set = materialization.shard_template.as_ref()?.labels.as_ref()?;
        labels::values(set, labels::RUNTIME_V2_FLAG)
            .first()
            .map(|l| l.value.clone())
    }

    // Rollout disabled: a materialization created now is left on v1.
    harness.runtime_v2_new_materializations = false;
    let draft = draft_catalog(serde_json::json!({
        "collections": { "cats/early-src": collection() },
        "materializations": { "cats/early": materialization("cats/early-src", "early") },
    }));
    let result = harness
        .user_publication(user, "rollout disabled", draft)
        .await;
    assert!(
        result.status.is_success(),
        "publication failed: {:?}",
        result.errors
    );
    assert_eq!(
        model_flag(&mut harness, "cats/early").await,
        None,
        "a materialization created while the rollout is off must be unflagged"
    );

    // Rollout enabled from here on.
    harness.runtime_v2_new_materializations = true;

    // A newly-created materialization is enabled onto v2; one that pins itself to
    // v1 is left alone.
    let mut pinned = materialization("cats/pinned-src", "pinned");
    pinned["shards"] = serde_json::json!({ "flags": { "enable-runtime-v2": "false" } });
    let draft = draft_catalog(serde_json::json!({
        "collections": { "cats/auto-src": collection(), "cats/pinned-src": collection() },
        "materializations": {
            "cats/auto": materialization("cats/auto-src", "auto"),
            "cats/pinned": pinned,
        },
    }));
    let result = harness
        .user_publication(user, "rollout enabled", draft)
        .await;
    assert!(
        result.status.is_success(),
        "publication failed: {:?}",
        result.errors
    );

    // cats/auto: enabled in the committed model AND emitted as the built-spec label.
    assert_eq!(
        model_flag(&mut harness, "cats/auto").await.as_deref(),
        Some("true"),
        "a new materialization is enabled in the model"
    );
    let state = harness.get_controller_state("cats/auto").await;
    assert_eq!(
        built_materialization_v2_label(state.built_spec.as_ref().unwrap()).as_deref(),
        Some("true"),
        "the flag is emitted as the built-spec shard label"
    );

    // cats/pinned: an explicit flag is never changed.
    assert_eq!(
        model_flag(&mut harness, "cats/pinned").await.as_deref(),
        Some("false"),
        "an explicit `false` is preserved"
    );

    // Republishing `cats/early` (created while the rollout was off) with a real
    // edit does NOT retroactively enable it: only new materializations are stamped.
    let draft = draft_catalog(serde_json::json!({
        "collections": { "cats/early-src": collection() },
        "materializations": { "cats/early": materialization("cats/early-src", "early-v2") },
    }));
    let result = harness
        .user_publication(user, "republish existing", draft)
        .await;
    assert!(
        result.status.is_success(),
        "publication failed: {:?}",
        result.errors
    );
    assert_eq!(
        model_flag(&mut harness, "cats/early").await,
        None,
        "an existing materialization must stay unflagged on republish"
    );
}

/// The runtime-v2 derivation rollout (`RuntimeV2Rollout` initializer) stamps
/// `enable-runtime-v2: true` into the model of a *newly-created* derivation when
/// enabled. A derivation is a collection carrying a `derive` block, so the flag
/// lives at `derive.shards.flags` (not `shards.flags`), and plain collections are
/// never candidates. Covers: a derivation created while it's off is untouched; a
/// new derivation created while it's on is enabled in both the committed model
/// and the built-spec shard label; an explicit flag is preserved; and an existing
/// derivation is never retroactively enabled on republish.
#[tokio::test]
async fn test_runtime_v2_new_derivations() {
    let mut harness = TestHarness::init("test_runtime_v2_new_derivations").await;
    let user = harness.setup_tenant("cats").await;

    let collection = || {
        serde_json::json!({
            "schema": { "type": "object", "properties": { "id": { "type": "string" } }, "required": ["id"] },
            "key": ["/id"]
        })
    };
    let derivation = |source: &str, lambda: &str| {
        serde_json::json!({
            "schema": { "type": "object", "properties": { "id": { "type": "string" } }, "required": ["id"] },
            "key": ["/id"],
            "derive": {
                "using": { "sqlite": { "migrations": [] } },
                "transforms": [
                    { "name": "fromSource", "source": source, "shuffle": "any", "lambda": lambda }
                ]
            }
        })
    };
    // The `enable-runtime-v2` value in a derivation's committed model, if any.
    async fn model_flag(harness: &mut TestHarness, name: &str) -> Option<String> {
        let state = harness.get_controller_state(name).await;
        let models::AnySpec::Collection(model) = state.live_spec.as_ref().unwrap() else {
            panic!("expected a collection model");
        };
        model
            .derive
            .as_ref()
            .expect("expected a derived collection")
            .shards
            .flags
            .get(&models::Token::new(models::ENABLE_RUNTIME_V2))
            .map(|v| v.as_str().to_string())
    }
    // The `enable-runtime-v2` value on a built derivation's shard template, if any.
    fn built_derivation_v2_label(spec: &proto_flow::AnyBuiltSpec) -> Option<String> {
        let proto_flow::AnyBuiltSpec::Collection(collection) = spec else {
            return None;
        };
        let set = collection
            .derivation
            .as_ref()?
            .shard_template
            .as_ref()?
            .labels
            .as_ref()?;
        labels::values(set, labels::RUNTIME_V2_FLAG)
            .first()
            .map(|l| l.value.clone())
    }

    // Rollout disabled: a derivation created now is left on v1.
    harness.runtime_v2_new_derivations = false;
    let draft = draft_catalog(serde_json::json!({
        "collections": {
            "cats/source": collection(),
            "cats/early": derivation("cats/source", "select $id;"),
        },
    }));
    let result = harness
        .user_publication(user, "rollout disabled", draft)
        .await;
    assert!(
        result.status.is_success(),
        "publication failed: {:?}",
        result.errors
    );
    assert_eq!(
        model_flag(&mut harness, "cats/early").await,
        None,
        "a derivation created while the rollout is off must be unflagged"
    );

    // Rollout enabled from here on.
    harness.runtime_v2_new_derivations = true;

    // A newly-created derivation is enabled onto v2; one that pins itself to v1 is
    // left alone. The plain source collection is never a candidate.
    let mut pinned = derivation("cats/source", "select $id;");
    pinned["derive"]["shards"] = serde_json::json!({ "flags": { "enable-runtime-v2": "false" } });
    let draft = draft_catalog(serde_json::json!({
        "collections": {
            "cats/source": collection(),
            "cats/auto": derivation("cats/source", "select $id;"),
            "cats/pinned": pinned,
        },
    }));
    let result = harness
        .user_publication(user, "rollout enabled", draft)
        .await;
    assert!(
        result.status.is_success(),
        "publication failed: {:?}",
        result.errors
    );

    // cats/auto: enabled in the committed model AND emitted as the built-spec label.
    assert_eq!(
        model_flag(&mut harness, "cats/auto").await.as_deref(),
        Some("true"),
        "a new derivation is enabled in the model"
    );
    let state = harness.get_controller_state("cats/auto").await;
    assert_eq!(
        built_derivation_v2_label(state.built_spec.as_ref().unwrap()).as_deref(),
        Some("true"),
        "the flag is emitted as the built-spec shard label"
    );

    // cats/pinned: an explicit flag is never changed.
    assert_eq!(
        model_flag(&mut harness, "cats/pinned").await.as_deref(),
        Some("false"),
        "an explicit `false` is preserved"
    );

    // cats/source: a plain (non-derived) collection has no derivation to stamp,
    // and never gains one.
    let source = harness.get_controller_state("cats/source").await;
    let models::AnySpec::Collection(source_model) = source.live_spec.as_ref().unwrap() else {
        panic!("expected a collection model");
    };
    assert!(
        source_model.derive.is_none(),
        "a plain collection must not gain a derivation"
    );

    // Republishing `cats/early` (created while the rollout was off) with a real
    // edit does NOT retroactively enable it: only new derivations are stamped.
    let draft = draft_catalog(serde_json::json!({
        "collections": {
            "cats/source": collection(),
            "cats/early": derivation("cats/source", "select $id, $id as also_id;"),
        },
    }));
    let result = harness
        .user_publication(user, "republish existing", draft)
        .await;
    assert!(
        result.status.is_success(),
        "publication failed: {:?}",
        result.errors
    );
    assert_eq!(
        model_flag(&mut harness, "cats/early").await,
        None,
        "an existing derivation must stay unflagged on republish"
    );
}

// The tests below pin publication authorization behaviors across the move of
// their enforcement off internal.user_roles() SQL and onto the pinned
// authorization Snapshot (issue #2781), mirroring the discovers migration.

// Neither `storage_mappings` nor `data_planes` are reset between tests, so a
// tenant's mapping reflects whichever planes existed when its first test
// provisioned it. Pin the mapping to just the default test plane so that
// data-plane resolution (and its error suggestions) are deterministic.
// Adds a plane outside the tenants' `ops/dp/public/` read grant: it exists
// but is not readable by the tests' users.
async fn add_private_plane(harness: &mut TestHarness) {
    harness
        .add_data_plane(
            "ops/dp/private/other",
            "ops-dp-private-other.dp.test",
            vec!["c2VjcmV0".to_string()],
        )
        .await;
}

async fn pin_storage_mapping_planes(harness: &TestHarness, catalog_prefix: &str) {
    sqlx::query!(
        r#"update storage_mappings
        set spec = jsonb_set(spec::jsonb, '{data_planes}', '["ops/dp/public/test"]'::jsonb)::json
        where catalog_prefix = $1"#,
        catalog_prefix,
    )
    .execute(&harness.pool)
    .await
    .expect("failed to pin storage mapping data-planes");
}

// A derivation is used because a new, unbound plain collection would be
// pruned by `PruneUnboundCollections`, turning a successful publication
// into an `EmptyDraft`.
fn pawed_collection() -> serde_json::Value {
    serde_json::json!({
        "collections": {
            "cats/paws": {
                "schema": { "type": "object", "properties": { "id": { "type": "string" } }, "required": ["id"] },
                "key": ["/id"]
            },
            "cats/paws-clean": {
                "schema": { "type": "object", "properties": { "id": { "type": "string" } }, "required": ["id"] },
                "key": ["/id"],
                "derive": {
                    "using": { "sqlite": { "migrations": [] } },
                    "transforms": [
                        { "name": "fromSource", "source": "cats/paws", "shuffle": "any", "lambda": "select $id;" }
                    ]
                }
            }
        }
    })
}

#[tokio::test]
async fn test_publication_drafted_name_requires_admin() {
    let mut harness = TestHarness::init("test_publication_drafted_name_requires_admin").await;

    let _cats_user = harness.setup_tenant("cats").await;
    let dogs_user = harness.setup_tenant("dogs").await;

    // `write` on cats/ is enough to read and write data, but drafting a spec
    // under cats/ requires `admin`.
    harness
        .add_user_grant(dogs_user, "cats/", Capability::Write)
        .await;

    let result = harness
        .user_publication(
            dogs_user,
            "expect fail not admin",
            draft_catalog(pawed_collection()),
        )
        .await;
    assert!(!result.status.is_success());
    insta::assert_debug_snapshot!(result.errors, @r#"
    [
        (
            "flow://collection/cats/paws",
            "User is not authorized to create or change this catalog name",
        ),
        (
            "flow://collection/cats/paws-clean",
            "User is not authorized to create or change this catalog name",
        ),
    ]
    "#);

    // A denied user capability requests an early background Snapshot refresh:
    // the needed grant may have been created after the Snapshot was taken, and
    // cancelling narrows the staleness window for a manual retry.
    let snapshot = harness.snapshot_watch.token();
    assert!(
        snapshot.result().unwrap().revoke.is_cancelled(),
        "expected the denied drafted name to cancel the Snapshot's revoke token"
    );

    harness
        .add_user_grant(dogs_user, "cats/", Capability::Admin)
        .await;
    let result = harness
        .user_publication(
            dogs_user,
            "expect success as admin",
            draft_catalog(pawed_collection()),
        )
        .await;
    assert!(
        result.status.is_success(),
        "pub failed with status {:?}: {:?}",
        result.status,
        result.errors
    );

    let snapshot = harness.snapshot_watch.token();
    assert!(
        !snapshot.result().unwrap().revoke.is_cancelled(),
        "a successful publication must not cancel the Snapshot's revoke token"
    );
}

#[tokio::test]
async fn test_publication_no_data_plane() {
    let mut harness = TestHarness::init("test_publication_no_data_plane").await;
    let user_id = harness.setup_tenant("cats").await;
    pin_storage_mapping_planes(&harness, "cats/").await;

    // The unreadable plane must be indistinguishable from a missing one.
    add_private_plane(&mut harness).await;

    let mut outcomes = Vec::new();
    for (case, data_plane_name) in [
        ("unauthorized plane", "ops/dp/private/other"),
        // The name falls under the tenant's read grant, but no such plane exists.
        ("missing plane", "ops/dp/public/missing"),
    ] {
        let result = harness
            .user_publication_in_plane(
                user_id,
                case,
                draft_catalog(pawed_collection()),
                data_plane_name,
            )
            .await;
        assert!(!result.status.is_success(), "{case}");
        outcomes.push((case, result.errors));
    }
    // A denied plane and a missing plane must fail identically, so that
    // authorization does not leak the existence of unauthorized planes.
    insta::assert_debug_snapshot!(outcomes, @r#"
    [
        (
            "unauthorized plane",
            [
                (
                    "file:///",
                    "data plane ops/dp/private/other, referenced by build parameter, was not found; did you mean data plane ops/dp/public/test?",
                ),
            ],
        ),
        (
            "missing plane",
            [
                (
                    "file:///",
                    "data plane ops/dp/public/missing, referenced by build parameter, was not found; did you mean data plane ops/dp/public/test?",
                ),
            ],
        ),
    ]
    "#);
}

// Uses its own `lynx` tenant: `storage_mappings` is not reset between tests,
// so mutating the shared `cats/` mapping would leak into other tests.
#[tokio::test]
async fn test_publication_storage_mapping_unreadable_plane() {
    let mut harness = TestHarness::init("test_publication_storage_mapping_unreadable_plane").await;
    let user_id = harness.setup_tenant("lynx").await;

    add_private_plane(&mut harness).await;

    // The tenant's storage mapping leads with a plane the user cannot read,
    // making it the mapping's default, with the readable test plane second.
    sqlx::query!(
        r#"update storage_mappings
        set spec = jsonb_set(
            spec::jsonb,
            '{data_planes}',
            '["ops/dp/private/other", "ops/dp/public/test"]'::jsonb
        )::json
        where catalog_prefix = 'lynx/'"#,
    )
    .execute(&harness.pool)
    .await
    .expect("failed to update storage mapping");

    let draft = || {
        draft_catalog(serde_json::json!({
            "collections": {
                "lynx/paws": {
                    "schema": { "type": "object", "properties": { "id": { "type": "string" } }, "required": ["id"] },
                    "key": ["/id"]
                },
                "lynx/paws-clean": {
                    "schema": { "type": "object", "properties": { "id": { "type": "string" } }, "required": ["id"] },
                    "key": ["/id"],
                    "derive": {
                        "using": { "sqlite": { "migrations": [] } },
                        "transforms": [
                            { "name": "fromSource", "source": "lynx/paws", "shuffle": "any", "lambda": "select $id;" }
                        ]
                    }
                }
            }
        }))
    };

    // Without an explicit plane, new collections fall back to the mapping's
    // default plane. The unreadable default is filtered from the resolved
    // data-planes, indistinguishable from a missing one.
    let result = harness
        .user_publication_in_plane(user_id, "mapping default plane", draft(), "")
        .await;
    assert!(!result.status.is_success());
    insta::assert_debug_snapshot!(result.errors, @r#"
    [
        (
            "flow://collection/lynx/paws",
            "data plane ops/dp/private/other, referenced by storage mapping lynx/, was not found; did you mean data plane ops/dp/public/test?",
        ),
        (
            "flow://collection/lynx/paws-clean",
            "data plane ops/dp/private/other, referenced by storage mapping lynx/, was not found; did you mean data plane ops/dp/public/test?",
        ),
    ]
    "#);

    // When the publication explicitly targets the readable plane, the
    // unreadable mapping entry goes unused and is silently tolerated.
    let result = harness
        .user_publication(user_id, "explicit readable plane", draft())
        .await;
    assert!(
        result.status.is_success(),
        "pub failed with status {:?}: {:?}",
        result.status,
        result.errors
    );

    // Though unused, the mapping plane was still denied at partition time,
    // which eagerly requests an early background refresh.
    let snapshot = harness.snapshot_watch.token();
    assert!(
        snapshot.result().unwrap().revoke.is_cancelled(),
        "expected the unused denied mapping plane to cancel the Snapshot's revoke token"
    );
}

// Unlike the characterization tests above, this asserts new Snapshot-only
// behavior: the SQL authorization path never cancelled the revoke token, so
// this test was committed RED and turned green when data-plane authorization
// moved onto the Snapshot.
#[tokio::test]
async fn test_publication_no_data_plane_requests_snapshot_refresh() {
    let mut harness =
        TestHarness::init("test_publication_no_data_plane_requests_snapshot_refresh").await;
    let user_id = harness.setup_tenant("cats").await;
    pin_storage_mapping_planes(&harness, "cats/").await;

    add_private_plane(&mut harness).await;

    let result = harness
        .user_publication_in_plane(
            user_id,
            "unauthorized plane",
            draft_catalog(pawed_collection()),
            "ops/dp/private/other",
        )
        .await;
    assert!(!result.status.is_success());

    // A denied data-plane requests an early background Snapshot refresh: the
    // plane or its grant may have been created after the Snapshot was taken,
    // and cancelling narrows the staleness window for a manual retry.
    let snapshot = harness.snapshot_watch.token();
    assert!(
        snapshot.result().unwrap().revoke.is_cancelled(),
        "expected the denied data-plane to cancel the Snapshot's revoke token"
    );
}
