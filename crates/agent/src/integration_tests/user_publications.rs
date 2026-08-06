use super::harness::{
    Either, TestHarness, draft_catalog, get_collection_generation_id, mock_inferred_schema, set_of,
};
use crate::{
    ControlPlane, controllers::ControllerState, integration_tests::harness::InjectBuildError,
};
use control_plane_api::publications;
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

/// A draft that materializes `cats/noms`, which `dogs` may only publish once it
/// holds both a user grant and a role grant to `cats/`.
fn dogs_materialize_cats_draft() -> tables::DraftCatalog {
    draft_catalog(serde_json::json!({
        "materializations": {
            "dogs/materialize": {
                "endpoint": {
                    "connector": {
                        "image": "materialize/test:test",
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
    }))
}

/// Publishes `cats/noms` and returns the `dogs` user id. Shared setup for the
/// stale-authorization publication tests below.
async fn setup_cross_tenant_publication(harness: &mut TestHarness) -> uuid::Uuid {
    let cats_user = harness.setup_tenant("cats").await;

    // The capture isn't incidental: a draft holding only a collection with no
    // writer builds to zero specs and is reported as an empty draft.
    let result = harness
        .user_publication(
            cats_user,
            "publish cats/noms",
            draft_catalog(serde_json::json!({
                "collections": {
                    "cats/noms": {
                        "schema": {
                            "type": "object",
                            "properties": { "id": { "type": "string" } }
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
                                "resource": { "id": "noms" },
                                "target": "cats/noms"
                            }
                        ]
                    }
                }
            })),
        )
        .await;
    assert!(
        result.status.is_success(),
        "setup publication failed: {:?} {:?}",
        result.status,
        result.errors
    );

    harness.setup_tenant("dogs").await
}

/// A publication must reschedule when its selected data plane is denied by a
/// Snapshot which predates the queued publication. This is the concrete race:
/// the grant is restored in Postgres before the publication is queued, but the
/// in-memory Snapshot still reflects the brief revocation.
#[tokio::test]
async fn test_publication_reschedules_on_stale_data_plane_authz() {
    let mut harness = TestHarness::init("test_publication_stale_data_plane_authz").await;
    let cats_user = harness.setup_tenant("cats").await;

    let deleted = sqlx::query(
        "delete from role_grants
         where subject_role = 'cats/' and object_role = 'ops/dp/public/'",
    )
    .execute(&harness.pool)
    .await
    .expect("failed to remove the tenant's public-plane grant");
    assert_eq!(1, deleted.rows_affected());

    // Snapshot A observes the revocation. Restore the grant without refreshing,
    // then queue the publication so A is not authoritative for its denial.
    harness.refresh_snapshot().await;
    harness
        .add_role_grant_unobserved("cats/", "ops/dp/public/", Capability::Read)
        .await;
    let pub_id = harness
        .queue_publication(
            cats_user,
            "public-plane grant awaiting Snapshot refresh",
            Either::L(draft_catalog(serde_json::json!({
                "collections": {
                    "cats/noms": {
                        "schema": {
                            "type": "object",
                            "properties": { "id": { "type": "string" } }
                        },
                        "key": ["/id"]
                    }
                },
                "captures": {
                    "cats/capture": {
                        "endpoint": {
                            "connector": { "image": "source/test:test", "config": {} }
                        },
                        "bindings": [
                            { "resource": { "id": "noms" }, "target": "cats/noms" }
                        ]
                    }
                }
            }))),
        )
        .await;

    let first = harness.poll_publication_once(pub_id).await;
    assert_eq!(
        publications::StatusType::Queued,
        first.status.r#type,
        "publication should reschedule while the restored plane grant is unobserved, got: {:?}",
        first.errors
    );

    harness.refresh_snapshot_authoritative().await;
    harness.set_min_task_wake_at(pub_id).await;

    let second = harness.poll_publication_once(pub_id).await;
    assert!(
        second.status.is_success(),
        "publication should succeed once the plane grant is observed, got: {:?}",
        second.errors
    );
}

/// After a stale-Snapshot denial, the executor persists the instant an
/// authoritative Snapshot must postdate (in `internal.tasks`, so whichever
/// agent instance dequeues the next poll applies the same criterion) and
/// defers re-polls without loading or building the draft. Once the local
/// Snapshot postdates that instant, the retry proceeds and succeeds.
#[tokio::test]
async fn test_publication_defers_polls_until_authoritative_snapshot() {
    let mut harness = TestHarness::init("test_publication_defers_polls").await;
    let dogs_user = setup_cross_tenant_publication(&mut harness).await;

    harness.refresh_snapshot_stale().await;
    harness
        .add_user_grant_unobserved(dogs_user, "cats/", Capability::Read)
        .await;
    harness
        .add_role_grant_unobserved("dogs/", "cats/", Capability::Read)
        .await;

    let pub_id = harness
        .queue_publication(
            dogs_user,
            "deferred until authoritative",
            Either::L(dogs_materialize_cats_draft()),
        )
        .await;

    let first = harness.poll_publication_once(pub_id).await;
    assert_eq!(
        publications::StatusType::Queued,
        first.status.r#type,
        "publication should reschedule while the grants are unobserved, got: {:?}",
        first.errors
    );

    let state: serde_json::Value = harness.get_task_state(pub_id).await;
    assert!(
        state
            .get("awaiting_snapshot_after")
            .is_some_and(|v| v.is_string()),
        "the executor should record the instant a Snapshot must postdate, got: {state}"
    );

    // Because the anchor is persisted, the re-poll may be dequeued by a
    // *different* agent instance whose own local Snapshot is stale — one whose
    // revoke token the original attempt never cancelled. Model that handoff by
    // replacing the watch with another stale Snapshot bearing a fresh token.
    harness.refresh_snapshot_stale().await;
    let handoff_revoke = harness
        .snapshot_watch
        .token()
        .result()
        .unwrap()
        .revoke
        .clone();
    assert!(!handoff_revoke.is_cancelled());

    // A re-poll under the still-stale Snapshot defers, leaving the row queued.
    harness.set_min_task_wake_at(pub_id).await;
    let deferred = harness.poll_publication_once(pub_id).await;
    assert_eq!(
        publications::StatusType::Queued,
        deferred.status.r#type,
        "a re-poll under a still-stale Snapshot should defer, got: {:?}",
        deferred.errors
    );

    // The deferring poll must request a refresh of the Snapshot it observed:
    // no prior cancellation covers this instance's Snapshot, and without one
    // the task would idle until the watch's ordinary refresh interval.
    assert!(
        handoff_revoke.is_cancelled(),
        "a deferring poll should cancel the stale Snapshot it observed"
    );

    harness.refresh_snapshot_authoritative().await;
    harness.set_min_task_wake_at(pub_id).await;
    let resolved = harness.poll_publication_once(pub_id).await;
    assert!(
        resolved.status.is_success(),
        "publication should succeed once the Snapshot postdates the anchor, got: {:?}",
        resolved.errors
    );
}

/// The variant of the late-grant race that the test above cannot catch: the
/// referenced spec is *old*. A Snapshot taken after the spec's publication but
/// before the new grants is inconclusive for a publication queued after those
/// grants — staleness is a property of the publication's queued time, not of
/// the referenced spec's age. The publication must remain queued under that
/// Snapshot and succeed once a refresh observes the grants.
#[tokio::test]
async fn test_old_spec_publication_succeeds_after_late_grant() {
    let mut harness =
        TestHarness::init("test_old_spec_publication_succeeds_after_late_grant").await;
    let dogs_user = setup_cross_tenant_publication(&mut harness).await;

    // `cats/noms` was published long before any of the events below.
    harness.age_live_spec("cats/noms").await;

    // Snapshot A: taken after the (old) spec but before the grants and the
    // publication, so it holds the pre-grant world.
    harness.refresh_snapshot_stale().await;
    harness
        .add_user_grant_unobserved(dogs_user, "cats/", Capability::Read)
        .await;
    harness
        .add_role_grant_unobserved("dogs/", "cats/", Capability::Read)
        .await;

    let pub_id = harness
        .queue_publication(
            dogs_user,
            "late grant, old spec",
            Either::L(dogs_materialize_cats_draft()),
        )
        .await;

    let first = harness.poll_publication_once(pub_id).await;
    assert_eq!(
        publications::StatusType::Queued,
        first.status.r#type,
        "a publication evaluated against a Snapshot older than its queued time \
         must reschedule regardless of the referenced spec's age, got: {:?}",
        first.errors
    );

    harness.refresh_snapshot_authoritative().await;
    harness.set_min_task_wake_at(pub_id).await;

    let second = harness.poll_publication_once(pub_id).await;
    assert!(
        second.status.is_success(),
        "publication should succeed once the grants are observed, got: {:?}",
        second.errors
    );
}

/// An `Initialize` stage which revokes the grants that authorize the test's
/// publication and pushes a refreshed, authoritative Snapshot into the watch —
/// exactly what a background refresh landing between draft initialization and
/// live-spec resolution does in production. Composed as the final Initialize
/// stage, it runs after `ExpandDraft` and before `build`, squarely on the
/// phase boundary that snapshot pinning exists to protect.
struct RevokeMidPublication<'h> {
    harness: &'h TestHarness,
    dogs_user: uuid::Uuid,
}

impl publications::Initialize for RevokeMidPublication<'_> {
    async fn initialize(
        &self,
        db: &sqlx::PgPool,
        _user_id: uuid::Uuid,
        _draft: &mut tables::DraftCatalog,
        _snapshot: &control_plane_api::Snapshot,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "delete from role_grants where subject_role = 'dogs/' and object_role = 'cats/'",
        )
        .execute(db)
        .await?;
        sqlx::query("delete from user_grants where user_id = $1 and object_role = 'cats/'")
            .bind(self.dogs_user)
            .execute(db)
            .await?;
        self.harness.refresh_snapshot_authoritative().await;
        Ok(())
    }
}

/// One publication must evaluate authorization against exactly one Snapshot:
/// `try_publish` resolves the watch once and threads that Snapshot through
/// both draft initialization and live-spec resolution. A refresh landing
/// between those phases must not swap the view mid-flight.
///
/// `RevokeMidPublication` deletes the authorizing grants and refreshes the
/// watch after expansion. Resolution still authorizes under the pinned
/// pre-revocation Snapshot, so the publication succeeds; if it consulted the
/// watch anew it would see the revoked world and deny. The guard publication
/// then proves the refreshed watch really does deny the same draft, so the
/// first result is attributable to pinning alone.
#[tokio::test]
async fn test_publication_uses_one_snapshot_across_phases() {
    let mut harness = TestHarness::init("test_publication_one_snapshot_across_phases").await;
    let dogs_user = setup_cross_tenant_publication(&mut harness).await;

    // Snapshot A: grants written and observed, stamped authoritative.
    harness
        .add_user_grant(dogs_user, "cats/", Capability::Read)
        .await;
    harness
        .add_role_grant("dogs/", "cats/", Capability::Read)
        .await;
    harness.refresh_snapshot_authoritative().await;

    // Pin the pre-revocation Snapshot which the whole publication evaluates
    // against; the mid-publication refresh below must not displace it.
    let refresh = harness.snapshot_watch.token();
    let publication = publications::DraftPublication {
        user_id: dogs_user,
        logs_token: uuid::Uuid::new_v4(),
        dry_run: true,
        detail: Some("one snapshot across phases".to_string()),
        draft: dogs_materialize_cats_draft(),
        started_at: Some(tokens::now()),
        snapshot: refresh
            .result()
            .expect("authorization snapshot is not ready"),
        verify_user_authz: true,
        default_data_plane_name: Some("ops/dp/public/test".to_string()),
        initialize: (
            publications::ExpandDraft {
                filter_user_has_admin: true,
            },
            RevokeMidPublication {
                harness: &harness,
                dogs_user,
            },
        ),
        finalize: publications::PruneUnboundCollections,
        retry: publications::DoNotRetry,
        with_commit: publications::NoopWithCommit,
    };
    let result = harness
        .publisher
        .publish(publication)
        .await
        .expect("publish should not error");
    assert!(
        result.status.is_success(),
        "the pinned pre-revocation Snapshot should authorize both phases, got: {:?} draft: {:?} live: {:?} built: {:?}",
        result.status,
        result.draft.errors,
        result.live.errors,
        result.built.errors,
    );

    // Guard: the same draft judged against the refreshed watch is denied —
    // the revocation above is real, and the success was due to pinning. The
    // extra refresh stamps the Snapshot authoritative for this publication's
    // `started_at`, making the denial terminal rather than a stale retry.
    let started_at = tokens::now();
    harness.refresh_snapshot_authoritative().await;
    let guard_refresh = harness.snapshot_watch.token();
    let guard = publications::DraftPublication {
        user_id: dogs_user,
        logs_token: uuid::Uuid::new_v4(),
        dry_run: true,
        detail: Some("post-revocation guard".to_string()),
        draft: dogs_materialize_cats_draft(),
        started_at: Some(started_at),
        snapshot: guard_refresh
            .result()
            .expect("authorization snapshot is not ready"),
        verify_user_authz: true,
        default_data_plane_name: Some("ops/dp/public/test".to_string()),
        initialize: publications::ExpandDraft {
            filter_user_has_admin: true,
        },
        finalize: publications::PruneUnboundCollections,
        retry: publications::DoNotRetry,
        with_commit: publications::NoopWithCommit,
    };
    let denied = harness
        .publisher
        .publish(guard)
        .await
        .expect("guard publish should not error");
    assert!(
        !denied.status.is_success(),
        "the revoked, authoritative Snapshot must deny the same draft, got: {:?}",
        denied.status,
    );
}

/// The guard on the test above: a genuinely unauthorized publication must not be
/// hidden by the reschedule path. It reschedules only while the Snapshot is
/// inconclusive, then fails with the same authorization errors as before.
/// This also pins the anchor's other boundary: changes committed after the
/// queued publication carry no observation guarantee, so an authoritative
/// denial is terminal regardless of what commits later.
#[tokio::test]
async fn test_publication_stale_then_authoritative_denial() {
    let mut harness = TestHarness::init("test_publication_stale_denial").await;
    let dogs_user = setup_cross_tenant_publication(&mut harness).await;

    // No grants are ever added — only the Snapshot's age changes.
    harness.refresh_snapshot_stale().await;
    let pub_id = harness
        .queue_publication(
            dogs_user,
            "never authorized",
            Either::L(dogs_materialize_cats_draft()),
        )
        .await;

    let first = harness.poll_publication_once(pub_id).await;
    assert_eq!(
        publications::StatusType::Queued,
        first.status.r#type,
        "an inconclusive denial should reschedule, got: {:?}",
        first.errors
    );

    harness.refresh_snapshot_authoritative().await;
    harness.set_min_task_wake_at(pub_id).await;

    let second = harness.poll_publication_once(pub_id).await;
    assert!(!second.status.is_success());
    insta::assert_debug_snapshot!(second.errors, @r#"
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
}

/// Rescheduling alone isn't enough: the raising site must also cancel the
/// Snapshot's `revoke` token, which is what asks the background watch to refresh
/// ahead of its normal interval. Without it a stale publication would sleep
/// against an unchanged Snapshot until the next scheduled refresh.
#[tokio::test]
async fn test_publication_requests_snapshot_refresh() {
    let mut harness = TestHarness::init("test_publication_requests_refresh").await;
    let dogs_user = setup_cross_tenant_publication(&mut harness).await;

    harness.refresh_snapshot_stale().await;
    let token = harness.snapshot_watch.token();
    let snapshot = token.result().expect("snapshot should be ready");
    assert!(
        !snapshot.revoke.is_cancelled(),
        "a freshly-published Snapshot should not already be revoked"
    );

    let pub_id = harness
        .queue_publication(
            dogs_user,
            "requests refresh",
            Either::L(dogs_materialize_cats_draft()),
        )
        .await;
    let result = harness.poll_publication_once(pub_id).await;
    assert_eq!(publications::StatusType::Queued, result.status.r#type);

    assert!(
        snapshot.revoke.is_cancelled(),
        "a stale-snapshot publication must request an early Snapshot refresh"
    );
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
