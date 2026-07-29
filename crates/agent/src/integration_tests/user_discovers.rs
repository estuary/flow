use super::{spec_fixture, wrap_connector_schema};
use crate::{
    ControlPlane,
    discovers::JobStatus,
    integration_tests::harness::{
        SnapshotRefresher, TestHarness, UserDiscoverResult, connectors::MockDiscoverConnectors,
        draft_catalog, set_of,
    },
};
use control_plane_api::{
    discovers::{Discover, DiscoverHandler},
    proxy_connectors::DiscoverConnectors,
};
use models::Id;
use proto_flow::capture;
use proto_flow::capture::response::{Discovered, discovered::Binding};
use uuid::Uuid;

#[tokio::test]
async fn test_user_discovers() {
    let mut harness = TestHarness::init("test_user_discovers").await;

    let user_id = harness.setup_tenant("squirrels").await;

    let initial_resp = Discovered {
        bindings: vec![
            Binding {
                recommended_name: String::from("acorns"),
                document_schema_json: document_schema(1),
                resource_config_json: r#"{"id": "acorns"}"#.into(),
                key: vec!["/id".to_string()],
                disable: false,
                resource_path: Vec::new(),
                is_fallback_key: false,
            },
            Binding {
                recommended_name: String::from("walnuts"),
                document_schema_json: document_schema(1),
                resource_config_json: r#"{"id": "walnuts"}"#.into(),
                key: vec!["/id".to_string()],
                disable: false,
                resource_path: Vec::new(),
                is_fallback_key: false,
            },
            Binding {
                recommended_name: String::from("crab apples"),
                document_schema_json: document_schema(1),
                resource_config_json: r#"{"id": "crab apples"}"#.into(),
                key: vec!["/id".to_string()],
                disable: true,
                resource_path: Vec::new(),
                is_fallback_key: false,
            },
        ],
    };
    // Start with an empty draft
    let draft_id = harness
        .create_draft(user_id, "initial", Default::default())
        .await;

    let endpoint_config = r#"{"tail": "shake"}"#;
    let result = harness
        .user_discover(
            "source/test",
            ":test",
            "squirrels/capture-1",
            draft_id,
            endpoint_config,
            false,
            Ok((spec_fixture(), initial_resp)),
        )
        .await;
    assert!(
        result.job_status.is_success(),
        "expected success, got: {:?}",
        result.job_status
    );
    assert!(result.errors.is_empty());
    assert_eq!(
        3,
        result.draft.collections.len(),
        "expected 3 collections in draft: {:?}",
        result.draft
    );

    insta::assert_debug_snapshot!("initial-discover", result.draft);

    let pub_result = harness
        .create_user_publication(user_id, draft_id, "initial publication")
        .await;

    pub_result.errors.iter().for_each(|e| {
        println!("Error: {:?}", e);
    });
    assert!(pub_result.status.is_success());

    let published_specs = pub_result
        .live_specs
        .into_iter()
        .map(|ls| (ls.catalog_name, ls.spec_type, ls.spec))
        .collect::<Vec<_>>();
    // Expect to see only the two enabled collections. The `crab apples` should have been pruned.
    insta::assert_debug_snapshot!("initial-publication", published_specs);

    // Now discover again, and have it return some different collections so we
    // can test the merge behavior. Start with some changes already in the
    // draft, so we can assert that the merge handles those properly.
    let draft_id = harness
        .create_draft(
            user_id,
            "second discover",
            draft_catalog(serde_json::json!({
                "captures": {
                    "squirrels/capture-1": {
                        "endpoint": {
                            "connector": {
                                "image": "drafted/different/image:tag",
                                "config": { "drafted": {"config": "should be overwritten by discovers endpoint config" }}
                            }
                        },
                        "bindings": [
                            {
                                // This binding is enabled in the draft, and so
                                // should still be enabled in the merged result,
                                // even though the discover response now
                                // indicates disabled.
                                "resource": {
                                    "id": "walnuts",
                                    "expect": "this config should be retained after merge"
                                },
                                "target": "squirrels/walnuts",
                            },
                            {
                                "resource": {
                                    "id": "drafted",
                                    // This behavior may be unexpected since
                                    // we're passing `update_only` on the
                                    // discover, but it is consistent with the
                                    // previous behavior, which is to always
                                    // remove bindings that are not discovered.
                                    // That's because we assume that bindings
                                    // omitted from Discovered _cannot_ be
                                    // captured.
                                    "expect": "binding removed because it is not in discover response"
                                },
                                "target": "squirrels/drafted-collection"
                            }
                        ]
                    }
                },
                "collections": {
                    "squirrels/acorns": {
                        "schema": wrap_connector_schema(serde_json::json!({
                            "type": "object",
                            "properties": {
                                "id": { "type": "string" },
                                "drafted": { "type": "string" }
                            },
                            "required": ["id", "drafted"]
                        })),
                        "projections": {
                            "iiiiiideeeee": "/id"
                        },
                        "key": ["/drafted"]
                    },
                    "squirrels/walnuts": {
                        "writeSchema": wrap_connector_schema(serde_json::json!({
                            "type": "object",
                            "properties": {
                                "id": { "type": "string" },
                                "drafted": { "type": "string" }
                            },
                            "required": ["id", "drafted"]
                        })),
                        "readSchema": {
                            "type": "object",
                            "properties": {
                                "id": { "type": "string" },
                                "drafted": { "type": "string" }
                            },
                            "required": ["id", "drafted"]
                        },
                        // This key should be overwritten by the discover
                        "key": ["/drafted"]
                    },
                    "squirrels/extra": {
                        "schema": wrap_connector_schema(serde_json::json!({
                            "type": "object",
                            "properties": {
                                "id": { "type": "string" },
                            },
                            "required": ["id"]
                        })),
                        "key": ["/id"]
                    }
                }
            })),
        )
        .await;

    let next_discover = Discovered {
        bindings: vec![
            Binding {
                recommended_name: String::from("acorns"),
                document_schema_json: document_schema(2),
                resource_config_json: r#"{"id": "acorns"}"#.into(),
                key: vec!["/id".to_string()],
                disable: false,
                resource_path: Vec::new(),
                is_fallback_key: false,
            },
            Binding {
                recommended_name: String::from("walnuts"),
                document_schema_json: document_schema(2),
                resource_config_json: r#"{"id": "walnuts"}"#.into(),
                key: vec!["/id".to_string()],
                disable: true,
                resource_path: Vec::new(),
                is_fallback_key: false,
            },
            Binding {
                recommended_name: String::from("hickory nuts!"),
                document_schema_json: document_schema(2),
                resource_config_json: r#"{"id": "hickory-nuts"}"#.into(),
                key: vec!["/id".to_string()],
                disable: false,
                resource_path: Vec::new(),
                is_fallback_key: false,
            },
        ],
    };
    let endpoint_config = r##"{ "newConfig": "forDiscover" }"##;
    let result = harness
        .user_discover(
            "source/test",
            ":test",
            "squirrels/capture-1",
            draft_id,
            endpoint_config,
            true,
            Ok((spec_fixture(), next_discover.clone())),
        )
        .await;
    assert!(result.job_status.is_success());
    // Expect that the extra collection is still present in the draft, and that
    // the walnuts binding is the only one enabled. Acorns should be disabled
    // because it was removed in the drafted capture spec, and `update_only` was
    // true.
    insta::assert_debug_snapshot!("second-discover", result.draft);

    let pub_result = harness
        .create_user_publication(user_id, draft_id, "initial publication")
        .await;
    assert!(
        pub_result.status.is_success(),
        "pub failed with errors: {:?}",
        pub_result.errors
    );
    // Ensure that the extra collections got pruned during publish.
    let published_specs = pub_result
        .live_specs
        .into_iter()
        .map(|ls| (ls.catalog_name, ls.spec_type))
        .collect::<Vec<_>>();
    // Expect to see only the two enabled collections. The `crab apples` should have been pruned.
    insta::assert_debug_snapshot!(published_specs, @r###"
    [
        (
            "squirrels/acorns",
            Some(
                "collection",
            ),
        ),
        (
            "squirrels/capture-1",
            Some(
                "capture",
            ),
        ),
        (
            "squirrels/walnuts",
            Some(
                "collection",
            ),
        ),
    ]
    "###);

    // Discover returns an identical response, and so nothing should be updated.
    let names = set_of(&[
        "squirrels/capture-1",
        "squirrels/acorns",
        "squirrels/walnuts",
    ]);
    let tables::LiveCatalog {
        captures: starting_captures,
        collections: starting_collections,
        ..
    } = harness
        .control_plane()
        .get_live_specs(names.clone())
        .await
        .unwrap();

    let draft_id = harness
        .create_draft(user_id, "identical discover", Default::default())
        .await;
    let result = harness
        .user_discover(
            "source/test",
            ":test",
            "squirrels/capture-1",
            draft_id,
            endpoint_config,
            true,
            Ok((spec_fixture(), next_discover)),
        )
        .await;
    assert!(result.job_status.is_success());
    assert_eq!(4, result.draft.spec_count());
    let UserDiscoverResult {
        draft:
            tables::DraftCatalog {
                captures: discovered_captures,
                collections: discovered_collections,
                ..
            },
        ..
    } = result;

    let expected_capture = starting_captures.into_iter().next().unwrap().model;
    let actual_capture = discovered_captures.into_iter().next().unwrap();
    assert_eq!(Some(expected_capture), actual_capture.model);

    for live_collection in starting_collections {
        let Some(discovered) = discovered_collections.get_by_key(&live_collection.collection)
        else {
            panic!(
                "missing discovered collection for {}",
                live_collection.collection
            );
        };
        assert_eq!(Some(live_collection.model), discovered.model);
    }
}

// A data-plane that exists but that no tenant in these tests is granted to
// read. Registering it (rather than using a bogus name) is what separates an
// authorization denial from `NoDataPlane`.
const FOREIGN_DATA_PLANE: &str = "dogs/dp/private/test";

/// Queues a discover for `capture_name` against `FOREIGN_DATA_PLANE`, which the
/// caller's tenant has no grant to read.
async fn queue_foreign_dp_discover(harness: &mut TestHarness, user_id: Uuid, name: &str) -> Id {
    let draft_id = harness
        .create_draft(user_id, name, Default::default())
        .await;
    harness
        .queue_discover("source/test", ":test", name, draft_id, FOREIGN_DATA_PLANE)
        .await
}

/// A discover whose data-plane authorization is denied by a Snapshot that
/// predates the discover row must be retried, not failed: a grant that would
/// authorize it may simply not be reflected in that Snapshot yet. The row stays
/// queued and the task reschedules.
#[tokio::test]
async fn test_discover_reschedules_on_stale_data_plane_authz() {
    let mut harness = TestHarness::init("test_discover_reschedules_on_stale_data_plane").await;
    let user_id = harness.setup_tenant("cats").await;
    harness.add_data_plane(FOREIGN_DATA_PLANE).await;

    let disco_id = queue_foreign_dp_discover(&mut harness, user_id, "cats/capture-stale").await;
    harness.refresh_snapshot_stale().await;

    let ran = harness
        .run_automation_task(automations::task_types::DISCOVERS)
        .await;
    assert_eq!(Some(disco_id), ran, "expected the stale discover to run");
    assert!(
        matches!(
            harness.discover_job_status(disco_id).await,
            JobStatus::Queued
        ),
        "stale-snapshot discover should stay queued (rescheduled), got: {:?}",
        harness.discover_job_status(disco_id).await,
    );
}

/// The converse: once the Snapshot is authoritative for the discover row, the
/// same denial is definitive and resolves terminally rather than looping.
/// This is the anchor's other boundary: changes committed after the queued
/// discover carry no observation guarantee, so an authoritative denial is
/// terminal regardless of what commits later.
#[tokio::test]
async fn test_discover_unauthorized_data_plane_is_terminal() {
    let mut harness = TestHarness::init("test_discover_unauthorized_data_plane").await;
    let user_id = harness.setup_tenant("cats").await;
    harness.add_data_plane(FOREIGN_DATA_PLANE).await;

    let disco_id = queue_foreign_dp_discover(&mut harness, user_id, "cats/capture-authz").await;
    harness.refresh_snapshot_authoritative().await;

    let ran = harness
        .run_automation_task(automations::task_types::DISCOVERS)
        .await;
    assert_eq!(
        Some(disco_id),
        ran,
        "expected the authoritative discover to run"
    );
    assert!(
        matches!(
            harness.discover_job_status(disco_id).await,
            JobStatus::NotAuthorized
        ),
        "authoritative unauthorized discover should resolve NotAuthorized, got: {:?}",
        harness.discover_job_status(disco_id).await,
    );
}

/// The motivating race, end to end: the grant that authorizes the data-plane
/// commits before the discover is queued, but the Snapshot predates both and
/// holds the pre-grant world. The denial is provisional until a Snapshot
/// postdating the queued discover is consulted, so the first poll must
/// reschedule rather than resolve `NotAuthorized` — and any such refreshed
/// Snapshot is guaranteed to include the pre-queue grant, so the discover
/// then succeeds.
#[tokio::test]
async fn test_discover_succeeds_after_late_data_plane_grant() {
    let mut harness = TestHarness::init("test_discover_late_data_plane_grant").await;
    let user_id = harness.setup_tenant("cats").await;
    harness.add_data_plane(FOREIGN_DATA_PLANE).await;

    // Take the Snapshot *before* the grant is written and the discover is
    // queued, so it holds the pre-grant world — exactly as in production
    // between a `role_grants` insert and the next Snapshot refresh. Stamping
    // it in the past makes the denial of the later-queued discover
    // provisional rather than definitive.
    harness.refresh_snapshot_stale().await;
    harness
        .add_role_grant_unobserved("cats/", "dogs/dp/private/", models::Capability::Read)
        .await;

    let capture_name = "cats/capture-late-grant";
    let disco_id = queue_foreign_dp_discover(&mut harness, user_id, capture_name).await;
    harness.discover_handler.connectors.mock_discover(
        capture_name,
        Ok((spec_fixture(), single_binding_response("acorns"))),
    );

    let ran = harness
        .run_automation_task(automations::task_types::DISCOVERS)
        .await;
    assert_eq!(Some(disco_id), ran);
    assert!(
        matches!(
            harness.discover_job_status(disco_id).await,
            JobStatus::Queued
        ),
        "discover should reschedule while the grant is unobserved, got: {:?}",
        harness.discover_job_status(disco_id).await,
    );

    // The Snapshot catches up and the discover proceeds normally.
    harness.refresh_snapshot_authoritative().await;
    harness.set_min_task_wake_at(disco_id).await;

    let ran = harness
        .run_automation_task(automations::task_types::DISCOVERS)
        .await;
    assert_eq!(Some(disco_id), ran);
    let status = harness.discover_job_status(disco_id).await;
    assert!(
        matches!(status, JobStatus::Success { .. }),
        "discover should succeed once the grant is observed, got: {status:?}",
    );
}

/// The second, independent stale path through `DiscoverExecutor::process`: the
/// data-plane check passes, but `prepare_discover`'s `get_live_specs` denies the
/// discover's own capture against a Snapshot older than that capture. The error
/// arrives as `AuthorizationSnapshotStale` from `control-plane-api` and must be
/// mapped to a reschedule rather than a `DiscoverFailed` status.
#[tokio::test]
async fn test_discover_reschedules_on_stale_live_spec_authz() {
    let mut harness = TestHarness::init("test_discover_stale_live_spec_authz").await;
    let cats_user = harness.setup_tenant("cats").await;
    let dogs_user = harness.setup_tenant("dogs").await;

    // Publish a capture owned by `cats`.
    let capture_name = "cats/capture-owned";
    let pub_result = harness
        .user_publication(
            cats_user,
            "publish cats capture",
            draft_catalog(serde_json::json!({
                "captures": { capture_name: minimal_capture() },
            })),
        )
        .await;
    assert!(
        pub_result.status.is_success(),
        "setup publication failed: {:?}",
        pub_result.errors
    );

    // `dogs` may read the shared data-plane (every tenant is granted
    // `ops/dp/public/`), so the data-plane precheck passes and we reach the
    // live-spec authorization inside `prepare_discover`. `dogs` has no grant to
    // `cats/`, and the Snapshot predates the capture's publication.
    let draft_id = harness
        .create_draft(dogs_user, "cross-tenant discover", Default::default())
        .await;
    let disco_id = harness
        .queue_discover(
            "source/test",
            ":test",
            capture_name,
            draft_id,
            "ops/dp/public/test",
        )
        .await;
    harness.refresh_snapshot_stale().await;

    let ran = harness
        .run_automation_task(automations::task_types::DISCOVERS)
        .await;
    assert_eq!(Some(disco_id), ran);
    assert!(
        matches!(
            harness.discover_job_status(disco_id).await,
            JobStatus::Queued
        ),
        "stale live-spec authorization should reschedule, got: {:?}",
        harness.discover_job_status(disco_id).await,
    );

    // With an authoritative Snapshot the denial stops being retryable and the
    // discover reaches a terminal status instead of looping forever.
    harness.refresh_snapshot_authoritative().await;
    harness.discover_handler.connectors.mock_discover(
        capture_name,
        Ok((spec_fixture(), single_binding_response("kibble"))),
    );
    harness.set_min_task_wake_at(disco_id).await;

    let ran = harness
        .run_automation_task(automations::task_types::DISCOVERS)
        .await;
    assert_eq!(Some(disco_id), ran);
    // Once authoritative, the denial stops being retryable. `get_live_specs`
    // falls back to its pre-existing behavior of silently omitting the
    // unreadable spec, so the discover proceeds as if the capture were new
    // rather than looping. Documenting the concrete status (not merely
    // "terminal") keeps that silent drop visible.
    let status = harness.discover_job_status(disco_id).await;
    assert!(
        matches!(status, JobStatus::Success { .. }),
        "discover should reach a terminal status once the Snapshot is authoritative, got: {status:?}",
    );
}

/// A collection which a capture binding targets across tenant lines. Its owner
/// (`dogs`) publishes it with a user-set projection; the discovering user
/// (`cats`) needs a read grant to it for the merge to see it.
const SHARED_COLLECTION: &str = "dogs/shared/data";

/// Publishes `SHARED_COLLECTION` under `dogs`, ages it so its publication time
/// cannot mask request-relative staleness, and drafts a `cats` capture whose
/// binding targets it. Returns the draft id for `queue_discover`.
async fn setup_shared_collection_discover(
    harness: &mut TestHarness,
    cats_user: Uuid,
    dogs_user: Uuid,
    capture_name: &str,
) -> Id {
    // The writer capture isn't incidental: a draft holding only a collection
    // with no writer builds to zero specs and is reported as an empty draft.
    let pub_result = harness
        .user_publication(
            dogs_user,
            "publish shared collection",
            draft_catalog(serde_json::json!({
                "collections": {
                    SHARED_COLLECTION: {
                        // Wrapped as a connector-managed schema: the merge
                        // refuses to update collections whose schemas
                        // auto-discover doesn't manage.
                        "schema": wrap_connector_schema(serde_json::json!({
                            "type": "object",
                            "properties": { "id": { "type": "string" } },
                            "required": ["id"]
                        })),
                        "key": ["/id"],
                        "projections": { "id_projection": "/id" }
                    }
                },
                "captures": {
                    "dogs/shared/writer": {
                        "endpoint": {
                            "connector": { "image": "source/test:test", "config": {} }
                        },
                        "bindings": [
                            { "resource": { "id": "data" }, "target": SHARED_COLLECTION }
                        ]
                    }
                },
            })),
        )
        .await;
    assert!(
        pub_result.status.is_success(),
        "setup publication failed: {:?} {:?}",
        pub_result.status,
        pub_result.errors
    );
    harness.age_live_spec(SHARED_COLLECTION).await;

    harness
        .create_draft(
            cats_user,
            "shared collection discover",
            draft_catalog(serde_json::json!({
                "captures": {
                    capture_name: {
                        "endpoint": {
                            "connector": { "image": "source/test:test", "config": {} }
                        },
                        "bindings": [
                            { "resource": { "id": "data" }, "target": SHARED_COLLECTION }
                        ],
                    }
                },
            })),
        )
        .await
}

/// Asserts the drafted `SHARED_COLLECTION` was merged *from the live
/// collection*: it expects the live, nonzero publication id and keeps the
/// owner's projection. A collection drafted from scratch — what a silently
/// dropped authorization produces — has `expect_pub_id: zero` and no
/// projections, so each assertion is discriminating on its own.
fn assert_live_collection_preserved(draft: &tables::DraftCatalog) {
    let drafted = draft
        .collections
        .get_by_key(&models::Collection::new(SHARED_COLLECTION))
        .expect("the target collection should be drafted");
    assert!(
        drafted.expect_pub_id.is_some_and(|id| !id.is_zero()),
        "the drafted collection should expect the live publication id, got: {:?}",
        drafted.expect_pub_id,
    );
    let model = drafted.model.as_ref().expect("drafted collection model");
    assert!(
        model
            .projections
            .contains_key(&models::Field::new("id_projection")),
        "the live collection's projection should be preserved, got: {:?}",
        model.projections,
    );
}

/// The merge phase fetches the capture's target collections with the user's
/// read capability, and its staleness anchor must be the discover request —
/// not the target collection's own age. This is the late-observation race for
/// a *collection*: the grant to `dogs/shared/` commits before the discover is
/// queued, but the Snapshot predates both. Judged spec-relatively the (aged)
/// collection makes the denial authoritative, and it is silently dropped: the
/// discover "succeeds", re-drafting the collection from scratch with a zeroed
/// publication id. Judged request-relatively the denial is provisional, the
/// discover reschedules, and a refreshed Snapshot — guaranteed to include the
/// pre-queue grant — preserves the live collection.
#[tokio::test]
async fn test_discover_reschedules_on_stale_collection_authz() {
    let mut harness = TestHarness::init("test_discover_stale_collection_authz").await;
    let cats_user = harness.setup_tenant("cats").await;
    let dogs_user = harness.setup_tenant("dogs").await;

    let capture_name = "cats/capture-shared";
    let draft_id =
        setup_shared_collection_discover(&mut harness, cats_user, dogs_user, capture_name).await;

    // The Snapshot holds the pre-grant world; the grant and the discover row
    // both come after it, in that order.
    harness.refresh_snapshot_stale().await;
    harness
        .add_role_grant_unobserved("cats/", "dogs/shared/", models::Capability::Read)
        .await;

    let disco_id = harness
        .queue_discover(
            "source/test",
            ":test",
            capture_name,
            draft_id,
            "ops/dp/public/test",
        )
        .await;
    harness.discover_handler.connectors.mock_discover(
        capture_name,
        Ok((spec_fixture(), single_binding_response("data"))),
    );

    let ran = harness
        .run_automation_task(automations::task_types::DISCOVERS)
        .await;
    assert_eq!(Some(disco_id), ran);
    assert!(
        matches!(
            harness.discover_job_status(disco_id).await,
            JobStatus::Queued
        ),
        "a stale denial of the binding's target collection should reschedule, got: {:?}",
        harness.discover_job_status(disco_id).await,
    );

    // The Snapshot catches up, observing the grant: the discover completes and
    // the merge is based on the live collection.
    harness.refresh_snapshot_authoritative().await;
    harness.set_min_task_wake_at(disco_id).await;

    let ran = harness
        .run_automation_task(automations::task_types::DISCOVERS)
        .await;
    assert_eq!(Some(disco_id), ran);

    let result = UserDiscoverResult::load(disco_id, &harness.pool).await;
    assert!(
        result.job_status.is_success(),
        "discover should succeed once the grant is observed, got: {:?} with errors: {:?}",
        result.job_status,
        result.errors,
    );
    assert_live_collection_preserved(&result.draft);
}

/// The authorized baseline for the case above: with the read grant already
/// observed, the same discover succeeds on its first poll and the merge
/// preserves the live collection. This pins the preservation observable
/// independently of any staleness handling, so the retry test can't pass
/// vacuously.
#[tokio::test]
async fn test_discover_preserves_authorized_live_collection() {
    let mut harness = TestHarness::init("test_discover_authorized_collection").await;
    let cats_user = harness.setup_tenant("cats").await;
    let dogs_user = harness.setup_tenant("dogs").await;

    let capture_name = "cats/capture-shared";
    let draft_id =
        setup_shared_collection_discover(&mut harness, cats_user, dogs_user, capture_name).await;
    harness
        .add_role_grant("cats/", "dogs/shared/", models::Capability::Read)
        .await;

    let disco_id = harness
        .queue_discover(
            "source/test",
            ":test",
            capture_name,
            draft_id,
            "ops/dp/public/test",
        )
        .await;
    harness.discover_handler.connectors.mock_discover(
        capture_name,
        Ok((spec_fixture(), single_binding_response("data"))),
    );
    harness.refresh_snapshot_authoritative().await;

    let ran = harness
        .run_automation_task(automations::task_types::DISCOVERS)
        .await;
    assert_eq!(Some(disco_id), ran);

    let result = UserDiscoverResult::load(disco_id, &harness.pool).await;
    assert!(
        result.job_status.is_success(),
        "an authorized discover should succeed, got: {:?} with errors: {:?}",
        result.job_status,
        result.errors,
    );
    assert_live_collection_preserved(&result.draft);
}

/// The complement of `test_discover_reschedules_on_stale_live_spec_authz`,
/// and the reported scenario end to end: an *existing* capture with
/// non-default bindings and settings, whose reader is granted access just
/// before queuing a re-discover — after the Snapshot was taken. The capture
/// is aged, so a spec-relative staleness anchor would call the stale denial
/// authoritative, silently filter the live capture, and "succeed" with a
/// starter baseline — `expect_pub_id: 0`, no bindings, default settings.
/// Anchored to the discover request the denial is provisional: the first
/// poll reschedules, and a refreshed Snapshot — guaranteed to include the
/// pre-queue grant — preserves the live capture: its nonzero publication id,
/// its binding, and its non-default interval. Each assertion is
/// discriminating on its own, because the starter baseline has none of them.
#[tokio::test]
async fn test_discover_preserves_live_capture_after_late_grant() {
    let mut harness = TestHarness::init("test_discover_capture_late_grant").await;
    let cats_user = harness.setup_tenant("cats").await;
    let dogs_user = harness.setup_tenant("dogs").await;

    // Publish a capture owned by `cats` with distinctive, non-default
    // properties: a bound collection and a 42-minute interval. The collection
    // schema is connector-managed so a re-discover may merge into it. The
    // capture and collection live under *different* sub-prefixes: `dogs` gets
    // an observed grant to the collection below, so that only the capture's
    // own authorization rides on the late grant — otherwise the (correctly
    // request-anchored) collection check would also reschedule and mask a
    // regression of the capture anchor.
    let capture_name = "cats/in/capture-owned";
    let pub_result = harness
        .user_publication(
            cats_user,
            "publish cats capture",
            draft_catalog(serde_json::json!({
                "collections": {
                    "cats/data/noms": {
                        "schema": wrap_connector_schema(serde_json::json!({
                            "type": "object",
                            "properties": { "id": { "type": "string" } },
                            "required": ["id"]
                        })),
                        "key": ["/id"]
                    }
                },
                "captures": {
                    capture_name: {
                        "endpoint": {
                            "connector": { "image": "source/test:test", "config": {} }
                        },
                        "bindings": [
                            { "resource": { "id": "noms" }, "target": "cats/data/noms" }
                        ],
                        "interval": "42m"
                    }
                },
            })),
        )
        .await;
    assert!(
        pub_result.status.is_success(),
        "setup publication failed: {:?} {:?}",
        pub_result.status,
        pub_result.errors
    );
    // Age the capture: a spec-relative anchor would now judge any recent
    // Snapshot authoritative for it, which is exactly the regression this
    // test discriminates against.
    harness.age_live_spec(capture_name).await;
    // The collection grant is observed from the start.
    harness
        .add_role_grant("dogs/", "cats/data/", models::Capability::Read)
        .await;

    let draft_id = harness
        .create_draft(dogs_user, "late-grant re-discover", Default::default())
        .await;

    // The Snapshot observes the collection grant but not the capture's,
    // which commits after it and just before the discover is queued.
    harness.refresh_snapshot_stale().await;
    harness
        .add_role_grant_unobserved("dogs/", "cats/in/", models::Capability::Read)
        .await;

    let disco_id = harness
        .queue_discover(
            "source/test",
            ":test",
            capture_name,
            draft_id,
            "ops/dp/public/test",
        )
        .await;
    harness.discover_handler.connectors.mock_discover(
        capture_name,
        Ok((spec_fixture(), single_binding_response("noms"))),
    );

    let ran = harness
        .run_automation_task(automations::task_types::DISCOVERS)
        .await;
    assert_eq!(Some(disco_id), ran);
    assert!(
        matches!(
            harness.discover_job_status(disco_id).await,
            JobStatus::Queued
        ),
        "a stale denial of the existing capture should reschedule, got: {:?}",
        harness.discover_job_status(disco_id).await,
    );

    // The Snapshot observes the grant; the discover completes against the
    // live capture rather than a starter baseline.
    harness.refresh_snapshot_authoritative().await;
    harness.set_min_task_wake_at(disco_id).await;

    let ran = harness
        .run_automation_task(automations::task_types::DISCOVERS)
        .await;
    assert_eq!(Some(disco_id), ran);

    let result = UserDiscoverResult::load(disco_id, &harness.pool).await;
    assert!(
        result.job_status.is_success(),
        "discover should succeed once the grant is observed, got: {:?} with errors: {:?}",
        result.job_status,
        result.errors,
    );

    let drafted = result
        .draft
        .captures
        .get_by_key(&models::Capture::new(capture_name))
        .expect("the capture should be drafted");
    assert!(
        drafted.expect_pub_id.is_some_and(|id| !id.is_zero()),
        "the drafted capture should expect the live publication id, got: {:?}",
        drafted.expect_pub_id,
    );
    let model = drafted.model.as_ref().expect("drafted capture model");
    assert_eq!(
        vec!["cats/data/noms"],
        model
            .bindings
            .iter()
            .map(|b| b.target.as_str())
            .collect::<Vec<_>>(),
        "the live capture's binding should be preserved",
    );
    assert_eq!(
        std::time::Duration::from_secs(42 * 60),
        model.interval,
        "the live capture's non-default interval should be preserved",
    );
    assert!(
        model.auto_discover.is_none(),
        "a preserved live capture must not gain the starter's auto_discover",
    );
}

/// A `DiscoverConnectors` which revokes the grant authorizing the discover's
/// target collection — and pushes a refreshed, authoritative Snapshot into
/// the watch — while the connector RPC is in flight, before answering with
/// the underlying mock. This is production's shape: connector RPCs run for
/// seconds, and grant changes and Snapshot refreshes land freely within them.
#[derive(Clone)]
struct RevokeMidRpc {
    pool: sqlx::PgPool,
    refresher: SnapshotRefresher,
    inner: MockDiscoverConnectors,
}

impl DiscoverConnectors for RevokeMidRpc {
    async fn discover<'a>(
        &'a self,
        data_plane: &'a tables::DataPlane,
        task: &'a models::Capture,
        logs_token: Uuid,
        request: capture::Request,
    ) -> anyhow::Result<(capture::response::Spec, capture::response::Discovered)> {
        sqlx::query(
            "delete from role_grants where subject_role = 'cats/' and object_role = 'dogs/shared/'",
        )
        .execute(&self.pool)
        .await?;
        self.refresher.refresh_authoritative().await;
        self.inner
            .discover(data_plane, task, logs_token, request)
            .await
    }
}

/// One discover must evaluate authorization against exactly one Snapshot: the
/// executor pins the watch's Snapshot once, and it rides the `Discover`
/// through the connector RPC into the merge. A grant change landing while the
/// RPC is in flight must not cause the capture baseline (resolved before the
/// RPC) and the collection baselines (resolved after it) to come from
/// different views.
///
/// `RevokeMidRpc` deletes the authorizing grant and refreshes the watch from
/// inside the RPC. The merge still resolves `SHARED_COLLECTION` under the
/// pinned pre-revocation Snapshot, so the discover succeeds and preserves the
/// live collection. The guard discover then pins the post-revocation Snapshot
/// and shows the same merge silently drops the collection — so the first
/// result is attributable to pinning alone.
#[tokio::test]
async fn test_discover_uses_one_snapshot_across_connector_rpc() {
    let mut harness = TestHarness::init("test_discover_one_snapshot_across_rpc").await;
    let cats_user = harness.setup_tenant("cats").await;
    let dogs_user = harness.setup_tenant("dogs").await;

    let capture_name = "cats/capture-shared";
    let draft_id =
        setup_shared_collection_discover(&mut harness, cats_user, dogs_user, capture_name).await;
    // Snapshot A: the grant written and observed, stamped authoritative.
    harness
        .add_role_grant("cats/", "dogs/shared/", models::Capability::Read)
        .await;
    harness.refresh_snapshot_authoritative().await;

    let mut mock = MockDiscoverConnectors::default();
    mock.mock_discover(
        capture_name,
        Ok((spec_fixture(), single_binding_response("data"))),
    );
    let handler = DiscoverHandler::new(RevokeMidRpc {
        pool: harness.pool.clone(),
        refresher: harness.snapshot_refresher(),
        inner: mock,
    });

    // Pin Snapshot A and assemble the request, as `DiscoverExecutor::process`
    // and `prepare_discover` do.
    let draft = control_plane_api::draft::load_draft(draft_id, &harness.pool)
        .await
        .unwrap();
    let snapshot = harness.snapshot_watch.token();
    let snapshot = snapshot.result().unwrap();
    let data_plane = snapshot
        .data_plane_by_catalog_name("ops/dp/public/test")
        .expect("test data-plane exists")
        .clone();
    let output = handler
        .discover(
            &harness.pool,
            Discover {
                capture_name: models::Capture::new(capture_name),
                data_plane,
                logs_token: Uuid::new_v4(),
                user_id: cats_user,
                filter_user_authz: true,
                update_only: false,
                reset_on_key_change: false,
                draft,
                created_at: String::new(),
                snapshot,
                started_at: Some(tokens::now()),
            },
        )
        .await
        .expect("discover should not error");
    assert!(
        output.is_success(),
        "the pinned pre-revocation Snapshot should authorize the merge, got: {:?}",
        output.draft.errors,
    );
    assert_live_collection_preserved(&output.draft);

    // Guard: the same discover pinning the post-revocation Snapshot cannot see
    // the live collection: the merge silently drops it and re-drafts it from
    // scratch. The extra refresh stamps the Snapshot authoritative for this
    // discover's `started_at`, making the denial terminal rather than stale.
    let started_at = tokens::now();
    harness.refresh_snapshot_authoritative().await;

    let mut mock = MockDiscoverConnectors::default();
    mock.mock_discover(
        capture_name,
        Ok((spec_fixture(), single_binding_response("data"))),
    );
    let handler = DiscoverHandler::new(mock);
    let draft = control_plane_api::draft::load_draft(draft_id, &harness.pool)
        .await
        .unwrap();
    let snapshot = harness.snapshot_watch.token();
    let snapshot = snapshot.result().unwrap();
    let data_plane = snapshot
        .data_plane_by_catalog_name("ops/dp/public/test")
        .expect("test data-plane exists")
        .clone();
    let output = handler
        .discover(
            &harness.pool,
            Discover {
                capture_name: models::Capture::new(capture_name),
                data_plane,
                logs_token: Uuid::new_v4(),
                user_id: cats_user,
                filter_user_authz: true,
                update_only: false,
                reset_on_key_change: false,
                draft,
                created_at: String::new(),
                snapshot,
                started_at: Some(started_at),
            },
        )
        .await
        .expect("guard discover should not error");
    assert!(
        output.is_success(),
        "an authoritative denial silently drops the collection, got: {:?}",
        output.draft.errors,
    );
    let drafted = output
        .draft
        .collections
        .get_by_key(&models::Collection::new(SHARED_COLLECTION))
        .expect("the target collection should be drafted");
    assert_eq!(
        Some(models::Id::zero()),
        drafted.expect_pub_id,
        "under the revoked view the live collection is invisible and re-drafted from scratch",
    );
}

/// A data-plane registration that lands after the current Snapshot must be
/// observable to a discover queued afterward. Until the Snapshot catches up,
/// the missing plane is provisional: the discover stays queued and requests an
/// early refresh. It proceeds once an authoritative Snapshot includes the
/// registration.
#[tokio::test]
async fn test_discover_succeeds_after_late_data_plane_registration() {
    let mut harness = TestHarness::init("test_discover_late_data_plane_registration").await;
    let user_id = harness.setup_tenant("cats").await;
    let data_plane_name = "ops/dp/public/late-registration";
    let capture_name = "cats/capture-late-data-plane";

    // Snapshot A includes the tenant's grant to `ops/dp/public/`, but not the
    // concrete plane which is registered immediately afterward.
    harness.refresh_snapshot_stale().await;
    let token = harness.snapshot_watch.token();
    let snapshot = token.result().expect("snapshot should be ready");
    assert!(
        !snapshot.revoke.is_cancelled(),
        "a freshly-published Snapshot should not already be revoked"
    );
    harness.add_data_plane(data_plane_name).await;

    let draft_id = harness
        .create_draft(user_id, "late data-plane discover", Default::default())
        .await;
    let disco_id = harness
        .queue_discover(
            "source/test",
            ":test",
            capture_name,
            draft_id,
            data_plane_name,
        )
        .await;
    harness.discover_handler.connectors.mock_discover(
        capture_name,
        Ok((spec_fixture(), single_binding_response("acorns"))),
    );

    let ran = harness
        .run_automation_task(automations::task_types::DISCOVERS)
        .await;
    assert_eq!(Some(disco_id), ran);
    assert!(
        matches!(
            harness.discover_job_status(disco_id).await,
            JobStatus::Queued
        ),
        "a plane missing from a stale Snapshot should reschedule, got: {:?}",
        harness.discover_job_status(disco_id).await,
    );
    assert!(
        snapshot.revoke.is_cancelled(),
        "a stale missing-plane decision must request an early Snapshot refresh"
    );

    harness.refresh_snapshot_authoritative().await;
    harness.set_min_task_wake_at(disco_id).await;

    let ran = harness
        .run_automation_task(automations::task_types::DISCOVERS)
        .await;
    assert_eq!(Some(disco_id), ran);
    let status = harness.discover_job_status(disco_id).await;
    assert!(
        matches!(status, JobStatus::Success { .. }),
        "discover should succeed once the plane is observed, got: {status:?}",
    );
}

/// An authorized plane that remains absent after an authoritative refresh must
/// resolve as `NoDataPlane`. The stale first poll is provisional, while the
/// authoritative second poll is terminal; neither may invoke the connector.
#[tokio::test]
async fn test_discover_missing_data_plane_is_terminal_after_refresh() {
    let mut harness = TestHarness::init("test_discover_missing_data_plane").await;
    let user_id = harness.setup_tenant("cats").await;
    let capture_name = "cats/capture-missing-dp";
    let data_plane_name = "ops/dp/public/does-not-exist";

    // `setup_tenant` grants `cats/ -> ops/dp/public/ read`, so this name passes
    // authorization; Snapshot A and Postgres both lack the concrete plane.
    harness.refresh_snapshot_stale().await;
    let draft_id = harness
        .create_draft(user_id, "missing dp discover", Default::default())
        .await;
    let disco_id = harness
        .queue_discover(
            "source/test",
            ":test",
            capture_name,
            draft_id,
            data_plane_name,
        )
        .await;

    let ran = harness
        .run_automation_task(automations::task_types::DISCOVERS)
        .await;
    assert_eq!(Some(disco_id), ran);
    assert!(
        matches!(
            harness.discover_job_status(disco_id).await,
            JobStatus::Queued
        ),
        "a plane missing from a stale Snapshot should reschedule, got: {:?}",
        harness.discover_job_status(disco_id).await,
    );
    assert!(
        harness
            .discover_handler
            .connectors
            .last_discover_request(capture_name)
            .is_none(),
        "the connector must not be invoked while plane existence is unknown"
    );

    harness.refresh_snapshot_authoritative().await;
    harness.set_min_task_wake_at(disco_id).await;

    let ran = harness
        .run_automation_task(automations::task_types::DISCOVERS)
        .await;
    assert_eq!(Some(disco_id), ran);
    assert!(
        matches!(
            harness.discover_job_status(disco_id).await,
            JobStatus::NoDataPlane
        ),
        "a plane missing from an authoritative Snapshot should be NoDataPlane, got: {:?}",
        harness.discover_job_status(disco_id).await,
    );
    assert!(
        harness
            .discover_handler
            .connectors
            .last_discover_request(capture_name)
            .is_none(),
        "the connector must not be invoked for a missing plane"
    );
}

/// `JobStatus::NotAuthorized` is new, and job statuses round-trip through a JSON
/// column. Pin its serialized form so a rename can't silently orphan rows that
/// were written with the old spelling.
#[test]
fn test_job_status_not_authorized_serde() {
    let encoded = serde_json::to_value(&JobStatus::NotAuthorized).unwrap();
    assert_eq!(serde_json::json!({"type": "notAuthorized"}), encoded);
    assert!(matches!(
        serde_json::from_value::<JobStatus>(encoded).unwrap(),
        JobStatus::NotAuthorized
    ));
}

/// A discovered response with a single enabled binding, sufficient for a
/// discover to merge and succeed.
fn single_binding_response(name: &str) -> Discovered {
    Discovered {
        bindings: vec![Binding {
            recommended_name: name.to_string(),
            document_schema_json: document_schema(1),
            resource_config_json: format!(r#"{{"id": "{name}"}}"#).into(),
            key: vec!["/id".to_string()],
            disable: false,
            resource_path: Vec::new(),
            is_fallback_key: false,
        }],
    }
}

fn minimal_capture() -> serde_json::Value {
    serde_json::json!({
        "endpoint": {
            "connector": {
                "image": "source/test:test",
                "config": {}
            }
        },
        "bindings": [],
    })
}

fn document_schema(version: usize) -> bytes::Bytes {
    serde_json::to_string(&serde_json::json!({
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "nuttiness": { "type": "number", "maximum": version },
        },
        "required": ["id"]
    }))
    .unwrap()
    .into()
}
