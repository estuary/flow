use super::{spec_fixture, wrap_connector_schema};
use crate::{
    ControlPlane,
    discovers::JobStatus,
    integration_tests::harness::{TestHarness, UserDiscoverResult, draft_catalog, set_of},
};
use models::Id;
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
/// lands in Postgres *after* the discover is queued and is not yet reflected in
/// the Snapshot. The first poll must reschedule rather than emit a spurious
/// `NotAuthorized`, and the discover must succeed once the Snapshot catches up.
#[tokio::test]
async fn test_discover_succeeds_after_late_data_plane_grant() {
    let mut harness = TestHarness::init("test_discover_late_data_plane_grant").await;
    let user_id = harness.setup_tenant("cats").await;
    harness.add_data_plane(FOREIGN_DATA_PLANE).await;

    let capture_name = "cats/capture-late-grant";
    let disco_id = queue_foreign_dp_discover(&mut harness, user_id, capture_name).await;
    harness.discover_handler.connectors.mock_discover(
        capture_name,
        Ok((spec_fixture(), single_binding_response("acorns"))),
    );

    // Take the Snapshot *before* the grant is written, so it holds the pre-grant
    // world — exactly as in production between a `role_grants` insert and the
    // next Snapshot refresh. Stamping it in the past makes the resulting denial
    // retryable rather than definitive.
    harness.refresh_snapshot_stale().await;
    harness
        .add_role_grant_unobserved("cats/", "dogs/dp/private/", models::Capability::Read)
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

/// Authorization and existence used to be one SQL query, so a missing data-plane
/// and an unauthorized one were indistinguishable. They are now separate checks:
/// an authorized-but-unregistered plane must still be `NoDataPlane`, and must not
/// be mistaken for a stale-authorization reschedule.
#[tokio::test]
async fn test_discover_missing_data_plane_is_terminal() {
    let mut harness = TestHarness::init("test_discover_missing_data_plane").await;
    let user_id = harness.setup_tenant("cats").await;

    // `setup_tenant` grants `cats/ -> ops/dp/public/ read`, so this name passes
    // authorization; it simply has no `data_planes` row.
    let draft_id = harness
        .create_draft(user_id, "missing dp discover", Default::default())
        .await;
    let disco_id = harness
        .queue_discover(
            "source/test",
            ":test",
            "cats/capture-missing-dp",
            draft_id,
            "ops/dp/public/does-not-exist",
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
            JobStatus::NoDataPlane
        ),
        "an authorized but unregistered data-plane should be NoDataPlane even against a stale Snapshot, got: {:?}",
        harness.discover_job_status(disco_id).await,
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
