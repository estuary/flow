use models::{CatalogType, Id};
use uuid::Uuid;

use crate::{
    integration_tests::harness::{draft_catalog, InjectBuildError, TestHarness},
    publications::{DefaultRetryPolicy, JobStatus, LockFailure, NoopWithCommit, RetryPolicy},
    ControlPlane,
};

#[tokio::test]
#[serial_test::serial]
async fn test_publication_concurrent_commits() {
    let harness = TestHarness::init("test_publication_concurrent_commits").await;

    let user_id = harness.setup_tenant("beavers").await;

    let draft = draft_catalog(serde_json::json!({
        "collections": {
            "beavers/dens": minimal_collection(None),
        },
        "captures": {
            "beavers/dams": minimal_capture(None, &["beavers/dens"]),
        }
    }));

    // Try to reproduce a scenario where multiple different publications all try to commit
    // concurrently. We'll expect exactly one of them to succeed, and the others to fail.
    for test_iteration in 0..5 {
        let publisher_1 = harness.publisher.clone();
        let publisher_2 = harness.publisher.clone();
        let publisher_3 = harness.publisher.clone();

        let build_1 = publisher_1
            .build(
                user_id,
                Id::new([test_iteration, 1, 1, 1, 1, 1, 1, 1]),
                Some("build_1".to_string()),
                draft.clone_specs(),
                Uuid::new_v4(),
                "ops/dp/public/test",
                true,
                0,
            )
            .await
            .unwrap();

        let build_2 = publisher_2
            .build(
                user_id,
                Id::new([test_iteration, 1, 1, 1, 1, 1, 1, 2]),
                Some("build_2".to_string()),
                draft.clone_specs(),
                Uuid::new_v4(),
                "ops/dp/public/test",
                true,
                0,
            )
            .await
            .unwrap();
        let build_3 = publisher_3
            .build(
                user_id,
                Id::new([test_iteration, 1, 1, 1, 1, 1, 1, 3]),
                Some("build_3".to_string()),
                draft.clone_specs(),
                Uuid::new_v4(),
                "ops/dp/public/test",
                true,
                0,
            )
            .await
            .unwrap();

        let handle_1 =
            tokio::spawn(async move { publisher_1.commit(build_1, NoopWithCommit).await.unwrap() });
        let handle_2 =
            tokio::spawn(async move { publisher_2.commit(build_2, NoopWithCommit).await.unwrap() });
        let handle_3 =
            tokio::spawn(async move { publisher_3.commit(build_3, NoopWithCommit).await.unwrap() });

        let mut success_count = 0;
        for (build, handle) in [handle_1, handle_2, handle_3].into_iter().enumerate() {
            let result = handle.await.unwrap();

            if result.status.is_success() {
                success_count += 1;
            }
            // if result.as_ref().is_ok_and(|s| s.is_success()) {
            //     success_count += 1;
            // }
            tracing::debug!(%test_iteration, %build, result = ?result.status, "finished commit");
        }
        assert_eq!(
            1, success_count,
            "expected exactly one successful commit for iteration {test_iteration}"
        );
    }
}

#[tokio::test]
#[serial_test::serial]
async fn test_publication_optimistic_locking_failures() {
    let mut harness = TestHarness::init("test_publication_optimistic_locking_failures").await;

    let user_id = harness.setup_tenant("mice").await;

    // If expect_pub_id is anything but `0` for a new spec, the publication should fail
    let wrong_expect_pub_ids = draft_catalog(serde_json::json!({
        "collections": {
            "mice/does-not-exist": minimal_collection(Some(Id::new([1, 2, 3, 4, 5, 6, 7, 8]))),
        },
        "captures": {
            "mice/also-new": minimal_capture(Some(Id::new([8, 7, 6, 5, 4, 3, 2, 1])), &["mice/does-not-exist"]),
        }
    }));
    let naughty_pub_id = Id::new([8; 8]);
    // If a user explicitly sets `expectPubId` in the model, then a mismatch gets returned as a
    // build error, before we even try to commit.
    let result = harness
        .publisher
        .build(
            user_id,
            naughty_pub_id,
            Some("wrong expect_pub_ids".to_string()),
            wrong_expect_pub_ids,
            Uuid::new_v4(),
            "ops/dp/public/test",
            true,
            0,
        )
        .await
        .expect("build failed");
    let errors = result
        .output
        .errors()
        .map(|e| (e.scope.to_string(), e.error.to_string()))
        .collect::<Vec<_>>();
    assert_eq!(
        vec![
            (
            "flow://collection/mice/does-not-exist".to_string(),
            "expected publication ID 0102030405060708 was not matched (it's actually 0000000000000000): your changes have already been published or another publication has modified this spec; please try again with a fresh copy of the spec.".to_string()
            )
        ],
        errors);
    assert!(
        !DefaultRetryPolicy.retry(&result.build_failed()),
        "expectPubId mismatch should be terminal"
    );

    // Simulate a race between two publications of this initial draft, and assert that only one
    // of them can successfully commit.
    let initial_catalog = serde_json::json!({
        "collections": {
            // Test that explicit zero-value `expect_pub_id` works the same
            "mice/cheese": minimal_collection(Some(Id::zero())),
            "mice/seeds": minimal_collection(None),
        },
        "captures": {
            "mice/capture": minimal_capture(None, &["mice/cheese", "mice/seeds"]),
        }
    });

    let will_fail_pub = Id::new([9; 8]);
    let will_fail_build = harness
        .publisher
        .build(
            user_id,
            will_fail_pub,
            Some("pub a".to_string()),
            draft_catalog(initial_catalog.clone()),
            Uuid::new_v4(),
            "ops/dp/public/test",
            true,
            0,
        )
        .await
        .expect("build a failed");

    let will_commit_pub = Id::new([10; 8]);
    let will_commit_build = harness
        .publisher
        .build(
            user_id,
            will_commit_pub,
            Some("pub b".to_string()),
            draft_catalog(initial_catalog.clone()),
            Uuid::new_v4(),
            "ops/dp/public/test",
            true,
            0,
        )
        .await
        .expect("build b failed");
    let will_commit_build_id = will_commit_build.build_id;

    let expect_success_result = harness
        .publisher
        .commit(will_commit_build, &NoopWithCommit)
        .await
        .expect("commit b failed");
    assert!(expect_success_result.status.is_success());

    let expect_fail_result = harness
        .publisher
        .commit(will_fail_build, &NoopWithCommit)
        .await
        .expect("commit a failed");
    assert_lock_failures(
        &[
            ("mice/cheese", Id::zero(), Some(will_commit_build_id)),
            ("mice/seeds", Id::zero(), Some(will_commit_build_id)),
            ("mice/capture", Id::zero(), Some(will_commit_build_id)),
        ],
        &expect_fail_result.status,
    );
    assert!(
        DefaultRetryPolicy.retry(&expect_fail_result),
        "build_id lock failure should be retryable"
    );

    // Now simulate raced publications of cheese and seeds, wich each publication having "expanded"
    // to include the capture.
    let expect_current_build_id = will_commit_build_id;
    let will_fail_pub_id = Id::new([11; 8]);
    let cheese_draft = draft_catalog(serde_json::json!({
        "collections": {
            "mice/cheese": minimal_collection(None),
        },
        "captures": {
            "mice/capture": minimal_capture(None, &["mice/cheese", "mice/seeds"]),
        }
    }));
    let will_fail_build = harness
        .publisher
        .build(
            user_id,
            will_fail_pub_id,
            Some("cheese pub".to_string()),
            cheese_draft,
            Uuid::new_v4(),
            "ops/dp/public/test",
            true,
            0,
        )
        .await
        .expect("cheese build failed");
    assert!(!will_fail_build.has_errors());

    let will_commit_pub = Id::new([12; 8]);
    let will_commit_draft = draft_catalog(serde_json::json!({
        "collections": {
            "mice/seeds": minimal_collection(None),
        },
        "captures": {
            "mice/capture": minimal_capture(None, &["mice/cheese", "mice/seeds"]),
        }
    }));
    let will_commit_build = harness
        .publisher
        .build(
            user_id,
            will_commit_pub,
            Some("seeds pub".to_string()),
            will_commit_draft,
            Uuid::new_v4(),
            "ops/dp/public/test",
            true,
            0,
        )
        .await
        .expect("seeds build failed");
    assert!(!will_commit_build.has_errors());
    let will_commit_build_id = will_commit_build.build_id;
    let expect_success_result = harness
        .publisher
        .commit(will_commit_build, &NoopWithCommit)
        .await
        .expect("failed to commit seeds");
    assert!(expect_success_result.status.is_success());
    assert_last_pub_build(
        &mut harness,
        "mice/seeds",
        will_commit_pub,
        will_commit_build_id,
    )
    .await;
    assert_last_pub_build(
        &mut harness,
        "mice/capture",
        will_commit_pub,
        will_commit_build_id,
    )
    .await;

    let expect_fail_result = harness
        .publisher
        .commit(will_fail_build, &NoopWithCommit)
        .await
        .expect("failed to commit cheese"); // lol
    assert_lock_failures(
        &[(
            "mice/capture",
            expect_current_build_id,
            Some(will_commit_build_id),
        )],
        &expect_fail_result.status,
    );

    // Assert that PublicationSuperseded and BuildSuperseded errors get retried
    let capture_draft = draft_catalog(serde_json::json!({
        "captures": {
            "mice/capture": minimal_capture(None, &["mice/cheese", "mice/seeds"]),
        }
    }));
    harness.control_plane().fail_next_build(
        "mice/capture",
        InjectBuildError::new(
            tables::synthetic_scope(CatalogType::Capture, "mice/capture"),
            validation::Error::BuildSuperseded {
                build_id: Id::zero(),
                larger_id: Id::zero(),
            },
        ),
    );
    harness.control_plane().fail_next_build(
        "mice/capture",
        InjectBuildError::new(
            tables::synthetic_scope(CatalogType::Capture, "mice/capture"),
            validation::Error::PublicationSuperseded {
                last_pub_id: Id::zero(),
                pub_id: Id::zero(),
            },
        ),
    );
    let result = harness
        .control_plane()
        .publish(
            Some("test retry Superseded errors".to_string()),
            Uuid::new_v4(),
            capture_draft.clone_specs(),
            Some("ops/dp/public/test".to_string()),
        )
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert_eq!(2, result.retry_count);
}

async fn assert_last_pub_build(
    harness: &mut TestHarness,
    catalog_name: &str,
    expect_last_pub: Id,
    expect_last_build: Id,
) {
    let mut names = std::collections::BTreeSet::new();
    names.insert(catalog_name.to_string());
    let live = harness
        .control_plane()
        .get_live_specs(names)
        .await
        .expect("failed to fetch live specs");
    let (last_pub, last_build) = live
        .captures
        .get(0)
        .map(|r| (r.last_pub_id, r.last_build_id))
        .or(live
            .collections
            .get(0)
            .map(|r| (r.last_pub_id, r.last_build_id)))
        .or(live
            .materializations
            .get(0)
            .map(|r| (r.last_pub_id, r.last_build_id)))
        .or(live.tests.get(0).map(|r| (r.last_pub_id, r.last_build_id)))
        .expect("no live spec found");
    assert_eq!(
        expect_last_pub, last_pub,
        "mismatched last_pub_id for {catalog_name}"
    );
    assert_eq!(
        expect_last_build, last_build,
        "mismatched last_build_id for {catalog_name}"
    );
}

fn assert_lock_failures(expected: &[(&'static str, Id, Option<Id>)], actual: &JobStatus) {
    let JobStatus::BuildIdLockFailure { failures } = actual else {
        panic!("unexpected publication status: {:?}", actual);
    };
    let mut act: Vec<LockFailure> = failures.iter().cloned().collect();
    act.sort_by(|l, r| l.catalog_name.cmp(&r.catalog_name));

    let mut exp: Vec<LockFailure> = expected
        .iter()
        .map(|(name, expect, last)| LockFailure {
            catalog_name: name.to_string(),
            expected: *expect,
            actual: *last,
        })
        .collect();
    exp.sort_by(|l, r| l.catalog_name.cmp(&r.catalog_name));

    assert_eq!(exp, act);
}

fn minimal_capture(expect_pub_id: Option<Id>, targets: &[&str]) -> serde_json::Value {
    let bindings = targets
        .iter()
        .map(|collection| {
            serde_json::json!({
                "resource": { "id": collection },
                "target": collection,
            })
        })
        .collect::<Vec<_>>();
    serde_json::json!({
        "expectPubId": expect_pub_id,
        "endpoint": {
            "connector": {
                "image": "source/test:dev",
                "config": {}
            }
        },
        "bindings": bindings,
    })
}

fn minimal_collection(expect_pub_id: Option<Id>) -> serde_json::Value {
    serde_json::json!({
        "expectPubId": expect_pub_id,
        "schema": {
            "type": "object",
            "properties": {
                "id": { "type": "string" }
            }
        },
        "key": ["/id"]
    })
}
