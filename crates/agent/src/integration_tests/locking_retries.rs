use models::Id;
use uuid::Uuid;

use crate::{
    integration_tests::harness::{draft_catalog, TestHarness},
    publications::{JobStatus, LockFailure},
    ControlPlane,
};

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
    let naughty_pub_id = harness.control_plane().next_pub_id();
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

    let pub_a = harness.control_plane().next_pub_id();
    let build_a = harness
        .publisher
        .build(
            user_id,
            pub_a,
            Some("pub a".to_string()),
            draft_catalog(initial_catalog.clone()),
            Uuid::new_v4(),
            "ops/dp/public/test",
        )
        .await
        .expect("build a failed");

    let pub_b = harness.control_plane().next_pub_id();
    let build_b = harness
        .publisher
        .build(
            user_id,
            pub_b,
            Some("pub b".to_string()),
            draft_catalog(initial_catalog.clone()),
            Uuid::new_v4(),
            "ops/dp/public/test",
        )
        .await
        .expect("build b failed");

    let result_b = harness
        .publisher
        .commit(build_b)
        .await
        .expect("commit b failed");
    assert!(result_b.status.is_success());

    let result_a = harness
        .publisher
        .commit(build_a)
        .await
        .expect("commit a failed");
    assert_lock_failures(
        &[
            ("mice/cheese", Id::zero(), Some(pub_b)),
            ("mice/seeds", Id::zero(), Some(pub_b)),
            ("mice/capture", Id::zero(), Some(pub_b)),
        ],
        &result_a.status,
    );

    // Now simulate raced publications of cheese and seeds, wich each publication having "expanded"
    // to include the capture.
    let cheese_pub = harness.control_plane().next_pub_id();
    let cheese_draft = draft_catalog(serde_json::json!({
        "collections": {
            "mice/cheese": minimal_collection(None),
        },
        "captures": {
            "mice/capture": minimal_capture(None, &["mice/cheese", "mice/seeds"]),
        }
    }));
    let cheese_build = harness
        .publisher
        .build(
            user_id,
            cheese_pub,
            Some("cheese pub".to_string()),
            cheese_draft,
            Uuid::new_v4(),
            "ops/dp/public/test",
        )
        .await
        .expect("cheese build failed");
    assert!(!cheese_build.has_errors());

    let seeds_pub = harness.control_plane().next_pub_id();
    let seeds_draft = draft_catalog(serde_json::json!({
        "collections": {
            "mice/seeds": minimal_collection(None),
        },
        "captures": {
            "mice/capture": minimal_capture(None, &["mice/cheese", "mice/seeds"]),
        }
    }));
    let seeds_build = harness
        .publisher
        .build(
            user_id,
            seeds_pub,
            Some("seeds pub".to_string()),
            seeds_draft,
            Uuid::new_v4(),
            "ops/dp/public/test",
        )
        .await
        .expect("seeds build failed");
    assert!(!seeds_build.has_errors());
    let seeds_result = harness
        .publisher
        .commit(seeds_build)
        .await
        .expect("failed to commit seeds");
    assert!(seeds_result.status.is_success());

    let cheese_result = harness
        .publisher
        .commit(cheese_build)
        .await
        .expect("failed to commit cheese"); // lol
    assert_lock_failures(
        &[("mice/capture", pub_b, Some(seeds_pub))],
        &cheese_result.status,
    );
}

fn assert_lock_failures(expected: &[(&'static str, Id, Option<Id>)], actual: &JobStatus) {
    let JobStatus::ExpectPubIdMismatch { failures } = actual else {
        panic!("unexpected publication status: {:?}", actual);
    };
    let mut act: Vec<LockFailure> = failures.iter().cloned().collect();
    act.sort_by(|l, r| l.catalog_name.cmp(&r.catalog_name));

    let mut exp: Vec<LockFailure> = expected
        .iter()
        .map(|(name, expect, last)| LockFailure {
            catalog_name: name.to_string(),
            expect_pub_id: *expect,
            last_pub_id: *last,
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
                "resource": {},
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
