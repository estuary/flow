//! Tests of task `created_at` stamping: built capture and materialization
//! specs carry the task's creation date (UTC, YYYY-MM-DD), derived from the
//! timestamp embedded in the task's control-plane Id. A first build — which
//! runs before the control-plane Id is assigned — leaves it empty, and the
//! task's next build stamps it. See issue #3131.
use crate::integration_tests::harness::{TestHarness, draft_catalog};

/// A new task's first built spec has an empty created_at, its next (touch)
/// build stamps its creation date from its control-plane Id, and the date is
/// thereafter stable.
#[tokio::test]
async fn created_at_is_stamped_and_stable_across_touches() {
    let mut harness = TestHarness::init("created_at_is_stamped_and_stable_across_touches").await;
    let user_id = harness.setup_tenant("wombats").await;

    let draft = draft_catalog(serde_json::json!({
        "captures": {
            "wombats/capture": {
                "endpoint": {
                    "connector": { "image": "source/test:test", "config": {} }
                },
                "bindings": [
                    { "resource": { "name": "burrows" }, "target": "wombats/burrows" }
                ]
            },
        },
        "collections": {
            "wombats/burrows": {
                "schema": {
                    "type": "object",
                    "properties": { "id": { "type": "string" } }
                },
                "key": ["/id"]
            },
        },
        "materializations": {
            "wombats/materialize": {
                "endpoint": {
                    "connector": { "image": "materialize/test:test", "config": {} }
                },
                "bindings": [
                    { "resource": { "table": "burrows" }, "source": "wombats/burrows" }
                ]
            }
        }
    }));

    let result = harness
        .user_publication(user_id, "initial publication", draft)
        .await;
    assert!(result.status.is_success(), "{:?}", result);

    // The very first build of each task leaves created_at empty: the
    // control-plane Id is assigned only as the publication commits, and the
    // connector of a brand-new task assumes a current date.
    let capture = fetch_created_at(&harness, "wombats/capture").await;
    assert_eq!("", capture.spec_created_at);

    let materialization = fetch_created_at(&harness, "wombats/materialize").await;
    assert_eq!("", materialization.spec_created_at);

    harness.run_pending_controllers(None).await;

    // Make periodic touch publications of both tasks come due, and run them.
    sqlx::query!(
        r#"update live_specs set updated_at = now() - '21days'::interval
        where catalog_name = any($1::text[]);"#,
        vec![
            "wombats/capture".to_string(),
            "wombats/materialize".to_string()
        ] as Vec<String>,
    )
    .execute(&harness.pool)
    .await
    .unwrap();

    for name in ["wombats/capture", "wombats/materialize"] {
        harness.run_pending_controller(name).await;
    }

    // Touches re-built both specs, stamping (exactly) from the stable
    // control-plane Id.
    let touched_capture = fetch_created_at(&harness, "wombats/capture").await;
    assert_ne!(capture.last_build_id, touched_capture.last_build_id);
    assert_eq!(touched_capture.id_date(), touched_capture.spec_created_at);

    let touched_materialization = fetch_created_at(&harness, "wombats/materialize").await;
    assert_ne!(
        materialization.last_build_id,
        touched_materialization.last_build_id
    );
    assert_eq!(
        touched_materialization.id_date(),
        touched_materialization.spec_created_at
    );
}

/// A pre-existing task whose built spec lacks `createdAt` — as the entire
/// fleet does when this feature ships — is stamped from its live Id by its
/// next touch publication.
#[tokio::test]
async fn created_at_is_backfilled_by_touch_publications() {
    let mut harness = TestHarness::init("created_at_is_backfilled_by_touch_publications").await;
    let user_id = harness.setup_tenant("yaks").await;

    let draft = draft_catalog(serde_json::json!({
        "captures": {
            "yaks/capture": {
                "endpoint": {
                    "connector": { "image": "source/test:test", "config": {} }
                },
                "bindings": [
                    { "resource": { "name": "herds" }, "target": "yaks/herds" }
                ]
            },
        },
        "collections": {
            "yaks/herds": {
                "schema": {
                    "type": "object",
                    "properties": { "id": { "type": "string" } }
                },
                "key": ["/id"]
            },
        },
    }));
    let result = harness
        .user_publication(user_id, "initial publication", draft)
        .await;
    assert!(result.status.is_success(), "{:?}", result);
    harness.run_pending_controllers(None).await;

    // Rewrite the task into the shape of one which predates this feature —
    // no `createdAt` in its built spec — and make a periodic touch come due.
    sqlx::query!(
        r#"update live_specs set
            built_spec = (built_spec::jsonb - 'createdAt')::json,
            updated_at = now() - '21days'::interval
        where catalog_name = 'yaks/capture'
        returning 1 as "must_exist!: bool";"#,
    )
    .fetch_one(&harness.pool)
    .await
    .unwrap();
    harness.run_pending_controller("yaks/capture").await;

    let backfilled = fetch_created_at(&harness, "yaks/capture").await;
    assert_eq!(backfilled.id_date(), backfilled.spec_created_at);
}

/// Discover requests carry the task's creation date when it exists — the
/// motivating case being auto-discovers, which must resolve connector
/// feature-flag defaults as the running task does — and an (honestly) absent
/// date for a task which doesn't exist yet.
#[tokio::test]
async fn discover_requests_carry_created_at() {
    let mut harness = TestHarness::init("discover_requests_carry_created_at").await;
    let user_id = harness.setup_tenant("gophers").await;

    let discovered = proto_flow::capture::response::Discovered {
        bindings: vec![proto_flow::capture::response::discovered::Binding {
            recommended_name: "tunnels".to_string(),
            document_schema_json: r#"{
                "type": "object",
                "properties": { "id": { "type": "string" } },
                "required": ["id"],
                "x-infer-schema": true
            }"#
            .into(),
            resource_config_json: r#"{"id": "tunnels"}"#.into(),
            key: vec!["/id".to_string()],
            disable: false,
            resource_path: Vec::new(),
            is_fallback_key: false,
        }],
    };

    // Discover of a capture which doesn't exist: created_at is absent, and
    // the connector assumes a current date.
    let draft_id = harness
        .create_draft(user_id, "initial", Default::default())
        .await;
    let result = harness
        .user_discover(
            "source/test",
            ":test",
            "gophers/capture",
            draft_id,
            r#"{"tail": "shake"}"#,
            false,
            Ok((super::spec_fixture(), discovered.clone())),
        )
        .await;
    assert!(result.job_status.is_success(), "{:?}", result.job_status);
    assert_eq!("", discover_request_date(&harness, "gophers/capture"));

    // Publish, and re-discover through a fresh draft: the request carries the
    // creation date embedded in the now-live task's Id.
    let result = harness
        .create_user_publication(user_id, draft_id, "initial publication")
        .await;
    assert!(result.status.is_success(), "{:?}", result);

    let live = fetch_created_at(&harness, "gophers/capture").await;
    let draft_id = harness
        .create_draft(user_id, "re-discover", Default::default())
        .await;
    let result = harness
        .user_discover(
            "source/test",
            ":test",
            "gophers/capture",
            draft_id,
            r#"{"tail": "shake"}"#,
            false,
            Ok((super::spec_fixture(), discovered)),
        )
        .await;
    assert!(result.job_status.is_success(), "{:?}", result.job_status);
    assert_eq!(
        live.id_date(),
        discover_request_date(&harness, "gophers/capture")
    );
}

fn discover_request_date(harness: &TestHarness, capture_name: &str) -> String {
    harness
        .discover_handler
        .connectors
        .last_discover_request(capture_name)
        .expect("a Discover request was made")
        .created_at
}

struct CreatedAt {
    id: models::Id,
    last_build_id: models::Id,
    // The `createdAt` date stamped into the built spec, or empty if absent.
    spec_created_at: String,
}

impl CreatedAt {
    // The UTC date embedded in the task's control-plane Id.
    fn id_date(&self) -> String {
        self.id.timestamp().date_naive().to_string()
    }
}

async fn fetch_created_at(harness: &TestHarness, catalog_name: &str) -> CreatedAt {
    let row = sqlx::query!(
        r#"select
            id as "id!: models::Id",
            last_build_id as "last_build_id!: models::Id",
            coalesce(built_spec->>'createdAt', '') as "spec_created_at!: String"
        from live_specs where catalog_name = $1;"#,
        catalog_name,
    )
    .fetch_one(&harness.pool)
    .await
    .unwrap();
    CreatedAt {
        id: row.id,
        last_build_id: row.last_build_id,
        spec_created_at: row.spec_created_at,
    }
}
