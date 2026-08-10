use crate::integration_tests::harness::{TestHarness, draft_catalog};

/// Exercises the real `connectors`/`connector_tags` join behind
/// `connector_tags.default_capture_interval`, which the validation-layer tests mock out entirely,
/// and confirms that a changed default reaches an already-published capture through its periodic
/// touch publication.
#[tokio::test]
async fn test_capture_interval_follows_connector_tag_default() {
    let mut harness =
        TestHarness::init("test_capture_interval_follows_connector_tag_default").await;
    let user_id = harness.setup_tenant("gadgets").await;

    set_default_capture_interval(&harness, Some("90 seconds")).await;

    let setup = draft_catalog(serde_json::json!({
        "collections": {
            "gadgets/widgets": {
                "schema": {
                    "type": "object",
                    "properties": { "id": { "type": "string" } },
                    "required": ["id"],
                },
                "key": ["/id"],
            }
        },
        "captures": {
            // Sets its own interval, which must win over the connector default.
            "gadgets/pinned": {
                "interval": "42s",
                "endpoint": { "connector": { "image": "source/test:test", "config": {} } },
                "bindings": [
                    { "resource": { "id": "widgets" }, "target": "gadgets/widgets" }
                ],
            },
            // Sets no interval, and so tracks the connector default.
            "gadgets/tracking": {
                "endpoint": { "connector": { "image": "source/test:test", "config": {} } },
                "bindings": [
                    { "resource": { "id": "widgets" }, "target": "gadgets/widgets" }
                ],
            },
        },
    }));
    let result = harness
        .user_publication(user_id, "initial publication", setup)
        .await;
    assert!(
        result.status.is_success(),
        "setup publication failed: {:?}",
        result.errors
    );
    harness.run_pending_controllers(None).await;

    assert_eq!(
        42,
        built_interval_seconds(&mut harness, "gadgets/pinned").await
    );
    assert_eq!(
        90,
        built_interval_seconds(&mut harness, "gadgets/tracking").await
    );

    // The connector default isn't baked into the model, so a capture which set
    // no interval must pick up a later change to its connector tag. There's no
    // fan-out which pushes the change out, so it lands with the next periodic
    // touch publication.
    set_default_capture_interval(&harness, Some("30 seconds")).await;
    touch_and_run(&mut harness, 30, 42).await;

    // Clearing the connector default falls back to the global default.
    set_default_capture_interval(&harness, None).await;
    touch_and_run(&mut harness, 300, 42).await;
}

async fn set_default_capture_interval(harness: &TestHarness, interval: Option<&str>) {
    sqlx::query!(
        r#"
        update connector_tags set default_capture_interval = $1::text::interval
        where image_tag = ':test'
          and connector_id = (select id from connectors where image_name = 'source/test')
        "#,
        interval as Option<&str>,
    )
    .execute(&harness.pool)
    .await
    .expect("failed to set default_capture_interval");
}

async fn built_interval_seconds(harness: &mut TestHarness, catalog_name: &str) -> u32 {
    let state = harness.get_controller_state(catalog_name).await;
    let Some(proto_flow::AnyBuiltSpec::Capture(spec)) = state.built_spec.as_ref() else {
        panic!("expected a capture spec, got: {:?}", state.built_spec);
    };
    spec.interval_seconds
}

/// Makes each capture's periodic touch publication come due, runs it, and asserts the intervals of
/// the resulting builds.
async fn touch_and_run(harness: &mut TestHarness, expect_tracking: u32, expect_pinned: u32) {
    let captures = vec!["gadgets/pinned".to_string(), "gadgets/tracking".to_string()];

    sqlx::query!(
        r#"update live_specs set updated_at = now() - '21days'::interval
        where catalog_name = any($1::text[])"#,
        &captures as &Vec<String>,
    )
    .execute(&harness.pool)
    .await
    .expect("failed to age live specs");

    for catalog_name in &captures {
        harness.run_pending_controller(catalog_name).await;
    }

    assert_eq!(
        expect_tracking,
        built_interval_seconds(harness, "gadgets/tracking").await
    );
    assert_eq!(
        expect_pinned,
        built_interval_seconds(harness, "gadgets/pinned").await
    );
}
