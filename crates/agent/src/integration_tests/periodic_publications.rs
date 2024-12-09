use std::collections::BTreeSet;

use crate::{
    controllers::ControllerState,
    integration_tests::harness::{
        draft_catalog, mock_inferred_schema, InjectBuildError, TestHarness,
    },
    ControlPlane,
};
use models::{status::Status, CatalogType};
use uuid::Uuid;

#[tokio::test]
#[serial_test::serial]
async fn specs_are_published_periodically() {
    let mut harness = TestHarness::init("specs_are_published_periodically").await;

    let user_id = harness.setup_tenant("cicadas").await;

    let draft = draft_catalog(serde_json::json!({
        "captures": {
            "cicadas/capture": {
                "endpoint": {
                    "connector": {
                        "image": "source/test:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": {
                            "name": "years",
                        },
                        "target": "cicadas/years"
                    }
                ]
            },
            "cicadas/disabled-capture": {
                "shards": {
                    "disable": true
                },
                "endpoint": {
                    "connector": {
                        "image": "source/test:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": {
                            "name": "years",
                        },
                        "target": "cicadas/years"
                    }
                ]
            },
        },
        "collections": {
            "cicadas/years": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    }
                },
                "key": ["/id"],
                "derive": {
                    "shards": {
                        "disable": true
                    },
                    "using": {
                        "sqlite": { "migrations": [] }
                    },
                    "transforms": [ ]
                }
            },
        },
        "materializations": {
            "cicadas/materialize": {
                "endpoint": {
                    "connector": {
                        "image": "materialize/test:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": { "table": "years" },
                        "source": "cicadas/years"
                    },
                ]
            },
            "cicadas/disabled-materialize": {
                "shards": {
                    "disable": true
                },
                "endpoint": {
                    "connector": {
                        "image": "materialize/test:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": { "table": "years" },
                        "source": "cicadas/years"
                    },
                ]
            }
        }
    }));

    let all_spec_names: Vec<String> = draft.all_spec_names().map(ToOwned::to_owned).collect();

    let result = harness
        .user_publication(user_id, "initial publication", draft)
        .await;
    assert!(result.status.is_success());
    harness.run_pending_controllers(None).await;

    // Get the starting states of the disabled tasks so we can assert that they don't get published.
    let disabled_mat_start = harness
        .get_controller_state("cicadas/disabled-materialize")
        .await;
    let disabled_cap_start = harness
        .get_controller_state("cicadas/disabled-capture")
        .await;

    // Everything else is expected to get published.
    let expect_touched_names: BTreeSet<String> = ["cicadas/capture", "cicadas/materialize"]
        .into_iter()
        .map(ToOwned::to_owned)
        .collect();

    // Update the `last_updated` time in the database to simulate the passage of time after their creation.
    sqlx::query!(
        r#"
        update live_specs
        set updated_at = now() - '21days'::interval
        where catalog_name = any($1::text[]);"#,
        all_spec_names as Vec<String>
    )
    .execute(&harness.pool)
    .await
    .unwrap();

    for name in expect_touched_names {
        tracing::info!(%name, "expecting to be touched");
        let before_state = harness.run_pending_controller(&name).await;

        let after_state = harness.get_controller_state(&name).await;
        let pub_status = match after_state.current_status {
            Status::Capture(cap) => cap.publications,
            Status::Materialization(m) => m.publications,
            Status::Collection(c) => c.publications,
            Status::Test(t) => t.publications,
            Status::Uninitialized => panic!("unexpected status"),
        };
        assert_eq!(
            Some("periodic publication"),
            pub_status.history[0].detail.as_deref()
        );
        assert!(pub_status.history[0].is_success());
        assert!(after_state.last_build_id > before_state.last_build_id);
        assert_eq!(after_state.last_pub_id, before_state.last_pub_id);
    }

    // Assert that the disabled tasks were not touched
    let disabled_mat_end = harness
        .get_controller_state("cicadas/disabled-materialize")
        .await;
    let disabled_cap_end = harness
        .get_controller_state("cicadas/disabled-capture")
        .await;
    assert_eq!(
        disabled_mat_start.last_build_id,
        disabled_mat_end.last_build_id
    );
    assert_eq!(
        disabled_cap_start.last_build_id,
        disabled_cap_end.last_build_id
    );
}
