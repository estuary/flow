use crate::integration_tests::harness::{TestHarness, draft_catalog};
use models::status::AlertType;
use serde_json::json;

const STATUS_QUERY: &str = r#"
query {
    liveSpecs(by: { names: ["ravens/materialize"] }) {
        edges {
            node {
                status {
                    controller {
                        autoDisabledBindings {
                            resourcePath
                            collection
                            reason
                            detail
                        }
                    }
                }
            }
        }
    }
}
"#;

fn materialization(sightings: serde_json::Value, nests_priority: i32) -> serde_json::Value {
    json!({
        "ravens/materialize": {
            "endpoint": {
                "connector": { "image": "materialize/test:test", "config": {} }
            },
            "onIncompatibleSchemaChange": "disableBinding",
            "bindings": [
                sightings,
                {
                    "resource": { "_meta": { "path": ["nests"] }, "table": "nests" },
                    "source": "ravens/nests",
                    "priority": nests_priority,
                },
            ]
        }
    })
}

fn materialization_draft(
    sightings: serde_json::Value,
    nests_priority: i32,
) -> tables::DraftCatalog {
    draft_catalog(json!({ "materializations": materialization(sightings, nests_priority) }))
}

#[tokio::test]
async fn test_bindings_auto_disabled() {
    let mut harness = TestHarness::init("bindings_auto_disabled").await;
    // The policy is explicit so this test does not depend on trial storage.
    let user_id = harness.setup_tenant("ravens").await;

    let schema = json!({
        "type": "object",
        "properties": {
            "id": { "type": "string" },
            "region": { "type": "string" }
        },
        "required": ["id", "region"]
    });
    let draft = draft_catalog(json!({
        "collections": {
            "ravens/sightings": { "schema": schema, "key": ["/id"] },
            "ravens/nests": { "schema": schema, "key": ["/id"] },
        },
        "materializations": materialization(
            json!({ "resource": { "table": "sightings" }, "source": "ravens/sightings" }),
            0,
        ),
    }));
    let result = harness
        .user_publication(user_id, "initial publication", draft)
        .await;
    assert!(result.status.is_success(), "{:?}", result.errors);
    harness.run_pending_controllers(None).await;

    // Change the group-by of the sightings binding, which validation resolves
    // by disabling the binding.
    let result = harness
        .user_publication(
            user_id,
            "change group-by",
            materialization_draft(
                json!({
                    "resource": { "_meta": { "path": ["sightings"] }, "table": "sightings" },
                    "source": "ravens/sightings",
                    "fields": { "groupBy": ["region"], "recommended": true },
                }),
                0,
            ),
        )
        .await;
    assert!(result.status.is_success(), "{:?}", result.errors);

    let state = harness.run_pending_controller("ravens/materialize").await;
    let model = state
        .live_spec
        .as_ref()
        .unwrap()
        .as_materialization()
        .unwrap();
    assert!(model.bindings[0].disable);
    insta::assert_json_snapshot!(
        state.current_status.unwrap_materialization().auto_disabled_bindings,
        { "[].pub_id" => "[pub_id]" },
        @r#"
    [
      {
        "resource_path": [
          "sightings"
        ],
        "collection": "ravens/sightings",
        "reason": "incompatible_schema_change",
        "detail": "group-by key changed from [\"id\"] to [\"region\"]",
        "pub_id": "[pub_id]"
      }
    ]
    "#
    );

    let alert = harness
        .assert_alert_firing("ravens/materialize", AlertType::BindingsAutoDisabled)
        .await;
    alert.assert_emails_sent(&["ravens@bindings_auto_disabled.test"]);
    assert_eq!(
        Some("1 binding disabled because of an incompatible schema change"),
        alert.error_arg_value()
    );
    assert_eq!(
        1,
        alert.alert.arguments.0["bindings"]
            .as_array()
            .unwrap()
            .len()
    );
    assert!(
        alert.notifications[0]
            .body
            .contains("group-by key changed from [&quot;id&quot;] to [&quot;region&quot;]"),
        "{}",
        alert.notifications[0].body
    );

    let status: serde_json::Value = harness
        .execute_graphql_query(user_id, STATUS_QUERY, &json!({}))
        .await
        .unwrap();
    insta::assert_json_snapshot!(status, @r#"
    {
      "liveSpecs": {
        "edges": [
          {
            "node": {
              "status": {
                "controller": {
                  "autoDisabledBindings": [
                    {
                      "collection": "ravens/sightings",
                      "detail": "group-by key changed from [\"id\"] to [\"region\"]",
                      "reason": "INCOMPATIBLE_SCHEMA_CHANGE",
                      "resourcePath": [
                        "sightings"
                      ]
                    }
                  ]
                }
              }
            }
          }
        ]
      }
    }
    "#);

    // A publication which changes only the other binding leaves the sightings
    // binding disabled, and its alert firing.
    let result = harness
        .user_publication(
            user_id,
            "change nests priority",
            materialization_draft(
                json!({
                    "resource": { "_meta": { "path": ["sightings"] }, "table": "sightings" },
                    "source": "ravens/sightings",
                    "fields": { "groupBy": ["region"], "recommended": true },
                    "disable": true,
                }),
                1,
            ),
        )
        .await;
    assert!(result.status.is_success(), "{:?}", result.errors);

    let state = harness.run_pending_controller("ravens/materialize").await;
    let status = state.current_status.unwrap_materialization();
    assert_eq!(1, status.auto_disabled_bindings.len());
    assert!(status.alerts.contains_key(&AlertType::BindingsAutoDisabled));

    // Re-enable the binding with a backfill, which rebuilds it under its new
    // group-by. The binding stays enabled, and the alert resolves.
    let result = harness
        .user_publication(
            user_id,
            "re-enable with backfill",
            materialization_draft(
                json!({
                    "resource": { "_meta": { "path": ["sightings"] }, "table": "sightings" },
                    "source": "ravens/sightings",
                    "fields": { "groupBy": ["region"], "recommended": true },
                    "backfill": 1,
                }),
                1,
            ),
        )
        .await;
    assert!(result.status.is_success(), "{:?}", result.errors);

    let state = harness.run_pending_controller("ravens/materialize").await;
    let model = state
        .live_spec
        .as_ref()
        .unwrap()
        .as_materialization()
        .unwrap();
    assert!(!model.bindings[0].disable);
    assert_eq!(1, model.bindings[0].backfill);
    let status = state.current_status.unwrap_materialization();
    assert!(status.auto_disabled_bindings.is_empty());

    let resolved = harness.assert_alert_resolved(alert.alert.id).await;
    resolved.assert_emails_sent(&[]);
}
