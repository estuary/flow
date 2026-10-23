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

    // A second publication joins the same consumed batch.
    let mut batch = materialization(
        json!({
            "resource": {"_meta": {"path": ["sightings"]}, "table": "sightings"},
            "source": "ravens/sightings", "fields": {"groupBy": ["region"], "recommended": true},
            "disable": true,
        }),
        0,
    );
    batch["ravens/materialize"]["bindings"][1]["fields"] =
        json!({"groupBy": ["region"], "recommended": true});
    let result = harness
        .user_publication(
            user_id,
            "change nests group-by",
            draft_catalog(json!({"materializations": batch})),
        )
        .await;
    assert!(result.status.is_success(), "{:?}", result.errors);

    // Fail at commit, after the executor has applied the outcome and inbox consumption.
    let controller_id: models::Id = sqlx::query_scalar(
        "select controller_task_id from live_specs where catalog_name = 'ravens/materialize'",
    )
    .fetch_one(&harness.pool)
    .await
    .unwrap();
    let before: serde_json::Value = sqlx::query_scalar(
        "select jsonb_build_object('task', to_jsonb(t), 'job', to_jsonb(c)) from internal.tasks t join controller_jobs c on c.live_spec_id = (select id from live_specs where controller_task_id = t.task_id) where t.task_id = $1")
        .bind(controller_id).fetch_one(&harness.pool).await.unwrap();
    let inbox = sqlx::query_scalar("select inbox from internal.tasks where task_id = $1")
        .bind(controller_id)
        .fetch_one(&harness.pool)
        .await
        .unwrap();
    sqlx::raw_sql("create function public.reject_binding_event() returns trigger language plpgsql as $$ begin raise exception 'injected event commit failure'; end $$; create constraint trigger reject_binding_event after insert on alert_history deferrable initially deferred for each row when (new.alert_type = 'bindings_auto_disabled') execute function public.reject_binding_event();")
        .execute(&harness.pool).await.unwrap();
    let failed = automations::executors::ObjSafe::poll(
        &harness.controller_exec,
        &harness.pool,
        controller_id,
        None,
        None,
        inbox,
    )
    .await;
    sqlx::raw_sql("drop function public.reject_binding_event() cascade")
        .execute(&harness.pool)
        .await
        .unwrap();
    assert!(format!("{:#}", failed.unwrap_err()).contains("injected event commit failure"));
    let after: serde_json::Value = sqlx::query_scalar(
        "select jsonb_build_object('task', to_jsonb(t), 'job', to_jsonb(c)) from internal.tasks t join controller_jobs c on c.live_spec_id = (select id from live_specs where controller_task_id = t.task_id) where t.task_id = $1")
        .bind(controller_id).fetch_one(&harness.pool).await.unwrap();
    assert_eq!(
        before, after,
        "rollback must retain status and the unconsumed inbox"
    );
    let counts: (i64, i64) = sqlx::query_as("select (select count(*) from alert_history where alert_type = 'bindings_auto_disabled'), (select count(*) from internal.tasks where task_type = 9)")
        .fetch_one(&harness.pool).await.unwrap();
    assert_eq!(counts, (0, 0));

    // An ordinary controller error still commits the binding event.
    harness
        .control_plane()
        .fail_next_activation("ravens/materialize");
    let state = harness.run_pending_controller("ravens/materialize").await;
    assert!(state.failures > 0);
    harness.control_plane().reset_activations();
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
      },
      {
        "resource_path": [
          "nests"
        ],
        "collection": "ravens/nests",
        "reason": "incompatible_schema_change",
        "detail": "group-by key changed from [\"id\"] to [\"region\"]",
        "pub_id": "[pub_id]"
      }
    ]
    "#
    );

    let alert = harness
        .assert_alert_event("ravens/materialize", AlertType::BindingsAutoDisabled)
        .await;
    alert.assert_emails_sent(&["ravens@bindings_auto_disabled.test"]);
    assert_eq!(
        Some("2 bindings disabled because of incompatible schema changes"),
        alert.error_arg_value()
    );
    assert_eq!(
        2,
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
                    },
                    {
                      "collection": "ravens/nests",
                      "detail": "group-by key changed from [\"id\"] to [\"region\"]",
                      "reason": "INCOMPATIBLE_SCHEMA_CHANGE",
                      "resourcePath": [
                        "nests"
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

    let mut model = state
        .live_spec
        .as_ref()
        .unwrap()
        .as_materialization()
        .unwrap()
        .clone();
    // Re-enable B while A stays disabled. Re-enablement sends no event.
    model.bindings[1].disable = false;
    model.bindings[1].backfill = 1;
    let result = harness
        .user_publication(
            user_id,
            "re-enable nests",
            draft_catalog(json!({"materializations": {"ravens/materialize": model}})),
        )
        .await;
    assert!(result.status.is_success(), "{:?}", result.errors);
    let state = harness.run_pending_controller("ravens/materialize").await;
    assert_eq!(
        state
            .current_status
            .unwrap_materialization()
            .auto_disabled_bindings
            .len(),
        1
    );

    // A later disabling batch notifies about B alone, even though A remains disabled.
    let mut model = state
        .live_spec
        .as_ref()
        .unwrap()
        .as_materialization()
        .unwrap()
        .clone();
    model.bindings[1].fields.group_by = vec![models::Field::new("id")];
    let result = harness
        .user_publication(
            user_id,
            "disable nests again",
            draft_catalog(json!({"materializations": {"ravens/materialize": model}})),
        )
        .await;
    assert!(result.status.is_success(), "{:?}", result.errors);
    let state = harness.run_pending_controller("ravens/materialize").await;
    assert_eq!(
        state
            .current_status
            .unwrap_materialization()
            .auto_disabled_bindings
            .len(),
        2
    );
    let later = harness
        .assert_alert_event("ravens/materialize", AlertType::BindingsAutoDisabled)
        .await;
    assert_ne!(alert.alert.id, later.alert.id);
    later.assert_emails_sent(&["ravens@bindings_auto_disabled.test"]);
    let bindings = later.alert.arguments.0["bindings"].as_array().unwrap();
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0]["resource_path"], json!(["nests"]));

    // An unrelated publication retains explanations without replaying an event.
    let mut model = state
        .live_spec
        .as_ref()
        .unwrap()
        .as_materialization()
        .unwrap()
        .clone();
    model.bindings[1].priority += 1;
    let result = harness
        .user_publication(
            user_id,
            "change nests priority",
            draft_catalog(json!({"materializations": {"ravens/materialize": model}})),
        )
        .await;
    assert!(result.status.is_success(), "{:?}", result.errors);
    let state = harness.run_pending_controller("ravens/materialize").await;
    assert_eq!(
        state
            .current_status
            .unwrap_materialization()
            .auto_disabled_bindings
            .len(),
        2
    );
    let count: i64 = sqlx::query_scalar("select count(*) from alert_history where catalog_name = 'ravens/materialize' and alert_type = 'bindings_auto_disabled'")
        .fetch_one(&harness.pool).await.unwrap();
    assert_eq!(count, 2);

    let mut model = state
        .live_spec
        .as_ref()
        .unwrap()
        .as_materialization()
        .unwrap()
        .clone();
    for binding in &mut model.bindings {
        binding.disable = false;
        binding.backfill += 1;
    }
    let result = harness
        .user_publication(
            user_id,
            "re-enable all with backfills",
            draft_catalog(json!({"materializations": {"ravens/materialize": model}})),
        )
        .await;
    assert!(result.status.is_success(), "{:?}", result.errors);
    let state = harness.run_pending_controller("ravens/materialize").await;
    assert!(
        state
            .current_status
            .unwrap_materialization()
            .auto_disabled_bindings
            .is_empty()
    );
    assert!(harness.alert_sender.take_sent().await.is_empty());
}
