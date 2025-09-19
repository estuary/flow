pub mod queries;

use std::collections::BTreeMap;

use crate::integration_tests::harness::{self, TestHarness};
use chrono::{DateTime, Utc};
use models::status::ControllerAlert;
use serde_json::json;
use sqlx::types::Uuid;

fn minimal_collection() -> serde_json::Value {
    serde_json::json!({
        "schema": {
            "type": "object",
            "properties": {
                "id": { "type": "string" }
            }
        },
        "key": ["/id"]
    })
}

/// Common setup for GraphQL integration tests.
/// Returns the test harness and user IDs for Alice and Bob.
pub async fn common_setup() -> (TestHarness, Uuid, Uuid) {
    let mut harness = TestHarness::init("graphql_tests").await;

    // Set up Alice's tenant
    let alice_user_id = harness.setup_tenant("aliceCo").await;

    // Create Alice's collections and tasks
    let alice_draft = json!({
        "collections": {
            "aliceCo/shared/a": minimal_collection(),
            "aliceCo/shared/b": minimal_collection(),
            "aliceCo/shared/c": minimal_collection(),
        },
        "captures": {
            "aliceCo/shared/capture": {
                "endpoint": {"connector": {"image": "source/test:test", "config": {}}},
                "bindings": [
                    {
                        "resource": {"id": "a"},
                        "target": "aliceCo/shared/a"
                    },
                    {
                        "resource": {"id": "b"},
                        "target": "aliceCo/shared/b"
                    },
                    {
                        "resource": {"id": "c"},
                        "target": "aliceCo/shared/c"
                    }
                ]
            },
            "aliceCo/private/capture": {
                "endpoint": {"connector": {"image": "source/test:test", "config": {}}},
                "bindings": [
                    {
                        "resource": {"id": "a"},
                        "target": "aliceCo/shared/a"
                    },
                    {
                        "resource": {"id": "b"},
                        "target": "aliceCo/shared/b"
                    },
                    {
                        "resource": {"id": "c"},
                        "target": "aliceCo/shared/c"
                    }
                ]
            },
            "aliceCo/shared/disabled": {
                "endpoint": {"connector": {"image": "source/test:test", "config": {}}},
                "bindings": [
                    {
                        "resource": {"id": "a"},
                        "target": "aliceCo/shared/a"
                    },
                    {
                        "resource": {"id": "b"},
                        "target": "aliceCo/shared/b"
                    },
                    {
                        "resource": {"id": "c"},
                        "target": "aliceCo/shared/c"
                    }
                ]
            }
        },
        "materializations": {
            "aliceCo/shared/materialize": {
                "endpoint": {"connector": {"image": "materialize/test:test", "config": {}}},
                "bindings": [
                    {
                        "resource": {"id": "a"},
                        "source": "aliceCo/shared/a"
                    },
                    {
                        "resource": {"id": "b"},
                        "source": "aliceCo/shared/b"
                    },
                    {
                        "resource": {"id": "c"},
                        "source": "aliceCo/shared/c"
                    }
                ]
            }
        }
    });

    let result = harness
        .user_publication(
            alice_user_id,
            "Alice's initial catalog",
            harness::draft_catalog(alice_draft),
        )
        .await;
    assert!(
        result.status.is_success(),
        "setup build failed : {:?}",
        result
    );

    // Disable the aliceCo/shared/disabled capture
    let disable_draft = json!({
        "captures": {
            "aliceCo/shared/disabled": {
                "endpoint": {"connector": {"image": "source/test:test", "config": {}}},
                "bindings": [
                    {
                        "resource": {"id": "a"},
                        "target": "aliceCo/shared/a",
                        "disable": true
                    },
                    {
                        "resource": {"id": "b"},
                        "target": "aliceCo/shared/b",
                        "disable": true
                    },
                    {
                        "resource": {"id": "c"},
                        "target": "aliceCo/shared/c",
                        "disable": true
                    }
                ],
                "shards": {
                    "disable": true
                }
            }
        }
    });

    let result = harness
        .user_publication(
            alice_user_id,
            "Disable capture",
            harness::draft_catalog(disable_draft),
        )
        .await;
    assert_eq!(
        result.status.r#type,
        control_plane_api::publications::StatusType::Success
    );

    // Set up Bob's tenant
    let bob_user_id = harness.setup_tenant("bobCo").await;

    // Grant bobCo/ read access to aliceCo/shared/
    harness
        .add_role_grant("bobCo/", "aliceCo/shared/", models::Capability::Read)
        .await;

    // Create Bob's materialization
    let bob_draft = json!({
        "materializations": {
            "bobCo/private/materialization": {
                "endpoint": {"connector": {"image": "materialize/test:test", "config": {}}},
                "bindings": [
                    {
                        "resource": {"id": "a"},
                        "source": "aliceCo/shared/a"
                    },
                    {
                        "resource": {"id": "b"},
                        "source": "aliceCo/shared/b"
                    },
                    {
                        "resource": {"id": "c"},
                        "source": "aliceCo/shared/c"
                    }
                ]
            }
        }
    });

    let result = harness
        .user_publication(
            bob_user_id,
            "Bob's materialization",
            harness::draft_catalog(bob_draft),
        )
        .await;
    assert_eq!(
        result.status.r#type,
        control_plane_api::publications::StatusType::Success
    );
    harness.run_pending_controllers(None).await;

    // Insert alert history for aliceCo/shared/capture
    let mut ts = chrono::DateTime::parse_from_rfc3339("2024-08-09T10:11:12Z")
        .unwrap()
        .to_utc();

    // Insert resolved alerts (oldest to newest)
    for i in 0..3 {
        let fired_at = ts;
        let resolved_at = fired_at + chrono::Duration::minutes(30);
        ts = resolved_at + chrono::Duration::minutes(15);

        let alert_type = if i % 2 == 0 {
            models::status::AlertType::AutoDiscoverFailed
        } else {
            models::status::AlertType::ShardFailed
        };

        let args = alert_args(models::CatalogType::Capture, fired_at, Some(resolved_at));
        // Make sure we have multiple alerts that have the exact same `fired_at` time.
        // This is typical in production, because the alert evaluation runs periodically
        // and will set the same `fired_at` time for every alert in the same run.
        sqlx::query!(
            r#"
            INSERT INTO alert_history (catalog_name, alert_type, arguments, fired_at, resolved_at)
            VALUES ('aliceCo/shared/capture', $1::alert_type, $2, $3, $4),
            ('aliceCo/private/capture', $1::alert_type, $2, $3, $4)
            "#,
            alert_type as models::status::AlertType,
            args,
            fired_at,
            resolved_at
        )
        .execute(&harness.pool)
        .await
        .expect("Failed to insert alert history");
    }

    let args = alert_args(models::CatalogType::Capture, ts, None);
    // Insert one unresolved alert (most recent)
    sqlx::query!(
        r#"
        INSERT INTO alert_history (catalog_name, alert_type, arguments, fired_at)
        VALUES ($1, $2::alert_type, $3, $4)
        "#,
        "aliceCo/shared/capture" as &str,
        models::status::AlertType::ShardFailed as models::status::AlertType,
        args,
        ts,
    )
    .execute(&harness.pool)
    .await
    .expect("Failed to insert alert history");

    // Refresh snapshot to ensure all data is visible
    harness.refresh_snapshot().await;

    (harness, alice_user_id, bob_user_id)
}

fn alert_args(
    spec_type: models::CatalogType,
    fired_at: DateTime<Utc>,
    resolved_at: Option<DateTime<Utc>>,
) -> serde_json::Value {
    let state = if resolved_at.is_some() {
        models::status::alerts::AlertState::Resolved
    } else {
        models::status::alerts::AlertState::Firing
    };
    let mut extra = BTreeMap::new();
    extra.insert(
        "recipients".to_string(),
        serde_json::Value::Array(Vec::new()),
    );
    let args = ControllerAlert {
        state,
        spec_type,
        first_ts: fired_at,
        last_ts: None,
        error: "fake alert for tests".to_string(),
        count: 1,
        resolved_at,
        extra,
    };
    serde_json::to_value(args).unwrap()
}
