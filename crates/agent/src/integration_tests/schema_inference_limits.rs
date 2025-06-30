use super::{
    harness::{draft_catalog, TestHarness},
    spec_fixture,
};
use crate::ControlPlane;
use models::Collection;
use proto_flow::capture::response::{discovered::Binding, Discovered};
use serde_json::json;
use std::str::FromStr;

fn document_schema_with_limit(limit: usize, include_meta: bool) -> serde_json::Value {
    let mut properties = json!({
        "id": {"type": "string"},
        "name": {"type": "string"}
    });

    if include_meta {
        properties["_meta"] = json!({
            "type": "object",
            "properties": {
                "uuid": {"type": "string"},
            }
        });
    }

    json!({
        "type": "object",
        "properties": properties,
        "x-infer-schema": {
            "x-inferred-schema-limit": limit
        }
    })
}

fn document_schema_boolean() -> serde_json::Value {
    json!({
        "type": "object",
        "properties": {
            "id": {"type": "string"}
        },
        "x-infer-schema": true,
    })
}

#[tokio::test]
#[serial_test::serial]
async fn test_x_infer_schema_in_read_schema() {
    let mut harness = TestHarness::init("test_x_infer_schema_in_read_schema").await;
    let user_id = harness.setup_tenant("testing").await;

    // Create discovered bindings with different complexity limits
    let discovered_response = Discovered {
        bindings: vec![
            Binding {
                recommended_name: "high_limit_collection".to_string(),
                document_schema_json: serde_json::to_string(&document_schema_with_limit(
                    5000, true,
                ))
                .unwrap(),
                resource_config_json: r#"{"id": "high_limit"}"#.to_string(),
                key: vec!["/id".to_string()],
                disable: false,
                resource_path: vec!["high_limit".to_string()],
                is_fallback_key: false,
            },
            Binding {
                recommended_name: "low_limit_collection".to_string(),
                document_schema_json: serde_json::to_string(&document_schema_with_limit(50, false))
                    .unwrap(),
                resource_config_json: r#"{"id": "low_limit"}"#.to_string(),
                key: vec!["/id".to_string()],
                disable: false,
                resource_path: vec!["low_limit".to_string()],
                is_fallback_key: false,
            },
            Binding {
                recommended_name: "old_flag_collection".to_string(),
                document_schema_json: serde_json::to_string(&document_schema_boolean()).unwrap(),
                resource_config_json: r#"{"id": "old_flag"}"#.to_string(),
                key: vec!["/id".to_string()],
                disable: false,
                resource_path: vec!["old_flag".to_string()],
                is_fallback_key: false,
            },
        ],
    };

    // Create a draft and run discovery
    let draft_id = harness
        .create_draft(user_id, "complexity_limits_test", Default::default())
        .await;

    let result = harness
        .user_discover(
            "source/test",
            ":test",
            "testing/capture-with-limits",
            draft_id,
            r#"{"test": "config"}"#,
            false,
            Ok((spec_fixture(), discovered_response)),
        )
        .await;

    if !result.errors.is_empty() {
        panic!("Discovery failed with errors: {:?}", result.errors);
    }

    assert!(
        result.job_status.is_success(),
        "Discovery should succeed, got: {:?}",
        result.job_status
    );
    assert!(result.errors.is_empty(), "Discovery should have no errors");
    assert_eq!(3, result.draft.collections.len());

    // Publish the draft to create live specs
    let pub_result = harness
        .create_user_publication(user_id, draft_id, "publish complexity limits test")
        .await;

    assert!(
        pub_result.status.is_success(),
        "Publication should succeed, got errors: {:?}",
        pub_result.errors
    );

    // Fetch the read schemas from the control plane for both collections
    let control_plane = harness.control_plane();

    let high_limit_spec = control_plane
        .get_collection(Collection::new("testing/high_limit_collection"))
        .await
        .expect("Should find high limit collection")
        .expect("High limit collection should exist")
        .spec
        .read_schema_json;

    let low_limit_spec = control_plane
        .get_collection(Collection::new("testing/low_limit_collection"))
        .await
        .expect("Should find low limit collection")
        .expect("Low limit collection should exist")
        .spec
        .read_schema_json;

    let bool_flag_spec = control_plane
        .get_collection(Collection::new("testing/old_flag_collection"))
        .await
        .expect("Should find old flag collection")
        .expect("Old flag collection should exist")
        .spec
        .read_schema_json;

    // Snapshot the shapes to verify correct complexity limits are applied
    insta::assert_json_snapshot!(
        "high_limit_shape",
        serde_json::Value::from_str(&high_limit_spec).unwrap()
    );
    insta::assert_json_snapshot!(
        "low_limit_shape",
        serde_json::Value::from_str(&low_limit_spec).unwrap()
    );
    insta::assert_json_snapshot!(
        "bool_flag_shape",
        serde_json::Value::from_str(&bool_flag_spec).unwrap()
    );
}
