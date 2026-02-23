use crate::integration_tests::harness::TestHarness;
use serde_json::json;

const CREATE_STORAGE_MAPPING_MUTATION: &str = r#"
mutation CreateStorageMapping($catalogPrefix: Prefix!, $spec: JSON!) {
    createStorageMapping(catalogPrefix: $catalogPrefix, spec: $spec) {
        catalogPrefix
    }
}
"#;

#[tokio::test]
async fn test_create_storage_mapping_validation_errors() {
    let mut harness = TestHarness::init("storage_mapping_validation").await;
    let alice_user_id = harness.setup_tenant("aliceCo").await;

    // Test: empty data_planes (use sub-prefix that doesn't have existing mapping)
    let result: Result<serde_json::Value, _> = harness
        .execute_graphql_query(
            alice_user_id,
            CREATE_STORAGE_MAPPING_MUTATION,
            &json!({
                "catalogPrefix": "aliceCo/sub/",
                "spec": {
                    "stores": [{"provider": "GCS", "bucket": "test-bucket"}],
                    "data_planes": []
                },
            }),
        )
        .await;
    let err = result.unwrap_err().to_string();

    assert!(
        err.contains("spec.data_planes must not be empty"),
        "expected empty data_planes error, got: {err}"
    );

    // Test: empty stores
    let result: Result<serde_json::Value, _> = harness
        .execute_graphql_query(
            alice_user_id,
            CREATE_STORAGE_MAPPING_MUTATION,
            &json!({
                "catalogPrefix": "aliceCo/sub/",
                "spec": {
                    "stores": [],
                    "data_planes": ["ops/dp/public/test"]
                },
            }),
        )
        .await;
    let err = result.unwrap_err().to_string();

    assert!(
        err.contains("spec.stores must not be empty"),
        "expected empty stores error, got: {err}"
    );

    // Test: invalid catalog prefix (missing trailing slash)
    let result: Result<serde_json::Value, _> = harness
        .execute_graphql_query(
            alice_user_id,
            CREATE_STORAGE_MAPPING_MUTATION,
            &json!({
                "catalogPrefix": "aliceCo",
                "spec": {
                    "stores": [{"provider": "GCS", "bucket": "test-bucket"}],
                    "data_planes": ["ops/dp/public/test"]
                },
            }),
        )
        .await;
    let err = result.unwrap_err().to_string();

    assert!(
        err.contains("invalid catalog prefix"),
        "expected invalid prefix error, got: {err}"
    );
}
