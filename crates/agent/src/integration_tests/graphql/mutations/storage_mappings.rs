use crate::integration_tests::harness::TestHarness;
use serde_json::json;

const CREATE_STORAGE_MAPPING_MUTATION: &str = r#"
mutation CreateStorageMapping($catalogPrefix: Prefix!, $storage: JSON!, $dryRun: Boolean!) {
    createStorageMapping(catalogPrefix: $catalogPrefix, storage: $storage, dryRun: $dryRun) {
        created
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
                "storage": {
                    "stores": [{"provider": "GCS", "bucket": "test-bucket"}],
                    "data_planes": []
                },
                "dryRun": false
            }),
        )
        .await;
    let err = result.unwrap_err().to_string();

    assert!(
        err.contains("storage.data_planes must not be empty"),
        "expected empty data_planes error, got: {err}"
    );

    // Test: empty stores
    let result: Result<serde_json::Value, _> = harness
        .execute_graphql_query(
            alice_user_id,
            CREATE_STORAGE_MAPPING_MUTATION,
            &json!({
                "catalogPrefix": "aliceCo/sub/",
                "storage": {
                    "stores": [],
                    "data_planes": ["ops/dp/public/test"]
                },
                "dryRun": false
            }),
        )
        .await;
    let err = result.unwrap_err().to_string();

    assert!(
        err.contains("storage.stores must not be empty"),
        "expected empty stores error, got: {err}"
    );

    // Test: invalid catalog prefix (missing trailing slash)
    let result: Result<serde_json::Value, _> = harness
        .execute_graphql_query(
            alice_user_id,
            CREATE_STORAGE_MAPPING_MUTATION,
            &json!({
                "catalogPrefix": "aliceCo",
                "storage": {
                    "stores": [{"provider": "GCS", "bucket": "test-bucket"}],
                    "data_planes": ["ops/dp/public/test"]
                },
                "dryRun": false
            }),
        )
        .await;
    let err = result.unwrap_err().to_string();

    assert!(
        err.contains("invalid catalog prefix"),
        "expected invalid prefix error, got: {err}"
    );
}
