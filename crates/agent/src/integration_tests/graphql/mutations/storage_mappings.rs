use crate::integration_tests::harness::TestHarness;
use serde_json::json;

const CREATE_STORAGE_MAPPING_MUTATION: &str = r#"
mutation CreateStorageMapping($input: CreateStorageMappingInput!) {
    createStorageMapping(input: $input) {
        success
        catalogPrefix
        healthChecks {
            dataPlaneName
            fragmentStore
            success
            error
        }
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
                "input": {
                    "catalogPrefix": "aliceCo/sub/",
                    "storage": {
                        "stores": [{"provider": "GCS", "bucket": "test-bucket"}],
                        "data_planes": []
                    }
                }
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
                "input": {
                    "catalogPrefix": "aliceCo/sub/",
                    "storage": {
                        "stores": [],
                        "data_planes": ["ops/dp/public/test"]
                    }
                }
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
                "input": {
                    "catalogPrefix": "aliceCo",
                    "storage": {
                        "stores": [{"provider": "GCS", "bucket": "test-bucket"}],
                        "data_planes": ["ops/dp/public/test"]
                    }
                }
            }),
        )
        .await;
    let err = result.unwrap_err().to_string();

    assert!(
        err.contains("Invalid catalog prefix"),
        "expected invalid prefix error, got: {err}"
    );
}

#[tokio::test]
async fn test_create_storage_mapping_duplicate_prefix() {
    let mut harness = TestHarness::init("storage_mapping_duplicate").await;
    // setup_tenant creates a storage mapping for "aliceCo/" via provision_tenant
    let alice_user_id = harness.setup_tenant("aliceCo").await;

    // Attempt to create a storage mapping for the same prefix (should fail)
    let result: Result<serde_json::Value, _> = harness
        .execute_graphql_query(
            alice_user_id,
            CREATE_STORAGE_MAPPING_MUTATION,
            &json!({
                "input": {
                    "catalogPrefix": "aliceCo/",
                    "storage": {
                        "stores": [{"provider": "GCS", "bucket": "new-bucket"}],
                        "data_planes": ["ops/dp/public/test"]
                    }
                }
            }),
        )
        .await;
    let err = result.unwrap_err().to_string();

    assert!(
        err.contains("A storage mapping already exists for catalog prefix"),
        "expected duplicate prefix error, got: {err}"
    );
}
