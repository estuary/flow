use super::common_setup;
use serde_json::Value;
use std::fs;

#[tokio::test]
#[serial_test::serial]
async fn test_graphql_queries() {
    let (mut harness, alice_user_id, bob_user_id) = common_setup().await;

    // Get the directory containing our query files
    let queries_dir = test_support::test_resource_path!("src/integration_tests/graphql/queries");

    // Read all .graphql files in the queries directory
    let entries = fs::read_dir(&queries_dir)
        .expect("Failed to read queries directory")
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext == "graphql")
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();

    // Sort entries by filename for consistent ordering
    let mut query_files: Vec<_> = entries.into_iter().map(|entry| entry.path()).collect();
    query_files.sort();

    let empty_vars = serde_json::json!({});

    for query_path in query_files {
        let query_content = fs::read_to_string(&query_path)
            .expect(&format!("Failed to read query file: {:?}", query_path));

        let file_stem = query_path
            .file_stem()
            .and_then(|s| s.to_str())
            .expect("Failed to get file stem");

        // Execute query as Alice
        let alice_result: Value = harness
            .execute_graphql_query(alice_user_id, &query_content, &empty_vars)
            .await
            .unwrap_or_else(|e| format!("GraphQL query failed with error: {}", e).into());

        // Execute query as Bob
        let bob_result: Value = harness
            .execute_graphql_query(bob_user_id, &query_content, &empty_vars)
            .await
            .unwrap_or_else(|e| format!("GraphQL query failed with error: {}", e).into());

        // Snapshot the results
        insta::assert_json_snapshot!(format!("alice-{}", file_stem), alice_result);

        insta::assert_json_snapshot!(format!("bob-{}", file_stem), bob_result);
    }
}
