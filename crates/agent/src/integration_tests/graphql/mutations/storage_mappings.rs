use crate::integration_tests::harness::TestHarness;
use serde_json::json;

const CREATE_STORAGE_MAPPING_MUTATION: &str = r#"
mutation CreateStorageMapping($catalogPrefix: Prefix!, $spec: JSON!) {
    createStorageMapping(catalogPrefix: $catalogPrefix, spec: $spec) {
        catalogPrefix
    }
}
"#;

/// Storage-mapping mutations require effective (attenuation-aware) admin over
/// the *entire* claimed prefix, because a mapping at any prefix shadows the
/// tenant mapping for everything under it (longest-match wins). This pins the
/// authorization boundary formerly guarded by the removed `storageMappings`
/// applied-directive, whose legacy `internal.user_roles()` SQL accepted the
/// sub-prefix shape (it compared prefixes in the loose direction) and was
/// blind to bundle attenuation.
#[tokio::test]
async fn test_create_storage_mapping_authorization_denials() {
    let mut harness = TestHarness::init("storage_mapping_authz").await;
    let _alice = harness.setup_tenant("aliceCo").await;

    // bob administers only a *sub*-prefix: authority flows downward, so it
    // must not reach the tenant root.
    let bob = uuid::uuid!("bbbbbbbb-0000-0000-0000-000000000000");
    // erin reaches `aliceCo/` through a raw-`admin` role grant, but her own
    // grant delegates only the `editor` bundle: the walk attenuates the
    // second hop's bits below Admin.
    let erin = uuid::uuid!("eeeeeeee-0000-0000-0000-000000000000");
    sqlx::query(
        r#"with users as (
            insert into auth.users (id, email) values
                ($1, 'subprefix-admin@example.test'), ($2, 'attenuated-admin@example.test')
        ),
        user_grants as (
            insert into user_grants (user_id, object_role, capability, bundles) values
                ($1, 'aliceCo/sub/', 'admin', '{}'),
                ($2, 'sharedCo/', 'none', '{editor}')
        )
        insert into role_grants (subject_role, object_role, capability) values
            ('sharedCo/', 'aliceCo/', 'admin')"#,
    )
    .bind(bob)
    .bind(erin)
    .execute(&harness.pool)
    .await
    .unwrap();
    // An authoritative Snapshot (taken after the request starts) makes the
    // denials terminal rather than retryable.
    harness.refresh_snapshot_authoritative().await;

    for user_id in [bob, erin] {
        let result: Result<serde_json::Value, _> = harness
            .execute_graphql_query(
                user_id,
                CREATE_STORAGE_MAPPING_MUTATION,
                &json!({
                    "catalogPrefix": "aliceCo/",
                    "spec": {
                        "stores": [{"provider": "GCS", "bucket": "test-bucket"}],
                        "data_planes": ["ops/dp/public/test"]
                    },
                }),
            )
            .await;
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("is not an authorized as an Admin of catalog prefix 'aliceCo/'"),
            "expected an admin denial for {user_id}, got: {err}"
        );
    }
}

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
