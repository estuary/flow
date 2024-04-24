use agent_sql::{CatalogType, Id};
use proto_flow::flow::CaptureSpec;
use serde_json::Value;
use sqlx::Connection;
use url::Url;

use crate::publications::linked_materializations::create_linked_materialization_publications;
use crate::FIXED_DATABASE_URL;

#[tokio::test]
#[serial_test::parallel]
async fn test_catpure_only_published() {
    let happy_spec = serde_json::json!({
        "name": "acmeCo/captureA/source-happy",
        "connector_type": "IMAGE",
        "config": {},
        "bindings": [
            {
                "resource_config_json": {},
                "collection": {
                    "name": "acmeCo/captureA/c1"
                }
            },
            // (c2 is removed)
            {
                "resource_config_json": {},
                "collection": {
                    "name": "acmeCo/captureA/c3"
                }
            },
            // c4 is added
            {
                "resource_config_json": {},
                "collection": {
                    "name": "acmeCo/captureA/c4"
                }
            },
        ]
    });

    test_create_linked_materialization_pubs(
        "capture_only_published",
        vec![happy_spec],
        vec!["acmeCo/captureA/source-happy".to_string()],
    )
    .await;
}

#[tokio::test]
#[serial_test::parallel]
async fn test_materialization_only_published() {
    // When only the materialization is published, the capture
    // spec must be queried from the database.
    test_create_linked_materialization_pubs(
        "materialization_only_published",
        Vec::new(),
        vec!["acmeCo/captureA/source-happy".to_string()],
    )
    .await;
}

#[tokio::test]
#[serial_test::parallel]
async fn test_empty_capture_bindings() {
    let spec = serde_json::json!({
        "name": "acmeCo/captureB/source-empty",
        "connector_type": "IMAGE",
        "config": {},
        "bindings": [ ]
    });

    test_create_linked_materialization_pubs(
        "empty_capture_bindings",
        vec![spec],
        vec!["acmeCo/captureB/source-empty".to_string()],
    )
    .await;
}

#[tokio::test]
#[serial_test::parallel]
async fn test_all_disabled_capture_bindings() {
    // Disabled capture bindings are not represented in the built specs.
    let spec = serde_json::json!({
        "name": "acmeCo/captureC/source-all-disabled",
        "connector_type": "IMAGE",
        "config": {},
        "bindings": [ ]
    });

    test_create_linked_materialization_pubs(
        "all_disabled_capture_bindings",
        vec![spec],
        vec!["acmeCo/captureC/source-all-disabled".to_string()],
    )
    .await;
}

#[tokio::test]
#[serial_test::parallel]
async fn test_multiple_materializations() {
    test_create_linked_materialization_pubs(
        "multiple_materializations",
        Vec::new(),
        vec![
            "acmeCo/captureA/source-happy".to_string(),
            "acmeCo/captureB/source-empty".to_string(),
            "acmeCo/captureC/source-all-disabled".to_string(),
            "acmeCo/captureD/nothing-sources-this".to_string(),
        ],
    )
    .await;
}

async fn test_create_linked_materialization_pubs(
    name: &str,
    built_captures: Vec<Value>,
    maybe_source_captures: Vec<String>,
) {
    let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
        .await
        .unwrap();
    let mut txn = conn.begin().await.unwrap();

    sqlx::query(include_str!(
        "../test_resources/linked_materializations.sql"
    ))
    .execute(&mut txn)
    .await
    .unwrap();

    let build_output = into_built_captures(built_captures);
    let pub_ids = create_linked_materialization_publications(
        "accounts@estuary.test",
        &build_output,
        maybe_source_captures,
        &mut txn,
    )
    .await
    .expect("create_linked_materialization_publications failed");

    let results = sqlx::query!(
        r#"
            select
                ds.catalog_name,
                ds.spec_type as "spec_type: CatalogType",
                ds.spec
            from publications pubs
            join draft_specs ds on pubs.draft_id = ds.draft_id
            where pubs.id = any ($1::flowid[])
            order by catalog_name asc;
        "#,
        pub_ids as Vec<Id>
    )
    .fetch_all(&mut txn)
    .await
    .unwrap();
    insta::assert_debug_snapshot!(name, results);
}

fn into_built_captures(built_captures: Vec<Value>) -> tables::BuiltCaptures {
    let mut out = tables::BuiltCaptures::new();

    for cap in built_captures {
        let spec: CaptureSpec = serde_json::from_value(cap).unwrap();
        let previous_spec = spec.clone();
        // The validated response isn't actually used during this process.
        let validated = Some(proto_flow::capture::response::Validated {
            bindings: Vec::new(),
        });
        out.insert(tables::BuiltCapture {
            capture: models::Capture::new("acmeCo/captureA/source-happy"),
            scope: Url::parse("test://not-real").unwrap(),
            expect_version_id: None,
            validated,
            spec: Some(spec),
            previous_spec: Some(previous_spec),
        });
    }
    out
}
