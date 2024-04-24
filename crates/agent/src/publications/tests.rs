use super::{JobStatus, Publisher, UncommittedBuild};
use crate::integration_tests::harness::{draft_catalog, TestHarness};
use crate::FIXED_DATABASE_URL;
use models::Id;
use reqwest::Url;
use serde::Deserialize;
use serde_json::Value;
use sqlx::{Connection, Postgres, Transaction};
use tempfile::tempdir;
use uuid::Uuid;

// struct TestPublisher {
//     publisher: Publisher,
//     builds_root: tempfile::TempDir,
// }

// async fn test_publisher(test_name: &'static str) -> TestPublisher {
//     let mut pool = sqlx::postgres::PgPool::connect(&FIXED_DATABASE_URL)
//         .await
//         .unwrap();
//     let builds_root = tempdir().expect("Failed to create tempdir");

//     let (logs_tx, logs_rx) = tokio::sync::mpsc::channel(8192);
//     tokio::spawn(async move {
//         let mut logs_rx = logs_rx;
//         while let Some(log) = logs_rx.recv().await {
//             eprintln!("{test_name}: {:?}", log);
//         }
//         eprintln!("end of '{test_name}' pub logs");
//     });

//     let publisher = Publisher::new(
//         "support@estuary.dev",
//         false,
//         true, // enable test mode to noop validations and activations
//         "../../../.build/package/bin",
//         &url::Url::parse("http://not-used.test/").unwrap(),
//         &url::Url::from_directory_path(builds_root.path()).unwrap(),
//         "some-connector-network",
//         &url::Url::parse("http://not-used.test/").unwrap(),
//         &logs_tx,
//         pool,
//         models::IdGenerator::new(3),
//     );

//     TestPublisher {
//         publisher,
//         builds_root,
//     }
// }

#[tokio::test]
#[serial_test::serial]
async fn test_forbidden_connector() {
    let mut harness = TestHarness::init("test_forbidden_connector").await;
    let user_id = harness.setup_tenant("sheep").await;

    let draft = draft_catalog(serde_json::json!({
        "collections": {
            "sheep/wool": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" },
                        "color": { "type": "string" },
                    },
                    "required": ["id"]
                },
                "key": ["/id"]
            }
        },
        "captures": {
            "sheep/capture": {
                "endpoint": {
                    "connector": {
                        "image": "forbidden_connector:v99",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "target": "sheep/wool",
                        "resource": {
                            "pasture": "green hill"
                        }
                    }
                ]
            }
        }
    }));
    let pub_id = Id::new([0, 0, 0, 0, 0, 0, 0, 9]);
    let built = harness
        .publisher
        .build(user_id, pub_id, None, draft, Uuid::new_v4())
        .await
        .expect("build failed");
    assert!(built.has_errors());

    let errors = built.errors().collect::<Vec<_>>();

    insta::assert_debug_snapshot!(errors, @r###"
    [
        Error {
            scope: flow://capture/sheep/capture,
            error: Forbidden connector image 'forbidden_connector',
        },
    ]
    "###);
}

// Squelch warnings about struct fields never being read.
// They actually are read by insta when snapshotting.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct LiveSpec {
    catalog_name: String,
    connector_image_name: Option<String>,
    connector_image_tag: Option<String>,
    reads_from: Option<Vec<String>>,
    writes_to: Option<Vec<String>>,
    spec: Option<Value>,
    spec_type: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug)]
struct ScenarioResult {
    draft_id: Id,
    status: JobStatus,
    errors: Vec<String>,
    live_specs: Vec<LiveSpec>,
}

// async fn execute_publications(
//     harness: &mut TestHarness,
//     draft: tables::DraftCatalog,
// ) -> Vec<ScenarioResult> {
//     let bs_url: Url = "http://example.com".parse().unwrap();

//     let mut results: Vec<ScenarioResult> = vec![];

//     //let result = harness.publisher.build(draft).await.unwrap();
//     while let Some(row) = agent_sql::publications::dequeue(&mut *txn, true)
//         .await
//         .unwrap()
//     {
//         let row_draft_id = row.draft_id.clone();
//         let result = handler.process(row, &mut *txn, true).await.unwrap();
//         let pub_id = result.publication_id.into();

//         agent_sql::publications::resolve(pub_id, &result.publication_status, &mut *txn)
//             .await
//             .unwrap();

//         match result.publication_status {
//             JobStatus::Success { .. } => {
//                 let specs = sqlx::query_as!(
//                     LiveSpec,
//                     r#"
//                         select catalog_name as "catalog_name!",
//                                connector_image_name,
//                                connector_image_tag,
//                                reads_from,
//                                writes_to,
//                                spec,
//                                spec_type as "spec_type: String"
//                         from live_specs
//                         where live_specs.last_pub_id = $1::flowid
//                         order by live_specs.catalog_name;"#,
//                     pub_id as Id
//                 )
//                 .fetch_all(&mut *txn)
//                 .await
//                 .unwrap();

//                 results.push(ScenarioResult {
//                     draft_id: row_draft_id,
//                     status: result.publication_status.clone(),
//                     errors: vec![],
//                     live_specs: specs,
//                 })
//             }
//             _ => {
//                 let errors = sqlx::query!(
//                     r#"
//                 select draft_id as "draft_id: Id", scope, detail
//                 from draft_errors
//                 where draft_errors.draft_id = $1::flowid;"#,
//                     row_draft_id as Id
//                 )
//                 .fetch_all(&mut *txn)
//                 .await
//                 .unwrap();

//                 let mut formatted_errors: Vec<String> =
//                     errors.into_iter().map(|e| e.detail).collect();
//                 // sort errors so that snapshot results are always consistent
//                 formatted_errors.sort();

//                 results.push(ScenarioResult {
//                     draft_id: row_draft_id,
//                     status: result.publication_status.clone(),
//                     errors: formatted_errors,
//                     live_specs: vec![],
//                 });
//             }
//         };
//     }

//     results
// }
