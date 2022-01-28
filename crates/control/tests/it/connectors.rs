use serde_json::Value as JsonValue;

use control::models::connectors::{ConnectorType, CreateConnector};
use control::repo::connectors::{fetch_all, insert};

use crate::support::{self, spawn_app};

#[tokio::test]
async fn connectors_index_test() {
    let db = support::test_db_pool(support::function_name!())
        .await
        .expect("Failed to acquire a database connection");
    let server_address = spawn_app(db.clone())
        .await
        .expect("Failed to spawn our app.");
    let client = reqwest::Client::new();

    insert(
        &db,
        CreateConnector {
            description: "A flood greetings.".to_owned(),
            name: "Hello World".to_owned(),
            owner: "Estuary Technologies".to_owned(),
            r#type: ConnectorType::Source,
        },
    )
    .await
    .expect("to insert test data");

    let response = client
        .get(format!("http://{}/connectors", server_address))
        .send()
        .await
        .expect("Failed to execute request.");

    assert!(response.status().is_success());
    assert_json_snapshot!(response.json::<JsonValue>().await.unwrap(), {
        ".data.*.id" => "[id]",
        ".data.*.created_at" => "[datetime]",
        ".data.*.updated_at" => "[datetime]",
    });
}

#[tokio::test]
async fn connectors_create_test() {
    let db = support::test_db_pool(support::function_name!())
        .await
        .expect("Failed to acquire a database connection");
    let server_address = spawn_app(db.clone())
        .await
        .expect("Failed to spawn our app.");
    let client = reqwest::Client::new();

    assert!(fetch_all(&db)
        .await
        .expect("to insert test data")
        .is_empty());

    let response = client
        .post(format!("http://{}/connectors", server_address))
        .json(&CreateConnector {
            description: "Reads data from Kafka topics.".to_owned(),
            name: "Kafka".to_owned(),
            owner: "Estuary Technologies".to_owned(),
            r#type: ConnectorType::Source,
        })
        .send()
        .await
        .expect("Failed to execute request.");

    assert_eq!(201, response.status().as_u16());
    assert_json_snapshot!(response.json::<JsonValue>().await.unwrap(), {
        ".data.id" => "[id]",
        ".data.created_at" => "[datetime]",
        ".data.updated_at" => "[datetime]",
    });

    let connectors = fetch_all(&db).await.expect("to insert test data");
    assert_eq!(1, connectors.len());
}
