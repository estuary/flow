use crate::support::{self, spawn_app};

use control::models::connectors::{ConnectorType, CreateConnector};
use control::repo::connectors::insert;
use serde_json::Value as JsonValue;

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
