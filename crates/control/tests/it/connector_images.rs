use serde_json::Value as JsonValue;

use control::models::connector_images::CreateConnectorImage;
use control::models::connectors::{ConnectorType, CreateConnector};
use control::repo::connector_images::{fetch_all, insert};
use control::repo::connectors::insert as insert_connector;

use crate::support::{self, spawn_app};

#[tokio::test]
async fn connector_images_index_test() {
    let db = support::test_db_pool(support::function_name!())
        .await
        .expect("Failed to acquire a database connection");
    let server_address = spawn_app(db.clone())
        .await
        .expect("Failed to spawn our app.");
    let client = reqwest::Client::new();

    let connector = insert_connector(
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

    insert(
        &db,
        CreateConnectorImage {
            connector_id: connector.id,
            image: "ghcr.io/estuary/source-hello-world".to_owned(),
            sha256: "15751ba960870e5ba233ebfe9663fe8a236c8ce213b43fbf4cccc4e485594600".to_owned(),
            tag: "01fb856".to_owned(),
        },
    )
    .await
    .expect("to insert test data");

    let response = client
        .get(format!("http://{}/connector_images", server_address))
        .send()
        .await
        .expect("Failed to execute request.");

    assert!(response.status().is_success());
    assert_json_snapshot!(response.json::<JsonValue>().await.unwrap(), {
        ".data.*.id" => "[id]",
        ".data.*.connector_id" => "[id]",
        ".data.*.created_at" => "[datetime]",
        ".data.*.updated_at" => "[datetime]",
    });
}

#[tokio::test]
async fn connector_images_create_test() {
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

    let connector = insert_connector(
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
        .post(format!("http://{}/connector_images", server_address))
        .json(&CreateConnectorImage {
            connector_id: connector.id,
            image: "ghcr.io/estuary/source-hello-world".to_owned(),
            sha256: "15751ba960870e5ba233ebfe9663fe8a236c8ce213b43fbf4cccc4e485594600".to_owned(),
            tag: "01fb856".to_owned(),
        })
        .send()
        .await
        .expect("Failed to execute request.");

    assert_eq!(201, response.status().as_u16());
    assert_json_snapshot!(response.json::<JsonValue>().await.unwrap(), {
        ".data.id" => "[id]",
        ".data.connector_id" => "[id]",
        ".data.created_at" => "[datetime]",
        ".data.updated_at" => "[datetime]",
    });

    let images = fetch_all(&db).await.expect("to insert test data");
    assert_eq!(1, images.len());
}
