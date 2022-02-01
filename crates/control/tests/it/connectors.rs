use control::models::connector_images::CreateConnectorImage;
use serde_json::Value as JsonValue;

use control::models::connectors::{ConnectorType, CreateConnector};
use control::repo::connector_images::insert as insert_image;
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

#[tokio::test]
async fn connectors_images_test() {
    let db = support::test_db_pool(support::function_name!())
        .await
        .expect("Failed to acquire a database connection");
    let server_address = spawn_app(db.clone())
        .await
        .expect("Failed to spawn our app.");
    let client = reqwest::Client::new();

    let connector = insert(
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

    insert_image(
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

    // These are just another connector and image that should not be returned in the results.
    let other_connector = insert(
        &db,
        CreateConnector {
            description: "Reads from a Kafka topic".to_owned(),
            name: "Kafka".to_owned(),
            owner: "Estuary Technologies".to_owned(),
            r#type: ConnectorType::Source,
        },
    )
    .await
    .expect("to insert test data");

    insert_image(
        &db,
        CreateConnectorImage {
            connector_id: other_connector.id,
            image: "ghcr.io/estuary/source-kafka".to_owned(),
            sha256: "34affba1ac24d67035309c64791e7c7b2f01fd26a934d91da16e262427b88a78".to_owned(),
            tag: "01fb856".to_owned(),
        },
    )
    .await
    .expect("to insert test data");

    let response = client
        .get(format!(
            "http://{}/connectors/{}/connector_images",
            server_address, connector.id
        ))
        .send()
        .await
        .expect("Failed to execute request.");

    assert_eq!(200, response.status().as_u16());
    assert_json_snapshot!(response.json::<JsonValue>().await.unwrap(), {
        ".data.*.id" => "[id]",
        ".data.*.connector_id" => "[id]",
        ".data.*.created_at" => "[datetime]",
        ".data.*.updated_at" => "[datetime]",
    });
}
