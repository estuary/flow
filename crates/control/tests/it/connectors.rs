use serde_json::Value as JsonValue;

use control::models::connectors::{ConnectorType, CreateConnector};
use control::repo::connectors::fetch_all;

use crate::support::redactor::Redactor;
use crate::support::{self, factory, spawn_app};

#[tokio::test]
async fn index_test() {
    let db = support::test_db_pool(support::function_name!())
        .await
        .expect("Failed to acquire a database connection");
    let server_address = spawn_app(db.clone())
        .await
        .expect("Failed to spawn our app.");
    let client = reqwest::Client::new();

    let connector = factory::HelloWorldConnector.create(&db).await;

    let response = client
        .get(format!("http://{}/connectors", server_address))
        .send()
        .await
        .expect("Failed to execute request.");

    assert!(response.status().is_success());

    let redactor = Redactor::default().redact(connector.id, "c1");
    assert_json_snapshot!(redactor.response_json(response).await.unwrap(), {
        ".data.*.attributes.created_at" => "[datetime]",
        ".data.*.attributes.updated_at" => "[datetime]",
    });
}

#[tokio::test]
async fn create_test() {
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
            maintainer: "Estuary Technologies".to_owned(),
            r#type: ConnectorType::Source,
        })
        .send()
        .await
        .expect("Failed to execute request.");

    let connectors = fetch_all(&db).await.expect("to insert test data");
    assert_eq!(1, connectors.len());

    assert_eq!(201, response.status().as_u16());
    let redactor = Redactor::default().redact(connectors[0].id, "c1");
    assert_json_snapshot!(redactor.response_json(response).await.unwrap(), {
        ".data.attributes.created_at" => "[datetime]",
        ".data.attributes.updated_at" => "[datetime]",
    });
}

#[tokio::test]
async fn images_test() {
    let db = support::test_db_pool(support::function_name!())
        .await
        .expect("Failed to acquire a database connection");
    let server_address = spawn_app(db.clone())
        .await
        .expect("Failed to spawn our app.");
    let client = reqwest::Client::new();

    let connector = factory::HelloWorldConnector.create(&db).await;
    let image = factory::HelloWorldImage.create(&db, &connector).await;

    // These are just another connector and image that should not be returned in the results.
    let other_connector = factory::KafkaConnector.create(&db).await;
    let _other_image = factory::KafkaImage.create(&db, &other_connector).await;

    let response = client
        .get(format!(
            "http://{}/connectors/{}/connector_images",
            server_address, connector.id
        ))
        .send()
        .await
        .expect("Failed to execute request.");

    assert_eq!(200, response.status().as_u16());
    let redactor = Redactor::default()
        .redact(connector.id, "c1")
        .redact(image.id, "i1");
    assert_json_snapshot!(redactor.response_json(response).await.unwrap(), {
        ".data.*.attributes.created_at" => "[datetime]",
        ".data.*.attributes.updated_at" => "[datetime]",
    });
}

#[tokio::test]
async fn duplicate_insertion_test() {
    let db = support::test_db_pool(support::function_name!())
        .await
        .expect("Failed to acquire a database connection");
    let server_address = spawn_app(db.clone())
        .await
        .expect("Failed to spawn our app.");
    let client = reqwest::Client::new();

    let input = factory::KafkaConnector.attrs();

    let first_response = client
        .post(format!("http://{}/connectors", server_address))
        .json(&input)
        .send()
        .await
        .expect("Failed to execute request.");

    assert_eq!(201, first_response.status().as_u16());

    let second_response = client
        .post(format!("http://{}/connectors", server_address))
        .json(&input)
        .send()
        .await
        .expect("Failed to execute request.");

    assert_eq!(400, second_response.status().as_u16());
    assert_json_snapshot!(second_response.json::<JsonValue>().await.unwrap(), {});

    let connectors = fetch_all(&db).await.expect("to insert test data");
    assert_eq!(1, connectors.len());
}
