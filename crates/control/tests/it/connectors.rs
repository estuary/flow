use serde_json::Value as JsonValue;

use control::repo::connectors::fetch_all;

use crate::support::redactor::Redactor;
use crate::support::{self, factory, test_context};

#[tokio::test]
async fn index_test() {
    // Arrange
    let t = test_context!();
    let connector = factory::HelloWorldConnector.create(t.db()).await;

    // Act
    let response = t.get("/connectors").await;

    // Assert
    assert!(response.status().is_success());
    let redactor = Redactor::default().redact(connector.id, "c1");
    assert_json_snapshot!(redactor.response_json(response).await.unwrap(), {
        ".data.*.attributes.created_at" => "[datetime]",
        ".data.*.attributes.updated_at" => "[datetime]",
    });
}

#[tokio::test]
async fn create_test() {
    // Arrange
    let t = test_context!();
    let input = factory::KafkaConnector.attrs();

    // Act
    let response = t.post("/connectors", &input).await;

    // Assert
    let connectors = fetch_all(t.db()).await.expect("to insert test data");
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
    // Arrange
    let t = test_context!();
    let connector = factory::HelloWorldConnector.create(t.db()).await;
    let image = factory::HelloWorldImage.create(t.db(), &connector).await;

    // These are just another connector and image that should not be returned in the results.
    let other_connector = factory::KafkaConnector.create(t.db()).await;
    let _other_image = factory::KafkaImage.create(t.db(), &other_connector).await;

    // Act
    let response = t
        .get(&format!("/connectors/{}/connector_images", &connector.id))
        .await;

    // Assert
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
    // Arrange
    let t = test_context!();
    let input = factory::KafkaConnector.attrs();

    // Act
    let first_response = t.post("/connectors", &input).await;
    let second_response = t.post("/connectors", &input).await;

    // Assert
    assert_eq!(201, first_response.status().as_u16());
    assert_eq!(400, second_response.status().as_u16());
    assert_json_snapshot!(second_response.json::<JsonValue>().await.unwrap(), {});
    let connectors = fetch_all(t.db()).await.expect("to insert test data");
    assert_eq!(1, connectors.len());
}
