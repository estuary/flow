use control::models::connectors::{ConnectorType, NewConnector};
use serde_json::Value as JsonValue;

use control::repo::connectors::fetch_all;

use crate::support::redactor::Redactor;
use crate::support::{factory, test_context};

#[tokio::test]
async fn index_test() {
    // Arrange
    let mut t = test_context!();
    let account = factory::AdminAccount.create(t.db()).await;
    t.login(account);
    let connector = factory::HelloWorldConnector.create(t.db()).await;

    // Act
    let mut response = t.get("/connectors").await;

    // Assert
    assert!(response.status().is_success());
    let redactor = Redactor::default().redact(connector.id, "c1");
    assert_json_snapshot!(redactor.response_json(&mut response).await.unwrap(), {
        ".data.*.attributes.created_at" => "[datetime]",
        ".data.*.attributes.updated_at" => "[datetime]",
    });
}

#[tokio::test]
async fn create_test() {
    // Arrange
    let mut t = test_context!();
    let account = factory::AdminAccount.create(t.db()).await;
    t.login(account);
    let input = factory::KafkaConnector.attrs();

    // Act
    let mut response = t.post("/connectors", &input).await;

    // Assert
    let connectors = fetch_all(t.db()).await.expect("to insert test data");
    assert_eq!(1, connectors.len());
    assert_eq!(201, response.status().as_u16());
    let redactor = Redactor::default().redact(connectors[0].id, "c1");
    assert_json_snapshot!(redactor.response_json(&mut response).await.unwrap(), {
        ".data.attributes.created_at" => "[datetime]",
        ".data.attributes.updated_at" => "[datetime]",
    });
}

#[tokio::test]
async fn images_test() {
    // Arrange
    let mut t = test_context!();
    let account = factory::AdminAccount.create(t.db()).await;
    t.login(account);
    let connector = factory::HelloWorldConnector.create(t.db()).await;
    let image = factory::HelloWorldImage.create(t.db(), &connector).await;

    // These are just another connector and image that should not be returned in the results.
    let other_connector = factory::KafkaConnector.create(t.db()).await;
    let _other_image = factory::KafkaImage.create(t.db(), &other_connector).await;

    // Act
    let mut response = t
        .get(&format!("/connectors/{}/connector_images", &connector.id))
        .await;

    // Assert
    assert_eq!(200, response.status().as_u16());
    let redactor = Redactor::default()
        .redact(connector.id, "c1")
        .redact(image.id, "i1");
    assert_json_snapshot!(redactor.response_json(&mut response).await.unwrap(), {
        ".data.*.attributes.created_at" => "[datetime]",
        ".data.*.attributes.updated_at" => "[datetime]",
    });
}

#[tokio::test]
async fn duplicate_insertion_test() {
    // Arrange
    let mut t = test_context!();
    let account = factory::AdminAccount.create(t.db()).await;
    t.login(account);
    let input = factory::KafkaConnector.attrs();

    // Act
    let first_response = t.post("/connectors", &input).await;
    let mut second_response = t.post("/connectors", &input).await;

    // Assert
    assert_eq!(201, first_response.status().as_u16());
    assert_eq!(400, second_response.status().as_u16());

    let body = hyper::body::to_bytes(second_response.body_mut())
        .await
        .expect("a response body");

    assert_json_snapshot!(serde_json::from_slice::<JsonValue>(body.as_ref()).expect("valid json"));
    let connectors = fetch_all(t.db()).await.expect("to insert test data");
    assert_eq!(1, connectors.len());
}

#[tokio::test]
async fn validation_test() {
    // Arrange
    let mut t = test_context!();
    let account = factory::AdminAccount.create(t.db()).await;
    t.login(account);
    let input = NewConnector {
        description: "d".to_owned(),
        name: "n".to_owned(),
        maintainer: "m".to_owned(),
        r#type: ConnectorType::Source,
    };

    // Act
    let mut response = t.post("/connectors", &input).await;

    // Assert
    let redactor = Redactor::default();
    assert_json_snapshot!(redactor.response_json(&mut response).await.unwrap(), {});
    assert_eq!(422, response.status().as_u16());
}
