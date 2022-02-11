use control::repo::connector_images::fetch_all;

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
    let image = factory::HelloWorldImage.create(&db, &connector).await;

    let response = client
        .get(format!("http://{}/connector_images", server_address))
        .send()
        .await
        .expect("Failed to execute request.");

    assert!(response.status().is_success());
    let redactor = Redactor::default()
        .redact(connector.id, "c1")
        .redact(image.id, "i1");
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

    let connector = factory::HelloWorldConnector.create(&db).await;
    let input = factory::HelloWorldImage.attrs(&connector);

    let response = client
        .post(format!("http://{}/connector_images", server_address))
        .json(&input)
        .send()
        .await
        .expect("Failed to execute request.");

    let images = fetch_all(&db).await.expect("to insert test data");
    assert_eq!(1, images.len());

    assert_eq!(201, response.status().as_u16());
    let redactor = Redactor::default()
        .redact(connector.id, "c1")
        .redact(images[0].id, "i1");
    assert_json_snapshot!(redactor.response_json(response).await.unwrap(), {
        ".data.attributes.created_at" => "[datetime]",
        ".data.attributes.updated_at" => "[datetime]",
    });
}

#[tokio::test]
async fn show_test() {
    let db = support::test_db_pool(support::function_name!())
        .await
        .expect("Failed to acquire a database connection");
    let server_address = spawn_app(db.clone())
        .await
        .expect("Failed to spawn our app.");
    let client = reqwest::Client::new();

    let connector = factory::HelloWorldConnector.create(&db).await;
    let image = factory::HelloWorldImage.create(&db, &connector).await;

    let response = client
        .get(format!(
            "http://{}/connector_images/{}",
            server_address, image.id
        ))
        .send()
        .await
        .expect("Failed to execute request.");

    assert_eq!(200, response.status().as_u16());
    let redactor = Redactor::default()
        .redact(connector.id, "c1")
        .redact(image.id, "i1");
    assert_json_snapshot!(redactor.response_json(response).await.unwrap(), {
        ".data.attributes.created_at" => "[datetime]",
        ".data.attributes.updated_at" => "[datetime]",
    });
}

#[tokio::test]
async fn spec_test() {
    let db = support::test_db_pool(support::function_name!())
        .await
        .expect("Failed to acquire a database connection");
    let server_address = spawn_app(db.clone())
        .await
        .expect("Failed to spawn our app.");
    let client = reqwest::Client::new();

    let connector = factory::HelloWorldConnector.create(&db).await;
    let image = factory::HelloWorldImage.create(&db, &connector).await;

    let response = client
        .get(format!(
            "http://{}/connector_images/{}/spec",
            server_address, image.id
        ))
        .send()
        .await
        .expect("Failed to execute request.");

    assert_eq!(200, response.status().as_u16());
    let redactor = Redactor::default()
        .redact(connector.id, "c1")
        .redact(image.id, "i1");
    assert_json_snapshot!(redactor.response_json(response).await.unwrap(), {
        ".data.id" => "[nonce]",
        ".data.attributes.created_at" => "[datetime]",
        ".data.attributes.updated_at" => "[datetime]",
    });
}

#[tokio::test]
async fn discovery_test() {
    let db = support::test_db_pool(support::function_name!())
        .await
        .expect("Failed to acquire a database connection");
    let server_address = spawn_app(db.clone())
        .await
        .expect("Failed to spawn our app.");
    let client = reqwest::Client::new();

    let connector = factory::HelloWorldConnector.create(&db).await;
    let image = factory::HelloWorldImage.create(&db, &connector).await;

    let response = client
        .post(format!(
            "http://{}/connector_images/{}/discovery",
            server_address, image.id
        ))
        .json(&serde_json::json!({"greetings": 10}))
        .send()
        .await
        .expect("Failed to execute request.");

    assert_eq!(200, response.status().as_u16());
    let redactor = Redactor::default()
        .redact(connector.id, "c1")
        .redact(image.id, "i1");
    assert_json_snapshot!(redactor.response_json(response).await.unwrap(), {
        ".data.id" => "[nonce]",
        ".data.attributes.created_at" => "[datetime]",
        ".data.attributes.updated_at" => "[datetime]",
    });
}
