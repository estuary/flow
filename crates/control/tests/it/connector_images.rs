use control::models::connector_images::CreateConnectorImage;
use control::models::connectors::{ConnectorType, CreateConnector};
use control::repo::connector_images::{fetch_all, insert};
use control::repo::connectors::insert as insert_connector;

use crate::support::redactor::Redactor;
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
            maintainer: "Estuary Technologies".to_owned(),
            r#type: ConnectorType::Source,
        },
    )
    .await
    .expect("to insert test data");

    let image = insert(
        &db,
        CreateConnectorImage {
            connector_id: connector.id,
            name: "ghcr.io/estuary/source-hello-world".to_owned(),
            digest: "15751ba960870e5ba233ebfe9663fe8a236c8ce213b43fbf4cccc4e485594600".to_owned(),
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
    let redactor = Redactor::default()
        .redact(connector.id, "c1")
        .redact(image.id, "i1");
    assert_json_snapshot!(redactor.response_json(response).await.unwrap(), {
        ".data.*.attributes.created_at" => "[datetime]",
        ".data.*.attributes.updated_at" => "[datetime]",
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
            maintainer: "Estuary Technologies".to_owned(),
            r#type: ConnectorType::Source,
        },
    )
    .await
    .expect("to insert test data");

    let response = client
        .post(format!("http://{}/connector_images", server_address))
        .json(&CreateConnectorImage {
            connector_id: connector.id,
            name: "ghcr.io/estuary/source-hello-world".to_owned(),
            digest: "15751ba960870e5ba233ebfe9663fe8a236c8ce213b43fbf4cccc4e485594600".to_owned(),
            tag: "01fb856".to_owned(),
        })
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
async fn connectors_show_test() {
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
            maintainer: "Estuary Technologies".to_owned(),
            r#type: ConnectorType::Source,
        },
    )
    .await
    .expect("to insert test data");

    let image = insert(
        &db,
        CreateConnectorImage {
            connector_id: connector.id,
            name: "ghcr.io/estuary/source-hello-world".to_owned(),
            digest: "15751ba960870e5ba233ebfe9663fe8a236c8ce213b43fbf4cccc4e485594600".to_owned(),
            tag: "01fb856".to_owned(),
        },
    )
    .await
    .expect("to insert test data");

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
async fn connectors_spec_test() {
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
            maintainer: "Estuary Technologies".to_owned(),
            r#type: ConnectorType::Source,
        },
    )
    .await
    .expect("to insert test data");

    let image = insert(
        &db,
        CreateConnectorImage {
            connector_id: connector.id,
            name: "ghcr.io/estuary/source-hello-world".to_owned(),
            digest: "15751ba960870e5ba233ebfe9663fe8a236c8ce213b43fbf4cccc4e485594600".to_owned(),
            tag: "01fb856".to_owned(),
        },
    )
    .await
    .expect("to insert test data");

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
async fn connectors_discovery_test() {
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
            maintainer: "Estuary Technologies".to_owned(),
            r#type: ConnectorType::Source,
        },
    )
    .await
    .expect("to insert test data");

    let image = insert(
        &db,
        CreateConnectorImage {
            connector_id: connector.id,
            name: "ghcr.io/estuary/source-hello-world".to_owned(),
            digest: "15751ba960870e5ba233ebfe9663fe8a236c8ce213b43fbf4cccc4e485594600".to_owned(),
            tag: "01fb856".to_owned(),
        },
    )
    .await
    .expect("to insert test data");

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
