use crate::support::spawn_app;

use serde_json::Value as JsonValue;

#[tokio::test]
async fn list_connectors_works() {
    let server_address = spawn_app().await.expect("Failed to spawn our app.");
    let client = reqwest::Client::new();

    let response = client
        .get(format!("http://{}/connectors", server_address))
        .send()
        .await
        .expect("Failed to execute request.");

    assert!(response.status().is_success());
    assert_json_snapshot!(response.json::<JsonValue>().await.unwrap())
}
