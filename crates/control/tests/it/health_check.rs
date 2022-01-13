use crate::support::spawn_app;

#[tokio::test]
async fn health_check_works() {
    let server_address = spawn_app().await.expect("Failed to spawn our app.");
    let client = reqwest::Client::new();

    let response = client
        .get(format!("http://{}/health_check", server_address))
        .send()
        .await
        .expect("Failed to execute request.");

    assert!(response.status().is_success());
}
