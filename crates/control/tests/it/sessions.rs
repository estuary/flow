use serde_json::Value as JsonValue;

use control::models::sessions::NewSession;
use control::repo::accounts as accounts_repo;
use control::repo::credentials as credentials_repo;

use crate::support::redactor::Redactor;
use crate::support::{self, test_context};

#[tokio::test]
async fn local_registration_test() {
    // Arrange
    let t = test_context!();
    let input = NewSession {
        auth_token: "batman".to_owned(),
    };

    // Act
    let mut response = t.post("/sessions/local", &input).await;

    // Assert
    let accounts = accounts_repo::fetch_all(t.db())
        .await
        .expect("to fetch new account");
    assert_eq!(1, accounts.len());
    let credentials = credentials_repo::fetch_all(t.db())
        .await
        .expect("to fetch new credential");
    assert_eq!(1, credentials.len());

    assert_eq!(201, response.status().as_u16());
    let redactor = Redactor::default().redact(accounts[0].id, "a1");
    assert_json_snapshot!(redactor.response_json(&mut response).await.unwrap(), {
        ".data.id" => "[nonce]",
        ".data.attributes.token" => "[session_token]",
        ".data.attributes.expires_at" => "[datetime]",
    });
}
