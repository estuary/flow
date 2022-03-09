use serde_json::Value as JsonValue;

use control::models::sessions::NewSession;
use control::repo::accounts as accounts_repo;
use control::repo::credentials as credentials_repo;

use crate::support::context::spawn_app;
use crate::support::redactor::Redactor;
use crate::support::{factory, test_context};

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

#[tokio::test]
async fn missing_authentication_test() {
    // Arrange
    let t = test_context!();
    let account = factory::BatmanAccount.create(t.db()).await;

    // Act
    let mut response = t.get(&format!("/accounts/{}", account.id)).await;

    // Assert
    assert_eq!(response.status().as_u16(), 401);
    let redactor = Redactor::default();
    assert_json_snapshot!(redactor.response_json(&mut response).await.unwrap());
}

#[tokio::test]
async fn full_authentication_test() {
    // A full end-to-end login + authenticated request test. This verifies that
    // sessions can be created and that our middleware will successfully
    // authenticate good requests.
    //
    // We're going to boot an http server for this one, since it's much easier
    // to manipulate Reqwest's Response body than directly with Axum's Response.

    // Arrange
    let t = test_context!();
    let account = factory::BatmanAccount.create(t.db()).await;
    let address = spawn_app(t.db().clone()).await.expect("app to boot");
    let client = reqwest::Client::new();
    let url = |path: &str| format!("http://{}{}", address, path);

    // Act 1: Login
    let login_response = client
        .post(url("/sessions/local"))
        .json(&serde_json::json!({"auth_token": &account.name}))
        .send()
        .await
        .expect("login request to succeed");

    // Assert 1: Login
    assert_eq!(201, login_response.status().as_u16());
    let body = login_response.text().await.expect("a response body");
    // We're going to need this token in the next step, and we don't want it redacted.
    let unredacted_value = serde_json::from_str::<JsonValue>(&body).expect("valid json");
    let token_value = &unredacted_value["data"]["attributes"]["token"];
    let token = serde_json::from_value::<String>(token_value.to_owned()).expect("a string token");
    // Now redact things like normal.
    let redactor = Redactor::default().redact(account.id, "a1");
    assert_json_snapshot!(
        serde_json::from_str::<JsonValue>(&redactor.apply(&body))
            .expect("valid redacted json"),
    {
        ".data.id" => "[nonce]",
        ".data.attributes.token" => "[session_token]",
        ".data.attributes.expires_at" => "[datetime]",

    });

    // Act 2: Poorly Authenticated Request
    let bad_response = client
        .get(url("/connectors"))
        // Use the wrong username
        .basic_auth("foobar", Some(&token))
        .send()
        .await
        .expect("authenticated request to succeed");

    // Assert 2: Poorly Authenticated
    assert_eq!(401, bad_response.status().as_u16());

    // Act 3: Authenticated Request
    let authenticated_response = client
        .get(url("/connectors"))
        .basic_auth(account.id, Some(&token))
        .send()
        .await
        .expect("authenticated request to succeed");

    // Assert 3: Authenticated
    assert_eq!(200, authenticated_response.status().as_u16());
}
