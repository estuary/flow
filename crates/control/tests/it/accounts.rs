use control::repo::accounts::fetch_all;

use crate::support::redactor::Redactor;
use crate::support::{factory, test_context};

#[tokio::test]
async fn index_test() {
    // Arrange
    let mut t = test_context!();
    let account = factory::BatmanAccount.create(t.db()).await;
    t.login(account.clone());

    // Act
    let mut response = t.get("/accounts").await;

    // Assert
    assert!(response.status().is_success());
    let redactor = Redactor::default().redact(account.id, "a1");
    assert_json_snapshot!(redactor.response_json(&mut response).await.unwrap(), {
        ".data.*.attributes.created_at" => "[datetime]",
        ".data.*.attributes.updated_at" => "[datetime]",
    });
}

#[tokio::test]
async fn create_test() {
    // Arrange
    let mut t = test_context!();
    let account = factory::JokerAccount.create(t.db()).await;
    t.login(account.clone());
    let input = factory::BatmanAccount.attrs();

    // Act
    let mut response = t.post("/accounts", &input).await;

    // Assert
    let accounts = fetch_all(t.db())
        .await
        .expect("to insert test data")
        .into_iter()
        .filter(|a| a.id != account.id)
        .collect::<Vec<_>>();
    assert_eq!(1, accounts.len());
    assert_eq!(201, response.status().as_u16());
    let redactor = Redactor::default().redact(accounts[0].id, "a1");
    assert_json_snapshot!(redactor.response_json(&mut response).await.unwrap(), {
        ".data.attributes.created_at" => "[datetime]",
        ".data.attributes.updated_at" => "[datetime]",
    });
}

#[tokio::test]
async fn show_test() {
    // Arrange
    let mut t = test_context!();
    let account = factory::BatmanAccount.create(t.db()).await;
    t.login(account.clone());

    // Act
    let mut response = t.get(&format!("/accounts/{}", account.id)).await;

    // Assert
    assert!(response.status().is_success());
    let redactor = Redactor::default().redact(account.id, "a1");
    assert_json_snapshot!(redactor.response_json(&mut response).await.unwrap(), {
        ".data.attributes.created_at" => "[datetime]",
        ".data.attributes.updated_at" => "[datetime]",
    });
}
