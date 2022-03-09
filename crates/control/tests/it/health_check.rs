use crate::support::test_context;

#[tokio::test]
async fn health_check_works() {
    // Arrange
    let t = test_context!();

    // Act
    let response = t.get("/health_check").await;

    // Assert
    assert!(response.status().is_success());
}
