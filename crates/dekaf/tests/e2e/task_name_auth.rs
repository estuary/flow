use super::DekafTestEnv;
use super::raw_kafka::TestKafkaClient;

const FIXTURE: &str = include_str!("fixtures/task_name_auth.flow.yaml");

/// Regression test: task names containing periods must authenticate successfully.
#[tokio::test]
async fn test_auth_task_name_with_period() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("auth_period", FIXTURE).await?;
    let materialization = env.materialization_name().unwrap();

    assert!(
        materialization.contains('.'),
        "expected period in task name: {}",
        materialization
    );

    let token = env.dekaf_token()?;
    let mut client =
        TestKafkaClient::connect(&env.connection_info().broker, materialization, &token).await?;

    let metadata = client.metadata(&["test_topic"]).await?;
    assert!(!metadata.topics.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_auth_wrong_password() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("auth_wrong_pw", FIXTURE).await?;
    let materialization = env.materialization_name().unwrap();

    let result = TestKafkaClient::connect(
        &env.connection_info().broker,
        materialization,
        "wrong-token",
    )
    .await;
    assert!(result.is_err());

    Ok(())
}
