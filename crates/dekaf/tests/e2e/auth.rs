use super::DekafTestEnv;
use super::raw_kafka::TestKafkaClient;

const PERIOD_FIXTURE: &str = include_str!("fixtures/task_name_auth.flow.yaml");

/// Regression test: task names containing periods must authenticate successfully.
#[tokio::test]
async fn test_auth_task_name_with_period() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("auth_period", PERIOD_FIXTURE).await?;
    let materialization = env.materialization_name().unwrap();

    assert!(
        materialization.contains('.'),
        "expected period in task name: {}",
        materialization
    );

    let token = env.dekaf_token()?;
    let info = env.connection_info().await?;
    let mut client = TestKafkaClient::connect(&info.broker, materialization, &token).await?;

    let metadata = client.metadata(&[]).await?;
    let mut topic_names: Vec<_> = metadata
        .topics
        .iter()
        .filter_map(|t| t.name.as_ref().map(|n| n.as_str()))
        .collect();
    topic_names.sort();
    insta::assert_debug_snapshot!(topic_names, @r###"
    [
        "test_topic",
    ]
    "###);

    Ok(())
}
