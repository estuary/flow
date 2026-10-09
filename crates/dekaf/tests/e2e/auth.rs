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

const BASIC_FIXTURE: &str = include_str!("fixtures/basic.flow.yaml");

/// Every rejected login must fail identically, so that a client can't use the
/// error to learn which task names exist. This covers a wrong password, a name
/// that matches no task, and a task that isn't a materialization.
#[tokio::test]
async fn test_auth_failures_are_indistinguishable() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("auth_uniform", BASIC_FIXTURE).await?;
    let materialization = env.materialization_name().unwrap();
    let capture = env.capture_name().unwrap();
    let unknown = format!("{}/no-such-task", env.namespace);
    let info = env.connection_info().await?;

    let cases = [
        ("wrong password", materialization, "not-the-token"),
        ("unknown task", unknown.as_str(), "not-the-token"),
        ("not a materialization", capture, "not-the-token"),
    ];

    let http = reqwest::Client::new();
    let mut outcomes = Vec::new();

    for (case, username, password) in cases {
        let sasl = match TestKafkaClient::connect(&info.broker, username, password).await {
            Ok(_) => "authenticated".to_string(),
            Err(err) => format!("{err:#}"),
        };
        let registry = http
            .get(format!("{}/subjects", info.registry))
            .basic_auth(username, Some(password))
            .send()
            .await?;
        let registry = format!("{} {}", registry.status(), registry.text().await?);

        outcomes.push((case, sasl, registry));
    }

    insta::assert_debug_snapshot!(outcomes);

    Ok(())
}
