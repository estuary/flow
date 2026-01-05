//! Regression tests for task name authentication.
//!
//! A bug caused task names containing periods (e.g. `ABCD.com/tenant/task`)
//! to be corrupted: `decode_safe_name` replaced `.` with `%`, so `ABCD.com`
//! became `ABCD%com`, and `%co` is invalid hex.

use super::raw_kafka::TestKafkaClient;
use super::DekafTestEnv;

const FIXTURE: &str = r#"
collections:
    test_data:
        schema:
            type: object
            properties:
                id: { type: string }
            required: [id]
        key: [/id]

captures:
    source_ingest:
        endpoint:
            connector:
                image: ghcr.io/estuary/source-http-ingest:dev
                config:
                    paths: ["/data"]
        bindings:
            - resource: { path: "/data", stream: "/data" }
              target: test_data

materializations:
    my.dekaf.task:
        endpoint:
            dekaf:
                variant: testing
                config:
                    token: "test-token"
                    strict_topic_names: false
        bindings:
            - source: test_data
              resource: { topic_name: test_topic }
              fields:
                  recommended: true
                  exclude: [flow_published_at]
"#;

/// Regression test: task names containing periods must authenticate successfully.
#[tokio::test]
async fn test_auth_task_name_with_period() -> anyhow::Result<()> {
    super::init_tracing();

    // Harness rewrites `my.dekaf.task` to `test/dekaf/.../my.dekaf.task`
    let env = DekafTestEnv::setup("auth_period", FIXTURE).await?;

    assert!(
        env.materialization.contains('.'),
        "expected period in task name: {}",
        env.materialization
    );

    let broker = std::env::var("DEKAF_BROKER").unwrap_or("localhost:9092".into());
    let mut client =
        TestKafkaClient::connect(&broker, &env.materialization, env.dekaf_token()?).await?;

    let metadata = client.metadata(&["test_topic"]).await?;
    assert!(!metadata.topics.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_auth_wrong_password() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("auth_wrong_pw", FIXTURE).await?;
    let broker = std::env::var("DEKAF_BROKER").unwrap_or("localhost:9092".into());

    let result = TestKafkaClient::connect(&broker, &env.materialization, "wrong-token").await;
    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
async fn test_auth_invalid_task_names() -> anyhow::Result<()> {
    super::init_tracing();

    let broker = std::env::var("DEKAF_BROKER").unwrap_or("localhost:9092".into());

    let invalid_names = [
        ("nonexistent/tenant/task", "non-existent task"),
        ("", "empty"),
        ("/leading/slash", "leading slash"),
        ("trailing/slash/", "trailing slash"),
        ("double//slash", "consecutive slashes"),
        ("no-slash", "single token"),
        ("has space/task", "contains space"),
    ];

    for (name, desc) in invalid_names {
        let result = TestKafkaClient::connect(&broker, name, "any-token").await;
        assert!(result.is_err(), "expected auth failure for {desc}: {name:?}");
    }

    Ok(())
}
