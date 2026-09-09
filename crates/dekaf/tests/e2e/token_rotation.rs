use super::DekafTestEnv;
use super::raw_kafka::TestKafkaClient;
use std::time::Duration;

const FIXTURE: &str = include_str!("fixtures/token_rotation.flow.yaml");

const FIRST_TOKEN: &str = "t0ken-before-rotation";
const SECOND_TOKEN: &str = "t0ken-after-rotation";

/// How long to wait for a rotated password to take effect. A session which is
/// refused with the current password revokes the task's authorization, so this
/// is bounded by `TaskDekafAuth`'s 20-second cool-off rather than by
/// `--spec-ttl` -- but publication itself has to finish first.
const ROTATION_TIMEOUT: Duration = Duration::from_secs(120);

/// A Dekaf password which lives in a `secrets` stanza rotates: the new value
/// is accepted, and the old one stops being.
///
/// Dekaf resolves its endpoint configuration when the task's build ID changes,
/// so a rotation is a `flowctl secret set` followed by a publication. This is
/// deliberate -- a rotated secret value changes neither `config_json` nor
/// `secrets`, so there's nothing else to key on, and established sessions are
/// never exposed to a transient failure of config-encryption.
#[tokio::test]
async fn test_dekaf_password_rotates_with_its_secret() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup_with_secrets(
        "token_rotation",
        FIXTURE,
        &[("dekaf-token", FIRST_TOKEN)],
    )
    .await?;

    let materialization = env.materialization_name().unwrap().to_string();
    let info = env.connection_info().await?;

    // The password the secret supplies is the one which authenticates: it
    // appears nowhere in the published specification.
    let mut client = TestKafkaClient::connect(&info.broker, &materialization, FIRST_TOKEN).await?;
    let metadata = client.metadata(&[]).await?;

    let topic_names: Vec<_> = metadata
        .topics
        .iter()
        .filter_map(|t| t.name.as_ref().map(|n| n.as_str()))
        .collect();
    insta::assert_debug_snapshot!(topic_names, @r###"
    [
        "test_topic",
    ]
    "###);

    assert!(
        TestKafkaClient::connect(&info.broker, &materialization, SECOND_TOKEN)
            .await
            .is_err(),
        "the rotated-to password must not be accepted before it is set",
    );

    env.set_secret("dekaf-token", SECOND_TOKEN).await?;
    env.republish_materialization("rotated").await?;

    // Poll until the new password is accepted. Each refused attempt asks the
    // task to re-fetch, so convergence is the cool-off, not the whole TTL.
    let deadline = tokio::time::Instant::now() + ROTATION_TIMEOUT;
    loop {
        match TestKafkaClient::connect(&info.broker, &materialization, SECOND_TOKEN).await {
            Ok(mut client) => {
                // The rotated session is fully usable, not merely authenticated.
                let metadata = client.metadata(&[]).await?;
                assert_eq!(metadata.topics.len(), 1);
                break;
            }
            Err(err) if tokio::time::Instant::now() < deadline => {
                tracing::debug!(?err, "rotated password not yet accepted");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(err) => {
                anyhow::bail!(
                    "rotated password was not accepted within {ROTATION_TIMEOUT:?}: {err:#}"
                )
            }
        }
    }

    assert!(
        TestKafkaClient::connect(&info.broker, &materialization, FIRST_TOKEN)
            .await
            .is_err(),
        "the rotated-from password must no longer be accepted",
    );

    Ok(())
}
