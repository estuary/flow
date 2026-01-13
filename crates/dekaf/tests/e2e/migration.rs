use super::{trigger_migration, wait_for_dekaf_redirect, wait_for_migration_complete, DekafTestEnv};
use anyhow::Context;
use serde_json::json;
use std::time::Duration;

const FIXTURE: &str = include_str!("fixtures/basic.flow.yaml");
const MIGRATION_TIMEOUT: Duration = Duration::from_secs(120);

fn extract_ids(records: &[super::kafka::DecodedRecord]) -> Vec<&str> {
    records
        .iter()
        .map(|r| r.value["id"].as_str().unwrap())
        .collect()
}

/// Verify that a standard Kafka consumer transparently handles a cross-dataplane
/// migration without data loss or manual intervention.
///
/// A single consumer instance stays alive across the migration. After migration,
/// rdkafka should automatically handle the redirect (via NotCoordinator →
/// FindCoordinator → reconnect) without any application-level intervention.
#[tokio::test]
async fn test_consumer_handles_migration_seamlessly() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("migration_seamless", FIXTURE).await?;

    // Inject documents A, B, C
    tracing::info!("Injecting initial documents A, B, C");
    env.inject_documents(
        "data",
        vec![
            json!({"id": "A", "value": "first"}),
            json!({"id": "B", "value": "second"}),
            json!({"id": "C", "value": "third"}),
        ],
    )
    .await?;

    // Create consumer connected to local-cluster, consume and commit A, B, C
    let consumer = env.kafka_consumer_for_dataplane("local-cluster").await?;
    consumer.subscribe(&["test_topic"])?;

    tracing::info!("Consuming and committing A, B, C");
    let records_before = consumer.fetch_n_with_commit(3).await?;
    insta::assert_debug_snapshot!("before_migration", extract_ids(&records_before));

    // Trigger migration with immediate cordon (cordon_at = now())
    tracing::info!("Triggering migration to local-cluster-2");
    let migration_id = trigger_migration(
        &env.namespace,
        "local-cluster",
        "local-cluster-2",
        true, // immediate
    )
    .await?;

    // Wait for migration to complete (polls DB until active = false)
    wait_for_migration_complete(migration_id, MIGRATION_TIMEOUT).await?;

    // Wait for capture shard to be PRIMARY in target dataplane
    // (needed so document injection works after migration)
    let capture_name = env.capture_name().context("no capture in fixture")?;
    env.wait_for_primary(capture_name).await?;

    // Wait until Dekaf on local-cluster knows about the redirect to local-cluster-2.
    // This polls until the control plane's background snapshot refresh (every 30s in
    // local testing) propagates the migration to Dekaf's authorization.
    let username = env.materialization_name().unwrap_or_default();
    let password = env.dekaf_token()?;
    wait_for_dekaf_redirect("local-cluster", "local-cluster-2", username, &password, MIGRATION_TIMEOUT).await?;
    tracing::info!("Migration completed, Dekaf redirect ready");

    // Inject documents D, E, F
    tracing::info!("Injecting documents D, E, F");
    env.inject_documents(
        "data",
        vec![
            json!({"id": "D", "value": "fourth"}),
            json!({"id": "E", "value": "fifth"}),
            json!({"id": "F", "value": "sixth"}),
        ],
    )
    .await?;

    // Same consumer should automatically reconnect and consume D, E, F
    // rdkafka handles NotCoordinator → FindCoordinator → reconnect internally
    tracing::info!("Consuming D, E, F (consumer should auto-reconnect)");
    let records_after = consumer.fetch_n_with_commit(3).await?;
    insta::assert_debug_snapshot!("after_migration", extract_ids(&records_after));

    // Verify all 6 documents consumed exactly once in order
    let all_records: Vec<_> = records_before
        .into_iter()
        .chain(records_after.into_iter())
        .collect();
    insta::assert_debug_snapshot!("all_consumed", extract_ids(&all_records));

    tracing::info!("Consumer handled migration seamlessly");
    Ok(())
}

/// Verify that committed offsets survive migration when a consumer reconnects
/// directly to the target dataplane.
///
/// This tests the low-level offset preservation mechanism: offsets are stored
/// in the upstream Kafka broker (shared by both Dekaf instances), so a new
/// consumer with the same group ID should resume from the committed position.
#[tokio::test]
async fn test_offset_preservation_across_migration() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("migration_offsets", FIXTURE).await?;
    let group_id = format!("offset-test-{}", uuid::Uuid::new_v4());

    // Inject 10 documents
    tracing::info!("Injecting 10 documents");
    env.inject_documents(
        "data",
        (1..=10).map(|i| json!({"id": format!("doc-{i}"), "value": i})),
    )
    .await?;

    // Consumer 1: consume first 5, commit offset, then stop
    tracing::info!(%group_id, "Consumer 1: consuming first 5 documents");
    let consumer1 = env
        .kafka_consumer_with_group_id("local-cluster", &group_id)
        .await?;
    consumer1.subscribe(&["test_topic"])?;

    let records1 = consumer1.fetch_n_with_commit(5).await?;
    insta::assert_debug_snapshot!("consumer1_docs", extract_ids(&records1));

    let last_offset = records1.last().unwrap().offset;
    tracing::info!(last_offset, "Consumer 1: committed offset after 5 docs");

    drop(consumer1);

    // Trigger migration with immediate cordon (cordon_at = now())
    tracing::info!("Triggering migration to local-cluster-2");
    let migration_id = trigger_migration(
        &env.namespace,
        "local-cluster",
        "local-cluster-2",
        true, // immediate
    )
    .await?;

    // Wait for migration to complete (polls DB until active = false)
    wait_for_migration_complete(migration_id, MIGRATION_TIMEOUT).await?;

    // Wait for capture shard to be PRIMARY in target dataplane
    let capture_name = env.capture_name().context("no capture in fixture")?;
    env.wait_for_primary(capture_name).await?;

    // Wait until Dekaf on local-cluster knows about the redirect to local-cluster-2.
    // This polls until the control plane's background snapshot refresh (every 30s in
    // local testing) propagates the migration to Dekaf's authorization.
    let username = env.materialization_name().unwrap_or_default();
    let password = env.dekaf_token()?;
    wait_for_dekaf_redirect("local-cluster", "local-cluster-2", username, &password, MIGRATION_TIMEOUT).await?;
    tracing::info!("Migration completed, Dekaf redirect ready");

    // Consumer 2: same group ID, connect directly to target dataplane
    tracing::info!(%group_id, "Consumer 2: connecting to local-cluster-2");
    let consumer2 = env
        .kafka_consumer_with_group_id("local-cluster-2", &group_id)
        .await?;
    consumer2.subscribe(&["test_topic"])?;

    // Should resume from doc-6, not doc-1
    let records2 = consumer2.fetch_n_with_commit(5).await?;
    insta::assert_debug_snapshot!("consumer2_docs", extract_ids(&records2));

    tracing::info!("Offset preservation verified across migration");
    Ok(())
}
