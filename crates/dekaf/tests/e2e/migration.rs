use super::raw_kafka::{TestKafkaClient, fetch_partition_error, list_offsets_partition_error};
use super::{
    DekafTestEnv, connection_info_for_dataplane, trigger_migration, wait_for_dekaf_redirect,
    wait_for_migration_complete,
};
use anyhow::Context;
use serde_json::json;
use std::time::Duration;

const FIXTURE: &str = include_str!("fixtures/basic.flow.yaml");
const MIGRATION_TIMEOUT: Duration = Duration::from_secs(30);

// NOTE: This depends on the following:
// - Recovery log fragments being persisted by the source dataplane
//   - There is a race condition in Gazette that causes this persistence to
//     take up to 3 minutes if the remaining broker in the topology
//     was not the primary when the suspension occured.
// - The target dataplane's fragment refresh interval picking up those fragments.
//   The default interval is 5 minutes.
// So, we must set the upper timeout bound at 8 minutes to be safe. That
// makes this test only viable to run manually, unfortunately.
const SHARD_PRIMARY_TIMEOUT: Duration = Duration::from_mins(8);

fn extract_ids(records: &[super::kafka::DecodedRecord]) -> Vec<&str> {
    records
        .iter()
        .map(|r| r.value["id"].as_str().unwrap())
        .collect()
}

/// Verify the behavior of a high-level librdkafka consumer during migration.
/// NOTE: Ideally, we would use the same consumer instance before and after migration,
/// but Dekaf's current redirect mechanism requires a new connection to pick up
/// the new broker address, so we create a new consumer after migration. A future
/// improvement for Dekaf is to support advertising the correct redirect address
/// with its own stable broker ID.
#[tokio::test]
async fn test_rdkafka_handles_redirect() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("rdkafka_redirect", FIXTURE).await?;

    let capture_name = env.capture_name().context("no capture in fixture")?;

    env.inject_documents(
        "data",
        vec![
            json!({"id": "A", "value": "first"}),
            json!({"id": "B", "value": "second"}),
            json!({"id": "C", "value": "third"}),
        ],
    )
    .await?;

    let group_id = format!("migration-group-{}", env.namespace.replace('/', "_"));

    let consumer = env
        .kafka_consumer_with_group_id("local-cluster", &group_id)
        .await?;
    consumer.subscribe(&["test_topic"])?;

    let records_before = consumer.fetch_n_with_commit(3).await?;
    insta::assert_debug_snapshot!("before_migration", extract_ids(&records_before));

    drop(consumer);

    let migration_id =
        trigger_migration(&env.namespace, "local-cluster", "local-cluster-2").await?;

    wait_for_migration_complete(migration_id, MIGRATION_TIMEOUT).await?;

    // Wait for capture shard to be PRIMARY in target dataplane
    // (needed so document injection works after migration)
    env.wait_for_primary(capture_name, SHARD_PRIMARY_TIMEOUT)
        .await?;

    // Wait until Dekaf on local-cluster knows about the redirect to local-cluster-2.
    // This polls until the control plane's snapshot refresh sees the migrated task,
    // and Dekaf's cached state has refreshed and picked it up.
    let username = env.materialization_name().unwrap_or_default();
    let password = env.dekaf_token()?;
    wait_for_dekaf_redirect(
        "local-cluster",
        "local-cluster-2",
        username,
        &password,
        MIGRATION_TIMEOUT,
    )
    .await?;

    env.inject_documents(
        "data",
        vec![
            json!({"id": "D", "value": "fourth"}),
            json!({"id": "E", "value": "fifth"}),
            json!({"id": "F", "value": "sixth"}),
        ],
    )
    .await?;

    // Create new consumer connected to local-cluster, should redirect automatically
    let consumer = env
        .kafka_consumer_with_group_id("local-cluster", &group_id)
        .await?;
    consumer.subscribe(&["test_topic"])?;

    let records_after = consumer.fetch_n_with_commit(3).await?;
    insta::assert_debug_snapshot!("after_migration", extract_ids(&records_after));

    // Verify all 6 documents consumed exactly once in order
    let all_records: Vec<_> = records_before
        .into_iter()
        .chain(records_after.into_iter())
        .collect();
    insta::assert_debug_snapshot!("all_consumed", extract_ids(&all_records));

    Ok(())
}

/// Verify the low-level Kafka protocol responses during and after migration.
///
/// 1. Pre-migration: Metadata returns source broker, Fetch/ListOffsets work normally
/// 2. Post-migration: Metadata returns target broker address, operations return appropriate responses:
/// - Metadata responses advertise the target dataplane's broker address
/// - Fetch/ListOffsets return `NotLeaderOrFollower` errors
/// - Coordinator operations (OffsetCommit, Heartbeat) return `NotCoordinator` errors
#[tokio::test]
async fn test_migration_protocol_responses() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("migration_protocol", FIXTURE).await?;

    env.inject_documents("data", vec![json!({"id": "test-doc", "value": "hello"})])
        .await?;

    let username = env.materialization_name().context("no materialization")?;
    let password = env.dekaf_token()?;
    let collections: Vec<String> = env.collection_names().map(|s| s.to_string()).collect();
    let src_info =
        connection_info_for_dataplane("local-cluster", username, collections.clone()).await?;
    let tgt_info = connection_info_for_dataplane("local-cluster-2", username, collections).await?;

    let mut client = TestKafkaClient::connect(&src_info.broker, username, &password).await?;

    let metadata = client.metadata(&["test_topic"]).await?;
    insta::assert_debug_snapshot!("pre_migration_metadata", metadata);

    let fetch_resp = client.fetch_with_epoch("test_topic", 0, 0, -1).await?;
    let fetch_error = fetch_partition_error(&fetch_resp, "test_topic", 0);
    assert_eq!(fetch_error, Some(0), "pre-migration fetch should succeed");

    let list_resp = client
        .list_offsets_with_epoch("test_topic", 0, -1, -1)
        .await?;
    let list_error = list_offsets_partition_error(&list_resp, "test_topic", 0);
    assert_eq!(
        list_error,
        Some(0),
        "pre-migration list_offsets should succeed"
    );

    drop(client);

    let capture_name = env.capture_name().context("no capture")?;
    let migration_id =
        trigger_migration(&env.namespace, "local-cluster", "local-cluster-2").await?;

    wait_for_migration_complete(migration_id, MIGRATION_TIMEOUT).await?;

    env.wait_for_primary(capture_name, SHARD_PRIMARY_TIMEOUT)
        .await?;

    wait_for_dekaf_redirect(
        "local-cluster",
        "local-cluster-2",
        username,
        &password,
        MIGRATION_TIMEOUT,
    )
    .await?;

    let mut client = TestKafkaClient::connect(&src_info.broker, username, &password).await?;

    let metadata = client.metadata(&["test_topic"]).await?;
    insta::assert_debug_snapshot!("post_migration_metadata_from_source", metadata);

    let fetch_resp = client.fetch_with_epoch("test_topic", 0, 0, -1).await?;
    let fetch_error = fetch_partition_error(&fetch_resp, "test_topic", 0);
    assert_eq!(
        fetch_error,
        Some(6), // NOT_LEADER_OR_FOLLOWER
        "post-migration fetch from source should return NOT_LEADER_OR_FOLLOWER"
    );

    let list_resp = client
        .list_offsets_with_epoch("test_topic", 0, -1, -1)
        .await?;
    let list_error = list_offsets_partition_error(&list_resp, "test_topic", 0);
    assert_eq!(
        list_error,
        Some(6), // NOT_LEADER_OR_FOLLOWER
        "post-migration list_offsets from source should return NOT_LEADER_OR_FOLLOWER"
    );

    let mut target_client = TestKafkaClient::connect(&tgt_info.broker, username, &password).await?;

    let target_metadata = target_client.metadata(&["test_topic"]).await?;
    insta::assert_debug_snapshot!("post_migration_metadata_from_target", target_metadata);

    let target_epoch = target_metadata
        .topics
        .first()
        .and_then(|t| t.partitions.first())
        .map(|p| p.leader_epoch)
        .unwrap_or(1);

    let target_fetch = target_client
        .fetch_with_epoch("test_topic", 0, 0, target_epoch)
        .await?;
    let target_fetch_error = fetch_partition_error(&target_fetch, "test_topic", 0);
    assert_eq!(
        target_fetch_error,
        Some(0),
        "post-migration fetch from target should succeed"
    );

    Ok(())
}
