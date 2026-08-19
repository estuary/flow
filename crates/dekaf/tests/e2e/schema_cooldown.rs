use super::DekafTestEnv;
use crate::raw_kafka::{TestKafkaClient, decode_fetch_records, fetch_partition_error};
use kafka_protocol::ResponseError;
use kafka_protocol::records::Record;
use serde_json::json;
use std::time::Duration;

const FIXTURE: &str = include_str!("fixtures/schema_cooldown.flow.yaml");

// The task_manager caches the MaterializationSpec and refreshes every `spec_ttl` (2m by default).
// We may need to wait this long after updating the schema for it to get picked up for use in a
// fetch.
const SCHEMA_PROPAGATION_TIMEOUT: Duration = Duration::from_secs(150);

fn widened_value_read_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "properties": {"id": {"type": "string"}, "value": {"type": ["integer", "string"]}},
        "required": ["id"],
    })
}

/// A partition whose documents fail Avro schema validation reports
/// LeaderNotAvailable instead of erroring the whole connection, and a
/// sibling partition on the same connection is unaffected. Once the
/// collection's schema catches up, the partition recovers without a
/// reconnect, and the previously-unreadable document is served.
#[tokio::test]
async fn test_schema_error_cooldown_isolates_partition_and_recovers() -> anyhow::Result<()> {
    super::init_tracing();

    let env = DekafTestEnv::setup("schema_cooldown", FIXTURE).await?;
    let collection_a = format!("{}/test_data_a", env.namespace);

    // Write a valid document to both topics.
    env.inject_documents("data_a", [json!({"id": "1", "value": 1})])
        .await?;
    env.inject_documents("data_b", [json!({"id": "1", "value": 1})])
        .await?;

    let info = env.connection_info().await?;
    let token = env.dekaf_token()?;
    let mut client = TestKafkaClient::connect(&info.broker, &info.username, &token).await?;

    let mut records_a: Vec<Record> = vec![];
    let mut records_b: Vec<Record> = vec![];

    // As a baseline check that everything is working by reading these documents back.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let resp = client
            .fetch_multi(&[("topic_a", 0, 0), ("topic_b", 0, 0)])
            .await?;

        assert!(fetch_partition_error(&resp, "topic_a", 0) == Some(0));
        records_a.extend(decode_fetch_records(&resp, "topic_a", 0)?);

        assert!(fetch_partition_error(&resp, "topic_b", 0) == Some(0));
        records_b.extend(decode_fetch_records(&resp, "topic_b", 0)?);

        if records_a.iter().filter(|r| !r.control).count() == 1
            && records_b.iter().filter(|r| !r.control).count() == 1
        {
            break;
        }
        if std::time::Instant::now() > deadline {
            anyhow::bail!("baseline fetch never succeeded for both topics");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Write an document that does not validate to the schema of topic_a, topic_b gets another
    // valid document.
    env.inject_documents("data_a", [json!({"id": "2", "value": "a string"})])
        .await?;
    env.inject_documents("data_b", [json!({"id": "2", "value": 2})])
        .await?;

    // Now topic_a should begin returning LeaderNotAvailable, while topic_b is unaffected.
    let leader_not_available = ResponseError::LeaderNotAvailable.code();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let resp = client
            .fetch_multi(&[
                ("topic_a", 0, records_a.iter().last().unwrap().offset + 1),
                ("topic_b", 0, records_b.iter().last().unwrap().offset + 1),
            ])
            .await?;

        // It is possible that the new document with the schema error hasn't yet landed, so we
        // still receive an success code.
        let err_a = fetch_partition_error(&resp, "topic_a", 0);
        anyhow::ensure!(
            err_a == Some(0) || err_a == Some(leader_not_available),
            "topic_a should be no error or LeaderNotAvailable, received {err_a:?}"
        );

        assert!(fetch_partition_error(&resp, "topic_b", 0) == Some(0));
        records_b.extend(decode_fetch_records(&resp, "topic_b", 0)?);

        if err_a == Some(leader_not_available)
            && records_b.iter().filter(|r| !r.control).count() == 2
        {
            break;
        }

        if std::time::Instant::now() > deadline {
            anyhow::bail!(
                "topic_a never entered schema cooldown after writing an invalid document"
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Widen topic_a readSchema to accept the string too, simulating
    // an inferred schema catching up with the data actually being written.
    env.set_collection_read_schema(&collection_a, widened_value_read_schema())
        .await?;

    // Once the schema propagates, topic_a should recover and
    // serve the previously-missing document; topic_b remains fine throughout.
    let deadline = std::time::Instant::now() + SCHEMA_PROPAGATION_TIMEOUT;
    loop {
        let resp = client
            .fetch_multi(&[
                ("topic_a", 0, records_a.iter().last().unwrap().offset + 1),
                ("topic_b", 0, records_b.iter().last().unwrap().offset + 1),
            ])
            .await?;

        if fetch_partition_error(&resp, "topic_a", 0) == Some(0) {
            let records = decode_fetch_records(&resp, "topic_a", 0)?;
            records_a.extend(records);
        }

        assert!(fetch_partition_error(&resp, "topic_b", 0) == Some(0));
        records_b.extend(decode_fetch_records(&resp, "topic_b", 0)?);

        if records_a.iter().filter(|r| !r.control).count() == 2 {
            break;
        }
        if std::time::Instant::now() > deadline {
            anyhow::bail!("topic_a never recovered its missing document after the schema update");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    Ok(())
}
