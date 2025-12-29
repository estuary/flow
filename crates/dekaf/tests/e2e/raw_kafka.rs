use dekaf::{KafkaApiClient, KafkaClientAuth};
use kafka_protocol::{messages, protocol::StrBytes};

/// Protocol versions to use for test requests.
mod protocol_versions {
    pub const FETCH: i16 = 12;
    pub const LIST_OFFSETS: i16 = 7;
    pub const OFFSET_FOR_LEADER_EPOCH: i16 = 2;
    pub const METADATA: i16 = 9;
}

fn topic_name(s: &str) -> messages::TopicName {
    messages::TopicName(StrBytes::from_string(s.to_string()))
}

pub struct TestKafkaClient {
    inner: KafkaApiClient,
}

impl TestKafkaClient {
    /// Connect to a Kafka broker with SASL PLAIN authentication.
    ///
    /// For Dekaf, `username` is the materialization name and `password` is the token.
    pub async fn connect(broker: &str, username: &str, password: &str) -> anyhow::Result<Self> {
        let auth = KafkaClientAuth::plain(username, password);
        let inner = KafkaApiClient::connect(&[format!("tcp://{broker}")], auth).await?;
        Ok(Self { inner })
    }

    /// Fetch with explicit `current_leader_epoch` to test epoch validation.
    ///
    /// When `leader_epoch` is less than the server's current epoch, Dekaf returns
    /// `FENCED_LEADER_EPOCH`. When greater, it returns `UNKNOWN_LEADER_EPOCH`.
    pub async fn fetch_with_epoch(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
        leader_epoch: i32,
    ) -> anyhow::Result<messages::FetchResponse> {
        let req = messages::FetchRequest::default()
            .with_max_wait_ms(1000)
            .with_min_bytes(1)
            .with_max_bytes(1024 * 1024)
            .with_topics(vec![
                messages::fetch_request::FetchTopic::default()
                    .with_topic(topic_name(topic))
                    .with_partitions(vec![
                        messages::fetch_request::FetchPartition::default()
                            .with_partition(partition)
                            .with_fetch_offset(offset)
                            .with_current_leader_epoch(leader_epoch)
                            .with_partition_max_bytes(1024 * 1024),
                    ]),
            ]);

        let header = messages::RequestHeader::default()
            .with_request_api_key(messages::ApiKey::Fetch as i16)
            .with_request_api_version(protocol_versions::FETCH);

        self.inner.send_request(req, Some(header)).await
    }

    /// ListOffsets with explicit `current_leader_epoch`.
    ///
    /// - `timestamp = -2`: earliest offset
    /// - `timestamp = -1`: latest offset
    pub async fn list_offsets_with_epoch(
        &mut self,
        topic: &str,
        partition: i32,
        timestamp: i64,
        leader_epoch: i32,
    ) -> anyhow::Result<messages::ListOffsetsResponse> {
        let req = messages::ListOffsetsRequest::default().with_topics(vec![
            messages::list_offsets_request::ListOffsetsTopic::default()
                .with_name(topic_name(topic))
                .with_partitions(vec![
                    messages::list_offsets_request::ListOffsetsPartition::default()
                        .with_partition_index(partition)
                        .with_timestamp(timestamp)
                        .with_current_leader_epoch(leader_epoch),
                ]),
        ]);

        let header = messages::RequestHeader::default()
            .with_request_api_key(messages::ApiKey::ListOffsets as i16)
            .with_request_api_version(protocol_versions::LIST_OFFSETS);

        self.inner.send_request(req, Some(header)).await
    }

    pub async fn offset_for_leader_epoch(
        &mut self,
        topic: &str,
        partition: i32,
        leader_epoch: i32,
    ) -> anyhow::Result<messages::OffsetForLeaderEpochResponse> {
        let req = messages::OffsetForLeaderEpochRequest::default().with_topics(vec![
            messages::offset_for_leader_epoch_request::OffsetForLeaderTopic::default()
                .with_topic(topic_name(topic))
                .with_partitions(vec![
                    messages::offset_for_leader_epoch_request::OffsetForLeaderPartition::default()
                        .with_partition(partition)
                        .with_leader_epoch(leader_epoch),
                ]),
        ]);

        let header = messages::RequestHeader::default()
            .with_request_api_key(messages::ApiKey::OffsetForLeaderEpoch as i16)
            .with_request_api_version(protocol_versions::OFFSET_FOR_LEADER_EPOCH);

        self.inner.send_request(req, Some(header)).await
    }

    pub async fn metadata(
        &mut self,
        topics: &[&str],
    ) -> anyhow::Result<messages::MetadataResponse> {
        let req = messages::MetadataRequest::default().with_topics(Some(
            topics
                .iter()
                .map(|t| {
                    messages::metadata_request::MetadataRequestTopic::default()
                        .with_name(Some(topic_name(t)))
                })
                .collect(),
        ));

        let header = messages::RequestHeader::default()
            .with_request_api_key(messages::ApiKey::Metadata as i16)
            .with_request_api_version(protocol_versions::METADATA);

        self.inner.send_request(req, Some(header)).await
    }
}

/// Extract the error code from a FetchResponse for a specific topic/partition.
pub fn fetch_partition_error(
    resp: &messages::FetchResponse,
    topic: &str,
    partition: i32,
) -> Option<i16> {
    resp.responses
        .iter()
        .find(|t| t.topic.as_str() == topic)
        .and_then(|t| t.partitions.iter().find(|p| p.partition_index == partition))
        .map(|p| p.error_code)
}

/// Extract the leader epoch from a FetchResponse's current_leader field.
pub fn fetch_current_leader_epoch(
    resp: &messages::FetchResponse,
    topic: &str,
    partition: i32,
) -> Option<i32> {
    resp.responses
        .iter()
        .find(|t| t.topic.as_str() == topic)
        .and_then(|t| t.partitions.iter().find(|p| p.partition_index == partition))
        .map(|p| p.current_leader.leader_epoch)
}

/// Extract the error code from a ListOffsetsResponse for a specific topic/partition.
pub fn list_offsets_partition_error(
    resp: &messages::ListOffsetsResponse,
    topic: &str,
    partition: i32,
) -> Option<i16> {
    resp.topics
        .iter()
        .find(|t| t.name.as_str() == topic)
        .and_then(|t| t.partitions.iter().find(|p| p.partition_index == partition))
        .map(|p| p.error_code)
}

/// Extract the leader epoch from a MetadataResponse for a specific topic/partition.
pub fn metadata_leader_epoch(
    resp: &messages::MetadataResponse,
    topic: &str,
    partition: i32,
) -> Option<i32> {
    resp.topics
        .iter()
        .find(|t| t.name.as_ref().map(|n| n.as_str()) == Some(topic))
        .and_then(|t| t.partitions.iter().find(|p| p.partition_index == partition))
        .map(|p| p.leader_epoch)
}
#[derive(Debug, Clone)]
pub struct OffsetForEpochResult {
    pub error_code: i16,
    pub leader_epoch: i32,
    pub end_offset: i64,
}

/// Extract OffsetForLeaderEpoch result for a specific topic/partition.
pub fn offset_for_epoch_result(
    resp: &messages::OffsetForLeaderEpochResponse,
    topic: &str,
    partition: i32,
) -> Option<OffsetForEpochResult> {
    resp.topics
        .iter()
        .find(|t| t.topic.as_str() == topic)
        .and_then(|t| t.partitions.iter().find(|p| p.partition == partition))
        .map(|p| OffsetForEpochResult {
            error_code: p.error_code,
            leader_epoch: p.leader_epoch,
            end_offset: p.end_offset,
        })
}
