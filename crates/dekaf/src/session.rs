use super::{fetch_all_collection_names, App, Collection, Read};
use anyhow::Context;
use kafka_protocol::{
    error::ResponseError,
    indexmap::IndexMap,
    messages::{self, metadata_response::MetadataResponseTopic, RequestHeader, TopicName},
    protocol::{Builder, StrBytes},
};
use std::collections::HashMap;
use std::sync::Arc;

struct PendingRead {
    offset: i64,          // Journal offset to be completed by this PendingRead.
    last_write_head: i64, // Most-recent observed journal write head.
    handle: tokio::task::JoinHandle<anyhow::Result<(Read, bytes::Bytes)>>,
}

pub struct Session {
    app: Arc<App>,
    client: postgrest::Postgrest,
    reads: HashMap<(TopicName, i32), PendingRead>,
}

impl Session {
    pub fn new(app: Arc<App>) -> Self {
        let client = app.anon_client.clone();
        Self {
            app,
            client,
            reads: HashMap::new(),
        }
    }

    /// SASL handshake responds with supported SASL mechanisms.
    /// We support PLAIN user/password, because we expect the password to be a control-plane token.
    pub async fn sasl_handshake(
        &mut self,
        request: messages::SaslHandshakeRequest,
    ) -> anyhow::Result<messages::SaslHandshakeResponse> {
        let mut response = messages::SaslHandshakeResponse::default();
        response.mechanisms.push(StrBytes::from_static_str("PLAIN"));

        if request.mechanism.ne("PLAIN") {
            response.error_code = ResponseError::UnsupportedSaslMechanism.code();
        }
        Ok(response)
    }

    /// Parse a PLAIN user/password to extract a control-plane access token.
    pub async fn sasl_authenticate(
        &mut self,
        request: messages::SaslAuthenticateRequest,
    ) -> anyhow::Result<messages::SaslAuthenticateResponse> {
        let mut it = request
            .auth_bytes
            .split(|b| *b == 0) // SASL uses NULL to separate components.
            .map(std::str::from_utf8);

        let _authzid = it.next().context("expected SASL authzid")??;
        let authcid = it.next().context("expected SASL authcid")??;
        let password = it.next().context("expected SASL passwd")??;

        let response = match self.app.authenticate(authcid, password).await {
            Ok(client) => {
                self.client = client;

                let mut response = messages::SaslAuthenticateResponse::default();
                response.session_lifetime_ms = i64::MAX; // TODO(johnny): Access token expiry.
                response
            }
            Err(err) => messages::SaslAuthenticateResponse::builder()
                .error_code(ResponseError::SaslAuthenticationFailed.code())
                .error_message(Some(StrBytes::from_string(format!(
                    "SASL authentication error: Authentication failed: {err:#}",
                ))))
                .build()
                .unwrap(),
        };

        Ok(response)
    }

    /// Serve metadata of topics and their partitions.
    /// For efficiency, we do NOT enumerate partitions when we receive an unqualified metadata request.
    /// Otherwise, if specific "topics" (collections) are listed, we fetch and map journals into partitions.
    pub async fn metadata(
        &mut self,
        mut request: messages::MetadataRequest,
    ) -> anyhow::Result<messages::MetadataResponse> {
        let topics = if let Some(topics) = request.topics.take() {
            self.metadata_select_topics(topics).await?
        } else {
            self.metadata_all_topics().await?
        };

        // We only ever advertise a single logical broker.
        let mut brokers = kafka_protocol::indexmap::IndexMap::new();
        brokers.insert(
            messages::BrokerId(1),
            messages::metadata_response::MetadataResponseBroker::builder()
                .host(StrBytes::from_string(self.app.advertise_host.clone()))
                .port(self.app.advertise_kafka_port as i32)
                .build()
                .unwrap(),
        );

        Ok(messages::MetadataResponse::builder()
            .brokers(brokers)
            .cluster_id(Some(StrBytes::from_static_str("estuary-dekaf")))
            .controller_id(messages::BrokerId(1))
            .topics(topics)
            .build()
            .unwrap())
    }

    // Lists all read-able collections as Kafka topics. Omits partition metadata.
    async fn metadata_all_topics(
        &mut self,
    ) -> anyhow::Result<IndexMap<TopicName, MetadataResponseTopic>> {
        let collections = fetch_all_collection_names(&self.client).await?;

        tracing::debug!(collections=?ops::DebugJson(&collections), "fetched all collections");

        let topics = collections
            .into_iter()
            .map(|name| {
                (
                    TopicName(StrBytes::from_string(name)),
                    MetadataResponseTopic::builder()
                        .is_internal(false)
                        .build()
                        .unwrap(),
                )
            })
            .collect();

        Ok(topics)
    }

    // Lists partitions of specific, requested collections.
    async fn metadata_select_topics(
        &mut self,
        requests: Vec<messages::metadata_request::MetadataRequestTopic>,
    ) -> anyhow::Result<IndexMap<TopicName, MetadataResponseTopic>> {
        let client = &self.client;

        // Concurrently fetch Collection instances for all requested topics.
        let collections: anyhow::Result<Vec<(TopicName, Option<Collection>)>> =
            futures::future::try_join_all(requests.into_iter().map(|topic| async move {
                let name = topic.name.unwrap_or_default();
                let maybe_collection = Collection::new(client, name.as_str()).await?;
                Ok((name, maybe_collection))
            }))
            .await;

        let mut topics = IndexMap::new();

        for (name, maybe_collection) in collections? {
            let Some(collection) = maybe_collection else {
                topics.insert(
                    name,
                    MetadataResponseTopic::builder()
                        .error_code(ResponseError::UnknownTopicOrPartition.code())
                        .build()
                        .unwrap(),
                );
                continue;
            };

            let partitions = collection
                .partitions
                .iter()
                .enumerate()
                .map(|(index, _)| {
                    messages::metadata_response::MetadataResponsePartition::builder()
                        .partition_index(index as i32)
                        .leader_id(messages::BrokerId(1))
                        .replica_nodes(vec![messages::BrokerId(1)])
                        .isr_nodes(vec![messages::BrokerId(1)])
                        .build()
                        .unwrap()
                })
                .collect();

            topics.insert(
                name,
                MetadataResponseTopic::builder()
                    .is_internal(false)
                    .partitions(partitions)
                    .build()
                    .unwrap(),
            );
        }

        Ok(topics)
    }

    /// FindCoordinator always responds with our single logical broker.
    pub async fn find_coordinator(
        &mut self,
        request: messages::FindCoordinatorRequest,
    ) -> anyhow::Result<messages::FindCoordinatorResponse> {
        let coordinators = request
            .coordinator_keys
            .iter()
            .map(|_key| {
                messages::find_coordinator_response::Coordinator::builder()
                    .node_id(messages::BrokerId(1))
                    .host(StrBytes::from_string(self.app.advertise_host.clone()))
                    .port(self.app.advertise_kafka_port as i32)
                    .build()
                    .unwrap()
            })
            .collect();

        Ok(messages::FindCoordinatorResponse::builder()
            .node_id(messages::BrokerId(1))
            .host(StrBytes::from_string(self.app.advertise_host.clone()))
            .port(self.app.advertise_kafka_port as i32)
            .coordinators(coordinators)
            .build()
            .unwrap())
    }

    pub async fn list_offsets(
        &mut self,
        request: messages::ListOffsetsRequest,
    ) -> anyhow::Result<messages::ListOffsetsResponse> {
        let client = &self.client;

        // Concurrently fetch Collection instances and offsets for all requested topics and partitions.
        // Map each "topic" into Vec<(Partition Index, Option<(Journal Offset, Timestamp))>.
        let collections: anyhow::Result<Vec<(TopicName, Vec<(i32, Option<(i64, i64)>)>)>> =
            futures::future::try_join_all(request.topics.into_iter().map(|topic| async move {
                let maybe_collection = Collection::new(client, topic.name.as_str()).await?;

                let Some(collection) = maybe_collection else {
                    return Ok((
                        topic.name,
                        topic
                            .partitions
                            .iter()
                            .map(|p| (p.partition_index, None))
                            .collect(),
                    ));
                };
                let collection = &collection;

                // Concurrently fetch requested offset for each named partition.
                let offsets: anyhow::Result<_> = futures::future::try_join_all(
                    topic.partitions.into_iter().map(|partition| async move {
                        Ok((
                            partition.partition_index,
                            collection
                                .fetch_partition_offset(
                                    partition.partition_index as usize,
                                    partition.timestamp, // In millis.
                                )
                                .await?,
                        ))
                    }),
                )
                .await;

                Ok((topic.name, offsets?))
            }))
            .await;

        use messages::list_offsets_response::{
            ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
        };

        // Map topics, partition indices, and fetched offsets into a comprehensive response.
        let response = collections?
            .into_iter()
            .map(|(topic_name, offsets)| {
                let partitions = offsets
                    .into_iter()
                    .map(|(partition_index, maybe_offset)| {
                        let Some((offset, timestamp)) = maybe_offset else {
                            return ListOffsetsPartitionResponse::builder()
                                .partition_index(partition_index)
                                .error_code(ResponseError::UnknownTopicOrPartition.code())
                                .build()
                                .unwrap();
                        };

                        ListOffsetsPartitionResponse::builder()
                            .partition_index(partition_index)
                            .offset(offset)
                            .timestamp(timestamp)
                            .build()
                            .unwrap()
                    })
                    .collect();

                ListOffsetsTopicResponse::builder()
                    .name(topic_name)
                    .partitions(partitions)
                    .build()
                    .unwrap()
            })
            .collect();

        Ok(messages::ListOffsetsResponse::builder()
            .topics(response)
            .build()
            .unwrap())
    }

    /// Fetch records from select "partitions" (journals) and "topics" (collections).
    pub async fn fetch(
        &mut self,
        request: messages::FetchRequest,
    ) -> anyhow::Result<messages::FetchResponse> {
        use messages::fetch_response::{FetchableTopicResponse, PartitionData};

        let messages::FetchRequest {
            topics: topic_requests,
            max_bytes: _, // Ignored.
            max_wait_ms,
            min_bytes: _, // Ignored.
            session_id,
            ..
        } = request;

        let client = &self.client;
        let timeout = tokio::time::sleep(std::time::Duration::from_millis(max_wait_ms as u64));
        let timeout = futures::future::maybe_done(timeout);
        tokio::pin!(timeout);

        // Start reads for all partitions which aren't already pending.
        for topic_request in &topic_requests {
            let mut key = (topic_request.topic.clone(), 0);

            for partition_request in &topic_request.partitions {
                key.1 = partition_request.partition;

                if matches!(self.reads.get(&key), Some(pending) if pending.offset == partition_request.fetch_offset)
                {
                    continue; // Common case: fetch is at the pending offset.
                }
                let Some(collection) = Collection::new(client, &key.0).await? else {
                    continue; // Collection doesn't exist.
                };
                let Some(partition) = collection
                    .partitions
                    .get(partition_request.partition as usize)
                else {
                    continue; // Partition doesn't exist.
                };
                let (key_schema_id, value_schema_id) =
                    collection.registered_schema_ids(&client).await?;

                let read = Read::new(
                    collection.journal_client.clone(),
                    &collection,
                    partition,
                    partition_request.fetch_offset,
                    key_schema_id,
                    value_schema_id,
                );
                let pending = PendingRead {
                    offset: partition_request.fetch_offset,
                    last_write_head: partition_request.fetch_offset,
                    handle: tokio::spawn(
                        read.next_batch(partition_request.partition_max_bytes as usize),
                    ),
                };

                tracing::info!(
                    journal = &partition.spec.name,
                    key_schema_id,
                    value_schema_id,
                    partition_request.fetch_offset,
                    "started read",
                );

                if let Some(old) = self.reads.insert(key.clone(), pending) {
                    tracing::warn!(
                        topic = topic_request.topic.as_str(),
                        partition = partition_request.partition,
                        old_offset = old.offset,
                        new_offset = partition_request.fetch_offset,
                        "discarding pending read due to offset jump",
                    );
                }
            }
        }

        // Poll pending reads across all requested topics.
        let mut topic_responses = Vec::with_capacity(topic_requests.len());

        for topic_request in &topic_requests {
            let mut key = (topic_request.topic.clone(), 0);
            let mut partition_responses = Vec::with_capacity(topic_request.partitions.len());

            for partition_request in &topic_request.partitions {
                key.1 = partition_request.partition;

                let Some(pending) = self.reads.get_mut(&key) else {
                    partition_responses.push(
                        PartitionData::builder()
                            .partition_index(partition_request.partition)
                            .error_code(ResponseError::UnknownTopicOrPartition.code())
                            .build()
                            .unwrap(),
                    );
                    continue;
                };

                let batch = if let Some((read, batch)) = tokio::select! {
                    biased; // Prefer to complete a pending read.
                    read  = &mut pending.handle => Some(read??),
                    _ = &mut timeout => None,
                } {
                    pending.offset = read.offset;
                    pending.last_write_head = read.last_write_head;
                    pending.handle = tokio::spawn(
                        read.next_batch(partition_request.partition_max_bytes as usize),
                    );
                    batch
                } else {
                    bytes::Bytes::new()
                };

                partition_responses.push(
                    PartitionData::builder()
                        .partition_index(partition_request.partition)
                        .records(Some(batch))
                        .high_watermark(pending.last_write_head) // Map to kafka cursor.
                        .last_stable_offset(pending.last_write_head)
                        .build()
                        .unwrap(),
                );
            }

            topic_responses.push(
                FetchableTopicResponse::builder()
                    .topic(topic_request.topic.clone())
                    .partitions(partition_responses)
                    .build()
                    .unwrap(),
            );
        }

        Ok(messages::FetchResponse::builder()
            .session_id(session_id)
            .responses(topic_responses)
            .build()
            .unwrap())
    }

    /// OffsetCommit is an ignored no-op.
    pub async fn offset_commit(
        &mut self,
        _req: messages::OffsetCommitRequest,
    ) -> anyhow::Result<messages::OffsetCommitResponse> {
        Ok(messages::OffsetCommitResponse::builder().build().unwrap())
    }

    /// DescribeConfigs lists configuration metadata of topics.
    /// This is used only by `kaf` thus far, is informational, and is currently just a stub.
    pub async fn describe_configs(
        &mut self,
        req: messages::DescribeConfigsRequest,
    ) -> anyhow::Result<messages::DescribeConfigsResponse> {
        use kafka_protocol::messages::describe_configs_response::*;

        let mut results = Vec::new();

        for resource in req.resources.iter() {
            if resource.resource_type == 2 {
                // Describe config of a named topic.
                let fixtures = [("some-key", "some-value"), ("another-key", "another-value")];

                let configs = fixtures
                    .into_iter()
                    .map(|(name, value)| {
                        DescribeConfigsResourceResult::builder()
                            .name(StrBytes::from_static_str(name))
                            .value(Some(StrBytes::from_static_str(value)))
                            .read_only(true)
                            .build()
                            .unwrap()
                    })
                    .collect();

                results.push(
                    DescribeConfigsResult::builder()
                        .resource_name(resource.resource_name.clone())
                        .configs(configs)
                        .build()
                        .unwrap(),
                )
            }
        }

        Ok(DescribeConfigsResponse::builder()
            .results(results)
            .build()
            .unwrap())
    }

    pub async fn join_group(
        &mut self,
        req: messages::JoinGroupRequest,
        header: RequestHeader,
    ) -> anyhow::Result<messages::JoinGroupResponse> {
        return self.app.kafka_client.send_request(req, Some(header)).await;
    }

    pub async fn list_group(
        &mut self,
        req: messages::ListGroupsRequest,
        header: RequestHeader,
    ) -> anyhow::Result<messages::ListGroupsResponse> {
        return self.app.kafka_client.send_request(req, Some(header)).await;
    }

    pub async fn sync_group(
        &mut self,
        req: messages::SyncGroupRequest,
        header: RequestHeader,
    ) -> anyhow::Result<messages::SyncGroupResponse> {
        return self.app.kafka_client.send_request(req, Some(header)).await;
    }

    pub async fn delete_group(
        &mut self,
        req: messages::DeleteGroupsRequest,
        header: RequestHeader,
    ) -> anyhow::Result<messages::DeleteGroupsResponse> {
        return self.app.kafka_client.send_request(req, Some(header)).await;
    }

    pub async fn heartbeat(
        &mut self,
        req: messages::HeartbeatRequest,
        header: RequestHeader,
    ) -> anyhow::Result<messages::HeartbeatResponse> {
        return self.app.kafka_client.send_request(req, Some(header)).await;
    }

    /// ApiVersions lists the APIs which are supported by this "broker".
    pub async fn api_versions(
        &mut self,
        _req: messages::ApiVersionsRequest,
    ) -> anyhow::Result<messages::ApiVersionsResponse> {
        use kafka_protocol::messages::{api_versions_response::ApiVersion, *};

        fn version<T: kafka_protocol::protocol::Message>() -> ApiVersion {
            let mut v = ApiVersion::default();
            v.max_version = T::VERSIONS.max;
            v.min_version = T::VERSIONS.min;
            v
        }
        let mut res = ApiVersionsResponse::default();

        res.api_keys.insert(
            ApiKey::ApiVersionsKey as i16,
            version::<ApiVersionsRequest>(),
        );
        res.api_keys.insert(
            ApiKey::SaslHandshakeKey as i16,
            version::<SaslHandshakeRequest>(),
        );
        res.api_keys.insert(
            ApiKey::SaslAuthenticateKey as i16,
            version::<SaslAuthenticateRequest>(),
        );
        res.api_keys
            .insert(ApiKey::MetadataKey as i16, version::<MetadataRequest>());
        res.api_keys.insert(
            ApiKey::FindCoordinatorKey as i16,
            version::<FindCoordinatorRequest>(),
        );
        res.api_keys.insert(
            ApiKey::ListOffsetsKey as i16,
            version::<ListOffsetsRequest>(),
        );
        res.api_keys
            .insert(ApiKey::FetchKey as i16, version::<FetchRequest>());
        res.api_keys.insert(
            ApiKey::OffsetCommitKey as i16,
            version::<OffsetCommitRequest>(),
        );

        // Needed by `kaf`.
        res.api_keys.insert(
            ApiKey::DescribeConfigsKey as i16,
            version::<DescribeConfigsRequest>(),
        );

        // UNIMPLEMENTED.
        /*
        res.api_keys
            .insert(ApiKey::ProduceKey as i16, version::<ProduceRequest>());
        res.api_keys.insert(
            ApiKey::LeaderAndIsrKey as i16,
            version::<LeaderAndIsrRequest>(),
        );
        res.api_keys.insert(
            ApiKey::StopReplicaKey as i16,
            version::<StopReplicaRequest>(),
        );
        res.api_keys
            .insert(ApiKey::JoinGroupKey as i16, version::<JoinGroupRequest>());
        res.api_keys
            .insert(ApiKey::HeartbeatKey as i16, version::<HeartbeatRequest>());
        res.api_keys
            .insert(ApiKey::ListGroupsKey as i16, version::<ListGroupsRequest>());
        res.api_keys
            .insert(ApiKey::SyncGroupKey as i16, version::<SyncGroupRequest>());
        res.api_keys.insert(
            ApiKey::CreateTopicsKey as i16,
            version::<CreateTopicsRequest>(),
        );
        res.api_keys.insert(
            ApiKey::DeleteGroupsKey as i16,
            version::<DeleteGroupsRequest>(),
        );
        res.api_keys
            .insert(ApiKey::ListGroupsKey as i16, version::<ListGroupsRequest>());
        res.api_keys.insert(
            ApiKey::DeleteTopicsKey as i16,
            version::<DeleteTopicsRequest>(),
        );
        */

        res.api_keys
            .insert(ApiKey::JoinGroupKey as i16, version::<JoinGroupRequest>());
        res.api_keys
            .insert(ApiKey::ListGroupsKey as i16, version::<ListGroupsRequest>());
        res.api_keys
            .insert(ApiKey::SyncGroupKey as i16, version::<SyncGroupRequest>());
        res.api_keys.insert(
            ApiKey::DeleteGroupsKey as i16,
            version::<DeleteGroupsRequest>(),
        );
        res.api_keys
            .insert(ApiKey::HeartbeatKey as i16, version::<HeartbeatRequest>());

        Ok(res)
    }
}
