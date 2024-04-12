use super::{fetch_all_collection_names, read_record_batch, App, Collection, Read};
use anyhow::Context;
use futures::FutureExt;
use kafka_protocol::{
    error::ResponseError,
    indexmap::IndexMap,
    messages::{self, metadata_response::MetadataResponseTopic, TopicName},
    protocol::{Builder, StrBytes},
};
use std::collections::HashMap;
use std::sync::Arc;

pub struct Session {
    app: Arc<App>,
    client: postgrest::Postgrest,
    reads: HashMap<(TopicName, i32), Read>,
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

        let authzid = it.next().context("expected SASL authzid")??;
        let authcid = it.next().context("expected SASL authcid")??;
        let password = it.next().context("expected SASL passwd")??;

        // The "username" will eventually hold session configuration state.
        // Reserve the ability to do this by ensuring it currently equals '{}'.
        if authcid != "{}" {
            tracing::warn!(authcid, "rejected authcid which is not '{{}}'");

            let response = messages::SaslAuthenticateResponse::builder()
                .error_code(ResponseError::SaslAuthenticationFailed.code())
                .error_message(Some(StrBytes::from_string(format!(
                    "SASL authentication error: Authentication failed: {}",
                    crate::RESERVED_USERNAME_ERR
                ))))
                .build()
                .unwrap();

            return Ok(response);
        }
        let _config: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(authcid).context("failed to parse SASL user as JSON object")?;

        tracing::info!(authzid, authcid, "sasl_authenticate");

        // TODO(johnny): Allow `password` to contain a refresh token _or_ access token.
        // If it's a refresh token, use it to mint a new access token.

        self.client = self
            .client
            .clone()
            .insert_header("Authorization", format!("Bearer {password}"));

        let mut response = messages::SaslAuthenticateResponse::default();
        response.session_lifetime_ms = i64::MAX; // TODO(johnny): Access token expiry.
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
        // Note that we shift `offset` left to reserve the low bit as a flag.
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
                            .offset(offset << 1) // Map into kafka cursor.
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
        use messages::fetch_request::{FetchPartition, FetchTopic};
        use messages::fetch_response::{FetchableTopicResponse, PartitionData};

        let messages::FetchRequest {
            topics,
            max_bytes: _, // Ignored.
            max_wait_ms,
            min_bytes: _, // Ignored.
            session_id,
            ..
        } = request;

        let timeout =
            tokio::time::sleep(std::time::Duration::from_millis(max_wait_ms as u64)).shared();
        let timeout = timeout.shared();
        let timeout = &timeout;

        // Resolve already-started reads for each fetched partition.
        let mut topics: Vec<(TopicName, Vec<(FetchPartition, Option<Read>)>)> = topics
            .into_iter()
            .map(
                |FetchTopic {
                     topic, partitions, ..
                 }| {
                    let partitions: Vec<_> = partitions
                        .into_iter()
                        .map(|fetch| {
                            let read = match self.reads.remove(&(topic.clone(), fetch.partition)) {
                                Some(read) if read.kafka_cursor == fetch.fetch_offset => Some(read),
                                Some(read) => {
                                    tracing::warn!(
                                        fetch.fetch_offset,
                                        read.kafka_cursor,
                                        "discarding active read",
                                    );
                                    None
                                }
                                _ => None,
                            };
                            (fetch, read)
                        })
                        .collect();

                    (topic, partitions)
                },
            )
            .collect();

        let client = &self.client;

        // Start any required partition reads.
        let _: Vec<()> = futures::future::try_join_all(topics.iter_mut().map(
            |(topic, partitions)| async move {
                if partitions.iter().all(|(_, read)| read.is_some()) {
                    return anyhow::Ok(());
                }
                // We must resolve the collection and its partitions before we can start reads.
                let Some(collection) = Collection::new(client, topic.as_str()).await? else {
                    return Ok(()); // Collection not found and reads are None.
                };
                let (key_schema_id, value_schema_id) =
                    collection.registered_schema_ids(client).await?;

                for (fetch, read) in partitions.iter_mut() {
                    if read.is_some() {
                        continue;
                    }
                    let Some(partition) = collection.partitions.get(fetch.partition as usize)
                    else {
                        continue;
                    };

                    *read = Some(Read::new(
                        collection.journal_client.clone(),
                        &collection,
                        partition,
                        fetch.fetch_offset,
                        key_schema_id,
                        value_schema_id,
                    )?);

                    tracing::info!(
                        journal = &partition.spec.name,
                        key_schema_id,
                        value_schema_id,
                        fetch.fetch_offset,
                        "started read",
                    );
                }

                return Ok(());
            },
        ))
        .await?;

        // Concurrently read across all requested topics.
        let responses: Vec<FetchableTopicResponse> = futures::future::try_join_all(
            topics.iter_mut().map(|(topic, partitions)| async move {
                // Concurrently read across all requested topic partitions.
                let partitions: anyhow::Result<Vec<PartitionData>> = futures::future::try_join_all(
                    partitions.iter_mut().map(|(fetch, maybe_read)| async move {
                        let Some(read) = maybe_read else {
                            return Ok(PartitionData::builder()
                                .partition_index(fetch.partition)
                                .error_code(ResponseError::UnknownTopicOrPartition.code())
                                .build()
                                .unwrap());
                        };

                        let batch = read_record_batch(
                            read,
                            fetch.partition_max_bytes as usize,
                            timeout.clone(),
                        )
                        .await?;

                        // Watermark and LSO offsets are shifted to reserve the low bit as a flag.
                        Ok(PartitionData::builder()
                            .partition_index(fetch.partition)
                            .records(Some(batch))
                            .high_watermark(read.last_write_head << 1) // Map to kafka cursor.
                            .last_stable_offset(read.last_write_head << 1)
                            .build()
                            .unwrap())
                    }),
                )
                .await;

                anyhow::Ok(
                    FetchableTopicResponse::builder()
                        .topic(topic.clone())
                        .partitions(partitions?)
                        .build()
                        .unwrap(),
                )
            }),
        )
        .await?;

        // Retain all still-active reads.
        for (topic, partitions) in topics {
            for (fetch, maybe_read) in partitions {
                if let Some(read) = maybe_read {
                    self.reads.insert((topic.clone(), fetch.partition), read);
                }
            }
        }

        Ok(messages::FetchResponse::builder()
            .session_id(session_id)
            .responses(responses)
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

        Ok(res)
    }
}
