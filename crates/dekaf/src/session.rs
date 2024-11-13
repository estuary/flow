use super::{App, Collection, Read};
use crate::{
    from_downstream_topic_name, from_upstream_topic_name, read::BatchResult,
    to_downstream_topic_name, to_upstream_topic_name, topology::PartitionOffset, KafkaApiClient,
    SessionAuthentication,
};
use anyhow::{bail, Context};
use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::{
    error::{ParseResponseErrorCode, ResponseError},
    messages::{
        self,
        metadata_response::{
            MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
        },
        ConsumerProtocolAssignment, ConsumerProtocolSubscription, ListGroupsResponse,
        RequestHeader, TopicName,
    },
    protocol::{buf::ByteBuf, Decodable, Encodable, Message, StrBytes},
};
use std::{cmp::max, sync::Arc, time::Duration};
use std::{
    collections::{hash_map::Entry, HashMap},
    time::SystemTime,
};
use tracing::instrument;

struct PendingRead {
    offset: i64,          // Journal offset to be completed by this PendingRead.
    last_write_head: i64, // Most-recent observed journal write head.
    handle: tokio_util::task::AbortOnDropHandle<anyhow::Result<(Read, BatchResult)>>,
}

#[derive(Clone, Debug)]
enum SessionDataPreviewState {
    Unknown,
    NotDataPreview,
    DataPreview(HashMap<(TopicName, i32), PartitionOffset>),
}

pub struct Session {
    app: Arc<App>,
    client: Option<KafkaApiClient>,
    reads: HashMap<(TopicName, i32), (PendingRead, std::time::Instant)>,
    secret: String,
    auth: Option<SessionAuthentication>,
    data_preview_state: SessionDataPreviewState,
    broker_url: String,
    broker_username: String,
    broker_password: String,
    pub client_id: Option<String>,
}

impl Session {
    pub fn new(
        app: Arc<App>,
        secret: String,
        broker_url: String,
        broker_username: String,
        broker_password: String,
    ) -> Self {
        Self {
            app,
            client: None,
            broker_url,
            broker_username,
            broker_password,
            reads: HashMap::new(),
            auth: None,
            secret,
            client_id: None,
            data_preview_state: SessionDataPreviewState::Unknown,
        }
    }

    async fn get_kafka_client(&mut self) -> anyhow::Result<&mut KafkaApiClient> {
        if let Some(ref mut client) = self.client {
            Ok(client)
        } else {
            self.client.replace(
                KafkaApiClient::connect(
                    &self.broker_url,
                    rsasl::config::SASLConfig::with_credentials(
                        None,
                        self.broker_username.clone(),
                        self.broker_password.clone(),
                    )?,
                ).await.context(
                    "failed to connect or authenticate to upstream Kafka broker used for serving group management APIs",
                )?
            );
            Ok(self.client.as_mut().expect("guarinteed to exist"))
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
            Ok(auth) => {
                let mut response = messages::SaslAuthenticateResponse::default();

                response.session_lifetime_ms = (auth
                    .valid_until()
                    .duration_since(SystemTime::now())?
                    .as_secs()
                    * 1000)
                    .try_into()?;

                self.auth.replace(auth);
                response
            }
            Err(err) => messages::SaslAuthenticateResponse::default()
                .with_error_code(ResponseError::SaslAuthenticationFailed.code())
                .with_error_message(Some(StrBytes::from_string(format!(
                    "SASL authentication error: Authentication failed: {err:#}",
                )))),
        };

        Ok(response)
    }

    /// Serve metadata of topics and their partitions.
    /// For efficiency, we do NOT enumerate partitions when we receive an unqualified metadata request.
    /// Otherwise, if specific "topics" (collections) are listed, we fetch and map journals into partitions.
    #[instrument(skip_all)]
    pub async fn metadata(
        &mut self,
        mut request: messages::MetadataRequest,
    ) -> anyhow::Result<messages::MetadataResponse> {
        let topics = match request.topics.take() {
            Some(topics) if topics.len() > 0 => self.metadata_select_topics(topics).await,
            _ => self.metadata_all_topics().await,
        }?;

        // We only ever advertise a single logical broker.
        let brokers = vec![MetadataResponseBroker::default()
            .with_node_id(messages::BrokerId(1))
            .with_host(StrBytes::from_string(self.app.advertise_host.clone()))
            .with_port(self.app.advertise_kafka_port as i32)];

        Ok(messages::MetadataResponse::default()
            .with_brokers(brokers)
            .with_cluster_id(Some(StrBytes::from_static_str("estuary-dekaf")))
            .with_controller_id(messages::BrokerId(1))
            .with_topics(topics))
    }

    // Lists all read-able collections as Kafka topics. Omits partition metadata.
    async fn metadata_all_topics(&mut self) -> anyhow::Result<Vec<MetadataResponseTopic>> {
        let collections = self
            .auth
            .as_mut()
            .ok_or(anyhow::anyhow!("Session not authenticated"))?
            .fetch_all_collection_names()
            .await?;

        tracing::debug!(collections=?ops::DebugJson(&collections), "fetched all collections");

        let topics = collections
            .into_iter()
            .map(|name| {
                MetadataResponseTopic::default()
                    .with_name(Some(self.encode_topic_name(name)))
                    .with_is_internal(false)
                    .with_partitions(vec![MetadataResponsePartition::default()
                        .with_partition_index(0)
                        .with_leader_id(0.into())])
            })
            .collect();

        Ok(topics)
    }

    // Lists partitions of specific, requested collections.
    async fn metadata_select_topics(
        &mut self,
        requests: Vec<messages::metadata_request::MetadataRequestTopic>,
    ) -> anyhow::Result<Vec<MetadataResponseTopic>> {
        let auth = self
            .auth
            .as_mut()
            .ok_or(anyhow::anyhow!("Session not authenticated"))?;

        let app = &self.app;
        let pg_client = &auth.flow_client(app).await?.pg_client();

        // Re-declare here to drop mutable reference
        let auth = self.auth.as_ref().unwrap();

        // Concurrently fetch Collection instances for all requested topics.
        let collections: anyhow::Result<Vec<(TopicName, Option<Collection>)>> =
            futures::future::try_join_all(requests.into_iter().map(|topic| async move {
                let maybe_collection = Collection::new(
                    app,
                    auth,
                    pg_client,
                    from_downstream_topic_name(topic.name.to_owned().unwrap_or_default()).as_str(),
                )
                .await?;
                Ok((topic.name.unwrap_or_default(), maybe_collection))
            }))
            .await;

        let mut topics = vec![];

        for (name, maybe_collection) in collections? {
            let Some(collection) = maybe_collection else {
                topics.push(
                    MetadataResponseTopic::default()
                        .with_name(Some(self.encode_topic_name(name.to_string())))
                        .with_error_code(ResponseError::UnknownTopicOrPartition.code()),
                );
                continue;
            };

            let partitions = collection
                .partitions
                .iter()
                .enumerate()
                .map(|(index, _)| {
                    messages::metadata_response::MetadataResponsePartition::default()
                        .with_partition_index(index as i32)
                        .with_leader_id(messages::BrokerId(1))
                        .with_replica_nodes(vec![messages::BrokerId(1)])
                        .with_isr_nodes(vec![messages::BrokerId(1)])
                })
                .collect();

            topics.push(
                MetadataResponseTopic::default()
                    .with_name(Some(name))
                    .with_is_internal(false)
                    .with_partitions(partitions),
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
                messages::find_coordinator_response::Coordinator::default()
                    .with_node_id(messages::BrokerId(1))
                    .with_host(StrBytes::from_string(self.app.advertise_host.clone()))
                    .with_port(self.app.advertise_kafka_port as i32)
            })
            .collect();

        Ok(messages::FindCoordinatorResponse::default()
            .with_node_id(messages::BrokerId(1))
            .with_host(StrBytes::from_string(self.app.advertise_host.clone()))
            .with_port(self.app.advertise_kafka_port as i32)
            .with_coordinators(coordinators))
    }

    pub async fn list_offsets(
        &mut self,
        request: messages::ListOffsetsRequest,
    ) -> anyhow::Result<messages::ListOffsetsResponse> {
        let auth = self
            .auth
            .as_mut()
            .ok_or(anyhow::anyhow!("Session not authenticated"))?;

        let app = &self.app;
        let pg_client = &auth.flow_client(app).await?.pg_client();

        // Re-declare here to drop mutable reference
        let auth = self.auth.as_ref().unwrap();

        // Concurrently fetch Collection instances and offsets for all requested topics and partitions.
        // Map each "topic" into Vec<(Partition Index, Option<PartitionOffset>.
        let collections: anyhow::Result<Vec<(TopicName, Vec<(i32, Option<PartitionOffset>)>)>> =
            futures::future::try_join_all(request.topics.into_iter().map(|topic| async move {
                let maybe_collection = Collection::new(
                    app,
                    auth,
                    pg_client,
                    from_downstream_topic_name(topic.name.clone()).as_str(),
                )
                .await?;

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

        let collections = collections?;

        use messages::list_offsets_response::{
            ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
        };

        // Map topics, partition indices, and fetched offsets into a comprehensive response.
        let response = collections
            .into_iter()
            .map(|(topic_name, offsets)| {
                let partitions = offsets
                    .into_iter()
                    .map(|(partition_index, maybe_offset)| {
                        let Some(PartitionOffset {
                            offset,
                            mod_time: timestamp,
                            ..
                        }) = maybe_offset
                        else {
                            return ListOffsetsPartitionResponse::default()
                                .with_partition_index(partition_index)
                                .with_error_code(ResponseError::UnknownTopicOrPartition.code());
                        };

                        ListOffsetsPartitionResponse::default()
                            .with_partition_index(partition_index)
                            .with_offset(offset)
                            .with_timestamp(timestamp)
                    })
                    .collect();

                ListOffsetsTopicResponse::default()
                    .with_name(topic_name)
                    .with_partitions(partitions)
            })
            .collect();

        Ok(messages::ListOffsetsResponse::default().with_topics(response))
    }

    /// Fetch records from select "partitions" (journals) and "topics" (collections).
    #[tracing::instrument(
        skip_all,
        fields(
            max_wait_ms=request.max_wait_ms
        )
    )]
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

        let timeout = std::time::Duration::from_millis(max_wait_ms as u64);

        // Start reads for all partitions which aren't already pending.
        for topic_request in &topic_requests {
            let mut key = (from_downstream_topic_name(topic_request.topic.clone()), 0);

            for partition_request in &topic_request.partitions {
                key.1 = partition_request.partition;
                let fetch_offset = partition_request.fetch_offset;

                let data_preview_params: Option<PartitionOffset> = match self
                    .data_preview_state
                    .to_owned()
                {
                    // On the first Fetch call, check to see whether it is considered a data-preview
                    // fetch or not. If so, flag the whole session as being tainted, and also keep track
                    // of the neccesary offset data in order to serve the rewritten data preview responses.
                    SessionDataPreviewState::Unknown => {
                        if let Some(state) = self
                            .is_fetch_data_preview(key.0.to_string(), key.1, fetch_offset)
                            .await?
                        {
                            let mut data_preview_state = HashMap::new();
                            data_preview_state.insert(key.to_owned(), state);
                            self.data_preview_state =
                                SessionDataPreviewState::DataPreview(data_preview_state);
                            Some(state)
                        } else {
                            self.data_preview_state = SessionDataPreviewState::NotDataPreview;
                            None
                        }
                    }
                    // If the first Fetch request in a session was not considered for data preview,
                    // then skip all further checks in order to avoid slowing down fetches.
                    SessionDataPreviewState::NotDataPreview => None,
                    SessionDataPreviewState::DataPreview(mut state) => {
                        match state.entry(key.to_owned()) {
                            // If a session is marked as being used for data preview, and this Fetch request
                            // is for a topic/partition that we've already loaded the offsets for, re-use them
                            // so long as the request is still a data preview request. If not, bail out
                            Entry::Occupied(entry) => {
                                let data_preview_state = entry.get();
                                if fetch_offset >= data_preview_state.offset
                                    || data_preview_state.offset - fetch_offset > 12
                                {
                                    bail!("Session was used for fetching preview data, cannot be used for fetching non-preview data.")
                                }
                                Some(data_preview_state.to_owned())
                            }
                            // Otherwise, load the offsets for this new topic/partition, and also ensure that this is
                            // still a data-preview request. If not, bail out.
                            Entry::Vacant(entry) => {
                                if let Some(state) = self
                                    .is_fetch_data_preview(key.0.to_string(), key.1, fetch_offset)
                                    .await?
                                {
                                    entry.insert(state);
                                    Some(state)
                                } else {
                                    bail!("Session was used for fetching preview data, cannot be used for fetching non-preview data.")
                                }
                            }
                        }
                    }
                };

                match self.reads.get(&key) {
                    Some((_, started_at))
                        if started_at.elapsed() > std::time::Duration::from_secs(60 * 5) =>
                    {
                        metrics::counter!(
                            "dekaf_fetch_requests",
                            "topic_name" => key.0.to_string(),
                            "partition_index" => key.1.to_string(),
                            "state" => "read_expired"
                        )
                        .increment(1);
                        tracing::debug!(lifetime=?started_at.elapsed(), topic_name=?key.0,partition_index=?key.1, "Restarting expired Read");
                        self.reads.remove(&key);

                        let auth = self
                            .auth
                            .as_mut()
                            .ok_or(anyhow::anyhow!("Session not authenticated"))?;

                        auth.refresh_gazette_clients();
                    }
                    Some(_) => {
                        metrics::counter!(
                            "dekaf_fetch_requests",
                            "topic_name" => key.0.to_string(),
                            "partition_index" => key.1.to_string(),
                            "state" => "read_pending"
                        )
                        .increment(1);
                        continue; // Common case: fetch is at the pending offset.
                    }
                    _ => {}
                }

                let auth = self.auth.as_mut().unwrap();
                let pg_client = auth.flow_client(&self.app).await?.pg_client();
                let Some(collection) =
                    Collection::new(&self.app, &auth, &pg_client, &key.0).await?
                else {
                    metrics::counter!(
                        "dekaf_fetch_requests",
                        "topic_name" => key.0.to_string(),
                        "partition_index" => key.1.to_string(),
                        "state" => "collection_not_found"
                    )
                    .increment(1);
                    tracing::debug!(collection = ?&key.0, "Collection doesn't exist!");
                    continue; // Collection doesn't exist.
                };
                let Some(partition) = collection
                    .partitions
                    .get(partition_request.partition as usize)
                else {
                    metrics::counter!(
                        "dekaf_fetch_requests",
                        "topic_name" => key.0.to_string(),
                        "partition_index" => key.1.to_string(),
                        "state" => "partition_not_found"
                    )
                    .increment(1);
                    tracing::debug!(collection = ?&key.0, partition=partition_request.partition, "Partition doesn't exist!");
                    continue; // Partition doesn't exist.
                };
                let (key_schema_id, value_schema_id) =
                    collection.registered_schema_ids(&pg_client).await?;
                let pending = PendingRead {
                    offset: fetch_offset,
                    last_write_head: fetch_offset,
                    handle: tokio_util::task::AbortOnDropHandle::new(match data_preview_params {
                        // Startree: 0, Tinybird: 12
                        Some(PartitionOffset {
                            fragment_start,
                            offset: latest_offset,
                            ..
                        }) if latest_offset - fetch_offset <= 12 => {
                            let diff = latest_offset - fetch_offset;
                            metrics::counter!(
                                "dekaf_fetch_requests",
                                "topic_name" => key.0.to_string(),
                                "partition_index" => key.1.to_string(),
                                "state" => "new_data_preview_read"
                            )
                            .increment(1);
                            tokio::spawn(
                                Read::new(
                                    collection.journal_client.clone(),
                                    &collection,
                                    partition,
                                    fragment_start,
                                    key_schema_id,
                                    value_schema_id,
                                    Some(partition_request.fetch_offset - 1),
                                    auth.deletions(),
                                )
                                .next_batch(
                                    // Have to read at least 2 docs, as the very last doc
                                    // will probably be a control document and will be
                                    // ignored by the consumer, looking like 0 docs were read
                                    crate::read::ReadTarget::Docs(max(diff as usize, 2)),
                                    std::time::Instant::now() + timeout,
                                ),
                            )
                        }
                        _ => {
                            metrics::counter!(
                                "dekaf_fetch_requests",
                                "topic_name" => key.0.to_string(),
                                "partition_index" => key.1.to_string(),
                                "state" => "new_regular_read"
                            )
                            .increment(1);
                            tokio::spawn(
                                Read::new(
                                    collection.journal_client.clone(),
                                    &collection,
                                    partition,
                                    fetch_offset,
                                    key_schema_id,
                                    value_schema_id,
                                    None,
                                    auth.deletions(),
                                )
                                .next_batch(
                                    crate::read::ReadTarget::Bytes(
                                        partition_request.partition_max_bytes as usize,
                                    ),
                                    std::time::Instant::now() + timeout,
                                ),
                            )
                        }
                    }),
                };

                tracing::info!(
                    journal = &partition.spec.name,
                    key_schema_id,
                    value_schema_id,
                    fetch_offset,
                    "started read",
                );

                if let Some((old, started_at)) = self
                    .reads
                    .insert(key.clone(), (pending, std::time::Instant::now()))
                {
                    tracing::warn!(
                        topic = topic_request.topic.as_str(),
                        partition = partition_request.partition,
                        old_offset = old.offset,
                        new_offset = fetch_offset,
                        read_lifetime = ?started_at.elapsed(),
                        "discarding pending read due to offset jump",
                    );
                }
            }
        }

        // Poll pending reads across all requested topics.
        let mut topic_responses = Vec::with_capacity(topic_requests.len());

        for topic_request in &topic_requests {
            let mut key = (from_downstream_topic_name(topic_request.topic.clone()), 0);
            let mut partition_responses = Vec::with_capacity(topic_request.partitions.len());

            for partition_request in &topic_request.partitions {
                key.1 = partition_request.partition;

                let Some((pending, _)) = self.reads.get_mut(&key) else {
                    partition_responses.push(
                        PartitionData::default()
                            .with_partition_index(partition_request.partition)
                            .with_error_code(ResponseError::UnknownTopicOrPartition.code()),
                    );
                    continue;
                };

                let (read, batch) = (&mut pending.handle).await??;

                let batch = match batch {
                    BatchResult::TargetExceededBeforeTimeout(b) => Some(b),
                    BatchResult::TimeoutExceededBeforeTarget(b) => Some(b),
                    BatchResult::TimeoutNoData => None,
                };

                let mut partition_data = PartitionData::default()
                    .with_partition_index(partition_request.partition)
                    // `kafka-protocol` encodes None here using a length of -1, but librdkafka client library
                    // complains with: `Protocol parse failure for Fetch v11 ... invalid MessageSetSize -1`
                    // An empty Bytes will get encoded with a length of 0, which works fine.
                    .with_records(batch.or(Some(Bytes::new())).to_owned());

                match &self.data_preview_state {
                    SessionDataPreviewState::Unknown => {
                        unreachable!("Must have already determined data-preview status of session")
                    }
                    SessionDataPreviewState::NotDataPreview => {
                        pending.offset = read.offset;
                        pending.last_write_head = read.last_write_head;
                        pending.handle = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(
                            read.next_batch(
                                crate::read::ReadTarget::Bytes(
                                    partition_request.partition_max_bytes as usize,
                                ),
                                std::time::Instant::now() + timeout,
                            ),
                        ));

                        partition_data = partition_data
                            .with_high_watermark(pending.last_write_head) // Map to kafka cursor.
                            .with_last_stable_offset(pending.last_write_head);
                    }
                    SessionDataPreviewState::DataPreview(data_preview_states) => {
                        let data_preview_state = data_preview_states
                            .get(&key)
                            .expect("should be able to find data preview state by this point");
                        partition_data = partition_data
                            .with_high_watermark(data_preview_state.offset) // Map to kafka cursor.
                            .with_last_stable_offset(data_preview_state.offset);
                        self.reads.remove(&key);
                    }
                }

                partition_responses.push(partition_data);
            }

            topic_responses.push(
                FetchableTopicResponse::default()
                    .with_topic(topic_request.topic.clone())
                    .with_partitions(partition_responses),
            );
        }

        Ok(messages::FetchResponse::default()
            .with_session_id(session_id)
            .with_responses(topic_responses))
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
                        DescribeConfigsResourceResult::default()
                            .with_name(StrBytes::from_static_str(name))
                            .with_value(Some(StrBytes::from_static_str(value)))
                            .with_read_only(true)
                    })
                    .collect();

                results.push(
                    DescribeConfigsResult::default()
                        .with_resource_name(resource.resource_name.clone())
                        .with_configs(configs),
                )
            }
        }

        Ok(DescribeConfigsResponse::default().with_results(results))
    }

    /// Produce is assumed to be supported in various places, and clients using librdkafka
    /// break when that assumption isn't satisfied. For example, the `Fetch` API > version 0
    /// appears to (indirectly) assume that the broker supports `Produce`.
    /// For example: Each of these 3 conditions (`MSGVER1`, `MSGVER2`, `THROTTLE_TIME`) require `Produce`,
    /// and when it's not present the consumer will sit in a tight loop endlessly failing to
    /// send a fetch request because it's missing an API version flag:
    /// https://github.com/confluentinc/librdkafka/blob/master/src/rdkafka_fetcher.c#L997-L1005
    pub async fn produce(
        &mut self,
        req: messages::ProduceRequest,
    ) -> anyhow::Result<messages::ProduceResponse> {
        use kafka_protocol::messages::produce_response::*;

        let responses = req
            .topic_data
            .into_iter()
            .map(|v| {
                TopicProduceResponse::default().with_partition_responses(
                    v.partition_data
                        .into_iter()
                        .map(|part| {
                            PartitionProduceResponse::default()
                                .with_index(part.index)
                                .with_error_code(
                                    kafka_protocol::error::ResponseError::InvalidRequest.code(),
                                )
                        })
                        .collect(),
                )
            })
            .collect();

        Ok(ProduceResponse::default().with_responses(responses))
    }

    #[instrument(skip_all, fields(group=?req.group_id))]
    pub async fn join_group(
        &mut self,
        req: messages::JoinGroupRequest,
        header: RequestHeader,
    ) -> anyhow::Result<messages::JoinGroupResponse> {
        let mut mutable_req = req.clone();
        for protocol in mutable_req.protocols.iter_mut() {
            let mut consumer_protocol_subscription_raw = protocol.metadata.clone();

            let consumer_protocol_subscription_version = consumer_protocol_subscription_raw
                .try_get_i16()
                .context("failed to parse consumer protocol message: subscription version")?;

            tracing::debug!(
                version = consumer_protocol_subscription_version,
                remaining_bytes = consumer_protocol_subscription_raw.len(),
                "Got consumer protocol message version"
            );

            if consumer_protocol_subscription_version > ConsumerProtocolSubscription::VERSIONS.max
                || consumer_protocol_subscription_version
                    < ConsumerProtocolSubscription::VERSIONS.min
            {
                anyhow::bail!(
                    "Recieved ConsumerProtocolSubscription message with version {} which is outside of the acceptable range of ({}, {})",
                    consumer_protocol_subscription_version,
                    ConsumerProtocolSubscription::VERSIONS.min,
                    ConsumerProtocolSubscription::VERSIONS.max
                )
            }

            let formatted = format!("{consumer_protocol_subscription_raw:?}");

            let mut consumer_protocol_subscription_msg = ConsumerProtocolSubscription::decode(
                &mut consumer_protocol_subscription_raw,
                consumer_protocol_subscription_version, // Seems that sometimes v >=1 doesn't decode properly
            )
            .context(format!(
                "failed to parse consumer protocol message body: {formatted}"
            ))?;

            consumer_protocol_subscription_msg
                .topics
                .iter_mut()
                .for_each(|topic| {
                    let transformed = self.encrypt_topic_name(topic.to_owned().into()).into();
                    tracing::info!(topic_name = ?topic, encrypted_name=?transformed, "Joining group");
                    *topic = transformed;
                });

            let mut new_protocol_subscription = BytesMut::new();

            new_protocol_subscription.put_i16(consumer_protocol_subscription_version);
            consumer_protocol_subscription_msg.encode(
                &mut new_protocol_subscription,
                consumer_protocol_subscription_version,
            )?;

            protocol.metadata = new_protocol_subscription.into();
        }

        let response = self
            .get_kafka_client()
            .await?
            .connect_to_group_coordinator(req.group_id.as_str())
            .await?
            .send_request(mutable_req.clone(), Some(header))
            .await?;

        if let Some(err) = response.error_code.err() {
            tracing::debug!(?err, req=?mutable_req, "Request errored");
            return Ok(response);
        }

        // Now re-translate response
        let mut mutable_resp = response.clone();
        for member in mutable_resp.members.iter_mut() {
            let mut consumer_protocol_subscription_raw = member.metadata.clone();

            let consumer_protocol_subscription_version =
                consumer_protocol_subscription_raw.try_get_i16().context(
                    "failed to parse consumer protocol message: subscription version re-encode",
                )?;

            let mut consumer_protocol_subscription_msg = ConsumerProtocolSubscription::decode(
                &mut consumer_protocol_subscription_raw,
                consumer_protocol_subscription_version, // it seems that sometimes v >= 1 doesn't decode properly
            )
            .context("failed to parse consumer protocol message: subscription re-encode")?;

            consumer_protocol_subscription_msg
                .topics
                .iter_mut()
                .for_each(|topic| *topic = self.decrypt_topic_name(topic.to_owned().into()).into());

            let mut new_protocol_subscription = BytesMut::new();

            new_protocol_subscription.put_i16(consumer_protocol_subscription_version);
            consumer_protocol_subscription_msg.encode(
                &mut new_protocol_subscription,
                consumer_protocol_subscription_version,
            )?;

            member.metadata = new_protocol_subscription.into();
        }

        Ok(mutable_resp)
    }

    #[instrument(skip_all, fields(group=?req.group_id))]
    pub async fn leave_group(
        &mut self,
        req: messages::LeaveGroupRequest,
        header: RequestHeader,
    ) -> anyhow::Result<messages::LeaveGroupResponse> {
        let client = self
            .get_kafka_client()
            .await?
            .connect_to_group_coordinator(req.group_id.as_str())
            .await?;
        let response = client.send_request(req, Some(header)).await?;
        Ok(response)
    }

    #[tracing::instrument(skip_all)]
    pub async fn list_groups(
        &mut self,
        req: messages::ListGroupsRequest,
        header: RequestHeader,
    ) -> anyhow::Result<messages::ListGroupsResponse> {
        // Redpanda seems to randomly disconnect this?
        let r = self
            .get_kafka_client()
            .await?
            .send_request(req, Some(header))
            .await;
        match r {
            Ok(mut e) => {
                if let Some(err) = e.error_code.err() {
                    tracing::warn!(err = ?err, "Error listing groups!");
                }
                // Multiple systems had trouble when this returned the actual list of groups...
                // and AFAICT nothing has any trouble when we return an empty list here.
                e.groups = vec![]; //e
                                   // .groups
                                   // .into_iter()
                                   // .filter(|grp| !grp.group_id.starts_with("amazon.msk"))
                                   // .collect_vec();

                return Ok(e);
            }
            Err(e) => {
                tracing::warn!(e=?e, "Failed to list_groups");
                Ok(ListGroupsResponse::default().with_groups(vec![]))
            }
        }
    }

    #[instrument(skip_all, fields(group=?req.group_id))]
    pub async fn sync_group(
        &mut self,
        req: messages::SyncGroupRequest,
        header: RequestHeader,
    ) -> anyhow::Result<messages::SyncGroupResponse> {
        let mut mutable_req = req.clone();
        for assignment in mutable_req.assignments.iter_mut() {
            let mut consumer_protocol_assignment_raw = assignment.assignment.clone();
            let consumer_protocol_assignment_version = consumer_protocol_assignment_raw
                .try_get_i16()
                .context("failed to parse consumer protocol message: assignment version")?;

            if consumer_protocol_assignment_version > ConsumerProtocolAssignment::VERSIONS.max
                || consumer_protocol_assignment_version < ConsumerProtocolAssignment::VERSIONS.min
            {
                anyhow::bail!(
                    "Recieved ConsumerProtocolAssignment message with version {} which is outside of the acceptable range of ({}, {})",
                    consumer_protocol_assignment_version,
                    ConsumerProtocolAssignment::VERSIONS.min,
                    ConsumerProtocolAssignment::VERSIONS.max
                )
            }

            let mut consumer_protocol_assignment_msg =
                ConsumerProtocolAssignment::decode(&mut consumer_protocol_assignment_raw, 0)
                    .context("failed to parse consumer protocol message: assignment body")?;

            consumer_protocol_assignment_msg.assigned_partitions = consumer_protocol_assignment_msg
                .assigned_partitions
                .into_iter()
                .map(|part| {
                    let transformed_topic = self.encrypt_topic_name(part.topic.to_owned());
                    tracing::info!(topic_name = ?part.topic, encrypted_name=?transformed_topic, "Syncing group");
                    part.with_topic(transformed_topic)
                })
                .collect();

            let mut new_protocol_assignment = BytesMut::new();

            new_protocol_assignment.put_i16(consumer_protocol_assignment_version);
            consumer_protocol_assignment_msg.encode(
                &mut new_protocol_assignment,
                consumer_protocol_assignment_version,
            )?;
            assignment.assignment = new_protocol_assignment.into();
        }

        let response = self
            .get_kafka_client()
            .await?
            .connect_to_group_coordinator(req.group_id.as_str())
            .await?
            .send_request(mutable_req.clone(), Some(header))
            .await?;

        if let Some(err) = response.error_code.err() {
            tracing::debug!(?err, req=?mutable_req, "Request errored");
            return Ok(response);
        }

        let mut mutable_resp = response.clone();
        let mut consumer_protocol_assignment_raw = mutable_resp.assignment.clone();
        let consumer_protocol_assignment_version =
            consumer_protocol_assignment_raw.try_get_i16().context(
                "failed to parse consumer protocol message: assignment re-encode version",
            )?;
        // TODO: validate acceptable version

        let mut consumer_protocol_assignment_msg =
            ConsumerProtocolAssignment::decode(&mut consumer_protocol_assignment_raw, 0)
                .context("failed to parse consumer protocol message: assignment re-encode body")?;
        consumer_protocol_assignment_msg.assigned_partitions = consumer_protocol_assignment_msg
            .assigned_partitions
            .into_iter()
            .map(|part| {
                let transformed_topic = self.decrypt_topic_name(part.topic.to_owned());
                part.with_topic(transformed_topic)
            })
            .collect();

        let mut new_protocol_assignment = BytesMut::new();

        new_protocol_assignment.put_i16(consumer_protocol_assignment_version);
        consumer_protocol_assignment_msg.encode(
            &mut new_protocol_assignment,
            consumer_protocol_assignment_version,
        )?;
        mutable_resp.assignment = new_protocol_assignment.into();

        Ok(mutable_resp)
    }

    #[instrument(skip_all, fields(groups=?req.groups_names))]
    pub async fn delete_group(
        &mut self,
        req: messages::DeleteGroupsRequest,
        header: RequestHeader,
    ) -> anyhow::Result<messages::DeleteGroupsResponse> {
        return self
            .get_kafka_client()
            .await?
            .send_request(req, Some(header))
            .await;
    }

    #[instrument(skip_all, fields(group=?req.group_id))]
    pub async fn heartbeat(
        &mut self,
        req: messages::HeartbeatRequest,
        header: RequestHeader,
    ) -> anyhow::Result<messages::HeartbeatResponse> {
        let client = self
            .get_kafka_client()
            .await?
            .connect_to_group_coordinator(req.group_id.as_str())
            .await?;
        return client.send_request(req, Some(header)).await;
    }

    #[instrument(skip_all, fields(group=?req.group_id))]
    pub async fn offset_commit(
        &mut self,
        req: messages::OffsetCommitRequest,
        header: RequestHeader,
    ) -> anyhow::Result<messages::OffsetCommitResponse> {
        let mut mutated_req = req.clone();
        for topic in &mut mutated_req.topics {
            let encrypted = self.encrypt_topic_name(topic.name.clone());
            tracing::info!(topic_name = ?topic.name, encrypted_name=?encrypted, "Committing offset");
            topic.name = encrypted;
        }

        let client = self
            .get_kafka_client()
            .await?
            .connect_to_group_coordinator(req.group_id.as_str())
            .await?;

        client
            .ensure_topics(
                mutated_req
                    .topics
                    .iter()
                    .map(|t| t.name.to_owned())
                    .collect(),
            )
            .await?;

        let mut resp = client.send_request(mutated_req, Some(header)).await?;

        for topic in resp.topics.iter_mut() {
            topic.name = self.decrypt_topic_name(topic.name.to_owned());

            let collection_partitions = Collection::new(
                &self.app,
                auth,
                &flow_client.pg_client(),
                topic.name.as_str(),
            )
            .await?
            .context(format!("unable to look up partitions for {:?}", topic.name))?
            .partitions;

            for partition in &topic.partitions {
                if let Some(error) = partition.error_code.err() {
                    tracing::warn!(topic=?topic.name,partition=partition.partition_index,?error,"Got error from upstream Kafka when trying to commit offsets");
                } else {
                    let journal_name = collection_partitions
                        .get(partition.partition_index as usize)
                        .context(format!(
                            "unable to find partition {} in collection {:?}",
                            partition.partition_index, topic.name
                        ))?
                        .spec
                        .name
                        .to_owned();

                    let committed_offset = req
                        .topics
                        .iter()
                        .find(|req_topic| req_topic.name == topic.name)
                        .context(format!("unable to find topic in request {:?}", topic.name))?
                        .partitions
                        .get(partition.partition_index as usize)
                        .context(format!(
                            "unable to find partition {}",
                            partition.partition_index
                        ))?
                        .committed_offset;

                    metrics::gauge!("dekaf_committed_offset", "group_id"=>req.group_id.to_string(),"journal_name"=>journal_name).set(committed_offset as f64);
                }
            }
        }

        Ok(resp)
    }

    #[instrument(skip_all, fields(group=?req.group_id))]
    pub async fn offset_fetch(
        &mut self,
        req: messages::OffsetFetchRequest,
        header: RequestHeader,
    ) -> anyhow::Result<messages::OffsetFetchResponse> {
        let mut mutated_req = req.clone();
        if let Some(ref mut topics) = mutated_req.topics {
            for topic in topics {
                let encrypted = self.encrypt_topic_name(topic.name.clone());
                tracing::info!(topic_name = ?topic.name, encrypted_name = ?encrypted, "Fetching offset");
                topic.name = encrypted;
            }
        }

        let client = self
            .get_kafka_client()
            .await?
            .connect_to_group_coordinator(req.group_id.as_str())
            .await?;

        if let Some(ref topics) = mutated_req.topics {
            client
                .ensure_topics(topics.iter().map(|t| t.name.to_owned()).collect())
                .await?;
        }
        let mut resp = client.send_request(mutated_req, Some(header)).await?;

        for topic in resp.topics.iter_mut() {
            topic.name = self.decrypt_topic_name(topic.name.to_owned());
        }

        Ok(resp)
    }

    /// ApiVersions lists the APIs which are supported by this "broker".
    pub async fn api_versions(
        &mut self,
        _req: messages::ApiVersionsRequest,
    ) -> anyhow::Result<messages::ApiVersionsResponse> {
        use kafka_protocol::messages::{api_versions_response::ApiVersion, *};

        let client = self.get_kafka_client().await?;

        fn version<T: kafka_protocol::protocol::Message>(api_key: ApiKey) -> ApiVersion {
            ApiVersion::default()
                .with_api_key(api_key as i16)
                .with_max_version(T::VERSIONS.max)
                .with_min_version(T::VERSIONS.min)
        }
        let res = ApiVersionsResponse::default().with_api_keys(vec![
            version::<ApiVersionsRequest>(ApiKey::ApiVersionsKey),
            version::<SaslHandshakeRequest>(ApiKey::SaslHandshakeKey),
            version::<SaslAuthenticateRequest>(ApiKey::SaslAuthenticateKey),
            version::<MetadataRequest>(ApiKey::MetadataKey),
            ApiVersion::default()
                .with_api_key(ApiKey::FindCoordinatorKey as i16)
                .with_min_version(0)
                .with_max_version(2),
            version::<ListOffsetsRequest>(ApiKey::ListOffsetsKey),
            ApiVersion::default()
                .with_api_key(ApiKey::FetchKey as i16)
                // This is another non-obvious requirement in librdkafka. If we advertise <4 as a minimum here, some clients'
                // fetch requests will sit in a tight loop erroring over and over. This feels like a bug... but it's probably
                // just the consequence of convergent development, where some implicit requirement got encoded both in the client
                // and server without being explicitly documented anywhere.
                .with_min_version(4)
                // Version >= 13 did away with topic names in favor of unique topic UUIDs, so we need to stick below that.
                .with_max_version(12),
            // Needed by `kaf`.
            version::<DescribeConfigsRequest>(ApiKey::DescribeConfigsKey),
            ApiVersion::default()
                .with_api_key(ApiKey::ProduceKey as i16)
                .with_min_version(3)
                .with_max_version(9),
            client.supported_versions::<JoinGroupRequest>()?,
            client.supported_versions::<LeaveGroupRequest>()?,
            client.supported_versions::<ListGroupsRequest>()?,
            client.supported_versions::<SyncGroupRequest>()?,
            client.supported_versions::<DeleteGroupsRequest>()?,
            client.supported_versions::<HeartbeatRequest>()?,
            client.supported_versions::<OffsetCommitRequest>()?,
            ApiVersion::default()
                .with_api_key(ApiKey::OffsetFetchKey as i16)
                .with_min_version(0)
                .with_max_version(7),
        ]);

        // UNIMPLEMENTED:
        /*
            ApiKey::LeaderAndIsrKey,
            ApiKey::StopReplicaKey,
            ApiKey::CreateTopicsKey,
            ApiKey::DeleteTopicsKey,
        */

        Ok(res)
    }

    fn encrypt_topic_name(&self, name: TopicName) -> TopicName {
        to_upstream_topic_name(
            name,
            self.secret.to_owned(),
            match self.auth.as_ref().expect("Must be authenticated") {
                SessionAuthentication::User(auth) => auth.claims.sub.to_string(),
                SessionAuthentication::Task(auth) => auth.config.token.to_string(),
            },
        )
    }
    fn decrypt_topic_name(&self, name: TopicName) -> TopicName {
        from_upstream_topic_name(
            name,
            self.secret.to_owned(),
            match self.auth.as_ref().expect("Must be authenticated") {
                SessionAuthentication::User(auth) => auth.claims.sub.to_string(),
                SessionAuthentication::Task(auth) => auth.config.token.to_string(),
            },
        )
    }

    fn encode_topic_name(&self, name: String) -> TopicName {
        if match self.auth.as_ref().expect("Must be authenticated") {
            SessionAuthentication::User(auth) => auth.config.strict_topic_names,
            SessionAuthentication::Task(auth) => auth.config.strict_topic_names,
        } {
            to_downstream_topic_name(TopicName(StrBytes::from_string(name)))
        } else {
            TopicName(StrBytes::from_string(name))
        }
    }

    /// If the fetched offset is within a fixed number of offsets from the end of the journal,
    /// return Some with a PartitionOffset containing the beginning and end of the latest fragment.
    #[tracing::instrument(skip(self))]
    async fn is_fetch_data_preview(
        &mut self,
        collection_name: String,
        partition: i32,
        fetch_offset: i64,
    ) -> anyhow::Result<Option<PartitionOffset>> {
        let auth = self
            .auth
            .as_mut()
            .ok_or(anyhow::anyhow!("Session not authenticated"))?;

        let client = auth.flow_client(&self.app).await?.clone();

        tracing::debug!(
            "Loading latest offset for this partition to check if session is data-preview"
        );
        let collection = Collection::new(
            &self.app,
            auth,
            &client.pg_client(),
            collection_name.as_str(),
        )
        .await?
        .ok_or(anyhow::anyhow!("Collection {} not found", collection_name))?;

        if let Some(
            partition_offset @ PartitionOffset {
                offset: latest_offset,
                ..
            },
        ) = collection
            .fetch_partition_offset(partition as usize, -1)
            .await?
        {
            // If fetch_offset is >= latest_offset, this is a caught-up consumer
            // polling for new documents, not a data preview request.
            if fetch_offset < latest_offset && latest_offset - fetch_offset < 13 {
                tracing::info!(
                    latest_offset,
                    diff = latest_offset - fetch_offset,
                    "Marking session as data-preview"
                );
                Ok(Some(partition_offset))
            } else {
                tracing::debug!(
                    fetch_offset,
                    latest_offset,
                    diff = latest_offset - fetch_offset,
                    "Marking session as non-data-preview"
                );
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}
