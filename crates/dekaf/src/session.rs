use super::{App, Collection, Read};
use crate::{
    from_downstream_topic_name, from_upstream_topic_name, read::BatchResult,
    to_downstream_topic_name, to_upstream_topic_name, topology::fetch_all_collection_names,
    Authenticated,
};
use anyhow::Context;
use bytes::{BufMut, BytesMut};
use kafka_protocol::{
    error::{ParseResponseErrorCode, ResponseError},
    indexmap::IndexMap,
    messages::{
        self,
        metadata_response::{MetadataResponsePartition, MetadataResponseTopic},
        ConsumerProtocolAssignment, ConsumerProtocolSubscription, ListGroupsResponse,
        RequestHeader, TopicName,
    },
    protocol::{buf::ByteBuf, Decodable, Encodable, Message, StrBytes},
};
use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};
use std::{sync::Arc, time::Duration};
use tracing::instrument;

struct PendingRead {
    offset: i64,          // Journal offset to be completed by this PendingRead.
    last_write_head: i64, // Most-recent observed journal write head.
    handle: tokio_util::task::AbortOnDropHandle<anyhow::Result<(Read, BatchResult)>>,
}

pub struct Session {
    app: Arc<App>,
    reads: HashMap<(TopicName, i32), PendingRead>,
    secret: String,
    auth: Option<Authenticated>,
}

impl Session {
    pub fn new(app: Arc<App>, secret: String) -> Self {
        Self {
            app,
            reads: HashMap::new(),
            auth: None,
            secret,
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
                let claims = auth.claims.clone();
                self.auth.replace(auth);

                let mut response = messages::SaslAuthenticateResponse::default();
                response.session_lifetime_ms = (1000
                    * (claims.exp
                        - SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .context("")?
                            .as_secs()))
                .try_into()?;
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
        let mut brokers = kafka_protocol::indexmap::IndexMap::new();
        brokers.insert(
            messages::BrokerId(1),
            messages::metadata_response::MetadataResponseBroker::default()
                .with_host(StrBytes::from_string(self.app.advertise_host.clone()))
                .with_port(self.app.advertise_kafka_port as i32),
        );

        Ok(messages::MetadataResponse::default()
            .with_brokers(brokers)
            .with_cluster_id(Some(StrBytes::from_static_str("estuary-dekaf")))
            .with_controller_id(messages::BrokerId(1))
            .with_topics(topics))
    }

    // Lists all read-able collections as Kafka topics. Omits partition metadata.
    async fn metadata_all_topics(
        &mut self,
    ) -> anyhow::Result<IndexMap<TopicName, MetadataResponseTopic>> {
        let collections = fetch_all_collection_names(
            &self
                .auth
                .as_mut()
                .ok_or(anyhow::anyhow!("Session not authenticated"))?
                .get_client()
                .await?
                .pg_client(),
        )
        .await?;

        tracing::debug!(collections=?ops::DebugJson(&collections), "fetched all collections");

        let topics = collections
            .into_iter()
            .map(|name| {
                (
                    self.encode_topic_name(name),
                    MetadataResponseTopic::default()
                        .with_is_internal(false)
                        .with_partitions(vec![MetadataResponsePartition::default()
                            .with_partition_index(0)
                            .with_leader_id(0.into())]),
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
        let client = self
            .auth
            .as_mut()
            .ok_or(anyhow::anyhow!("Session not authenticated"))?
            .get_client()
            .await?;

        // Concurrently fetch Collection instances for all requested topics.
        let collections: anyhow::Result<Vec<(TopicName, Option<Collection>)>> =
            tokio::time::timeout(
                Duration::from_secs(10),
                futures::future::try_join_all(requests.into_iter().map(|topic| async move {
                    let maybe_collection = Collection::new(
                        client,
                        from_downstream_topic_name(topic.name.to_owned().unwrap_or_default())
                            .as_str(),
                    )
                    .await?;
                    Ok((topic.name.unwrap_or_default(), maybe_collection))
                })),
            )
            .await
            .map_err(|e| anyhow::anyhow!("Timed out loading metadata {e}"))?;

        let mut topics = IndexMap::new();

        for (name, maybe_collection) in collections? {
            let Some(collection) = maybe_collection else {
                topics.insert(
                    self.encode_topic_name(name.to_string()),
                    MetadataResponseTopic::default()
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

            topics.insert(
                name,
                MetadataResponseTopic::default()
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
        let client = self
            .auth
            .as_mut()
            .ok_or(anyhow::anyhow!("Session not authenticated"))?
            .get_client()
            .await?;

        // Concurrently fetch Collection instances and offsets for all requested topics and partitions.
        // Map each "topic" into Vec<(Partition Index, Option<(Journal Offset, Timestamp))>.
        let collections: anyhow::Result<Vec<(TopicName, Vec<(i32, Option<(i64, i64)>)>)>> =
            futures::future::try_join_all(request.topics.into_iter().map(|topic| async move {
                let maybe_collection = Collection::new(
                    client,
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
                        let Some((offset, timestamp)) = maybe_offset else {
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

        let client = self
            .auth
            .as_mut()
            .ok_or(anyhow::anyhow!("Session not authenticated"))?
            .get_client()
            .await?;

        let timeout_at =
            std::time::Instant::now() + std::time::Duration::from_millis(max_wait_ms as u64);

        let mut hit_timeout = false;

        // Start reads for all partitions which aren't already pending.
        for topic_request in &topic_requests {
            let mut key = (from_downstream_topic_name(topic_request.topic.clone()), 0);

            for partition_request in &topic_request.partitions {
                key.1 = partition_request.partition;

                let fetch_offset = partition_request.fetch_offset;

                if matches!(self.reads.get(&key), Some(pending) if pending.offset == fetch_offset) {
                    continue; // Common case: fetch is at the pending offset.
                }
                let Some(collection) = Collection::new(client, &key.0).await? else {
                    tracing::debug!(collection = ?&key.0, "Collection doesn't exist!");
                    continue; // Collection doesn't exist.
                };
                let Some(partition) = collection
                    .partitions
                    .get(partition_request.partition as usize)
                else {
                    tracing::debug!(collection = ?&key.0, partition=partition_request.partition, "Partition doesn't exist!");
                    continue; // Partition doesn't exist.
                };
                let (key_schema_id, value_schema_id) = collection
                    .registered_schema_ids(&client.pg_client())
                    .await?;

                let read: Read = Read::new(
                    collection.journal_client.clone(),
                    &collection,
                    partition,
                    fetch_offset,
                    key_schema_id,
                    value_schema_id,
                );
                let pending = PendingRead {
                    offset: fetch_offset,
                    last_write_head: fetch_offset,
                    handle: tokio_util::task::AbortOnDropHandle::new(tokio::spawn(
                        read.next_batch(partition_request.partition_max_bytes as usize, timeout_at),
                    )),
                };

                tracing::info!(
                    journal = &partition.spec.name,
                    key_schema_id,
                    value_schema_id,
                    fetch_offset,
                    "started read",
                );

                if let Some(old) = self.reads.insert(key.clone(), pending) {
                    tracing::warn!(
                        topic = topic_request.topic.as_str(),
                        partition = partition_request.partition,
                        old_offset = old.offset,
                        new_offset = fetch_offset,
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

                let Some(pending) = self.reads.get_mut(&key) else {
                    partition_responses.push(
                        PartitionData::default()
                            .with_partition_index(partition_request.partition)
                            .with_error_code(ResponseError::UnknownTopicOrPartition.code()),
                    );
                    continue;
                };

                let (read, batch) = (&mut pending.handle).await??;
                pending.offset = read.offset;
                pending.last_write_head = read.last_write_head;
                pending.handle = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(
                    read.next_batch(partition_request.partition_max_bytes as usize, timeout_at),
                ));

                let (timeout, batch) = match batch {
                    BatchResult::TargetExceededBeforeTimeout(b) => (false, Some(b)),
                    BatchResult::TimeoutExceededBeforeTarget(b) => (true, Some(b)),
                    BatchResult::TimeoutNoData => (true, None),
                };

                if timeout {
                    hit_timeout = true
                }

                partition_responses.push(
                    PartitionData::default()
                        .with_partition_index(partition_request.partition)
                        .with_records(batch.to_owned())
                        .with_high_watermark(pending.last_write_head) // Map to kafka cursor.
                        .with_last_stable_offset(pending.last_write_head),
                );
            }

            topic_responses.push(
                FetchableTopicResponse::default()
                    .with_topic(topic_request.topic.clone())
                    .with_partitions(partition_responses),
            );
        }

        Ok(messages::FetchResponse::default()
            .with_session_id(session_id)
            .with_throttle_time_ms(if hit_timeout { 10000 } else { 0 })
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
            .map(|(k, v)| {
                (
                    k,
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
                    ),
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
        let client = self
            .app
            .kafka_client
            .connect_to_group_coordinator(req.group_id.as_str())
            .await?;

        let mut mutable_req = req.clone();
        for (_, protocol) in mutable_req.protocols.iter_mut() {
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
                .for_each(|topic| *topic = self.encrypt_topic_name(topic.to_owned().into()).into());

            let mut new_protocol_subscription = BytesMut::new();

            new_protocol_subscription.put_i16(consumer_protocol_subscription_version);
            consumer_protocol_subscription_msg.encode(
                &mut new_protocol_subscription,
                consumer_protocol_subscription_version,
            )?;

            protocol.metadata = new_protocol_subscription.into();
        }

        let response = client
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
            .app
            .kafka_client
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
        let r = self.app.kafka_client.send_request(req, Some(header)).await;
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
        let client = self
            .app
            .kafka_client
            .connect_to_group_coordinator(req.group_id.as_str())
            .await?;

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
                .map(|(name, item)| (self.encrypt_topic_name(name.to_owned().into()).into(), item))
                .collect();

            let mut new_protocol_assignment = BytesMut::new();

            new_protocol_assignment.put_i16(consumer_protocol_assignment_version);
            consumer_protocol_assignment_msg.encode(
                &mut new_protocol_assignment,
                consumer_protocol_assignment_version,
            )?;
            assignment.assignment = new_protocol_assignment.into();
        }

        let response = client
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
            .map(|(name, item)| (self.decrypt_topic_name(name.to_owned().into()).into(), item))
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
        return self.app.kafka_client.send_request(req, Some(header)).await;
    }

    #[instrument(skip_all, fields(group=?req.group_id))]
    pub async fn heartbeat(
        &mut self,
        req: messages::HeartbeatRequest,
        header: RequestHeader,
    ) -> anyhow::Result<messages::HeartbeatResponse> {
        let client = self
            .app
            .kafka_client
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
            topic.name = self.encrypt_topic_name(topic.name.clone())
        }

        let client = self
            .app
            .kafka_client
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
                topic.name = self.encrypt_topic_name(topic.name.clone())
            }
        }

        let client = self
            .app
            .kafka_client
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
            ApiVersion::default()
                .with_min_version(0)
                .with_max_version(2),
        );
        res.api_keys.insert(
            ApiKey::ListOffsetsKey as i16,
            version::<ListOffsetsRequest>(),
        );
        res.api_keys.insert(
            ApiKey::FetchKey as i16,
            ApiVersion::default()
                // This is another non-obvious requirement in librdkafka. If we advertise <4 as a minimum here, some clients'
                // fetch requests will sit in a tight loop erroring over and over. This feels like a bug... but it's probably
                // just the consequence of convergent development, where some implicit requirement got encoded both in the client
                // and server without being explicitly documented anywhere.
                .with_min_version(4)
                // I don't understand why, but some kafka clients don't seem to be able to send flexver fetch requests correctly
                // For example, `kcat` sends an empty topic name when >= v12. I'm 99% sure there's more to this however.
                .with_max_version(11),
        );

        // Needed by `kaf`.
        res.api_keys.insert(
            ApiKey::DescribeConfigsKey as i16,
            version::<DescribeConfigsRequest>(),
        );

        res.api_keys.insert(
            ApiKey::ProduceKey as i16,
            ApiVersion::default()
                .with_min_version(3)
                .with_max_version(9),
        );

        res.api_keys.insert(
            ApiKey::JoinGroupKey as i16,
            self.app
                .kafka_client
                .supported_versions::<JoinGroupRequest>()?,
        );
        res.api_keys.insert(
            ApiKey::LeaveGroupKey as i16,
            self.app
                .kafka_client
                .supported_versions::<LeaveGroupRequest>()?,
        );
        res.api_keys.insert(
            ApiKey::ListGroupsKey as i16,
            self.app
                .kafka_client
                .supported_versions::<ListGroupsRequest>()?,
        );
        res.api_keys.insert(
            ApiKey::SyncGroupKey as i16,
            self.app
                .kafka_client
                .supported_versions::<SyncGroupRequest>()?,
        );
        res.api_keys.insert(
            ApiKey::DeleteGroupsKey as i16,
            self.app
                .kafka_client
                .supported_versions::<DeleteGroupsRequest>()?,
        );
        res.api_keys.insert(
            ApiKey::HeartbeatKey as i16,
            self.app
                .kafka_client
                .supported_versions::<HeartbeatRequest>()?,
        );

        res.api_keys.insert(
            ApiKey::OffsetCommitKey as i16,
            self.app
                .kafka_client
                .supported_versions::<OffsetCommitRequest>()?,
        );
        res.api_keys.insert(
            ApiKey::OffsetFetchKey as i16,
            ApiVersion::default()
                .with_min_version(0)
                .with_max_version(7),
        );

        // UNIMPLEMENTED:
        /*
        res.api_keys.insert(
            ApiKey::LeaderAndIsrKey as i16,
            version::<LeaderAndIsrRequest>(),
        );
        res.api_keys.insert(
            ApiKey::StopReplicaKey as i16,
            version::<StopReplicaRequest>(),
        );
        res.api_keys.insert(
            ApiKey::CreateTopicsKey as i16,
            version::<CreateTopicsRequest>(),
        );
        res.api_keys.insert(
            ApiKey::DeleteTopicsKey as i16,
            version::<DeleteTopicsRequest>(),
        );
        */

        Ok(res)
    }

    fn encrypt_topic_name(&self, name: TopicName) -> TopicName {
        to_upstream_topic_name(
            name,
            self.secret.to_owned(),
            self.auth
                .as_ref()
                .expect("Must be authenticated")
                .claims
                .sub
                .to_string(),
        )
    }
    fn decrypt_topic_name(&self, name: TopicName) -> TopicName {
        from_upstream_topic_name(
            name,
            self.secret.to_owned(),
            self.auth
                .as_ref()
                .expect("Must be authenticated")
                .claims
                .sub
                .to_string(),
        )
    }

    fn encode_topic_name(&self, name: String) -> TopicName {
        if self
            .auth
            .as_ref()
            .expect("Must be authenticated")
            .task_config
            .strict_topic_names
        {
            to_downstream_topic_name(TopicName(StrBytes::from_string(name)))
        } else {
            TopicName(StrBytes::from_string(name))
        }
    }
}
