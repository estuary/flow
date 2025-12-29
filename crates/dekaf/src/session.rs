use super::{App, Collection, CollectionStatus, Read};
use crate::{
    DekafError, KafkaApiClient, KafkaClientAuth, SessionAuthentication, TaskState,
    from_downstream_topic_name, from_upstream_topic_name, logging::propagate_task_forwarder,
    read::BatchResult, to_downstream_topic_name, to_upstream_topic_name, topology::PartitionOffset,
};
use anyhow::{Context, bail};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::{
    error::{ParseResponseErrorCode, ResponseError},
    messages::{
        self, ConsumerProtocolAssignment, ConsumerProtocolSubscription, ListGroupsResponse,
        RequestHeader, TopicName,
        metadata_response::{
            MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
        },
    },
    protocol::{Decodable, Encodable, Message, StrBytes},
};
use std::{cmp::max, sync::Arc};
use std::{
    collections::{HashMap, hash_map::Entry},
    time::SystemTime,
};
use tracing::instrument;

struct PendingRead {
    offset: i64,          // Journal offset to be completed by this PendingRead.
    last_write_head: i64, // Most-recent observed journal write head.
    leader_epoch: i32,    // Leader epoch (binding backfill counter) for this read.
    handle: tokio_util::task::AbortOnDropHandle<anyhow::Result<(Read, BatchResult)>>,
}

#[derive(Clone, Debug)]
enum SessionDataPreviewState {
    Unknown,
    NotDataPreview,
    DataPreview(HashMap<(TopicName, i32), PartitionOffset>),
}

/// Outcome of fetching an offset for a single partition.
enum PartitionOffsetOutcome {
    Success(PartitionOffset),
    /// Consumer's epoch is behind the current epoch
    Fenced,
    /// Consumer's epoch is ahead of the current epoch
    UnknownEpoch,
    /// Partition index is out of range
    PartitionNotFound,
}

pub struct Session {
    app: Arc<App>,
    client: Option<KafkaApiClient>,
    reads: HashMap<(TopicName, i32), (PendingRead, std::time::Instant)>,
    secret: String,
    auth: Option<SessionAuthentication>,
    data_preview_state: SessionDataPreviewState,
    broker_urls: Vec<String>,
    upstream_auth: KafkaClientAuth,
    // Number of ReadResponses to buffer in PendingReads
    read_buffer_size: usize,
}

impl Session {
    pub fn new(
        app: Arc<App>,
        secret: String,
        broker_urls: Vec<String>,
        upstream_auth: KafkaClientAuth,
        read_buffer_size: usize,
    ) -> Self {
        Self {
            app,
            client: None,
            broker_urls,
            upstream_auth,
            read_buffer_size,
            reads: HashMap::new(),
            auth: None,
            secret,
            data_preview_state: SessionDataPreviewState::Unknown,
        }
    }

    /// Helper function to check if an error is a TaskRedirected error
    fn is_redirect_error(err: &anyhow::Error) -> bool {
        err.downcast_ref::<crate::DekafError>().map_or(false, |e| {
            matches!(e, crate::DekafError::TaskRedirected { .. })
        })
    }

    /// For redirected tasks, this returns the target dataplane's broker address.
    async fn get_redirect_address(&self) -> anyhow::Result<Option<(String, i32)>> {
        let fqdn = match self.auth.as_ref() {
            Some(SessionAuthentication::Task(auth)) => {
                match auth.task_state_listener.get().await?.as_ref() {
                    TaskState::Redirect {
                        target_dataplane_fqdn,
                        ..
                    } => anyhow::Ok(Some(target_dataplane_fqdn.clone())),
                    _ => Ok(None),
                }
            }
            Some(SessionAuthentication::Redirect {
                target_dataplane_fqdn,
                ..
            }) => Ok(Some(target_dataplane_fqdn.clone())),
            _ => Ok(None),
        }?;

        if let Some(fqdn) = fqdn {
            return Ok(Some((
                format!("dekaf.{fqdn}"),
                self.app.advertise_kafka_port as i32,
            )));
        }
        Ok(None)
    }

    async fn get_kafka_client(&mut self) -> anyhow::Result<&mut KafkaApiClient> {
        if let Some(ref mut client) = self.client {
            Ok(client)
        } else {
            if self.auth.is_none() {
                anyhow::bail!("Must be authenticated");
            }

            self.client.replace(
                KafkaApiClient::connect(&self.broker_urls, self.upstream_auth.clone())
                    .await
                    .context("failed to connect or authenticate to upstream Kafka broker used for serving group management APIs")?
            );
            Ok(self.client.as_mut().expect("guaranteed to exist"))
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
        let username = it.next().context("expected SASL authcid (username)")??;
        let password = it.next().context("expected SASL password")??;

        match &self.auth {
            Some(SessionAuthentication::Task(auth)) if auth.task_name != username => {
                return Ok(messages::SaslAuthenticateResponse::default()
                    .with_error_code(ResponseError::SaslAuthenticationFailed.code())
                    .with_error_message(Some(StrBytes::from_string(
                        "Session cannot be reauthenticated with a different username".to_string(),
                    ))));
            }
            _ => {}
        };

        let mut attempts = 0;
        loop {
            let response = match self.app.authenticate(username, password).await {
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
                Err(DekafError::Authentication(e)) => messages::SaslAuthenticateResponse::default()
                    .with_error_code(ResponseError::SaslAuthenticationFailed.code())
                    .with_error_message(Some(StrBytes::from_string(format!("{e}")))),
                Err(DekafError::TaskRedirected { .. }) => {
                    unreachable!("This error should not be returned here.")
                }
                Err(DekafError::Unknown(e)) => {
                    tracing::warn!(
                        ?attempts,
                        "unknown error during session authentication: {:?}",
                        e
                    );
                    if attempts < 4 {
                        attempts += 1;
                        tokio::time::sleep(std::time::Duration::from_secs(3 * attempts)).await;
                        continue;
                    }
                    messages::SaslAuthenticateResponse::default()
                        .with_error_code(ResponseError::UnknownServerError.code())
                }
            };

            return Ok(response);
        }
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
            Some(topics) if !topics.is_empty() => self.metadata_select_topics(topics).await,
            _ => self.metadata_all_topics().await,
        }?;

        // If the session needs to be redirected, this causes the consumer to
        // connect to the correct Dekaf instance by advertising it as the
        // only broker in the response. Otherwise advertise ourselves as the broker.
        let broker = if let Some((broker_host, broker_port)) = self.get_redirect_address().await? {
            MetadataResponseBroker::default()
                .with_node_id(messages::BrokerId(1))
                .with_host(StrBytes::from_string(broker_host))
                .with_port(broker_port)
        } else {
            MetadataResponseBroker::default()
                .with_node_id(messages::BrokerId(1))
                .with_host(StrBytes::from_string(self.app.advertise_host.clone()))
                .with_port(self.app.advertise_kafka_port as i32)
        };

        Ok(messages::MetadataResponse::default()
            .with_brokers(vec![broker])
            .with_cluster_id(Some(StrBytes::from_static_str("estuary-dekaf")))
            .with_controller_id(messages::BrokerId(1))
            .with_topics(topics))
    }

    // Lists all read-able collections as Kafka topics
    async fn metadata_all_topics(&mut self) -> anyhow::Result<Vec<MetadataResponseTopic>> {
        let collection_names = self
            .auth
            .as_mut()
            .ok_or(anyhow::anyhow!("Session not authenticated"))?
            .fetch_all_collection_names()
            .await?;
        tracing::debug!(collections=?ops::DebugJson(&collection_names), "fetched all collections");

        let collection_statuses = self
            .fetch_collections_for_metadata(collection_names)
            .await?;

        collection_statuses
            .into_iter()
            .map(|(name, status)| {
                let encoded_name = self.encode_topic_name(name.clone())?;
                match status {
                    CollectionStatus::Ready(coll) => self.build_topic_metadata(encoded_name, &coll),
                    CollectionStatus::NotFound => {
                        anyhow::bail!("Collection '{}' not found or not accessible", name)
                    }
                    CollectionStatus::NotReady => {
                        // Collection exists but journals aren't available - return LeaderNotAvailable so clients will retry
                        Ok(MetadataResponseTopic::default()
                            .with_name(Some(encoded_name))
                            .with_error_code(ResponseError::LeaderNotAvailable.code()))
                    }
                }
            })
            .collect()
    }

    // Lists partitions of specific, requested collections.
    async fn metadata_select_topics(
        &mut self,
        requests: Vec<messages::metadata_request::MetadataRequestTopic>,
    ) -> anyhow::Result<Vec<MetadataResponseTopic>> {
        let topics: anyhow::Result<_> = async {
            let names: Vec<_> = requests
                .iter()
                .map(|t| from_downstream_topic_name(t.name.clone().unwrap_or_default()).to_string())
                .collect();

            let collection_statuses = self.fetch_collections_for_metadata(names).await?;

            requests
                .iter()
                .zip(collection_statuses)
                .map(|(request, (_, status))| {
                    let topic_name = request.name.to_owned().ok_or_else(|| {
                        anyhow::anyhow!("Topic name is missing in metadata request")
                    })?;

                    match status {
                        CollectionStatus::Ready(collection) => {
                            self.build_topic_metadata(topic_name, &collection)
                        }
                        CollectionStatus::NotFound => Ok(MetadataResponseTopic::default()
                            .with_name(Some(self.encode_topic_name(topic_name.to_string())?))
                            .with_error_code(ResponseError::UnknownTopicOrPartition.code())),
                        CollectionStatus::NotReady => Ok(MetadataResponseTopic::default()
                            .with_name(Some(self.encode_topic_name(topic_name.to_string())?))
                            .with_error_code(ResponseError::LeaderNotAvailable.code())),
                    }
                })
                .collect()
        }
        .await;

        match topics {
            Ok(topics) => Ok(topics),
            Err(e) if Self::is_redirect_error(&e) => {
                // For redirects, return minimal metadata with single partition.
                // The consumer will fetch the full metadata from the target broker.
                Ok(requests
                    .into_iter()
                    .map(|req| {
                        MetadataResponseTopic::default()
                            .with_name(req.name)
                            .with_is_internal(false)
                            .with_partitions(vec![Self::build_partition(0, 0)])
                    })
                    .collect())
            }
            Err(e) => Err(e),
        }
    }

    /// FindCoordinator always responds with our single logical broker.
    pub async fn find_coordinator(
        &mut self,
        request: messages::FindCoordinatorRequest,
    ) -> anyhow::Result<messages::FindCoordinatorResponse> {
        let (broker_host, broker_port) = match self.get_redirect_address().await? {
            Some((host, port)) => (host, port),
            None => (
                self.app.advertise_host.clone(),
                self.app.advertise_kafka_port as i32,
            ),
        };

        let coordinators = request
            .coordinator_keys
            .iter()
            .map(|_key| {
                messages::find_coordinator_response::Coordinator::default()
                    .with_node_id(messages::BrokerId(1))
                    .with_host(StrBytes::from_string(broker_host.clone()))
                    .with_port(broker_port)
            })
            .collect();

        Ok(messages::FindCoordinatorResponse::default()
            .with_node_id(messages::BrokerId(1))
            .with_host(StrBytes::from_string(broker_host))
            .with_port(broker_port)
            .with_coordinators(coordinators))
    }

    async fn fetch_partition_offset(
        collection: &Collection,
        current_epoch: i32,
        partition: messages::list_offsets_request::ListOffsetsPartition,
    ) -> anyhow::Result<(i32, PartitionOffsetOutcome)> {
        if partition.current_leader_epoch >= 0 && partition.current_leader_epoch < current_epoch {
            return Ok((partition.partition_index, PartitionOffsetOutcome::Fenced));
        }

        if partition.current_leader_epoch > current_epoch {
            return Ok((
                partition.partition_index,
                PartitionOffsetOutcome::UnknownEpoch,
            ));
        }

        let outcome = match collection
            .fetch_partition_offset(partition.partition_index as usize, partition.timestamp)
            .await?
        {
            Some(offset) => PartitionOffsetOutcome::Success(offset),
            None => PartitionOffsetOutcome::PartitionNotFound,
        };

        Ok((partition.partition_index, outcome))
    }

    /// Fetch offsets for all partitions of a topic and build the response.
    async fn list_topic_offsets(
        auth: &SessionAuthentication,
        topic: &messages::list_offsets_request::ListOffsetsTopic,
    ) -> anyhow::Result<messages::list_offsets_response::ListOffsetsTopicResponse> {
        use messages::list_offsets_response::{
            ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
        };

        let maybe_collection = match Collection::new(
            auth,
            from_downstream_topic_name(topic.name.clone()).as_str(),
        )
        .await?
        {
            CollectionStatus::Ready(c) => Ok(c),
            CollectionStatus::NotFound => Err(ResponseError::UnknownTopicOrPartition.code()),
            CollectionStatus::NotReady => {
                // Collection exists but journals aren't available yet - return LeaderNotAvailable
                // so clients will retry
                Err(ResponseError::LeaderNotAvailable.code())
            }
        };

        let collection = match maybe_collection {
            Ok(c) => c,
            Err(error_code) => {
                let partitions = topic
                    .partitions
                    .iter()
                    .map(|partition| {
                        ListOffsetsPartitionResponse::default()
                            .with_partition_index(partition.partition_index)
                            .with_error_code(error_code)
                    })
                    .collect();

                return Ok(ListOffsetsTopicResponse::default()
                    .with_name(topic.name.clone())
                    .with_partitions(partitions));
            }
        };

        let current_epoch = collection.binding_backfill_counter as i32;

        let partition_results =
            futures::future::try_join_all(topic.partitions.iter().cloned().map(|partition| {
                Self::fetch_partition_offset(&collection, current_epoch, partition)
            }))
            .await?;

        let partitions = partition_results
            .into_iter()
            .map(|(partition_index, outcome)| match outcome {
                PartitionOffsetOutcome::Success(PartitionOffset {
                    offset,
                    mod_time: timestamp,
                    ..
                }) => ListOffsetsPartitionResponse::default()
                    .with_partition_index(partition_index)
                    .with_offset(offset)
                    .with_timestamp(timestamp)
                    .with_leader_epoch(current_epoch),
                PartitionOffsetOutcome::Fenced => ListOffsetsPartitionResponse::default()
                    .with_partition_index(partition_index)
                    .with_error_code(ResponseError::FencedLeaderEpoch.code())
                    .with_leader_epoch(current_epoch),
                PartitionOffsetOutcome::UnknownEpoch => ListOffsetsPartitionResponse::default()
                    .with_partition_index(partition_index)
                    .with_error_code(ResponseError::UnknownLeaderEpoch.code())
                    .with_leader_epoch(current_epoch),
                PartitionOffsetOutcome::PartitionNotFound => {
                    ListOffsetsPartitionResponse::default()
                        .with_partition_index(partition_index)
                        .with_error_code(ResponseError::UnknownTopicOrPartition.code())
                }
            })
            .collect();

        Ok(ListOffsetsTopicResponse::default()
            .with_name(topic.name.clone())
            .with_partitions(partitions))
    }

    pub async fn list_offsets(
        &mut self,
        request: messages::ListOffsetsRequest,
    ) -> anyhow::Result<messages::ListOffsetsResponse> {
        let result: anyhow::Result<_> = async {
            let auth = self.auth.as_ref().unwrap();

            let topics = futures::future::try_join_all(
                request
                    .topics
                    .iter()
                    .map(|topic| Self::list_topic_offsets(auth, topic)),
            )
            .await?;

            Ok(messages::ListOffsetsResponse::default().with_topics(topics))
        }
        .await;

        match result {
            Ok(response) => Ok(response),
            Err(e) if Self::is_redirect_error(&e) => {
                let topics = request
                    .topics
                    .into_iter()
                    .map(|topic| {
                        messages::list_offsets_response::ListOffsetsTopicResponse::default()
                            .with_name(topic.name)
                            .with_partitions(
                                topic
                                    .partitions
                                    .into_iter()
                                    .map(|p| {
                                        messages::list_offsets_response::ListOffsetsPartitionResponse::default()
                                            .with_partition_index(p.partition_index)
                                            .with_error_code(ResponseError::NotLeaderOrFollower.code())
                                    })
                                    .collect(),
                            )
                    })
                    .collect();
                Ok(messages::ListOffsetsResponse::default().with_topics(topics))
            }
            Err(e) => Err(e),
        }
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

        if self.get_redirect_address().await?.is_some() {
            let responses = request
                .topics
                .iter()
                .map(|topic_req| {
                    let partitions = topic_req
                        .partitions
                        .iter()
                        .map(|p| {
                            PartitionData::default()
                                .with_partition_index(p.partition)
                                .with_error_code(ResponseError::NotLeaderOrFollower.code())
                        })
                        .collect();
                    FetchableTopicResponse::default()
                        .with_topic(topic_req.topic.clone())
                        .with_partitions(partitions)
                })
                .collect();
            return Ok(messages::FetchResponse::default().with_responses(responses));
        }

        let task_name = match &self.auth {
            Some(SessionAuthentication::Task(auth)) => auth.task_name.clone(),
            Some(SessionAuthentication::Redirect { .. }) => {
                bail!("Redirected sessions cannot fetch data")
            }
            None => bail!("Not authenticated"),
        };

        let messages::FetchRequest {
            topics: ref topic_requests,
            max_bytes: _, // Ignored.
            max_wait_ms,
            min_bytes: _, // Ignored.
            session_id,
            ..
        } = request;

        let timeout = std::time::Duration::from_millis(max_wait_ms as u64);

        // Start reads for all partitions which aren't already pending.
        for topic_request in topic_requests {
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
                                    bail!(
                                        "Session was used for fetching preview data, cannot be used for fetching non-preview data."
                                    )
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
                                    bail!(
                                        "Session was used for fetching preview data, cannot be used for fetching non-preview data."
                                    )
                                }
                            }
                        }
                    }
                };

                let pending_info = self
                    .reads
                    .get(&key)
                    .map(|(p, s)| (p.offset, p.leader_epoch, s.elapsed()));

                match pending_info {
                    Some((_, _, elapsed)) if elapsed > std::time::Duration::from_secs(60 * 5) => {
                        metrics::counter!(
                            "dekaf_fetch_requests",
                            "topic_name" => key.0.to_string(),
                            "partition_index" => key.1.to_string(),
                            "task_name" => task_name.to_string(),
                            "state" => "read_expired"
                        )
                        .increment(1);
                        tracing::debug!(lifetime=?elapsed, topic_name=?key.0,partition_index=?key.1, "Restarting expired Read");
                        self.reads.remove(&key);
                    }
                    Some((pending_offset, pending_epoch, _)) if pending_offset == fetch_offset => {
                        // Validate pending read's epoch is still current
                        let auth = self.auth.as_ref().unwrap();
                        let current_epoch = match Collection::new(&auth, &key.0).await? {
                            CollectionStatus::Ready(c) => Some(c.binding_backfill_counter as i32),
                            // If NotReady or NotFound, remove the pending read
                            CollectionStatus::NotReady | CollectionStatus::NotFound => None,
                        };

                        match current_epoch {
                            Some(epoch) if pending_epoch < epoch => {
                                metrics::counter!(
                                    "dekaf_fetch_requests",
                                    "topic_name" => key.0.to_string(),
                                    "partition_index" => key.1.to_string(),
                                    "task_name" => task_name.to_string(),
                                    "state" => "pending_read_epoch_stale"
                                )
                                .increment(1);
                                tracing::info!(
                                    topic_name=?key.0,
                                    partition_index=?key.1,
                                    pending_epoch,
                                    current_epoch = epoch,
                                    "Pending read epoch is stale, removing"
                                );
                                self.reads.remove(&key);
                            }
                            Some(_) => {
                                metrics::counter!(
                                    "dekaf_fetch_requests",
                                    "topic_name" => key.0.to_string(),
                                    "partition_index" => key.1.to_string(),
                                    "task_name" => task_name.to_string(),
                                    "state" => "read_pending"
                                )
                                .increment(1);
                                continue; // Valid pending read at correct offset and epoch
                            }
                            None => {
                                // Collection no longer exists
                                self.reads.remove(&key);
                            }
                        }
                    }
                    _ => {}
                }

                let auth = self.auth.as_mut().unwrap();
                let pg_client = match auth.flow_client().await {
                    Ok(client) => client.pg_client(),
                    Err(crate::DekafError::TaskRedirected { .. }) => {
                        // Task was redirected mid-fetch, stop processing and return redirect response
                        let responses = request
                            .topics
                            .iter()
                            .map(|topic_req| {
                                let partitions = topic_req
                                    .partitions
                                    .iter()
                                    .map(|p| {
                                        PartitionData::default()
                                            .with_partition_index(p.partition)
                                            .with_error_code(
                                                ResponseError::NotLeaderOrFollower.code(),
                                            )
                                    })
                                    .collect();
                                FetchableTopicResponse::default()
                                    .with_topic(topic_req.topic.clone())
                                    .with_partitions(partitions)
                            })
                            .collect();
                        return Ok(messages::FetchResponse::default().with_responses(responses));
                    }
                    Err(e) => return Err(e.into()),
                };
                let collection = match Collection::new(&auth, &key.0).await? {
                    CollectionStatus::Ready(c) => c,
                    CollectionStatus::NotFound => {
                        metrics::counter!(
                            "dekaf_fetch_requests",
                            "topic_name" => key.0.to_string(),
                            "partition_index" => key.1.to_string(),
                            "task_name" => task_name.to_string(),
                            "state" => "collection_not_found"
                        )
                        .increment(1);
                        tracing::debug!(collection = ?&key.0, "Collection doesn't exist!");
                        self.reads.remove(&key);
                        continue;
                    }
                    CollectionStatus::NotReady => {
                        metrics::counter!(
                            "dekaf_fetch_requests",
                            "topic_name" => key.0.to_string(),
                            "partition_index" => key.1.to_string(),
                            "task_name" => task_name.to_string(),
                            "state" => "collection_not_ready"
                        )
                        .increment(1);
                        tracing::debug!(collection = ?&key.0, "Collection not ready (no journals)");
                        self.reads.remove(&key);
                        continue;
                    }
                };

                // Validate consumer's leader epoch against current collection epoch
                if partition_request.current_leader_epoch >= 0 {
                    if partition_request.current_leader_epoch
                        < collection.binding_backfill_counter as i32
                    {
                        metrics::counter!(
                            "dekaf_fetch_requests",
                            "topic_name" => key.0.to_string(),
                            "partition_index" => key.1.to_string(),
                            "task_name" => task_name.to_string(),
                            "state" => "fenced_leader_epoch"
                        )
                        .increment(1);
                        tracing::info!(
                            collection = ?&key.0,
                            partition = partition_request.partition,
                            consumer_epoch = partition_request.current_leader_epoch,
                            current_epoch = collection.binding_backfill_counter,
                            "Consumer epoch is stale, skipping read start"
                        );
                        // Remove stale pending read if it exists. Error will be returned during poll phase
                        self.reads.remove(&key);
                        continue;
                    } else if partition_request.current_leader_epoch
                        > collection.binding_backfill_counter as i32
                    {
                        metrics::counter!(
                            "dekaf_fetch_requests",
                            "topic_name" => key.0.to_string(),
                            "partition_index" => key.1.to_string(),
                            "task_name" => task_name.to_string(),
                            "state" => "unknown_leader_epoch"
                        )
                        .increment(1);
                        tracing::info!(
                            collection = ?&key.0,
                            partition = partition_request.partition,
                            consumer_epoch = partition_request.current_leader_epoch,
                            current_epoch = collection.binding_backfill_counter,
                            "Consumer epoch is ahead of broker epoch, skipping read start"
                        );
                        self.reads.remove(&key);
                        continue;
                    }
                }

                let Some(partition) = collection
                    .partitions
                    .get(partition_request.partition as usize)
                else {
                    metrics::counter!(
                        "dekaf_fetch_requests",
                        "topic_name" => key.0.to_string(),
                        "partition_index" => key.1.to_string(),
                        "task_name" => task_name.to_string(),
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
                    leader_epoch: collection.binding_backfill_counter as i32,
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
                                "task_name" => task_name.to_string(),
                                "state" => "new_data_preview_read"
                            )
                            .increment(1);
                            tokio::spawn(propagate_task_forwarder(
                                Read::new(
                                    self.app.task_manager.get_listener(task_name.as_str()),
                                    &collection,
                                    partition,
                                    fragment_start,
                                    key_schema_id,
                                    value_schema_id,
                                    Some(partition_request.fetch_offset - 1),
                                    &auth,
                                    self.read_buffer_size,
                                )
                                .await?
                                .next_batch(
                                    // Have to read at least 2 docs, as the very last doc
                                    // will probably be a control document and will be
                                    // ignored by the consumer, looking like 0 docs were read
                                    crate::read::ReadTarget::Docs(max(diff as usize, 2)),
                                    timeout,
                                ),
                            ))
                        }
                        _ => {
                            metrics::counter!(
                                "dekaf_fetch_requests",
                                "topic_name" => key.0.to_string(),
                                "partition_index" => key.1.to_string(),
                                "task_name" => task_name.to_string(),
                                "state" => "new_regular_read"
                            )
                            .increment(1);
                            tokio::spawn(propagate_task_forwarder(
                                Read::new(
                                    self.app.task_manager.get_listener(task_name.as_str()),
                                    &collection,
                                    partition,
                                    fetch_offset,
                                    key_schema_id,
                                    value_schema_id,
                                    None,
                                    &auth,
                                    self.read_buffer_size,
                                )
                                .await?
                                .next_batch(
                                    crate::read::ReadTarget::Bytes(
                                        partition_request.partition_max_bytes as usize,
                                    ),
                                    timeout,
                                ),
                            ))
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

        for topic_request in topic_requests {
            let mut key = (from_downstream_topic_name(topic_request.topic.clone()), 0);
            let mut partition_responses = Vec::with_capacity(topic_request.partitions.len());

            for partition_request in &topic_request.partitions {
                key.1 = partition_request.partition;

                let Some((pending, _)) = self.reads.get_mut(&key) else {
                    // No pending read. Check if this is due to epoch validation failure
                    let auth = self.auth.as_ref().unwrap();
                    match Collection::new(&auth, &key.0).await {
                        Ok(CollectionStatus::Ready(collection)) => {
                            if partition_request.current_leader_epoch >= 0 {
                                if partition_request.current_leader_epoch
                                    < collection.binding_backfill_counter as i32
                                {
                                    // Epoch validation failed. Return FENCED_LEADER_EPOCH
                                    partition_responses.push(
                                        PartitionData::default()
                                            .with_partition_index(partition_request.partition)
                                            .with_error_code(ResponseError::FencedLeaderEpoch.code())
                                            .with_current_leader(
                                                messages::fetch_response::LeaderIdAndEpoch::default()
                                                    .with_leader_id(messages::BrokerId(1))
                                                    .with_leader_epoch(
                                                        collection.binding_backfill_counter as i32,
                                                    ),
                                            ),
                                    );
                                    continue;
                                } else if partition_request.current_leader_epoch
                                    > collection.binding_backfill_counter as i32
                                {
                                    partition_responses.push(
                                        PartitionData::default()
                                            .with_partition_index(partition_request.partition)
                                            .with_error_code(ResponseError::UnknownLeaderEpoch.code())
                                            .with_current_leader(
                                                messages::fetch_response::LeaderIdAndEpoch::default()
                                                    .with_leader_id(messages::BrokerId(1))
                                                    .with_leader_epoch(
                                                        collection.binding_backfill_counter as i32,
                                                    ),
                                            ),
                                    );
                                    continue;
                                }
                            }
                            // Fall through to UnknownTopicOrPartition
                        }
                        Ok(CollectionStatus::NotReady) => {
                            // Collection exists but journals aren't available - return LeaderNotAvailable
                            partition_responses.push(
                                PartitionData::default()
                                    .with_partition_index(partition_request.partition)
                                    .with_error_code(ResponseError::LeaderNotAvailable.code()),
                            );
                            continue;
                        }
                        Ok(CollectionStatus::NotFound) | Err(_) => {
                            // Fall through to UnknownTopicOrPartition
                        }
                    }
                    // Collection doesn't exist or other error
                    partition_responses.push(
                        PartitionData::default()
                            .with_partition_index(partition_request.partition)
                            .with_error_code(ResponseError::UnknownTopicOrPartition.code()),
                    );
                    continue;
                };

                if partition_request.current_leader_epoch >= 0
                    && partition_request.current_leader_epoch > pending.leader_epoch
                {
                    partition_responses.push(
                        PartitionData::default()
                            .with_partition_index(partition_request.partition)
                            .with_error_code(ResponseError::UnknownLeaderEpoch.code())
                            .with_current_leader(
                                messages::fetch_response::LeaderIdAndEpoch::default()
                                    .with_leader_id(messages::BrokerId(1))
                                    .with_leader_epoch(pending.leader_epoch),
                            ),
                    );
                    self.reads.remove(&key);
                    continue;
                }

                let (read, batch) = (&mut pending.handle).await??;

                let batch = match batch {
                    BatchResult::TargetExceededBeforeTimeout(b) => Some(b),
                    BatchResult::TimeoutExceededBeforeTarget(b) => Some(b),
                    BatchResult::TimeoutNoData | BatchResult::Suspended => None,
                    BatchResult::JournalNotFound => {
                        partition_responses.push(
                            PartitionData::default()
                                .with_partition_index(partition_request.partition)
                                .with_error_code(ResponseError::UnknownTopicOrPartition.code()),
                        );
                        self.reads.remove(&key);
                        continue;
                    }
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
                            propagate_task_forwarder(read.next_batch(
                                crate::read::ReadTarget::Bytes(
                                    partition_request.partition_max_bytes as usize,
                                ),
                                timeout,
                            )),
                        ));

                        partition_data = partition_data
                            .with_high_watermark(pending.last_write_head)
                            .with_last_stable_offset(pending.last_write_head)
                            .with_current_leader(
                                messages::fetch_response::LeaderIdAndEpoch::default()
                                    .with_leader_id(messages::BrokerId(1))
                                    .with_leader_epoch(pending.leader_epoch),
                            );
                    }
                    SessionDataPreviewState::DataPreview(data_preview_states) => {
                        let data_preview_state = data_preview_states
                            .get(&key)
                            .expect("should be able to find data preview state by this point");
                        partition_data = partition_data
                            .with_high_watermark(data_preview_state.offset)
                            .with_last_stable_offset(data_preview_state.offset)
                            .with_current_leader(
                                messages::fetch_response::LeaderIdAndEpoch::default()
                                    .with_leader_id(messages::BrokerId(1))
                                    .with_leader_epoch(pending.leader_epoch),
                            );
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
        if matches!(self.auth, Some(SessionAuthentication::Redirect { .. })) {
            return Ok(messages::JoinGroupResponse::default()
                .with_error_code(ResponseError::NotCoordinator.code()));
        }

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

            // Fetch collections to get backfill counters
            let topic_names: Vec<_> = consumer_protocol_subscription_msg
                .topics
                .iter()
                .map(|t| TopicName::from(t.clone()))
                .collect();
            let collection_statuses = self
                .fetch_collections(topic_names.iter())
                .await
                .unwrap_or_default();

            for topic in consumer_protocol_subscription_msg.topics.iter_mut() {
                let backfill_counter = match collection_statuses
                    .iter()
                    .find(|(name, _)| name.as_str() == topic.as_str())
                    .map(|(_, status)| status)
                {
                    Some(CollectionStatus::Ready(c)) => c.binding_backfill_counter,
                    Some(CollectionStatus::NotReady) => {
                        tracing::warn!(
                            topic = ?topic,
                            "Collection exists but has no journals available"
                        );
                        return Ok(messages::JoinGroupResponse::default()
                            .with_error_code(ResponseError::LeaderNotAvailable.code()));
                    }
                    Some(CollectionStatus::NotFound) | None => {
                        tracing::warn!(topic = ?topic, "Collection not found");
                        return Ok(messages::JoinGroupResponse::default()
                            .with_error_code(ResponseError::UnknownTopicOrPartition.code()));
                    }
                };
                let transformed = self
                    .encrypt_topic_name(topic.to_owned().into(), Some(backfill_counter))?
                    .into();
                tracing::info!(topic_name = ?topic, backfill_counter = ?backfill_counter, "Request to join group");
                *topic = transformed;
            }

            let mut new_protocol_subscription = BytesMut::new();

            new_protocol_subscription.put_i16(consumer_protocol_subscription_version);
            consumer_protocol_subscription_msg.encode(
                &mut new_protocol_subscription,
                consumer_protocol_subscription_version,
            )?;

            protocol.metadata = new_protocol_subscription.into();
        }

        let response: anyhow::Result<_> = async {
            let client = self.get_kafka_client().await?;
            client
                .connect_to_group_coordinator(req.group_id.as_str())
                .await?
                .send_request(mutable_req.clone(), Some(header))
                .await
        }
        .await;

        let response = match response {
            Ok(response) => response,
            Err(e) if Self::is_redirect_error(&e) => {
                return Ok(messages::JoinGroupResponse::default()
                    .with_error_code(ResponseError::NotCoordinator.code()));
            }
            Err(e) => return Err(e),
        };

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
                .try_for_each(|topic| {
                    *topic = self.decrypt_topic_name(topic.to_owned().into())?.into();
                    Ok::<(), anyhow::Error>(())
                })?;

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
        if matches!(self.auth, Some(SessionAuthentication::Redirect { .. })) {
            return Ok(messages::LeaveGroupResponse::default()
                .with_error_code(ResponseError::NotCoordinator.code()));
        }

        let response: anyhow::Result<_> = async {
            let client = self.get_kafka_client().await?;
            client
                .connect_to_group_coordinator(req.group_id.as_str())
                .await?
                .send_request(req, Some(header))
                .await
        }
        .await;

        let response = match response {
            Ok(response) => response,
            Err(e) if Self::is_redirect_error(&e) => {
                return Ok(messages::LeaveGroupResponse::default()
                    .with_error_code(ResponseError::NotCoordinator.code()));
            }
            Err(e) => return Err(e),
        };
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
            Err(e) if Self::is_redirect_error(&e) => {
                // Return empty list for redirect errors
                tracing::debug!("list_groups called during redirect, returning empty list");
                Ok(ListGroupsResponse::default().with_groups(vec![]))
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
        if matches!(self.auth, Some(SessionAuthentication::Redirect { .. })) {
            return Ok(messages::SyncGroupResponse::default()
                .with_error_code(ResponseError::NotCoordinator.code()));
        }

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

            // Fetch collections to get backfill counters
            let topic_names: Vec<_> = consumer_protocol_assignment_msg
                .assigned_partitions
                .iter()
                .map(|p| p.topic.clone())
                .collect();
            let collection_statuses = self
                .fetch_collections(topic_names.iter())
                .await
                .unwrap_or_default();

            // Check for NotReady/NotFound before processing
            for part in &consumer_protocol_assignment_msg.assigned_partitions {
                match collection_statuses
                    .iter()
                    .find(|(name, _)| name.as_str() == part.topic.as_str())
                    .map(|(_, status)| status)
                {
                    Some(CollectionStatus::Ready(_)) => {}
                    Some(CollectionStatus::NotReady) => {
                        tracing::warn!(
                            topic = ?part.topic,
                            "Collection exists but has no journals available"
                        );
                        return Ok(messages::SyncGroupResponse::default()
                            .with_error_code(ResponseError::LeaderNotAvailable.code()));
                    }
                    Some(CollectionStatus::NotFound) | None => {
                        tracing::warn!(topic = ?part.topic, "Collection not found");
                        return Ok(messages::SyncGroupResponse::default()
                            .with_error_code(ResponseError::UnknownTopicOrPartition.code()));
                    }
                }
            }

            consumer_protocol_assignment_msg.assigned_partitions = consumer_protocol_assignment_msg
                .assigned_partitions
                .into_iter()
                .map(|part| {
                    let backfill_counter = collection_statuses
                        .iter()
                        .find(|(name, _)| name.as_str() == part.topic.as_str())
                        .and_then(|(_, status)| match status {
                            CollectionStatus::Ready(c) => Some(c.binding_backfill_counter),
                            _ => None,
                        })
                        .context("collection status missing after validation")?;
                    let transformed_topic =
                        self.encrypt_topic_name(part.topic.to_owned(), Some(backfill_counter))?;
                    tracing::info!(topic_name = ?part.topic, backfill_counter = ?backfill_counter, "Syncing group");
                    Ok(part.with_topic(transformed_topic))
                })
                .collect::<anyhow::Result<_>>()?;

            let mut new_protocol_assignment = BytesMut::new();

            new_protocol_assignment.put_i16(consumer_protocol_assignment_version);
            consumer_protocol_assignment_msg.encode(
                &mut new_protocol_assignment,
                consumer_protocol_assignment_version,
            )?;
            assignment.assignment = new_protocol_assignment.into();
        }

        let response: anyhow::Result<_> = async {
            let client = self.get_kafka_client().await?;
            client
                .connect_to_group_coordinator(req.group_id.as_str())
                .await?
                .send_request(mutable_req.clone(), Some(header))
                .await
        }
        .await;

        let response = match response {
            Ok(response) => response,
            Err(e) if Self::is_redirect_error(&e) => {
                return Ok(messages::SyncGroupResponse::default()
                    .with_error_code(ResponseError::NotCoordinator.code()));
            }
            Err(e) => return Err(e),
        };

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
                let transformed_topic = self.decrypt_topic_name(part.topic.to_owned())?;
                Ok(part.with_topic(transformed_topic))
            })
            .collect::<anyhow::Result<_>>()?;

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
        if matches!(self.auth, Some(SessionAuthentication::Redirect { .. })) {
            anyhow::bail!("Redirected sessions cannot delete groups");
        }

        let response: anyhow::Result<_> = async {
            let client = self.get_kafka_client().await?;
            client.send_request(req, Some(header)).await
        }
        .await;

        match response {
            Ok(response) => Ok(response),
            Err(e) if Self::is_redirect_error(&e) => {
                // For delete groups, we should probably return an error rather than silently fail
                anyhow::bail!("Cannot delete groups: task has been redirected")
            }
            Err(e) => Err(e),
        }
    }

    #[instrument(skip_all, fields(group=?req.group_id))]
    pub async fn heartbeat(
        &mut self,
        req: messages::HeartbeatRequest,
        header: RequestHeader,
    ) -> anyhow::Result<messages::HeartbeatResponse> {
        let redirect_response = messages::HeartbeatResponse::default()
            .with_error_code(ResponseError::NotCoordinator.code());

        if matches!(self.auth, Some(SessionAuthentication::Redirect { .. })) {
            return Ok(redirect_response);
        }

        let response: anyhow::Result<_> = async {
            let client = self
                .get_kafka_client()
                .await?
                .connect_to_group_coordinator(req.group_id.as_str())
                .await?;
            client.send_request(req, Some(header)).await
        }
        .await;

        match response {
            Ok(r) => Ok(r),
            Err(e) if Self::is_redirect_error(&e) => Ok(redirect_response),
            Err(e) => Err(e),
        }
    }

    #[instrument(skip_all, fields(group=?req.group_id))]
    pub async fn offset_commit(
        &mut self,
        mut req: messages::OffsetCommitRequest,
        header: RequestHeader,
    ) -> anyhow::Result<messages::OffsetCommitResponse> {
        let redirect_response = messages::OffsetCommitResponse::default().with_topics(
            req.topics
                .clone()
                .into_iter()
                .map(|t| {
                    messages::offset_commit_response::OffsetCommitResponseTopic::default()
                    .with_name(t.name)
                    .with_partitions(t.partitions.into_iter().map(|p| {
                        messages::offset_commit_response::OffsetCommitResponsePartition::default()
                            .with_partition_index(p.partition_index)
                            .with_error_code(ResponseError::NotCoordinator.code())
                    }).collect())
                })
                .collect(),
        );

        if matches!(self.auth, Some(SessionAuthentication::Redirect { .. })) {
            return Ok(redirect_response);
        }

        let resp: anyhow::Result<_> = async {

            let collection_statuses = self
                .fetch_collections(req.topics.iter().map(|topic| &topic.name))
                .await?;

            // Check for NotFound/NotReady collections - return per-topic errors
            for (topic_name, status) in &collection_statuses {
                let error_code = match status {
                    CollectionStatus::Ready(_) => continue,
                    CollectionStatus::NotFound => {
                        tracing::warn!(topic = ?topic_name, "Collection not found");
                        ResponseError::UnknownTopicOrPartition.code()
                    }
                    CollectionStatus::NotReady => {
                        tracing::warn!(
                            topic = ?topic_name,
                            "Collection exists but has no journals available"
                        );
                        ResponseError::LeaderNotAvailable.code()
                    }
                };
                // Return error response for all topics
                let topics = req
                    .topics
                    .iter()
                    .map(|t| {
                        messages::offset_commit_response::OffsetCommitResponseTopic::default()
                            .with_name(t.name.clone())
                            .with_partitions(
                                t.partitions
                                    .iter()
                                    .map(|p| {
                                        messages::offset_commit_response::OffsetCommitResponsePartition::default()
                                            .with_partition_index(p.partition_index)
                                            .with_error_code(error_code)
                                    })
                                    .collect(),
                            )
                    })
                    .collect();
                return Ok(messages::OffsetCommitResponse::default().with_topics(topics));
            }

            // All collections are Ready at this point
            let desired_topic_partitions = collection_statuses
                .iter()
                .filter_map(|(topic_name, status)| match status {
                    CollectionStatus::Ready(collection) => Some(
                        self.encrypt_topic_name(
                            topic_name.clone(),
                            Some(collection.binding_backfill_counter),
                        )
                        .map(|encrypted_name| (encrypted_name, collection.partitions.len())),
                    ),
                    CollectionStatus::NotReady | CollectionStatus::NotFound => None,
                })
                .collect::<Result<Vec<_>, _>>()?;

            let original_topics = req.topics.clone();
            let secret = self.secret.clone();
            let token = match self.auth.as_ref().context("Must be authenticated")? {
                SessionAuthentication::Task(auth) => auth.config.token.to_string(),
                SessionAuthentication::Redirect { config, .. } => config.token.to_string(),
            };

            for topic in &mut req.topics {
                let backfill_counter = collection_statuses
                    .iter()
                    .find(|(name, _)| name == &topic.name)
                    .and_then(|(_, status)| match status {
                        CollectionStatus::Ready(c) => Some(c.binding_backfill_counter),
                        CollectionStatus::NotReady | CollectionStatus::NotFound => None,
                    })
                    .context(format!("Collection not found for topic {:?}", topic.name))?;
                let encrypted = self.encrypt_topic_name(topic.name.clone(), Some(backfill_counter))?;
                topic.name = encrypted;
            }

            let mut resp = {
                let mut client = self
                    .get_kafka_client()
                    .await?
                    .connect_to_group_coordinator(req.group_id.as_str())
                    .await?;

                client.ensure_topics(desired_topic_partitions).await?;

                let resp = client.send_request(req.clone(), Some(header.clone())).await?;
                let cleanup_ready = resp.topics.iter().all(|topic| {
                    topic
                        .partitions
                        .iter()
                        .all(|partition| partition.error_code.err().is_none())
                });

                if cleanup_ready {
                    if let Err(e) = Self::send_legacy_offset_cleanup(
                        &mut client,
                        req.group_id.clone(),
                        &original_topics,
                        &secret,
                        &token,
                        &header,
                    )
                    .await
                    {
                        tracing::warn!(
                            group_id = ?req.group_id,
                            error = ?e,
                            "Failed to clean up legacy offsets (non-fatal)"
                        );
                    }
                } else {
                    tracing::info!(
                        group_id = ?req.group_id,
                        "Skipping legacy offset cleanup because the new commit returned errors"
                    );
                }

                resp
            };

            for topic in resp.topics.iter_mut() {
                let encrypted_name = topic.name.clone();
                let decrypted_name = self.decrypt_topic_name(topic.name.to_owned())?;

                // Restore plaintext topic name for the response
                topic.name = decrypted_name.clone();

                let collection_partitions = match collection_statuses
                    .iter()
                    .find(|(topic_name, _)| topic_name == &decrypted_name)
                    .context(format!(
                        "unable to look up partitions for {:?}",
                        decrypted_name
                    ))?
                {
                    (_, CollectionStatus::Ready(c)) => &c.partitions,
                    (_, CollectionStatus::NotReady) => {
                        // For NotReady topics, skip partition processing since we have no journals
                        continue;
                    }
                    (_, CollectionStatus::NotFound) => {
                        continue;
                    }
                };

                for partition in &topic.partitions {
                    if let Some(error) = partition.error_code.err() {
                        tracing::warn!(
                            topic = decrypted_name.as_str(),
                            partition = partition.partition_index,
                            ?error,
                            "Got error from upstream Kafka when trying to commit offsets"
                        );
                    } else {
                        let response_partition_index = partition.partition_index;

                        let journal_name = collection_partitions
                            .get(response_partition_index as usize)
                            .context(format!(
                                "unable to find collection partition idx {} in collection {:?}",
                                response_partition_index,
                                decrypted_name.as_str()
                            ))?
                            .spec
                            .name
                            .to_owned();

                        let request_partitions = &req
                            .topics
                            .iter()
                            .find(|req_topic| req_topic.name == encrypted_name)
                            .context(format!(
                                "unable to find topic in request {:?}",
                                decrypted_name.as_str()
                            ))?
                            .partitions;

                        let committed_offset = request_partitions
                            .iter()
                            .find(|req_part| req_part.partition_index == response_partition_index)
                            .context(format!(
                                "Unable to find partition index {} in request partitions for topic {:?}, though response contained it. Request partitions: {:?}. Flow has: {:?}",
                                response_partition_index,
                                decrypted_name.as_str(),
                                request_partitions,
                                collection_partitions
                            ))?
                            .committed_offset;

                        metrics::gauge!("dekaf_committed_offset", "group_id"=>req.group_id.to_string(),"journal_name"=>journal_name.clone()).set(committed_offset as f64);
                        tracing::info!(topic_name = decrypted_name.as_str(), journal_name, partitions = ?topic.partitions, committed_offset, "Committed offset");
                    }
                }
            }

            Ok(resp)
        }.await;

        match resp {
            Ok(r) => Ok(r),
            Err(e) if Self::is_redirect_error(&e) => Ok(redirect_response),
            Err(e) => Err(e),
        }
    }

    /// Clears non-epoch-stamped offsets after a successful epoch-qualified commit.
    ///
    /// This protects against a narrow edge case:
    /// 1. Consumer commits offsets before we started reporting epochs. This will be stored with the
    ///    endpoint config token as the nonce, and without a `-e{epoch}` suffix.
    /// 2. Consumer upgrades, commits new offsets using the task name as the nonce and with the epoch suffix.
    /// 3. Binding backfill counter increments
    /// 4. Consumer restarts before committing any offsets, tries to fetch epoch-qualified offsets, finds nothing
    /// 5. Falls back to non-epoch-stamped offsets, finds the old pre-upgrade offsets
    /// 6. Those offsets are invalid in the current context and will either cause the consumer to skip data or sit around
    ///    waiting for data that will never arrive.
    ///
    /// By clearing the old offsets after a successful commit, step 5 finds nothing and the consumer resets properly.
    ///
    /// TODO(jshearer): This logic can be removed once all consumers have committed after the collection reset deploy.
    async fn send_legacy_offset_cleanup(
        client: &mut KafkaApiClient,
        group_id: messages::GroupId,
        topics: &[messages::offset_commit_request::OffsetCommitRequestTopic],
        secret: &str,
        token: &str,
        header: &RequestHeader,
    ) -> anyhow::Result<()> {
        let mut cleanup_topics = Vec::new();

        for topic in topics {
            let legacy_name = to_upstream_topic_name(
                topic.name.clone(),
                secret.to_owned(),
                token.to_string(),
                None,
            );

            let cleanup_partitions = topic
                .partitions
                .iter()
                .map(|p| {
                    messages::offset_commit_request::OffsetCommitRequestPartition::default()
                        .with_partition_index(p.partition_index)
                        .with_committed_offset(-1)
                })
                .collect();

            cleanup_topics.push(
                messages::offset_commit_request::OffsetCommitRequestTopic::default()
                    .with_name(legacy_name)
                    .with_partitions(cleanup_partitions),
            );
        }

        let cleanup_req = messages::OffsetCommitRequest::default()
            .with_group_id(group_id.clone())
            .with_topics(cleanup_topics);

        client
            .send_request(cleanup_req, Some(header.clone()))
            .await?;

        tracing::debug!(
            group_id = ?group_id,
            "Sent legacy offset cleanup request"
        );

        Ok(())
    }

    #[instrument(skip_all, fields(group=?req.group_id))]
    pub async fn offset_fetch(
        &mut self,
        mut req: messages::OffsetFetchRequest,
        header: RequestHeader,
    ) -> anyhow::Result<messages::OffsetFetchResponse> {
        let redirect_response = messages::OffsetFetchResponse::default().with_topics(
            req.topics
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(|t| {
                    messages::offset_fetch_response::OffsetFetchResponseTopic::default()
                        .with_name(t.name)
                        .with_partitions(
                            // We have no good way to know partitions here. We could either return a high level error
                            // on the OffsetFetchResponse, or return a single partition with NotCoordinator.
                            vec![messages::offset_fetch_response::OffsetFetchResponsePartition::default()
                                .with_partition_index(0)
                                .with_error_code(ResponseError::NotCoordinator.code())]
                        )
                })
                .collect(),
        );

        if matches!(self.auth, Some(SessionAuthentication::Redirect { .. })) {
            return Ok(redirect_response);
        }

        let collection_statuses = if let Some(topics) = &req.topics {
            match self
                .fetch_collections(topics.iter().map(|topic| &topic.name))
                .await
            {
                Ok(statuses) => Some(statuses),
                Err(e) if Self::is_redirect_error(&e) => return Ok(redirect_response),
                Err(e) => return Err(e),
            }
        } else {
            None
        };

        // Check for NotFound/NotReady collections - return per-topic errors
        if let Some(ref statuses) = collection_statuses {
            for (topic_name, status) in statuses {
                let error_code = match status {
                    CollectionStatus::Ready(_) => continue,
                    CollectionStatus::NotFound => {
                        tracing::warn!(topic = ?topic_name, "Collection not found");
                        ResponseError::UnknownTopicOrPartition.code()
                    }
                    CollectionStatus::NotReady => {
                        tracing::warn!(
                            topic = ?topic_name,
                            "Collection exists but has no journals available"
                        );
                        ResponseError::LeaderNotAvailable.code()
                    }
                };
                // Return error response for all topics
                let topics = req
                    .topics
                    .as_ref()
                    .map(|topics| {
                        topics
                            .iter()
                            .map(|t| {
                                messages::offset_fetch_response::OffsetFetchResponseTopic::default()
                                    .with_name(t.name.clone())
                                    .with_partitions(
                                        t.partition_indexes
                                            .iter()
                                            .map(|&p| {
                                                messages::offset_fetch_response::OffsetFetchResponsePartition::default()
                                                    .with_partition_index(p)
                                                    .with_error_code(error_code)
                                            })
                                            .collect(),
                                    )
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                return Ok(messages::OffsetFetchResponse::default().with_topics(topics));
            }
        }

        // All collections are Ready at this point
        let collection_partitions = if let Some(ref statuses) = collection_statuses {
            statuses
                .iter()
                .filter_map(|(topic_name, status)| match status {
                    CollectionStatus::Ready(collection) => Some(
                        self.encrypt_topic_name(
                            topic_name.clone(),
                            Some(collection.binding_backfill_counter),
                        )
                        .map(|encrypted_name| (encrypted_name, collection.partitions.len())),
                    ),
                    CollectionStatus::NotReady | CollectionStatus::NotFound => None,
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            vec![]
        };

        // Encrypt topic names with epoch-qualified names
        if let Some(ref mut topics) = req.topics {
            for topic in topics.iter_mut() {
                let backfill_counter = collection_statuses
                    .as_ref()
                    .and_then(|statuses| statuses.iter().find(|(name, _)| name == &topic.name))
                    .and_then(|(_, status)| match status {
                        CollectionStatus::Ready(c) => Some(c.binding_backfill_counter),
                        CollectionStatus::NotReady | CollectionStatus::NotFound => None,
                    })
                    .context(format!("Collection not found for topic {:?}", topic.name))?;
                topic.name = self.encrypt_topic_name(topic.name.clone(), Some(backfill_counter))?;
            }
        }

        // Prepare fallback request (non-epoch names) ahead of time
        let mut fallback_req = req.clone();
        if let Some(ref mut topics) = fallback_req.topics {
            for topic in topics.iter_mut() {
                let decrypted = self.decrypt_topic_name(topic.name.clone())?;
                topic.name = self.encrypt_topic_name(decrypted, None)?;
            }
        }

        let client = self
            .get_kafka_client()
            .await?
            .connect_to_group_coordinator(req.group_id.as_str())
            .await?;

        if !collection_partitions.is_empty() {
            client.ensure_topics(collection_partitions.clone()).await?;
        }

        let mut resp = client
            .send_request(req.clone(), Some(header.clone()))
            .await?;

        // try fallback to previous topic names to serve the committed offset upgrade path
        let should_fallback = resp.topics.iter().all(|topic| {
            topic.partitions.iter().all(|p| {
                // -1 = no committed offset
                p.committed_offset == -1
            })
        });

        if should_fallback && collection_statuses.is_some() {
            tracing::info!(group_id = ?req.group_id, "No offsets found with epoch, falling back to legacy format");
            resp = client.send_request(fallback_req, Some(header)).await?;
        }

        for topic in resp.topics.iter_mut() {
            topic.name = self.decrypt_topic_name(topic.name.to_owned())?;
            let maybe_backfill_counter = collection_statuses.as_ref().and_then(|statuses| {
                statuses
                    .iter()
                    .find(|(topic_name, _)| topic_name == &topic.name)
                    .and_then(|(_, status)| match status {
                        CollectionStatus::Ready(c) => Some(c.binding_backfill_counter),
                        // NotReady/NotFound: don't set committed_leader_epoch
                        CollectionStatus::NotReady | CollectionStatus::NotFound => None,
                    })
            });

            if let Some(backfill_counter) = maybe_backfill_counter {
                for partition in topic.partitions.iter_mut() {
                    // Only set epoch for partitions that have committed offsets.
                    // When committed_offset is -1 (no offset), committed_leader_epoch
                    // must be -1 per Kafka protocol semantics.
                    if partition.committed_offset >= 0 {
                        partition.committed_leader_epoch = backfill_counter as i32;
                    } else {
                        partition.committed_leader_epoch = -1;
                    }
                }
            }
        }

        Ok(resp)
    }

    /// OffsetForLeaderEpoch handles consumer requests to validate their position after receiving FENCED_LEADER_EPOCH.
    /// Returns the end offset for a given leader epoch, allowing consumers to reset their position.
    ///
    /// When a consumer receives FENCED_LEADER_EPOCH during a fetch, the standard recovery flow is:
    /// 1. Consumer calls OffsetForLeaderEpoch for their old epoch
    /// 2. Broker returns the end offset where that epoch ended
    /// 3. Consumer compares their position to the end offset
    /// 4. Consumer decides whether to continue or reset
    /// Since the intent is for a consumer to reset its offset back to 0 when the backfill counter changes, the behavior we want is:
    /// - If the requested epoch is less than the current epoch, return end offset 0 (indicating reset to beginning)
    /// - If the requested epoch equals the current epoch, return the current write head
    #[instrument(skip_all, fields(topics=?req.topics.len()))]
    pub async fn offset_for_leader_epoch(
        &mut self,
        req: messages::OffsetForLeaderEpochRequest,
    ) -> anyhow::Result<messages::OffsetForLeaderEpochResponse> {
        let auth = self
            .auth
            .as_ref()
            .ok_or(anyhow::anyhow!("Not authenticated"))?;

        let topics = futures::future::try_join_all(
            req.topics
                .into_iter()
                .map(|topic| Self::fetch_topic_leader_epochs(auth, topic)),
        )
        .await?;

        Ok(messages::OffsetForLeaderEpochResponse::default().with_topics(topics))
    }

    async fn fetch_topic_leader_epochs(
        auth: &SessionAuthentication,
        topic: messages::offset_for_leader_epoch_request::OffsetForLeaderTopic,
    ) -> anyhow::Result<messages::offset_for_leader_epoch_response::OffsetForLeaderTopicResult>
    {
        use messages::offset_for_leader_epoch_response::OffsetForLeaderTopicResult;

        use messages::offset_for_leader_epoch_response::EpochEndOffset;

        let collection_name = from_downstream_topic_name(topic.topic.clone());
        let collection = match Collection::new(auth, &collection_name).await? {
            CollectionStatus::Ready(c) => c,
            CollectionStatus::NotFound => {
                let partitions = topic
                    .partitions
                    .iter()
                    .map(|p| {
                        EpochEndOffset::default()
                            .with_partition(p.partition)
                            .with_error_code(ResponseError::UnknownTopicOrPartition.code())
                    })
                    .collect();
                return Ok(OffsetForLeaderTopicResult::default()
                    .with_topic(topic.topic)
                    .with_partitions(partitions));
            }
            CollectionStatus::NotReady => {
                // Collection exists but journals aren't available - return LeaderNotAvailable
                let partitions = topic
                    .partitions
                    .iter()
                    .map(|p| {
                        EpochEndOffset::default()
                            .with_partition(p.partition)
                            .with_error_code(ResponseError::LeaderNotAvailable.code())
                    })
                    .collect();
                return Ok(OffsetForLeaderTopicResult::default()
                    .with_topic(topic.topic)
                    .with_partitions(partitions));
            }
        };

        let current_epoch = collection.binding_backfill_counter as i32;

        let partitions =
            futures::future::try_join_all(topic.partitions.into_iter().map(|partition| {
                Self::fetch_partition_leader_epoch(
                    &collection,
                    &collection_name,
                    partition,
                    current_epoch,
                )
            }))
            .await?;

        Ok(OffsetForLeaderTopicResult::default()
            .with_topic(topic.topic)
            .with_partitions(partitions))
    }

    async fn fetch_partition_leader_epoch(
        collection: &Collection,
        collection_name: &str,
        partition: messages::offset_for_leader_epoch_request::OffsetForLeaderPartition,
        current_epoch: i32,
    ) -> anyhow::Result<messages::offset_for_leader_epoch_response::EpochEndOffset> {
        use messages::offset_for_leader_epoch_response::EpochEndOffset;

        if partition.leader_epoch < current_epoch {
            tracing::info!(
                collection = collection_name,
                partition = partition.partition,
                consumer_epoch = partition.leader_epoch,
                current_epoch,
                "Consumer querying for offset of old epoch, returning reset to beginning"
            );
            Ok(EpochEndOffset::default()
                .with_partition(partition.partition)
                .with_leader_epoch(current_epoch)
                .with_end_offset(0))
        } else if partition.leader_epoch == current_epoch {
            let high_watermark = collection
                .fetch_partition_offset(partition.partition as usize, -1)
                .await?
                .map(|po| po.offset)
                .unwrap_or(0);

            tracing::debug!(
                collection = collection_name,
                partition = partition.partition,
                epoch = current_epoch,
                high_watermark,
                "Returning high watermark for current epoch"
            );
            Ok(EpochEndOffset::default()
                .with_partition(partition.partition)
                .with_leader_epoch(current_epoch)
                .with_end_offset(high_watermark))
        } else {
            tracing::warn!(
                collection = collection_name,
                partition = partition.partition,
                consumer_epoch = partition.leader_epoch,
                current_epoch,
                "Consumer querying future epoch"
            );
            Ok(EpochEndOffset::default()
                .with_partition(partition.partition)
                .with_error_code(ResponseError::UnknownLeaderEpoch.code()))
        }
    }
    /// ApiVersions lists the APIs which are supported by this "broker".
    pub async fn api_versions(
        &mut self,
        _req: messages::ApiVersionsRequest,
    ) -> anyhow::Result<messages::ApiVersionsResponse> {
        use kafka_protocol::messages::{api_versions_response::ApiVersion, *};

        fn version<T: kafka_protocol::protocol::Message>(api_key: ApiKey) -> ApiVersion {
            ApiVersion::default()
                .with_api_key(api_key as i16)
                .with_max_version(T::VERSIONS.max)
                .with_min_version(T::VERSIONS.min)
        }
        /*
           V2_1_0_0,
           map[int16]int16{
               apiKeyProduce:              7,
               apiKeyFetch:                10,
               apiKeyListOffsets:          4,
               apiKeyMetadata:             7,
               apiKeyOffsetCommit:         6,
               apiKeyOffsetFetch:          5,
               apiKeyOffsetForLeaderEpoch: 2,
               apiKeyTxnOffsetCommit:      2,
               apiKeyDeleteTopics:         3,
           },
        */
        let res = ApiVersionsResponse::default().with_api_keys(vec![
            version::<ApiVersionsRequest>(ApiKey::ApiVersions),
            version::<SaslHandshakeRequest>(ApiKey::SaslHandshake),
            version::<SaslAuthenticateRequest>(ApiKey::SaslAuthenticate),
            ApiVersion::default()
                .with_api_key(ApiKey::Metadata as i16)
                .with_min_version(9)
                .with_max_version(9),
            ApiVersion::default()
                .with_api_key(ApiKey::FindCoordinator as i16)
                .with_min_version(0)
                .with_max_version(2),
            ApiVersion::default()
                .with_api_key(ApiKey::ListOffsets as i16)
                .with_min_version(4)
                .with_max_version(10),
            ApiVersion::default()
                .with_api_key(ApiKey::Fetch as i16)
                // This is another non-obvious requirement in librdkafka. If we advertise <4 as a minimum here, some clients'
                // fetch requests will sit in a tight loop erroring over and over. This feels like a bug... but it's probably
                // just the consequence of convergent development, where some implicit requirement got encoded both in the client
                // and server without being explicitly documented anywhere.
                .with_min_version(4)
                // Version >= 13 did away with topic names in favor of unique topic UUIDs, so we need to stick below that.
                .with_max_version(12),
            // Needed by `kaf`.
            version::<DescribeConfigsRequest>(ApiKey::DescribeConfigs),
            version::<JoinGroupRequest>(ApiKey::JoinGroup),
            version::<LeaveGroupRequest>(ApiKey::LeaveGroup),
            version::<ListGroupsRequest>(ApiKey::ListGroups),
            version::<SyncGroupRequest>(ApiKey::SyncGroup),
            version::<DeleteGroupsRequest>(ApiKey::DeleteGroups),
            version::<HeartbeatRequest>(ApiKey::Heartbeat),
            ApiVersion::default()
                .with_api_key(ApiKey::OffsetCommit as i16)
                .with_min_version(6)
                .with_max_version(6),
            ApiVersion::default()
                .with_api_key(ApiKey::OffsetForLeaderEpoch as i16)
                .with_min_version(2)
                .with_max_version(2),
            ApiVersion::default()
                .with_api_key(ApiKey::OffsetFetch as i16)
                .with_min_version(0)
                .with_max_version(7),
            // Unsupported
            ApiVersion::default()
                .with_api_key(ApiKey::Produce as i16)
                .with_min_version(3)
                .with_max_version(9),
            ApiVersion::default()
                .with_api_key(ApiKey::TxnOffsetCommit as i16)
                .with_min_version(2)
                .with_max_version(2),
            ApiVersion::default()
                .with_api_key(ApiKey::DeleteTopics as i16)
                .with_min_version(3)
                .with_max_version(3),
        ]);

        // UNIMPLEMENTED:
        /*
            ApiKey::LeaderAndIsr,
            ApiKey::StopReplica,
            ApiKey::CreateTopics,
        */

        Ok(res)
    }

    fn get_topic_name_nonce(&self, use_task_name: bool) -> anyhow::Result<String> {
        match self.auth.as_ref().context("Must be authenticated")? {
            SessionAuthentication::Task(auth) => Ok(if use_task_name {
                auth.task_name.to_string()
            } else {
                auth.config.token.to_string()
            }),
            SessionAuthentication::Redirect { .. } => {
                anyhow::bail!("Redirect sessions should not encrypt/decrypt topic names")
            }
        }
    }

    fn encrypt_topic_name(
        &self,
        name: TopicName,
        backfill_counter: Option<u32>,
    ) -> anyhow::Result<TopicName> {
        let nonce = self.get_topic_name_nonce(backfill_counter.is_some())?;
        Ok(to_upstream_topic_name(
            name,
            self.secret.to_owned(),
            nonce,
            backfill_counter,
        ))
    }

    fn decrypt_topic_name(&self, name: TopicName) -> anyhow::Result<TopicName> {
        // Auto-detect which nonce was used based on epoch suffix presence
        let use_task_name = crate::has_epoch_suffix(name.as_str());
        let nonce = self.get_topic_name_nonce(use_task_name)?;
        Ok(from_upstream_topic_name(
            name,
            self.secret.to_owned(),
            nonce,
        ))
    }

    fn encode_topic_name(&self, name: String) -> anyhow::Result<TopicName> {
        if match self.auth.as_ref().context("Must be authenticated")? {
            SessionAuthentication::Task(auth) => auth.config.strict_topic_names,
            SessionAuthentication::Redirect { config, .. } => config.strict_topic_names,
        } {
            Ok(to_downstream_topic_name(TopicName(StrBytes::from_string(
                name,
            ))))
        } else {
            Ok(TopicName(StrBytes::from_string(name)))
        }
    }

    async fn fetch_collections(
        &mut self,
        topics: impl IntoIterator<Item = &TopicName>,
    ) -> anyhow::Result<Vec<(TopicName, CollectionStatus)>> {
        // Re-declare here to drop mutable reference
        let auth = self.auth.as_ref().unwrap();

        futures::future::try_join_all(topics.into_iter().map(|topic| async move {
            let status = Collection::new(auth, topic.as_ref()).await?;
            Ok::<(TopicName, CollectionStatus), anyhow::Error>((topic.clone(), status))
        }))
        .await
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
        let result: anyhow::Result<_> = async {
            let auth = self
                .auth
                .as_ref()
                .ok_or(anyhow::anyhow!("Session not authenticated"))?;

            tracing::debug!(
                "Loading latest offset for this partition to check if session is data-preview"
            );
            let collection = match Collection::new(auth, collection_name.as_str()).await? {
                CollectionStatus::Ready(c) => c,
                CollectionStatus::NotFound => {
                    anyhow::bail!("Collection {} not found", collection_name);
                }
                CollectionStatus::NotReady => {
                    // Can't determine data preview status without journals - assume not data preview
                    return Ok(None);
                }
            };

            match collection
                .fetch_partition_offset(partition as usize, -1)
                .await
            {
                Ok(Some(
                    partition_offset @ PartitionOffset {
                        offset: latest_offset,
                        ..
                    },
                )) => {
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
                }
                Ok(_) => Ok(None),
                // Handle Suspended errors as not data preiew
                Err(e)
                    if e.downcast_ref::<gazette::Error>().map_or(false, |err| {
                        matches!(
                            err,
                            gazette::Error::BrokerStatus(gazette::broker::Status::Suspended { .. })
                        )
                    }) =>
                {
                    tracing::debug!(
                        "Partition is suspended, treating as non-data-preview: {:?}",
                        e
                    );
                    Ok(None)
                }
                Err(e) => return Err(e),
            }
        }
        .await;

        match result {
            Ok(result) => Ok(result),
            Err(e) if Self::is_redirect_error(&e) => {
                // Task was redirected, treat as non-data-preview
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    fn build_partition(index: i32, leader_epoch: i32) -> MetadataResponsePartition {
        MetadataResponsePartition::default()
            .with_partition_index(index)
            .with_leader_id(messages::BrokerId(1))
            .with_leader_epoch(leader_epoch)
            .with_replica_nodes(vec![messages::BrokerId(1)])
            .with_isr_nodes(vec![messages::BrokerId(1)])
    }

    fn build_topic_metadata(
        &self,
        name: TopicName,
        collection: &Collection,
    ) -> anyhow::Result<MetadataResponseTopic> {
        let leader_epoch = collection.binding_backfill_counter as i32;

        // Collections with empty partitions should be handled as NotReady before
        // reaching this function, so we can safely iterate over partitions here
        let partitions = collection
            .partitions
            .iter()
            .enumerate()
            .map(|(index, _)| Self::build_partition(index as i32, leader_epoch))
            .collect();

        Ok(MetadataResponseTopic::default()
            .with_name(Some(name))
            .with_is_internal(false)
            .with_partitions(partitions))
    }

    async fn fetch_collections_for_metadata(
        &self,
        names: impl IntoIterator<Item = String>,
    ) -> anyhow::Result<Vec<(String, CollectionStatus)>> {
        let auth = self
            .auth
            .as_ref()
            .ok_or(anyhow::anyhow!("Session not authenticated"))?;

        futures::future::try_join_all(names.into_iter().map(|name| async move {
            Collection::new(auth, &name)
                .await
                .map(|status| (name, status))
        }))
        .await
    }
}
