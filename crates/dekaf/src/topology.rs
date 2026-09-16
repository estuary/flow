use crate::{
    SessionAuthentication,
    task::{Binding, Source, Topic},
};
use anyhow::Context;
use futures::StreamExt;
use gazette::{
    broker::{self, ReadResponse, journal_spec},
    journal, uuid,
};

/// Collection is a Topic as of one request: the Topic, the partitions its
/// listing currently holds, and the `not_before` those partitions impose.
pub struct Collection {
    pub topic: Topic,
    /// Journals of the binding, in stable Kafka partition order.
    pub partitions: Vec<Partition>,
    /// The binding's `not_before`, raised to the greatest `truncated-at`
    /// label among its partitions: a truncated journal has no documents
    /// before that clock to serve.
    pub not_before: Option<uuid::Clock>,
}

/// Represents why a collection is unavailable.
#[derive(Debug, Clone, Copy)]
pub enum CollectionUnavailable {
    /// The binding doesn't exist in the materialization spec.
    NotFound,
    /// The binding exists but journals are not yet available. This can happen when:
    /// - A collection was recently reset and the writer hasn't created journals yet
    /// - The collection exists in the control plane but no data has been written
    /// Callers should return a retryable error (e.g., LeaderNotAvailable) to clients.
    NotReady,
}

impl CollectionUnavailable {
    pub fn code(self) -> i16 {
        use kafka_protocol::error::ResponseError;
        match self {
            CollectionUnavailable::NotFound => ResponseError::UnknownTopicOrPartition.code(),
            CollectionUnavailable::NotReady => ResponseError::LeaderNotAvailable.code(),
        }
    }
}

pub enum CollectionStatus {
    Ready(Collection),
    Unavailable(CollectionUnavailable),
}

impl CollectionStatus {
    pub fn not_found() -> Self {
        Self::Unavailable(CollectionUnavailable::NotFound)
    }

    pub fn not_ready() -> Self {
        Self::Unavailable(CollectionUnavailable::NotReady)
    }

    /// Returns the Kafka error code for non-ready states, None if Ready.
    pub fn error_code(&self) -> Option<i16> {
        match self {
            CollectionStatus::Ready(_) => None,
            CollectionStatus::Unavailable(reason) => Some(reason.code()),
        }
    }

    /// Convert to Result, returning the unavailability reason for non-Ready states.
    pub fn ready(self) -> Result<Collection, CollectionUnavailable> {
        match self {
            CollectionStatus::Ready(c) => Ok(c),
            CollectionStatus::Unavailable(reason) => Err(reason),
        }
    }
}

/// Partition is a collection journal which is mapped into a stable Kafka partition order.
#[derive(Debug, Clone)]
pub struct Partition {
    pub create_revision: i64,
    pub spec: broker::JournalSpec,
    #[allow(unused)]
    pub mod_revision: i64,
    #[allow(unused)]
    pub route: broker::Route,
}

#[derive(Clone, Copy, Debug)]
pub struct PartitionOffset {
    pub fragment_start: i64,
    pub offset: i64,
    pub mod_time: i64,
}

const OFFSET_REQUEST_EARLIEST: i64 = -2;
const OFFSET_REQUEST_LATEST: i64 = -1;

impl Collection {
    pub fn binding(&self) -> &Binding {
        self.topic.binding()
    }

    pub fn source(&self) -> &Source {
        self.topic.source()
    }

    /// Client authorized to list and read the source's journals.
    pub fn client(&self) -> &journal::Client {
        self.topic.client()
    }

    /// Assemble the Topic named `topic_name` with the partitions which its
    /// listing currently holds.
    pub async fn new(
        auth: &SessionAuthentication,
        topic_name: &str,
    ) -> anyhow::Result<CollectionStatus> {
        // A redirected task has no journals to serve, and errors here.
        let authorized = auth.authorized()?;

        let Some(topic) = authorized.topic(topic_name) else {
            // The client is asking for a topic our Task doesn't have, which a
            // publication adding the binding would explain. Ask for a
            // re-fetch; the cool-off bounds what a polling client can cost.
            authorized.revoke.cancel();

            tracing::debug!("{topic_name} is not a binding of {}", auth.task_name);
            return Ok(CollectionStatus::not_found());
        };

        // Only a topic which is actually asked for starts its listing, and
        // then blocks on that listing's first response.
        let partitions = topic
            .partitions()
            .await
            .result()
            .map_err(proto_grpc::status_to_anyhow)?
            .clone();

        // Honor truncated-at journal labels: if any partition carries a
        // truncated-at label, use the max across all partitions and the
        // binding's not_before as the effective not_before.
        let mut not_before = topic.binding().not_before;
        for partition in &partitions {
            if let Some(truncated_at_str) = partition
                .spec
                .labels
                .as_ref()
                .and_then(|ls| ls.labels.iter().find(|l| l.name == ::labels::TRUNCATED_AT))
                .map(|l| &l.value)
            {
                let truncated_clock =
                    uuid::Clock::from_u64(::labels::parse_truncated_at(truncated_at_str)?);
                not_before = Some(not_before.map_or(truncated_clock, |nb| nb.max(truncated_clock)));
            }
        }

        tracing::debug!(
            collection = topic.source().collection_name,
            partitions = partitions.len(),
            "built collection"
        );

        // If there are no partitions/journals, the collection exists but isn't ready to serve.
        // This happens when a collection was reset and journals haven't been created yet,
        // or when a collection exists but no data has ever been written.
        if partitions.is_empty() {
            tracing::debug!(
                collection = topic.source().collection_name,
                "Collection binding exists but has no journals available"
            );
            return Ok(CollectionStatus::not_ready());
        }

        Ok(CollectionStatus::Ready(Self {
            topic,
            partitions,
            not_before,
        }))
    }

    /// Map the collection's key and value Avro schema into globally unique registry IDs.
    /// This will content-address each schema to fetch a current registry ID if one is available,
    /// or will register a new schema if not.
    pub async fn registered_schema_ids(
        &self,
        auth: &SessionAuthentication,
        client: &postgrest::Postgrest,
    ) -> anyhow::Result<(u32, u32)> {
        let catalog_name = &self.source().collection_name;
        let (key_id, value_id) = futures::try_join!(
            Self::registered_schema_id(auth, client, catalog_name, &self.source().key_schema),
            Self::registered_schema_id(auth, client, catalog_name, &self.binding().value_schema),
        )?;
        Ok((key_id, value_id))
    }

    /// Map a partition and timestamp into the newest covering fragment offset.
    /// Request latest offset
    ///     - `suspend::Level::Full | suspend::Level::Partial`: `suspend.offset`
    ///     - `suspend::Level::None`: write offset returned by non-blocking read at `offset = -1`
    /// Request earliest offset
    ///     - `suspend::Level::Full`: `suspend.offset`
    ///     - `suspend::Level::Partial | suspend::Level::None`: fragment listing with `begin_mod_time = 0`, return 0th fragment’s begin
    pub async fn fetch_partition_offset(
        &self,
        partition_index: usize,
        timestamp_millis: i64,
    ) -> anyhow::Result<Option<PartitionOffset>> {
        let Some(partition) = self.partitions.get(partition_index) else {
            return Ok(None);
        };

        let offset_data = match timestamp_millis {
            OFFSET_REQUEST_LATEST => {
                match partition.spec.suspend {
                    Some(suspend)
                        if suspend.level == journal_spec::suspend::Level::Full as i32
                            || suspend.level == journal_spec::suspend::Level::Partial as i32 =>
                    {
                        Some(PartitionOffset {
                            fragment_start: suspend.offset,
                            offset: suspend.offset,
                            mod_time: -1, // UNKNOWN_TIMESTAMP
                        })
                    }
                    // Not suspended, so return high-water mark.
                    _ => self.fetch_write_head(partition_index).await?,
                }
            }
            OFFSET_REQUEST_EARLIEST => {
                match partition.spec.suspend {
                    Some(suspend) if suspend.level == journal_spec::suspend::Level::Full as i32 => {
                        Some(PartitionOffset {
                            fragment_start: suspend.offset,
                            offset: suspend.offset,
                            mod_time: -1, // UNKNOWN_TIMESTAMP
                        })
                    }
                    // Not suspended or partially suspended, so return earliest available fragment offset.
                    _ => self.fetch_earliest_offset(partition_index).await?,
                }
            }
            _ => {
                // If fully suspended, there are no actual fragments to search through, so we have no way to correlate
                // timestamps with offsets. Kafka returns UNKNOWN_OFFSET in this case, so we do the same.
                if let Some(suspend) = &partition.spec.suspend {
                    if suspend.level == journal_spec::suspend::Level::Full as i32 {
                        return Ok(Some(PartitionOffset {
                            fragment_start: suspend.offset,
                            offset: -1,   // UNKNOWN_OFFSET
                            mod_time: -1, // UNKNOWN_TIMESTAMP
                        }));
                    }
                }

                // Otherwise, list fragments with begin_mod_time <= timestamp_millis and return the latest fragment's begin offset.
                // This will return the currently open fragment if there is one and `timestamp_millis` is after any other fragment's
                // `begin_mod_time` since because the fragment is still open and hasn't been persisted to cloud storage, it doesn't
                // have a `begin_mod_time` at all. Not all journals will have an open fragment though, so we need to consider that.
                let (not_before_sec, _) = self
                    .not_before
                    .map(|not_before| not_before.to_unix())
                    .unwrap_or((0, 0));

                let timestamp = timestamp_millis / 1_000;
                let begin_mod_time = if timestamp < not_before_sec as i64 {
                    not_before_sec as i64
                } else {
                    timestamp as i64
                };

                let request = broker::FragmentsRequest {
                    journal: partition.spec.name.clone(),
                    begin_mod_time,
                    page_limit: 1,
                    ..Default::default()
                };
                let response = self.client().list_fragments(request).await?;

                match response.fragments.get(0) {
                    // We found a fragment covering the requested timestamp, or we found the currently open fragment.
                    Some(broker::fragments_response::Fragment {
                        spec: Some(spec), ..
                    }) => Some(PartitionOffset {
                        fragment_start: spec.begin,
                        offset: spec.begin,
                        mod_time: spec.mod_time,
                    }),
                    // The cases where this line hits are:
                    // * `suspend::Level::Partial` so there is no open fragment, and the provided timestamp is after any
                    //    existing persisted fragment's `mod_time` (and there cannot be an open fragment since the journal is partially suspended)
                    // * Not suspended, and either all fragments have expired from cloud storage, no data has ever been written,
                    //   or the provided timestamp is after any persisted fragment's `mod_time` and there is no open fragment
                    //   (maybe the collection hasn't seen any new data for longer than its flush interval?)
                    // Both of these cases are the same case as above when the journal is fully suspended: a request for offsets
                    // when there are no covering fragments. As I discovered above, Kafka returns `UNKNOWN_OFFSET` (-1) in this case,
                    // so I believe that Dekaf should too.
                    None => Some(PartitionOffset {
                        fragment_start: -1,
                        offset: -1,   // UNKNOWN_OFFSET
                        mod_time: -1, // UNKNOWN_TIMESTAMP
                    }),
                    Some(broker::fragments_response::Fragment { spec: None, .. }) => {
                        anyhow::bail!("fragment missing spec");
                    }
                }
            }
        };

        tracing::debug!(
            collection = self.source().collection_name,
            ?offset_data,
            partition_index,
            timestamp_millis,
            "fetched offset"
        );

        Ok(offset_data)
    }

    #[tracing::instrument(skip(self))]
    async fn fetch_earliest_offset(
        &self,
        partition_index: usize,
    ) -> anyhow::Result<Option<PartitionOffset>> {
        let Some(partition) = self.partitions.get(partition_index) else {
            return Ok(None);
        };

        let request = broker::FragmentsRequest {
            journal: partition.spec.name.clone(),
            begin_mod_time: 0, // Fetch earliest offset
            page_limit: 1,
            ..Default::default()
        };
        let response = self
            .client()
            .list_fragments(request)
            .await
            .context("listing fragments to fetch earliest offset")?;

        match response.fragments.get(0) {
            Some(broker::fragments_response::Fragment {
                spec: Some(spec), ..
            }) => Ok(Some(PartitionOffset {
                fragment_start: spec.begin,
                offset: spec.begin,
                mod_time: spec.mod_time,
            })),
            _ => Ok(None),
        }
    }

    /// Fetch the write head of a journal by issuing a non-blocking read request at offset -1
    #[tracing::instrument(skip(self))]
    async fn fetch_write_head(
        &self,
        partition_index: usize,
    ) -> anyhow::Result<Option<PartitionOffset>> {
        let Some(partition) = self.partitions.get(partition_index) else {
            return Ok(None);
        };

        let request = broker::ReadRequest {
            journal: partition.spec.name.clone(),
            offset: 0,
            metadata_only: true,
            ..Default::default()
        };
        let response_stream = self.client().clone().read(request);
        tokio::pin!(response_stream);

        // Continue polling the stream until we get Ok or a non-transient error
        loop {
            match response_stream.next().await {
                Some(Ok(ReadResponse {
                    write_head,
                    fragment,
                    ..
                })) => {
                    return Ok(Some(PartitionOffset {
                        fragment_start: fragment.map(|f| f.begin).unwrap_or(0),
                        offset: write_head,
                        mod_time: -1,
                    }));
                }
                Some(Err(e)) => {
                    if e.inner.is_transient() {
                        continue;
                    } else {
                        let msg = e.inner.to_string();
                        return Err(anyhow::Error::new(e.inner).context(format!(
                            "failed to fetch write head after {} retries: {msg}",
                            e.attempt
                        )));
                    }
                }
                None => anyhow::bail!("read stream ended unexpectedly"),
            }
        }
    }

    async fn registered_schema_id(
        auth: &SessionAuthentication,
        client: &postgrest::Postgrest,
        catalog_name: &str,
        schema: &avro::Schema,
    ) -> anyhow::Result<u32> {
        #[derive(serde::Deserialize)]
        struct Row {
            registry_id: u32,
        }

        // We map into a serde_json::Value to ensure stability of property order when content-summing.
        let schema: serde_json::Value = serde_json::to_value(&schema).unwrap();
        let schema_md5 = format!("{:x}", md5::compute(&schema.to_string()));

        let mut rows: Vec<Row> = crate::exec_postgrest(
            auth,
            client
                .from("registered_avro_schemas")
                .eq("avro_schema_md5", &schema_md5)
                .select("registry_id"),
        )
        .await
        .context("querying for an already-registered schema")?;

        if let Some(Row { registry_id }) = rows.pop() {
            return Ok(registry_id);
        }

        let mut rows: Vec<Row> = crate::exec_postgrest(
            auth,
            client.from("registered_avro_schemas").insert(
                serde_json::json!([{
                    "avro_schema": schema,
                    "catalog_name": catalog_name,
                }])
                .to_string(),
            ),
        )
        .await
        .context("inserting new registered schema")?;

        let registry_id = rows.pop().unwrap().registry_id;
        tracing::info!(schema_md5, registry_id, "registered new Avro schema");

        Ok(registry_id)
    }
}
