<<<<<<< HEAD
use crate::{connector, utils, SessionAuthentication, TaskState};
=======
use crate::{connector, utils, SessionAuthentication, TaskState, UserAuth};
>>>>>>> 6a10084407 (Update the world WIP)
use anyhow::{anyhow, bail, Context};
use futures::StreamExt;
use gazette::{
    broker::{self, journal_spec, ReadResponse},
    journal, uuid,
};
use models::RawValue;
use proto_flow::flow;

impl SessionAuthentication {
    pub async fn fetch_all_collection_names(&mut self) -> anyhow::Result<Vec<String>> {
        match self {
            SessionAuthentication::Task(auth) => auth.fetch_all_collection_names().await,
            SessionAuthentication::Redirect { spec, .. } => utils::fetch_all_collection_names(spec),
        }
    }

    pub async fn get_collection_for_topic(&self, topic_name: &str) -> anyhow::Result<String> {
        match self {
            SessionAuthentication::Task(auth) => {
                let binding = auth
                    .get_binding_for_topic(topic_name)
                    .await?
                    .ok_or(anyhow::anyhow!("Unrecognized topic {topic_name}"))?;

                Ok(binding
                    .collection
                    .as_ref()
                    .context("missing collection in materialization binding")?
                    .name
                    .clone())
            }
            SessionAuthentication::Redirect { spec, .. } => {
                let binding = utils::get_binding_for_topic(spec, topic_name)?
                    .ok_or(anyhow::anyhow!("Unrecognized topic {topic_name}"))?;

                Ok(binding
                    .collection
                    .as_ref()
                    .context("missing collection in materialization binding")?
                    .name
                    .clone())
            }
        }
    }
}

/// Collection is the assembled metadata of a collection being accessed as a Kafka topic.
pub struct Collection {
    pub name: String,
    pub journal_client: journal::Client,
    pub key_ptr: Vec<doc::Pointer>,
    pub key_schema: avro::Schema,
    pub not_before: Option<uuid::Clock>,
    pub not_after: Option<uuid::Clock>,
    pub partitions: Vec<Partition>,
    pub spec: flow::CollectionSpec,
    pub uuid_ptr: doc::Pointer,
    pub value_schema: avro::Schema,
    pub extractors: Vec<(avro::Schema, utils::CustomizableExtractor)>,
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
    /// Build a Collection by fetching its spec, an authenticated data-plane access token, and its partitions.
    pub async fn new(
        auth: &SessionAuthentication,
        topic_name: &str,
    ) -> anyhow::Result<Option<Self>> {
        let binding = match auth {
            SessionAuthentication::Task(task_auth) => {
                if let Some(binding) = task_auth.get_binding_for_topic(topic_name).await? {
                    binding
                } else {
                    tracing::warn!("{topic_name} is not a binding of {}", task_auth.task_name);
                    return Ok(None);
                }
            }
            SessionAuthentication::Redirect { spec, .. } => {
                let Some(binding) = utils::get_binding_for_topic(spec, topic_name)
                    .context("failed to get binding for topic in redirected session")?
                else {
                    tracing::warn!("{topic_name} is not a binding of {}", spec.name);
                    return Ok(None);
                };
                binding
            }
        };

        let collection_spec = binding
            .collection
            .clone()
            .ok_or_else(|| anyhow::anyhow!("missing collection in materialization binding"))?;

        let collection_name = &auth.get_collection_for_topic(topic_name).await?;

        let partition_template_name = collection_spec
            .partition_template
            .as_ref()
            .map(|spec| spec.name.to_owned())
            .ok_or(anyhow!("missing partition template"))?;

        let (journal_client, partitions) = match auth {
            SessionAuthentication::Task(task_auth) => {
                let state = task_auth.task_state_listener.get().await?;

                let partitions = match state.as_ref() {
                    TaskState::Authorized { partitions, .. } => partitions,
                    TaskState::Redirect {
                        target_dataplane_fqdn,
                        spec,
                        ..
                    } => {
                        return Err(crate::DekafError::from_redirect(
                            target_dataplane_fqdn.to_owned(),
                            spec.clone(),
                        )
                        .await?
                        .into());
                    }
                };

                let (_, parts) = partitions
                    .into_iter()
                    .find(|(name, _)| name == &partition_template_name)
                    .context("missing partition template")?;

                parts
                    .clone()
                    .map(|(client, _, parts)| (client, parts))
                    .map_err(|e| anyhow::Error::from(e))?
            }
            SessionAuthentication::Redirect {
                target_dataplane_fqdn,
                spec,
                config,
                ..
            } => {
                return Err(crate::DekafError::TaskRedirected {
                    target_dataplane_fqdn: target_dataplane_fqdn.clone(),
                    spec: spec.clone(),
                    config: config.clone(),
                }
                .into());
            }
        };

        tracing::debug!(?partitions, "Got partitions");

        let key_ptr: Vec<doc::Pointer> = collection_spec
            .key
            .iter()
            .map(|p| doc::Pointer::from_str(p))
            .collect();
        let uuid_ptr = doc::Pointer::from_str(&collection_spec.uuid_ptr);

        let json_schema = if collection_spec.read_schema_json.is_empty() {
            &collection_spec.write_schema_json
        } else {
            &collection_spec.read_schema_json
        };

        let json_schema = doc::validation::build_bundle(json_schema)?;
        let validator = doc::Validator::new(json_schema)?;
        let collection_schema_shape =
            doc::Shape::infer(&validator.schemas()[0], validator.schema_index());

        let selection = binding
            .field_selection
            .clone()
            .context("missing field selection in materialization binding")?;

<<<<<<< HEAD
        let (value_schema, extractors) = utils::build_field_extractors(
            collection_schema_shape.clone(),
            selection,
            collection_spec.projections.clone(),
            auth.deletions(),
        )?;
=======
            utils::build_field_extractors(
                collection_schema_shape.clone(),
                selection,
                collection_spec.projections.clone(),
                auth.deletions(),
            )?
        } else {
            utils::build_legacy_field_extractors(collection_schema_shape.clone(), auth.deletions())?
        };
>>>>>>> 6a10084407 (Update the world WIP)

        let key_schema = avro::key_to_avro(&key_ptr, collection_schema_shape);

        let (not_before, not_after) = (
            binding.not_before.map(|b| {
                uuid::Clock::from_unix(b.seconds.try_into().unwrap(), b.nanos.try_into().unwrap())
            }),
            binding.not_after.map(|b| {
                uuid::Clock::from_unix(b.seconds.try_into().unwrap(), b.nanos.try_into().unwrap())
            }),
        );

        tracing::debug!(
            collection_name,
            partitions = partitions.len(),
            "built collection"
        );

        Ok(Some(Self {
            name: collection_name.to_string(),
            journal_client,
            key_ptr,
            key_schema,
            not_before,
            not_after,
            partitions,
            spec: collection_spec,
            uuid_ptr,
            value_schema,
            extractors,
        }))
    }

    /// Map the collection's key and value Avro schema into globally unique registry IDs.
    /// This will content-address each schema to fetch a current registry ID if one is available,
    /// or will register a new schema if not.
    pub async fn registered_schema_ids(
        &self,
        client: &postgrest::Postgrest,
    ) -> anyhow::Result<(u32, u32)> {
        let (key_id, value_id) = futures::try_join!(
            Self::registered_schema_id(client, &self.spec.name, &self.key_schema),
            Self::registered_schema_id(client, &self.spec.name, &self.value_schema),
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
                let response = self.journal_client.list_fragments(request).await?;

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
            collection = self.spec.name,
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
            .journal_client
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
            offset: -1, // Fetch write head
            ..Default::default()
        };
        let response_stream = self.journal_client.clone().read(request);
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
                    }))
                }
                Some(Err(e)) => {
                    if e.inner.is_transient() {
                        continue;
                    } else {
                        return Err(anyhow::Error::new(e.inner).context(format!(
                            "failed to fetch write head after {} retries",
                            e.attempt
                        )));
                    }
                }
                None => anyhow::bail!("read stream ended unexpectedly"),
            }
        }
    }

    async fn registered_schema_id(
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

        let mut rows: Vec<Row> = handle_postgrest_response(
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

        let mut rows: Vec<Row> = handle_postgrest_response(
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

async fn handle_postgrest_response<T: serde::de::DeserializeOwned>(
    builder: postgrest::Builder,
) -> anyhow::Result<T> {
    let resp = builder.execute().await?;
    let status = resp.status();

    if status.is_client_error() || status.is_server_error() {
        bail!(
            "{}: {}",
            status.canonical_reason().unwrap_or(status.as_str()),
            resp.text().await?
        )
    } else {
        let bytes = resp.bytes().await?;
        let result: T = serde_json::from_slice(&bytes)?;
        Ok(result)
    }
}

pub async fn extract_dekaf_config(
    spec: &proto_flow::flow::MaterializationSpec,
) -> anyhow::Result<connector::DekafConfig> {
    if spec.connector_type != proto_flow::flow::materialization_spec::ConnectorType::Dekaf as i32 {
        anyhow::bail!("Not a Dekaf materialization")
    }
    let config = serde_json::from_slice::<models::DekafConfig>(&spec.config_json)?;

    let decrypted_endpoint_config =
        unseal::decrypt_sops(&RawValue::from_str(&config.config.to_string())?).await?;

    let dekaf_config =
        serde_json::from_str::<connector::DekafConfig>(decrypted_endpoint_config.get())?;
    Ok(dekaf_config)
}
