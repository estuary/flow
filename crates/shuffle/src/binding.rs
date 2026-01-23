use anyhow::Context;
use proto_flow::{flow, shuffle};
use proto_gazette::{broker, uuid};

/// Binding represents the shuffle configuration for a single binding
/// (derivation transform, materialization binding, or ad-hoc collection read).
///
/// This struct captures the configuration needed to coordinate document shuffling
/// across task shards, independent of the specific task type.
#[derive(Debug, Clone)]
pub struct Binding {
    /// Index of this Binding within the task.
    pub index: usize,
    /// Source collection name (for logging/debugging).
    pub collection: models::Collection,
    /// Should documents be filtered on their r-clocks?
    /// Only true for read-only derivation transforms.
    pub filter_r_clocks: bool,
    /// Path metadata suffix attached to journals read by this binding.
    /// Used to uniquely identify journal read checkpoints.
    pub journal_read_suffix: String,
    /// Priority of this binding with respect to others of the task.
    /// Higher values imply higher priority. Documents are ordered by
    /// (priority DESC, adjusted_clock ASC).
    pub priority: u32,
    /// Projections of the shuffled source collection.
    /// Used for schema validation and field extraction.
    pub projections: Vec<flow::Projection>,
    /// Read delay as a relative Clock delta.
    /// Applied to document clocks to impose ordering across transforms
    /// and gate documents until wall-time catches up.
    pub read_delay: uuid::Clock,
    /// Shuffle key as JSON pointers, or empty if `uses_lambda` is true.
    pub shuffle_key: Vec<json::Pointer>,
    /// Partitioned projection fields which fully cover the shuffle key.
    /// When non-empty, enables static key hash computation from partition labels.
    /// None if shuffle key is not fully covered by partition fields.
    pub shuffle_key_partition_fields: Option<Vec<String>>,
    /// Partition selector for filtering source collection journals.
    pub partition_selector: broker::LabelSelector,
    /// JSON pointer for extracting document UUIDs.
    pub source_uuid_ptr: json::Pointer,
    /// True if shuffle key is dynamically computed via a lambda function.
    pub uses_lambda: bool,
    /// True if shuffle key equals the source collection key.
    pub uses_source_key: bool,
    /// JSON schema for validating documents on read.
    /// Only used for derivation transforms; None for materializations.
    pub validate_schema: Option<bytes::Bytes>,
    /// Non-ACK documents with clocks before this value are filtered.
    /// Clock::UNIX_EPOCH means no lower bound.
    pub not_before: uuid::Clock,
    /// Non-ACK documents with clocks after this value are filtered.
    /// Clock::from_u64(u64::MAX) means no upper bound.
    pub not_after: uuid::Clock,
}

impl Binding {
    /// Extract the task name and bindings of a shuffle::Task.
    pub fn from_task(task: &shuffle::Task) -> anyhow::Result<(models::Name, Vec<Self>)> {
        match &task.task {
            Some(shuffle::task::Task::Derivation(collection_spec)) => {
                let derivation = collection_spec
                    .derivation
                    .as_ref()
                    .context("CollectionSpec missing derivation")?;

                let bindings = derivation
                    .transforms
                    .iter()
                    .enumerate()
                    .map(|(index, transform)| Self::from_derivation_transform(index, transform))
                    .collect::<anyhow::Result<Vec<_>>>()?;

                let shard_template_id = derivation
                    .shard_template
                    .as_ref()
                    .context("Derivation missing ShardTemplate")?
                    .id
                    .clone();

                Ok((models::Name::new(shard_template_id), bindings))
            }
            Some(shuffle::task::Task::Materialization(materialization)) => {
                let bindings = materialization
                    .bindings
                    .iter()
                    .enumerate()
                    .map(|(index, binding)| Self::from_materialization_binding(index, binding))
                    .collect::<anyhow::Result<Vec<_>>>()?;

                let shard_template_id = materialization
                    .shard_template
                    .as_ref()
                    .context("Materialization missing ShardTemplate")?
                    .id
                    .clone();

                Ok((models::Name::new(shard_template_id), bindings))
            }
            Some(shuffle::task::Task::CollectionPartitions(collection_partitions)) => {
                let shuffle::CollectionPartitions {
                    collection,
                    partition_selector,
                } = collection_partitions;

                let collection_spec = collection
                    .as_ref()
                    .context("CollectionPartitions missing CollectionSpec")?;

                let partition_selector = partition_selector
                    .as_ref()
                    .context("CollectionPartitions missing partition selector")?;

                let bindings = vec![Self::from_collection_partitions(
                    collection_spec,
                    partition_selector,
                )?];

                // NOTE(johnny): In practice, this name doesn't matter. Data-plane
                // tasks don't perform ad-hoc collection reads -- only flowctl does,
                // and flowctl uses flow_client::workflows::UserCollectionAuth which
                // doesn't require the task shard ID or template ID.
                let partition_template_name = collection_spec
                    .partition_template
                    .as_ref()
                    .context("CollectionSpec missing PartitionTemplate")?
                    .name
                    .clone();

                Ok((models::Name::new(partition_template_name), bindings))
            }
            None => anyhow::bail!("missing task variant"),
        }
    }

    fn from_derivation_transform(
        index: usize,
        spec: &flow::collection_spec::derivation::Transform,
    ) -> anyhow::Result<Self> {
        let flow::collection_spec::derivation::Transform {
            backfill: _,
            collection,
            journal_read_suffix,
            lambda_config_json: _,
            name: _,
            not_after,
            not_before,
            partition_selector,
            priority,
            read_delay_seconds,
            read_only,
            shuffle_key,
            shuffle_lambda_config_json,
        } = spec;

        let flow::CollectionSpec {
            ack_template_json: _,
            derivation: _,
            key,
            name: collection_name,
            partition_fields: _,
            partition_template: _,
            projections,
            read_schema_json,
            uuid_ptr,
            write_schema_json,
        } = collection.as_ref().context("missing source collection")?;

        let read_delay = uuid::Clock::from_unix(*read_delay_seconds as u64, 0);
        let (not_before, not_after) = not_before_after(not_before.as_ref(), not_after.as_ref());

        let partition_selector = partition_selector
            .as_ref()
            .context("missing partition selector")?;

        // Prefer read schema, fall back to write schema.
        let validate_schema = if !read_schema_json.is_empty() {
            Some(read_schema_json.clone())
        } else if !write_schema_json.is_empty() {
            Some(write_schema_json.clone())
        } else {
            None
        };

        // Determine shuffle key configuration.
        let (shuffle_key, uses_lambda, uses_source_key, shuffle_key_partition_fields) =
            if !shuffle_key.is_empty() {
                // Explicit shuffle key provided.
                let uses_source_key = shuffle_key == key;
                let partition_fields = compute_partition_fields(shuffle_key, projections);
                (
                    shuffle_key
                        .iter()
                        .map(|p| json::Pointer::from_str(p))
                        .collect(),
                    false,
                    uses_source_key,
                    partition_fields,
                )
            } else if !shuffle_lambda_config_json.is_empty() {
                // Lambda-computed shuffle key.
                (Vec::new(), true, false, None)
            } else {
                // Default: use source collection key.
                (
                    key.iter().map(|p| json::Pointer::from_str(p)).collect(),
                    false,
                    true,
                    None,
                )
            };

        Ok(Binding {
            index,
            filter_r_clocks: *read_only,
            journal_read_suffix: journal_read_suffix.clone(),
            priority: *priority,
            projections: projections.clone(),
            read_delay,
            shuffle_key,
            shuffle_key_partition_fields,
            partition_selector: partition_selector.clone(),
            collection: models::Collection::new(collection_name),
            source_uuid_ptr: json::Pointer::from_str(uuid_ptr),
            uses_lambda,
            uses_source_key,
            validate_schema,
            not_before,
            not_after,
        })
    }

    fn from_materialization_binding(
        index: usize,
        spec: &flow::materialization_spec::Binding,
    ) -> anyhow::Result<Self> {
        let flow::materialization_spec::Binding {
            backfill: _,
            collection,
            delta_updates: _,
            deprecated_shuffle: _,
            field_selection: _,
            journal_read_suffix,
            not_after,
            not_before,
            partition_selector,
            priority,
            resource_config_json: _,
            resource_path: _,
            ser_policy: _,
            state_key: _,
        } = spec;

        let flow::CollectionSpec {
            ack_template_json: _,
            derivation: _,
            key,
            name: collection_name,
            partition_fields: _,
            partition_template: _,
            projections,
            read_schema_json: _,
            uuid_ptr,
            write_schema_json: _,
        } = collection.as_ref().context("missing source collection")?;

        let (not_before, not_after) = not_before_after(not_before.as_ref(), not_after.as_ref());
        let shuffle_key = key.iter().map(|p| json::Pointer::from_str(p)).collect();

        let partition_selector = partition_selector
            .as_ref()
            .context("missing partition selector")?;

        Ok(Binding {
            index,
            filter_r_clocks: false, // Always false for materializations.
            journal_read_suffix: journal_read_suffix.clone(),
            priority: *priority,
            projections: projections.clone(),
            read_delay: uuid::Clock::UNIX_EPOCH, // Always zero for materializations.
            shuffle_key,
            shuffle_key_partition_fields: None, // Not computed for materializations.
            partition_selector: partition_selector.clone(),
            collection: models::Collection::new(collection_name),
            source_uuid_ptr: json::Pointer::from_str(uuid_ptr),
            uses_lambda: false,    // Always false for materializations.
            uses_source_key: true, // Always true for materializations.
            validate_schema: None, // Never validate on read for materializations.
            not_before,
            not_after,
        })
    }

    fn from_collection_partitions(
        spec: &flow::CollectionSpec,
        source_partitions: &broker::LabelSelector,
    ) -> anyhow::Result<Self> {
        let flow::CollectionSpec {
            ack_template_json: _,
            derivation: _,
            key,
            name: collection_name,
            partition_fields: _,
            partition_template: _,
            projections,
            read_schema_json: _,
            uuid_ptr,
            write_schema_json: _,
        } = spec;

        let shuffle_key = key.iter().map(|p| json::Pointer::from_str(p)).collect();

        Ok(Binding {
            index: 0,
            filter_r_clocks: false,
            journal_read_suffix: String::new(), // No suffix for ad-hoc reads.
            priority: 0,
            projections: projections.clone(),
            read_delay: uuid::Clock::UNIX_EPOCH,
            shuffle_key,
            shuffle_key_partition_fields: None,
            partition_selector: source_partitions.clone(),
            collection: models::Collection::new(collection_name),
            source_uuid_ptr: json::Pointer::from_str(uuid_ptr),
            uses_lambda: false,
            uses_source_key: true,
            validate_schema: None,
            not_before: uuid::Clock::UNIX_EPOCH,
            not_after: uuid::Clock::from_u64(u64::MAX),
        })
    }
}

/// Compute partition fields that fully cover the shuffle key.
/// Returns None if any shuffle key pointer lacks a corresponding partition projection.
fn compute_partition_fields(
    shuffle_key: &[String],
    projections: &[flow::Projection],
) -> Option<Vec<String>> {
    let mut fields = Vec::with_capacity(shuffle_key.len());

    for ptr in shuffle_key {
        let field = projections
            .iter()
            .find(|p| &p.ptr == ptr && p.is_partition_key)
            .map(|p| p.field.clone());

        match field {
            Some(f) => fields.push(f),
            None => return None, // Not all keys covered by partitions.
        }
    }

    Some(fields)
}

/// Convert optional Timestamps to Clock bounds.
fn not_before_after(
    not_before: Option<&pbjson_types::Timestamp>,
    not_after: Option<&pbjson_types::Timestamp>,
) -> (uuid::Clock, uuid::Clock) {
    let before = not_before
        .map(|ts| uuid::Clock::from_unix(ts.seconds as u64, ts.nanos as u32))
        .unwrap_or(uuid::Clock::UNIX_EPOCH);
    let after = not_after
        .map(|ts| uuid::Clock::from_unix(ts.seconds as u64, ts.nanos as u32))
        .unwrap_or_else(|| uuid::Clock::from_u64(u64::MAX));

    (before, after)
}
