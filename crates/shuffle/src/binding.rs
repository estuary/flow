use anyhow::Context;
use proto_flow::{flow, shuffle};
use proto_gazette::{broker, uuid};

/// Binding represents the shuffle configuration for a single binding
/// (derivation transform, materialization binding, or ad-hoc collection read).
///
/// This struct captures the configuration needed to coordinate document shuffling
/// across task shards, independent of the specific task type.
#[derive(Debug)]
pub struct Binding {
    /// Index of this Binding within the task.
    pub index: u32,
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
    /// Pre-built key extractors for the shuffle key pointers.
    /// Empty if `uses_lambda` is true.
    pub key_extractors: Vec<doc::Extractor>,
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
    /// Non-ACK documents with clocks before this value are filtered.
    /// Clock::UNIX_EPOCH means no lower bound.
    pub not_before: uuid::Clock,
    /// Non-ACK documents with clocks after this value are filtered.
    /// Clock::from_u64(u64::MAX) means no upper bound.
    pub not_after: uuid::Clock,
    /// Cohort index for this binding. Bindings sharing the same
    /// (priority, read_delay) tuple belong to the same cohort.
    /// Assigned as ascending integers by walking bindings in index order
    /// and identifying unique (priority, read_delay) tuples.
    pub cohort: u32,
    /// Partition template name for this binding's source collection.
    /// Prefixes all partition journal names. Used to build the hint
    /// projection index.
    pub partition_template_name: Box<str>,
}

impl Binding {
    /// Extract the task name, bindings, and per-binding validators of a shuffle::Task.
    /// Validators are returned separately so that callers can store them independently
    /// (e.g. on SliceActor) or discard them (e.g. SessionActor).
    pub fn from_task(
        task: &shuffle::Task,
    ) -> anyhow::Result<(models::Name, Vec<Self>, Vec<doc::Validator>)> {
        let (name, pairs) = match &task.task {
            Some(shuffle::task::Task::Derivation(collection_spec)) => {
                let derivation = collection_spec
                    .derivation
                    .as_ref()
                    .context("CollectionSpec missing derivation")?;

                let pairs = derivation
                    .transforms
                    .iter()
                    .enumerate()
                    .map(|(index, transform)| {
                        Self::from_derivation_transform(index as u32, transform)
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;

                let shard_template_id = derivation
                    .shard_template
                    .as_ref()
                    .context("Derivation missing ShardTemplate")?
                    .id
                    .clone();

                (models::Name::new(shard_template_id), pairs)
            }
            Some(shuffle::task::Task::Materialization(materialization)) => {
                let pairs = materialization
                    .bindings
                    .iter()
                    .enumerate()
                    .map(|(index, binding)| {
                        Self::from_materialization_binding(index as u32, binding)
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;

                let shard_template_id = materialization
                    .shard_template
                    .as_ref()
                    .context("Materialization missing ShardTemplate")?
                    .id
                    .clone();

                (models::Name::new(shard_template_id), pairs)
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

                let pairs = vec![Self::from_collection_partitions(
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

                (models::Name::new(partition_template_name), pairs)
            }
            None => anyhow::bail!("missing task variant"),
        };

        let (mut bindings, validators): (Vec<Self>, Vec<doc::Validator>) =
            pairs.into_iter().unzip();

        assign_cohorts(&mut bindings);

        Ok((name, bindings, validators))
    }

    fn from_derivation_transform(
        index: u32,
        spec: &flow::collection_spec::derivation::Transform,
    ) -> anyhow::Result<(Self, doc::Validator)> {
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
            partition_template,
            projections,
            read_schema_json,
            uuid_ptr,
            write_schema_json,
        } = collection.as_ref().context("missing source collection")?;

        let partition_template_name = partition_template
            .as_ref()
            .context("missing partition template")?
            .name
            .as_str()
            .into();

        // read_delay is a duration, not an absolute timestamp.
        // Clock's internal representation is (100ns_ticks << 4 | sequence_counter),
        // so a duration of N seconds is (N * 10_000_000) << 4.
        let read_delay = uuid::Clock::from_u64((*read_delay_seconds as u64 * 10_000_000) << 4);
        let (not_before, not_after) = not_before_after(not_before.as_ref(), not_after.as_ref());

        let partition_selector = partition_selector
            .as_ref()
            .context("missing partition selector")?;

        let (validator, shape) = build_schema(read_schema_json, write_schema_json)?;

        // Determine shuffle key configuration.
        let (key_extractors, uses_lambda, uses_source_key, shuffle_key_partition_fields) =
            if !shuffle_key.is_empty() {
                // Explicit shuffle key provided.
                let uses_source_key = shuffle_key == key;
                let partition_fields = compute_partition_fields(shuffle_key, projections);
                (
                    build_key_extractors(shuffle_key, &shape),
                    false,
                    uses_source_key,
                    partition_fields,
                )
            } else if !shuffle_lambda_config_json.is_empty() {
                // Lambda-computed shuffle key.
                (Vec::new(), true, false, None)
            } else {
                // Default: use source collection key.
                (build_key_extractors(key, &shape), false, true, None)
            };

        let binding = Self {
            index,
            filter_r_clocks: *read_only,
            journal_read_suffix: journal_read_suffix.clone(),
            priority: *priority,
            projections: projections.clone(),
            read_delay,
            key_extractors,
            shuffle_key_partition_fields,
            partition_selector: partition_selector.clone(),
            collection: models::Collection::new(collection_name),
            source_uuid_ptr: json::Pointer::from_str(uuid_ptr),
            uses_lambda,
            uses_source_key,
            not_before,
            not_after,
            cohort: 0, // Assigned by assign_cohorts().
            partition_template_name,
        };

        Ok((binding, validator))
    }

    fn from_materialization_binding(
        index: u32,
        spec: &flow::materialization_spec::Binding,
    ) -> anyhow::Result<(Self, doc::Validator)> {
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
            partition_template,
            projections,
            read_schema_json,
            uuid_ptr,
            write_schema_json,
        } = collection.as_ref().context("missing source collection")?;

        let partition_template_name = partition_template
            .as_ref()
            .context("missing partition template")?
            .name
            .as_str()
            .into();

        let (not_before, not_after) = not_before_after(not_before.as_ref(), not_after.as_ref());

        let partition_selector = partition_selector
            .as_ref()
            .context("missing partition selector")?;

        let (validator, shape) = build_schema(read_schema_json, write_schema_json)?;

        let binding = Self {
            index,
            filter_r_clocks: false, // Always false for materializations.
            journal_read_suffix: journal_read_suffix.clone(),
            priority: *priority,
            projections: projections.clone(),
            read_delay: uuid::Clock::from_u64(0), // Always zero for materializations.
            key_extractors: build_key_extractors(key, &shape),
            shuffle_key_partition_fields: None, // Not computed for materializations.
            partition_selector: partition_selector.clone(),
            collection: models::Collection::new(collection_name),
            source_uuid_ptr: json::Pointer::from_str(uuid_ptr),
            uses_lambda: false,    // Always false for materializations.
            uses_source_key: true, // Always true for materializations.
            not_before,
            not_after,
            cohort: 0, // Assigned by assign_cohorts().
            partition_template_name,
        };

        Ok((binding, validator))
    }

    fn from_collection_partitions(
        spec: &flow::CollectionSpec,
        source_partitions: &broker::LabelSelector,
    ) -> anyhow::Result<(Self, doc::Validator)> {
        let flow::CollectionSpec {
            ack_template_json: _,
            derivation: _,
            key,
            name: collection_name,
            partition_fields: _,
            partition_template,
            projections,
            read_schema_json,
            uuid_ptr,
            write_schema_json,
        } = spec;

        let partition_template_name = partition_template
            .as_ref()
            .context("missing partition template")?
            .name
            .as_str()
            .into();

        let (validator, shape) = build_schema(read_schema_json, write_schema_json)?;

        let binding = Self {
            index: 0,
            filter_r_clocks: false,
            journal_read_suffix: "ad-hoc".to_string(),
            priority: 0,
            projections: projections.clone(),
            read_delay: uuid::Clock::from_u64(0),
            key_extractors: build_key_extractors(key, &shape),
            shuffle_key_partition_fields: None,
            partition_selector: source_partitions.clone(),
            collection: models::Collection::new(collection_name),
            source_uuid_ptr: json::Pointer::from_str(uuid_ptr),
            uses_lambda: false,
            uses_source_key: true,
            not_before: uuid::Clock::UNIX_EPOCH,
            not_after: uuid::Clock::from_u64(u64::MAX),
            cohort: 0, // Assigned by assign_cohorts().
            partition_template_name,
        };

        Ok((binding, validator))
    }

    pub fn state_key(&self) -> &str {
        self.journal_read_suffix.rsplit("/").next().unwrap()
    }
}

/// Assign cohort indices to bindings. Bindings sharing the same
/// (priority, read_delay) tuple belong to the same cohort. Cohorts are
/// assigned ascending integers by walking bindings in index order.
fn assign_cohorts(bindings: &mut [Binding]) {
    let mut seen: Vec<(u32, uuid::Clock)> = Vec::new();

    for binding in bindings.iter_mut() {
        let key = (binding.priority, binding.read_delay);

        let cohort = match seen.iter().position(|entry| *entry == key) {
            Some(idx) => idx,
            None => {
                seen.push(key);
                seen.len() - 1
            }
        };
        binding.cohort = cohort as u32;
    }
}

/// Parse a collection schema bundle into a Validator and inferred Shape.
/// Prefers the read schema; falls back to the write schema.
fn build_schema(
    read_schema_json: &bytes::Bytes,
    write_schema_json: &bytes::Bytes,
) -> anyhow::Result<(doc::validation::Validator, doc::Shape)> {
    let bundle = if !read_schema_json.is_empty() {
        read_schema_json
    } else {
        write_schema_json
    };
    let schema = doc::validation::build_bundle(bundle).context("failed to parse schema bundle")?;
    let validator =
        doc::validation::Validator::new(schema).context("failed to index schema bundle")?;
    let shape = doc::Shape::infer(validator.schema(), validator.schema_index());
    Ok((validator, shape))
}

/// Build key extractors from string-encoded JSON pointers,
/// using schema-annotated defaults from the inferred shape.
fn build_key_extractors(pointers: &[String], shape: &doc::Shape) -> Vec<doc::Extractor> {
    let policy = doc::SerPolicy::noop();
    pointers
        .iter()
        .map(|p| {
            let ptr = json::Pointer::from_str(p);
            let (located, _exists) = shape.locate(&ptr);
            let default = located
                .default
                .as_ref()
                .map(|d| d.0.clone())
                .unwrap_or(serde_json::Value::Null);
            doc::Extractor::with_default(p, &policy, default)
        })
        .collect()
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
