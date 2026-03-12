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
    pub index: u16,
    /// Source collection name (for logging/debugging).
    pub collection: models::Collection,
    /// Should documents be filtered on their r-clocks?
    ///
    /// Intuitively, the purpose of r-clock filtering is to enable scale-out of
    /// CQRS workflows. Suppose a large collection L being joined with a small one S.
    /// L is high volume, so we want to divide work across many task shards.
    /// Each would split using the same, full key range, but non-overlapping r-clock spans.
    ///
    /// * A read/write transform of S would broadcast to all shards and update
    ///   internal derivation state of each (e.x., indexing on a join key).
    ///
    /// * A read-only transform of L would route to a *single* shard on its r-clock.
    ///   That shard would query internal state (on the join key) to publish an
    ///   enriched document.
    ///
    /// This field is only true for read-only derivation transforms.
    pub filter_r_clocks: bool,
    /// Path metadata suffix attached to journals read by this binding.
    /// Used to uniquely identify journal read checkpoints.
    pub journal_read_suffix: String,
    /// Priority of this binding with respect to others of the task.
    /// Higher values imply higher priority. Documents are ordered by
    /// (priority DESC, adjusted_clock ASC).
    pub priority: u32,
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
    /// Prefixes all partition journal names, and does not end in '/'.
    /// Used to build the hint projection index.
    pub partition_template_name: Box<str>,
    /// Sorted partition field names for this binding's source collection.
    /// Used to build partition filters for hint projection.
    pub partition_fields: Vec<String>,
}

impl Binding {
    /// Extract the task name, bindings, and per-binding validators of a shuffle::Task.
    /// Validators are returned separately so that callers can store them independently
    /// (e.g. on SliceActor) or discard them (e.g. SessionActor).
    pub fn from_task(
        task: &shuffle::Task,
    ) -> anyhow::Result<(models::Name, Vec<Self>, Vec<doc::Validator>, u64)> {
        let (name, pairs, disk_backlog_threshold) = match &task.task {
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
                        Self::from_derivation_transform(index as u16, transform)
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;

                let shard_template_id = derivation
                    .shard_template
                    .as_ref()
                    .context("Derivation missing ShardTemplate")?
                    .id
                    .clone();

                // TODO: extract from task spec.
                let disk_backlog_threshold = 10 * 1024 * 1024 * 1024u64;

                (
                    models::Name::new(shard_template_id),
                    pairs,
                    disk_backlog_threshold,
                )
            }
            Some(shuffle::task::Task::Materialization(materialization)) => {
                let pairs = materialization
                    .bindings
                    .iter()
                    .enumerate()
                    .map(|(index, binding)| {
                        Self::from_materialization_binding(index as u16, binding)
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;

                let shard_template_id = materialization
                    .shard_template
                    .as_ref()
                    .context("Materialization missing ShardTemplate")?
                    .id
                    .clone();

                // TODO: extract from task spec.
                let disk_backlog_threshold = 10 * 1024 * 1024 * 1024u64;

                (
                    models::Name::new(shard_template_id),
                    pairs,
                    disk_backlog_threshold,
                )
            }
            Some(shuffle::task::Task::CollectionPartitions(collection_partitions)) => {
                let shuffle::CollectionPartitions {
                    collection,
                    partition_selector,
                    disk_backlog_threshold,
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

                (
                    models::Name::new(partition_template_name),
                    pairs,
                    *disk_backlog_threshold,
                )
            }
            None => anyhow::bail!("missing task variant"),
        };

        let (mut bindings, validators): (Vec<Self>, Vec<doc::Validator>) =
            pairs.into_iter().unzip();

        assign_cohorts(&mut bindings);

        Ok((name, bindings, validators, disk_backlog_threshold))
    }

    fn from_derivation_transform(
        index: u16,
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
            partition_fields,
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
            partition_fields: partition_fields.clone(),
        };

        Ok((binding, validator))
    }

    fn from_materialization_binding(
        index: u16,
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
            partition_fields,
            partition_template,
            projections: _,
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
            partition_fields: partition_fields.clone(),
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
            partition_fields,
            partition_template,
            projections: _,
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
            partition_fields: partition_fields.clone(),
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

/// Filter that matches journal name suffixes against a binding's partition selector.
///
/// Journal names are structured as `{partition_template_name}/{field1}={val1}/.../{fieldN}={valN}/pivot={hex}`.
/// The suffix (everything after the template name plus trailing `/`) encodes sorted partition
/// field values. `PartitionFilter` checks each field value against include/exclude constraints
/// derived from the binding's `LabelSelector`.
#[derive(Debug, Clone)]
pub struct PartitionFilter {
    /// 1:1 with `CollectionSpec.partition_fields`, same order.
    constraints: Vec<FieldConstraint>,
}

#[derive(Debug, Clone)]
struct FieldConstraint {
    /// Partition field name (bare, without `estuary.dev/field/` prefix).
    field: Box<str>,
    /// Sorted encoded values that the field MUST match (OR semantics).
    /// Empty means no include constraint — any value is accepted.
    include: Vec<Box<str>>,
    /// Sorted encoded values that the field MUST NOT match (OR semantics).
    /// Empty means no exclude constraint.
    exclude: Vec<Box<str>>,
}

impl PartitionFilter {
    /// Build a filter from sorted partition field names and a label selector.
    pub fn new(partition_fields: &[String], selector: &broker::LabelSelector) -> Self {
        let include_set = selector.include.as_ref();
        let exclude_set = selector.exclude.as_ref();

        let constraints = partition_fields
            .iter()
            .map(|field| {
                let label_name = format!("{}{field}", labels::FIELD_PREFIX);

                let include: Vec<Box<str>> = include_set
                    .map(|set| {
                        let mut vals: Vec<Box<str>> = labels::values(set, &label_name)
                            .iter()
                            .map(|l| Box::from(l.value.as_str()))
                            .collect();
                        vals.sort();
                        vals
                    })
                    .unwrap_or_default();

                let exclude: Vec<Box<str>> = exclude_set
                    .map(|set| {
                        let mut vals: Vec<Box<str>> = labels::values(set, &label_name)
                            .iter()
                            .map(|l| Box::from(l.value.as_str()))
                            .collect();
                        vals.sort();
                        vals
                    })
                    .unwrap_or_default();

                FieldConstraint {
                    field: Box::from(field.as_str()),
                    include,
                    exclude,
                }
            })
            .collect();

        Self { constraints }
    }

    /// Check whether a journal name suffix matches this filter.
    ///
    /// `suffix` is the journal name after removing the partition template name
    /// and its trailing `/`, e.g. `field1=val1/field2=val2/.../pivot=hex`.
    pub fn matches_name_suffix(&self, suffix: &str) -> anyhow::Result<bool> {
        // Walk partition field segments and constraints in lockstep.
        let mut remaining = suffix;
        for (i, constraint) in self.constraints.iter().enumerate() {
            if remaining.is_empty() {
                anyhow::bail!(
                    "partition filter has {} constraints but suffix {suffix:?} has only {i} field segment(s)",
                    self.constraints.len(),
                );
            }

            let segment = match remaining.find('/') {
                Some(pos) => {
                    let segment = &remaining[..pos];
                    remaining = &remaining[pos + 1..];
                    segment
                }
                // Last segment (the pivot) — but we still have constraints to match.
                None => {
                    anyhow::bail!(
                        "partition filter has {} constraints but suffix {suffix:?} \
                         has only {i} field segment(s)",
                        self.constraints.len(),
                    );
                }
            };

            let (parsed_field, parsed_value) = segment.split_once('=').ok_or_else(|| {
                anyhow::anyhow!("malformed partition segment (no '='): {segment:?}")
            })?;

            if parsed_field != constraint.field.as_ref() {
                anyhow::bail!(
                    "partition field mismatch: expected {:?}, got {parsed_field:?} in suffix {suffix:?}",
                    constraint.field,
                );
            }

            if !constraint.include.is_empty()
                && constraint
                    .include
                    .binary_search_by(|v| v.as_ref().cmp(parsed_value))
                    .is_err()
            {
                return Ok(false);
            }

            if !constraint.exclude.is_empty()
                && constraint
                    .exclude
                    .binary_search_by(|v| v.as_ref().cmp(parsed_value))
                    .is_ok()
            {
                return Ok(false);
            }
        }

        // Verify the remaining portion is exactly `pivot=...` (no more field segments).
        if remaining.contains('/') {
            anyhow::bail!(
                "partition filter has {} constraints but suffix {suffix:?} has {} field segment(s)",
                self.constraints.len(),
                suffix.matches('/').count(),
            );
        }

        Ok(true)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn selector(include: &[(&str, &str)], exclude: &[(&str, &str)]) -> broker::LabelSelector {
        let mk = |pairs: &[(&str, &str)]| {
            labels::build_set(
                pairs
                    .iter()
                    .map(|(k, v)| (format!("estuary.dev/field/{k}"), v.to_string())),
            )
        };
        broker::LabelSelector {
            include: if include.is_empty() {
                None
            } else {
                Some(mk(include))
            },
            exclude: if exclude.is_empty() {
                None
            } else {
                Some(mk(exclude))
            },
        }
    }

    fn fields(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_partition_filter() {
        // (partition_fields, selector_include, selector_exclude, suffix, expected)
        let cases: &[(
            &[&str],
            &[(&str, &str)],
            &[(&str, &str)],
            &str,
            Result<bool, &str>,
        )] = &[
            // No partition fields: any journal matches.
            (&[], &[], &[], "pivot=00", Ok(true)),
            // No selector constraints: passthrough.
            (&["region"], &[], &[], "region=us/pivot=00", Ok(true)),
            // Include match.
            (
                &["region"],
                &[("region", "us")],
                &[],
                "region=us/pivot=00",
                Ok(true),
            ),
            // Include miss.
            (
                &["region"],
                &[("region", "us")],
                &[],
                "region=eu/pivot=00",
                Ok(false),
            ),
            // Exclude match (journal excluded).
            (
                &["region"],
                &[],
                &[("region", "eu")],
                "region=eu/pivot=00",
                Ok(false),
            ),
            // Exclude miss (journal accepted).
            (
                &["region"],
                &[],
                &[("region", "eu")],
                "region=us/pivot=00",
                Ok(true),
            ),
            // Include + exclude: included value that is also excluded.
            (
                &["region"],
                &[("region", "eu"), ("region", "us")],
                &[("region", "eu")],
                "region=eu/pivot=00",
                Ok(false), // Exclude takes precedence.
            ),
            // Include + exclude: included value that is not excluded.
            (
                &["region"],
                &[("region", "eu"), ("region", "us")],
                &[("region", "eu")],
                "region=us/pivot=00",
                Ok(true),
            ),
            // Multiple fields: both pass.
            (
                &["category", "region"],
                &[("category", "alpha")],
                &[("region", "bad")],
                "category=alpha/region=good/pivot=00",
                Ok(true),
            ),
            // Multiple fields: include fails on first field.
            (
                &["category", "region"],
                &[("category", "alpha")],
                &[],
                "category=beta/region=good/pivot=00",
                Ok(false),
            ),
            // Multiple fields: exclude triggers on second field.
            (
                &["category", "region"],
                &[],
                &[("region", "bad")],
                "category=alpha/region=bad/pivot=00",
                Ok(false),
            ),
            // Selector constrains a field not in partition_fields: passthrough
            // (the constraint simply doesn't appear in the filter).
            (
                &["region"],
                &[("color", "red")],
                &[],
                "region=us/pivot=00",
                Ok(true),
            ),
            // Multiple include values (OR semantics).
            (
                &["region"],
                &[("region", "eu"), ("region", "us")],
                &[],
                "region=eu/pivot=00",
                Ok(true),
            ),
            (
                &["region"],
                &[("region", "eu"), ("region", "us")],
                &[],
                "region=ap/pivot=00",
                Ok(false),
            ),
            // Encoded non-string values.
            (
                &["active"],
                &[("active", "%_true")],
                &[],
                "active=%_true/pivot=00",
                Ok(true),
            ),
            (
                &["active"],
                &[("active", "%_true")],
                &[],
                "active=%_false/pivot=00",
                Ok(false),
            ),
            (
                &["count"],
                &[("count", "%_42")],
                &[],
                "count=%_42/pivot=00",
                Ok(true),
            ),
            // Error: too few field segments.
            (
                &["category", "region"],
                &[],
                &[],
                "category=alpha/pivot=00",
                Err("has only 1 field segment(s)"),
            ),
            // Error: too many field segments.
            (
                &["region"],
                &[],
                &[],
                "region=us/extra=oops/pivot=00",
                Err("has 2 field segment(s)"),
            ),
            // Error: field name mismatch.
            (
                &["region"],
                &[],
                &[],
                "zone=us/pivot=00",
                Err("partition field mismatch"),
            ),
            // Error: malformed segment (no '=').
            (
                &["region"],
                &[],
                &[],
                "badstuff/pivot=00",
                Err("malformed partition segment"),
            ),
            // Error: suffix with no pivot (just a bare segment, no '/').
            (
                &["region"],
                &[],
                &[],
                "pivot=00",
                Err("has only 0 field segment(s)"),
            ),
        ];

        for (i, &(pf, inc, exc, suffix, ref expected)) in cases.iter().enumerate() {
            let filter = PartitionFilter::new(&fields(pf), &selector(inc, exc));
            let result = filter.matches_name_suffix(suffix);

            match expected {
                Ok(want) => {
                    assert_eq!(
                        result.unwrap(),
                        *want,
                        "case {i}: suffix={suffix:?}, fields={pf:?}"
                    );
                }
                Err(msg) => {
                    let err = result.unwrap_err();
                    assert!(
                        err.to_string().contains(msg),
                        "case {i}: expected error containing {msg:?}, got: {err}"
                    );
                }
            }
        }
    }
}
