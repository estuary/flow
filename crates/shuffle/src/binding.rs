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
    /// Index of this Binding's source collection within the task's Sources.
    pub source: u32,
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
}

/// Source is a source collection read by one or more Bindings of a task.
///
/// Sources follow the spec's declared indirection: bindings sharing a
/// `collection_index` share one Source, while a binding of an inline-form spec
/// carries no index and is its own Source.
#[derive(Debug)]
pub struct Source {
    /// Source collection name (for logging/debugging).
    pub collection: models::Collection,
    /// JSON pointer for extracting document UUIDs.
    pub uuid_ptr: json::Pointer,
    /// Sorted partition field names of the collection.
    /// Used to build partition filters for hint projection.
    pub partition_fields: Vec<String>,
    /// Prefix of journal partition names of the collection, including a
    /// trailing '/'. Used to build the hint index and gazette clients.
    pub partition_prefix: Box<str>,
}

impl Source {
    /// Build the Source for `collection`, returning it with the Validator and
    /// inferred Shape of its schema.
    fn new(
        collection: &flow::CollectionSpec,
    ) -> anyhow::Result<(Self, doc::Validator, doc::Shape)> {
        let partition_template_name = collection
            .partition_template
            .as_ref()
            .context("missing partition template")?
            .name
            .as_str();

        // Read documents are validated against the read schema,
        // or the write schema when no read schema is defined.
        let bundle = if !collection.read_schema_json.is_empty() {
            &collection.read_schema_json
        } else {
            &collection.write_schema_json
        };
        let schema = doc::validation::build_bundle(bundle)
            .with_context(|| format!("parsing schema of collection {}", collection.name))?;
        let validator = doc::validation::Validator::new(schema)
            .with_context(|| format!("indexing schema of collection {}", collection.name))?;
        let shape = doc::Shape::infer(validator.schema(), validator.schema_index());

        Ok((
            Self {
                collection: models::Collection::new(&collection.name),
                uuid_ptr: json::Pointer::from_str(&collection.uuid_ptr),
                partition_fields: collection.partition_fields.clone(),
                partition_prefix: format!("{partition_template_name}/").into(),
            },
            validator,
            shape,
        ))
    }
}

/// Resolve `collection` to its Source, building one when it's first referenced.
/// `validators` and `shapes` parallel `sources`, holding each schema's
/// Validator and inferred Shape.
fn resolve_source(
    sources: &mut Vec<Source>,
    validators: &mut Vec<doc::Validator>,
    shapes: &mut Vec<doc::Shape>,
    by_identity: &mut std::collections::BTreeMap<u32, u32>,
    collection: &flow::CollectionSpec,
    identity: Option<u32>,
) -> anyhow::Result<u32> {
    if let Some(&source) = identity.and_then(|i| by_identity.get(&i)) {
        return Ok(source);
    }
    let (source, validator, shape) = Source::new(collection)?;
    sources.push(source);
    validators.push(validator);
    shapes.push(shape);

    let index = sources.len() as u32 - 1;
    if let Some(identity) = identity {
        by_identity.insert(identity, index);
    }
    Ok(index)
}

impl Binding {
    /// Extract the Bindings, Sources, and per-Source document Validators of a
    /// shuffle::Task. Validators parallel `sources` and are indexed by
    /// `Binding::source`. Callers which don't validate read documents (the
    /// Session handler) simply drop them: building a Validator is not extra
    /// work, because inferring each source's Shape requires one regardless.
    pub fn from_task(
        task: &shuffle::Task,
    ) -> anyhow::Result<(Vec<Self>, Vec<Source>, Vec<doc::Validator>)> {
        let mut bindings = Vec::new();
        let mut sources = Vec::<Source>::new();
        let mut validators = Vec::<doc::Validator>::new();
        let mut shapes = Vec::<doc::Shape>::new();
        let mut by_identity = std::collections::BTreeMap::<u32, u32>::new();

        match &task.task {
            Some(shuffle::task::Task::Derivation(collection_spec)) => {
                let derivation = collection_spec
                    .derivation
                    .as_ref()
                    .context("CollectionSpec missing derivation")?;

                guard_index_width("derivation", derivation.transforms.len())?;

                for (index, (transform, resolved)) in derivation.resolved_transforms().enumerate() {
                    let (collection, identity) = resolved.context("missing source collection")?;
                    let source = resolve_source(
                        &mut sources,
                        &mut validators,
                        &mut shapes,
                        &mut by_identity,
                        collection,
                        identity,
                    )?;
                    bindings.push(Self::from_derivation_transform(
                        index as u16,
                        transform,
                        collection,
                        source,
                        &shapes[source as usize],
                    )?);
                }
            }
            Some(shuffle::task::Task::Materialization(materialization)) => {
                guard_index_width("materialization", materialization.bindings.len())?;

                for (index, (binding, resolved)) in materialization.resolved_bindings().enumerate()
                {
                    let (collection, identity) = resolved.context("missing source collection")?;
                    let source = resolve_source(
                        &mut sources,
                        &mut validators,
                        &mut shapes,
                        &mut by_identity,
                        collection,
                        identity,
                    )?;
                    bindings.push(Self::from_materialization_binding(
                        index as u16,
                        binding,
                        collection,
                        source,
                        &shapes[source as usize],
                    )?);
                }
            }
            Some(shuffle::task::Task::CollectionPartitions(collection_partitions)) => {
                let shuffle::CollectionPartitions {
                    collection,
                    partition_selector,
                    not_before,
                    not_after,
                } = collection_partitions;

                let collection_spec = collection
                    .as_ref()
                    .context("CollectionPartitions missing CollectionSpec")?;

                let partition_selector = partition_selector
                    .as_ref()
                    .context("CollectionPartitions missing partition selector")?;

                let source = resolve_source(
                    &mut sources,
                    &mut validators,
                    &mut shapes,
                    &mut by_identity,
                    collection_spec,
                    None,
                )?;
                bindings.push(Self::from_collection_partitions(
                    collection_spec,
                    partition_selector,
                    not_before.as_ref(),
                    not_after.as_ref(),
                    source,
                    &shapes[source as usize],
                )?);
            }
            None => anyhow::bail!("missing task variant"),
        };

        assign_cohorts(&mut bindings);

        Ok((bindings, sources, validators))
    }

    fn from_derivation_transform(
        index: u16,
        spec: &flow::collection_spec::derivation::Transform,
        collection: &flow::CollectionSpec,
        source: u32,
        shape: &doc::Shape,
    ) -> anyhow::Result<Self> {
        let flow::collection_spec::derivation::Transform {
            backfill: _,
            collection: _,
            collection_index: _,
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
            state_key: _,
        } = spec;

        let flow::CollectionSpec {
            key, projections, ..
        } = collection;

        // read_delay is a duration, not an absolute timestamp.
        // Clock's internal representation is (100ns_ticks << 4 | sequence_counter),
        // so a duration of N seconds is (N * 10_000_000) << 4.
        let read_delay = uuid::Clock::from_u64((*read_delay_seconds as u64 * 10_000_000) << 4);
        let (not_before, not_after) = not_before_after(not_before.as_ref(), not_after.as_ref());

        let partition_selector = partition_selector
            .as_ref()
            .context("missing partition selector")?;

        // Determine shuffle key configuration.
        let (key_extractors, uses_lambda, uses_source_key, shuffle_key_partition_fields) =
            if !shuffle_key.is_empty() {
                // Explicit shuffle key provided.
                let uses_source_key = shuffle_key == key;
                let partition_fields = compute_partition_fields(shuffle_key, projections);
                (
                    build_key_extractors(shuffle_key, shape),
                    false,
                    uses_source_key,
                    partition_fields,
                )
            } else if !shuffle_lambda_config_json.is_empty() {
                // Lambda-computed shuffle key.
                (Vec::new(), true, false, None)
            } else {
                // Default: use source collection key.
                (build_key_extractors(key, shape), false, true, None)
            };

        Ok(Self {
            index,
            source,
            filter_r_clocks: *read_only,
            journal_read_suffix: journal_read_suffix.clone(),
            priority: *priority,
            read_delay,
            key_extractors,
            shuffle_key_partition_fields,
            partition_selector: partition_selector.clone(),
            uses_lambda,
            uses_source_key,
            not_before,
            not_after,
            cohort: 0, // Assigned by assign_cohorts().
        })
    }

    fn from_materialization_binding(
        index: u16,
        spec: &flow::materialization_spec::Binding,
        collection: &flow::CollectionSpec,
        source: u32,
        shape: &doc::Shape,
    ) -> anyhow::Result<Self> {
        let flow::materialization_spec::Binding {
            backfill: _,
            collection: _,
            collection_index: _,
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

        let (not_before, not_after) = not_before_after(not_before.as_ref(), not_after.as_ref());

        let partition_selector = partition_selector
            .as_ref()
            .context("missing partition selector")?;

        Ok(Self {
            index,
            source,
            filter_r_clocks: false, // Always false for materializations.
            journal_read_suffix: journal_read_suffix.clone(),
            priority: *priority,
            read_delay: uuid::Clock::from_u64(0), // Always zero for materializations.
            key_extractors: build_key_extractors(&collection.key, shape),
            shuffle_key_partition_fields: None, // Not computed for materializations.
            partition_selector: partition_selector.clone(),
            uses_lambda: false,    // Always false for materializations.
            uses_source_key: true, // Always true for materializations.
            not_before,
            not_after,
            cohort: 0, // Assigned by assign_cohorts().
        })
    }

    fn from_collection_partitions(
        spec: &flow::CollectionSpec,
        source_partitions: &broker::LabelSelector,
        not_before: Option<&pbjson_types::Timestamp>,
        not_after: Option<&pbjson_types::Timestamp>,
        source: u32,
        shape: &doc::Shape,
    ) -> anyhow::Result<Self> {
        let (not_before, not_after) = not_before_after(not_before, not_after);

        Ok(Self {
            index: 0,
            source,
            filter_r_clocks: false,
            journal_read_suffix: "ad-hoc".to_string(),
            priority: 0,
            read_delay: uuid::Clock::from_u64(0),
            key_extractors: build_key_extractors(&spec.key, shape),
            shuffle_key_partition_fields: None,
            partition_selector: source_partitions.clone(),
            uses_lambda: false,
            uses_source_key: true,
            not_before,
            not_after,
            cohort: 0, // Assigned by assign_cohorts().
        })
    }

    pub fn state_key(&self) -> &str {
        self.journal_read_suffix.rsplit("/").next().unwrap()
    }
}

/// Guard [`Binding::index`]'s u16 width, which is also the width of
/// `doc::combine`'s binding index. This is a *format* limit and deliberately
/// shares no constant with `validation::MAX_BINDINGS`, which gates published
/// tasks far below it: tripping this means an unvalidated spec reached the
/// runtime.
fn guard_index_width(entity: &str, count: usize) -> anyhow::Result<()> {
    if count > u16::MAX as usize {
        anyhow::bail!(
            "{entity} has {count} bindings, which exceeds the shuffle limit of {}",
            u16::MAX,
        );
    }
    Ok(())
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

/// Build key extractors from string-encoded JSON pointers,
/// using schema-annotated defaults from the inferred shape.
pub fn build_key_extractors(pointers: &[String], shape: &doc::Shape) -> Vec<doc::Extractor> {
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
    /// Encoded values the field MUST match (OR semantics).
    /// Empty means no include constraint — any value is accepted.
    include: Vec<ConstraintValue>,
    /// Encoded values the field MUST NOT match (OR semantics).
    /// Empty means no exclude constraint.
    exclude: Vec<ConstraintValue>,
}

/// A single selector value for a partition field, carrying the same matching
/// semantics as `labels::matches`: an empty `value` is a wildcard matching any
/// value; a `prefix` value matches any value it is a prefix of; otherwise the
/// match is exact.
#[derive(Debug, Clone)]
struct ConstraintValue {
    value: Box<str>,
    prefix: bool,
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

                // Preserve each label's `prefix` flag (and empty-value wildcard)
                // so matching mirrors `labels::matches` exactly.
                let collect_values = |set: Option<&broker::LabelSet>| -> Vec<ConstraintValue> {
                    set.map(|set| {
                        labels::values(set, &label_name)
                            .iter()
                            .map(|l| ConstraintValue {
                                value: Box::from(l.value.as_str()),
                                prefix: l.prefix,
                            })
                            .collect()
                    })
                    .unwrap_or_default()
                };

                FieldConstraint {
                    field: Box::from(field.as_str()),
                    include: collect_values(include_set),
                    exclude: collect_values(exclude_set),
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

            // Mirror `labels::matches`: an include set requires at least one
            // value to match; an exclude set rejects when any value matches.
            if !constraint.include.is_empty()
                && !constraint.include.iter().any(|v| v.matches(parsed_value))
            {
                return Ok(false);
            }

            if constraint.exclude.iter().any(|v| v.matches(parsed_value)) {
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

impl ConstraintValue {
    fn matches(&self, value: &str) -> bool {
        self.value.is_empty()
            || (self.prefix && value.starts_with(self.value.as_ref()))
            || (!self.prefix && self.value.as_ref() == value)
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

    /// `Binding::index` is a u16, so a task past the format limit must error
    /// early -- before any per-binding work, and independently of
    /// `validation::MAX_BINDINGS`, which gates published tasks far below it.
    #[test]
    fn from_task_guards_the_index_width() {
        let over = u16::MAX as usize + 1;

        for task in [
            shuffle::task::Task::Materialization(flow::MaterializationSpec {
                bindings: vec![Default::default(); over],
                ..Default::default()
            }),
            shuffle::task::Task::Derivation(flow::CollectionSpec {
                derivation: Some(flow::collection_spec::Derivation {
                    transforms: vec![Default::default(); over],
                    ..Default::default()
                }),
                ..Default::default()
            }),
        ] {
            let err = Binding::from_task(&shuffle::Task { task: Some(task) }).unwrap_err();
            assert!(
                err.to_string()
                    .contains("65536 bindings, which exceeds the shuffle limit of 65535"),
                "unexpected error: {err}",
            );
        }
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
            // Prefix include: selector value is a prefix of the journal value.
            (
                &["region"],
                &[("region:prefix", "u")],
                &[],
                "region=us/pivot=00",
                Ok(true),
            ),
            // Prefix include miss.
            (
                &["region"],
                &[("region:prefix", "x")],
                &[],
                "region=us/pivot=00",
                Ok(false),
            ),
            // Empty include value is a wildcard: any value matches.
            (
                &["region"],
                &[("region", "")],
                &[],
                "region=anything/pivot=00",
                Ok(true),
            ),
            // Prefix exclude: a prefix-matching value is excluded.
            (
                &["region"],
                &[],
                &[("region:prefix", "ba")],
                "region=bad/pivot=00",
                Ok(false),
            ),
            // Empty exclude value is a wildcard: any value is excluded.
            (
                &["region"],
                &[],
                &[("region", "")],
                "region=us/pivot=00",
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
