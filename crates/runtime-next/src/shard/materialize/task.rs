use anyhow::Context;
use proto_flow::flow;

/// Task configuration for a materialization shard.
///
/// A materialization is one-or-more bindings, each maintaining a view of a
/// source collection within an endpoint resource. The shard combines source
/// documents read from its bindings' collections with the `C:Loaded` documents
/// of the endpoint, keyed by each binding's field selection, and drains the
/// result to the connector as `C:Store`.
pub(super) struct Task {
    /// Bindings of the materialization.
    pub bindings: Vec<Binding>,
    /// Source collections read by the task's bindings.
    pub sources: Vec<Source>,
    /// Stable RocksDB `state_key` of each binding, indexed by binding index.
    /// Used to map the leader's frontier binding indices to the `FC:`/`FH:`
    /// key layout.
    pub binding_state_keys: Vec<String>,
}

/// Binding configuration for a materialization shard.
pub(super) struct Binding {
    /// Index of this binding's source collection within [`Task::sources`].
    pub source: u32,
    /// Delta updates, or standard?
    pub delta_updates: bool,
    /// Extractors of this binding's field-selected key, applied to source and
    /// loaded documents to populate combiner keys and `Load.key_json`.
    pub key_extractors: Vec<doc::Extractor>,
    /// Serialization policy applied to this binding's field-selected values.
    pub ser_policy: doc::SerPolicy,
    /// Are we storing the root document (often `flow_document`)?
    pub store_document: bool,
    /// Extraction plan of this binding's field-selected values.
    pub value_plan: doc::ExtractorPlan,
}

/// A source collection, read by one or more bindings of the materialization.
///
/// Sources follow the spec's declared indirection: bindings sharing a
/// `collection_index` share one Source, while a binding of an inline-form
/// spec carries no index and is its own Source.
pub(super) struct Source {
    /// Source collection name.
    pub collection_name: String,
    /// JSON pointer at which document UUIDs are found. A binding which is
    /// backfill-truncating classifies each loaded document against its
    /// boundary by the clock of the UUID at this pointer.
    pub document_uuid_ptr: json::Pointer,
    /// Schema the shuffle read pipeline validates source documents against:
    /// the source collection's read schema, or its write schema when no read
    /// schema is defined (mirroring `shuffle::Source::new`).
    pub read_schema_json: bytes::Bytes,
}

/// Build the runtime [`Binding`] for a single materialization binding.
fn build_binding(
    b: &flow::materialization_spec::Binding,
    collection: &flow::CollectionSpec,
    source: u32,
) -> anyhow::Result<Binding> {
    let flow::materialization_spec::Binding {
        backfill: _,
        collection: _,
        collection_index: _,
        delta_updates,
        deprecated_shuffle: _,
        field_selection,
        journal_read_suffix: _,
        not_after: _,
        not_before: _,
        partition_selector: _,
        priority: _,
        resource_config_json: _,
        resource_path: _,
        state_key: _, // Hoisted into `Task::binding_state_keys`.
        ser_policy: binding_ser_policy,
    } = b;

    let flow::FieldSelection {
        document: selected_root,
        field_config_json_map: _,
        keys: selected_key,
        values: selected_values,
    } = field_selection
        .as_ref()
        .context("missing field selection")?;

    // The policy is negotiated via the connector protocol: connectors return it
    // in their Validated response and it's baked into the built binding spec.
    // Absent or zero-valued limits mean the connector doesn't require that kind
    // of truncation, and map to the no-op policy's unbounded limits.
    let mut ser_policy = doc::SerPolicy::noop();
    if let Some(binding_ser_policy) = binding_ser_policy {
        if binding_ser_policy.str_truncate_after > 0 {
            ser_policy.str_truncate_after = binding_ser_policy.str_truncate_after as usize;
        }
        if binding_ser_policy.nested_obj_truncate_after > 0 {
            ser_policy.nested_obj_truncate_after =
                binding_ser_policy.nested_obj_truncate_after as usize;
        }
        if binding_ser_policy.array_truncate_after > 0 {
            ser_policy.array_truncate_after = binding_ser_policy.array_truncate_after as usize;
        }
    }

    // Keys are extracted with a no-op policy, never the binding's `ser_policy`:
    // a truncated key would collide distinct rows, and matching the shuffle
    // writer's no-op extraction lets the scan reuse the log's packed-key prefix
    // (and keeps Load, Store, and combiner keys byte-identical). Only values
    // carry the serialization policy.
    let key_extractors = extractors::for_fields(
        selected_key,
        &collection.projections,
        &doc::SerPolicy::noop(),
    )?;
    let value_plan = doc::ExtractorPlan::new(&extractors::for_fields(
        selected_values,
        &collection.projections,
        &ser_policy,
    )?);

    Ok(Binding {
        source,
        delta_updates: *delta_updates,
        key_extractors,
        ser_policy,
        store_document: !selected_root.is_empty(),
        value_plan,
    })
}

/// Build the runtime [`Source`] for a source collection.
fn build_source(collection: &flow::CollectionSpec) -> Source {
    // Prefer the read schema, falling back to the write schema, so
    // re-validation uses the same schema the shuffle read pipeline
    // validated against when it set `FLAGS_SCHEMA_VALID`.
    let read_schema_json = if collection.read_schema_json.is_empty() {
        collection.write_schema_json.clone()
    } else {
        collection.read_schema_json.clone()
    };

    Source {
        collection_name: collection.name.clone(),
        document_uuid_ptr: json::Pointer::from(collection.uuid_ptr.as_str()),
        read_schema_json,
    }
}

impl Task {
    pub fn new(
        spec: &flow::MaterializationSpec,
        shard: &ops::proto::ShardLabeling,
    ) -> anyhow::Result<Self> {
        let flow::MaterializationSpec {
            bindings: spec_bindings,
            config_json: _,
            connector_type: _,
            name: _,
            network_ports: _,
            recovery_log_template: _,
            shard_template: _,
            inactive_bindings: _,
            triggers_json: _,
            created_at: _,
            sync_schedule_json: _,
            linked_collections: _,
        } = spec;

        let ops::proto::ShardLabeling { range, .. } = shard;
        let range = range.context("missing range")?;

        if range.r_clock_begin != 0 || range.r_clock_end != u32::MAX {
            anyhow::bail!("materialization cannot split on r-clock: {range:?}");
        }

        // `doc::combine` packs its binding index into a u16. This guards the
        // *format* limit and deliberately shares no constant with
        // `validation::MAX_BINDINGS`, which gates published tasks far below it:
        // tripping this means an unvalidated spec reached the runtime.
        if spec_bindings.len() > u16::MAX as usize {
            anyhow::bail!(
                "materialization has {} bindings, which exceeds the combiner limit of {}",
                spec_bindings.len(),
                u16::MAX,
            );
        }

        // Unlike a derivation's transforms, a materialization's built bindings
        // carry a populated `state_key`, so it needs no recomputation here.
        let binding_state_keys = spec_bindings
            .iter()
            .map(|b| b.state_key.clone())
            .collect::<Vec<String>>();

        let mut bindings = Vec::with_capacity(spec_bindings.len());
        let mut sources = Vec::<Source>::new();
        let mut sources_by_identity = std::collections::BTreeMap::<u32, u32>::new();

        for (index, (b, resolved)) in spec.resolved_bindings().enumerate() {
            let (collection, identity) = resolved.context("missing collection").context(index)?;

            let source = match identity.and_then(|i| sources_by_identity.get(&i)) {
                Some(&source) => source,
                None => {
                    let source = sources.len() as u32;
                    sources.push(build_source(collection));

                    if let Some(identity) = identity {
                        sources_by_identity.insert(identity, source);
                    }
                    source
                }
            };
            bindings.push(build_binding(b, collection, source).context(index)?);
        }

        Ok(Self {
            bindings,
            sources,
            binding_state_keys,
        })
    }

    /// Build a source-document validator per Source.
    fn source_validators(&self) -> anyhow::Result<Vec<doc::Validator>> {
        self.sources
            .iter()
            .map(
                |Source {
                     collection_name,
                     read_schema_json,
                     ..
                 }| {
                    let built_schema = doc::validation::build_bundle(read_schema_json)
                        .with_context(|| {
                            format!(
                                "source collection {collection_name} schema is not a JSON schema",
                            )
                        })?;
                    doc::Validator::new(built_schema).with_context(|| {
                        format!(
                            "could not build a schema validator for collection {collection_name}",
                        )
                    })
                },
            )
            .collect()
    }

    /// Combiner over the task's bindings: one validator per Source, shared by
    /// every binding which reads it.
    ///
    /// Keys and full-reduction flags stay per-binding: they follow the
    /// binding's field selection and `delta_updates`, not its collection.
    pub fn combine_spec(&self) -> anyhow::Result<doc::combine::Spec> {
        let validators = self
            .source_validators()?
            .into_iter()
            .zip(self.sources.iter())
            .map(|(validator, source)| {
                (
                    format!("materialized collection {}", source.collection_name),
                    validator,
                )
            })
            .collect::<Vec<_>>();

        let spec_bindings = self.bindings.iter().map(|binding| {
            (
                !binding.delta_updates,
                binding.key_extractors.clone(),
                binding.source,
            )
        });

        Ok(doc::combine::Spec::with_bindings(
            spec_bindings,
            validators,
            Vec::new(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Bindings reading one collection share a combiner validator.
    #[test]
    fn combine_spec_groups_validators_by_source() {
        let source = || Source {
            collection_name: "acmeCo/collection".to_string(),
            document_uuid_ptr: json::Pointer::from("/_meta/uuid"),
            read_schema_json: bytes::Bytes::from_static(br#"{"type":"object"}"#),
        };
        let binding = |source| Binding {
            source,
            delta_updates: false,
            key_extractors: Vec::new(),
            ser_policy: doc::SerPolicy::noop(),
            store_document: false,
            value_plan: doc::ExtractorPlan::new(&[]),
        };

        // Five bindings over two sources.
        let task = Task {
            bindings: [0, 1, 0, 1, 0].into_iter().map(binding).collect(),
            sources: vec![source(), source()],
            binding_state_keys: Vec::new(),
        };

        let spec = task.combine_spec().unwrap();
        assert_eq!(spec.binding_count(), 5);
        assert_eq!(spec.validator_count(), 2);
    }

    /// An indirect-form spec groups bindings onto shared Sources; an inline-form
    /// spec, which carries no identity, keeps one Source per binding.
    #[test]
    fn new_groups_sources_by_identity() {
        let collection = |index: usize| flow::CollectionSpec {
            name: format!("acmeCo/collection-{index}"),
            uuid_ptr: "/_meta/uuid".to_string(),
            write_schema_json: bytes::Bytes::from_static(br#"{"type":"object"}"#),
            ..Default::default()
        };
        let binding = |collection_index: u32| flow::materialization_spec::Binding {
            collection_index,
            field_selection: Some(flow::FieldSelection::default()),
            state_key: format!("state-{collection_index}"),
            ..Default::default()
        };
        let shard = ops::proto::ShardLabeling {
            range: Some(flow::RangeSpec {
                key_begin: 0,
                key_end: u32::MAX,
                r_clock_begin: 0,
                r_clock_end: u32::MAX,
            }),
            ..Default::default()
        };

        // Indirect form: five bindings over two linked collections.
        let spec = flow::MaterializationSpec {
            bindings: [0, 1, 0, 1, 0].into_iter().map(binding).collect(),
            linked_collections: vec![collection(0), collection(1)],
            ..Default::default()
        };
        let task = Task::new(&spec, &shard).unwrap();
        assert_eq!(
            task.bindings.iter().map(|b| b.source).collect::<Vec<_>>(),
            [0, 1, 0, 1, 0],
        );
        assert_eq!(
            task.sources
                .iter()
                .map(|s| s.collection_name.as_str())
                .collect::<Vec<_>>(),
            ["acmeCo/collection-0", "acmeCo/collection-1"],
        );

        // Inline form: each binding carries its own copy of one collection.
        let spec = flow::MaterializationSpec {
            bindings: (0..3)
                .map(|_| flow::materialization_spec::Binding {
                    collection: Some(collection(0)),
                    ..binding(0)
                })
                .collect(),
            ..Default::default()
        };
        let task = Task::new(&spec, &shard).unwrap();
        assert_eq!(
            task.bindings.iter().map(|b| b.source).collect::<Vec<_>>(),
            [0, 1, 2],
        );
        assert_eq!(task.sources.len(), 3);
    }
}
