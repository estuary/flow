use super::Binding;
use anyhow::Context;
use proto_flow::flow;

/// Build binding structures and shard_ref for a materialization task.
pub fn build_bindings(
    spec: &flow::MaterializationSpec,
    shard: &ops::proto::ShardLabeling,
) -> anyhow::Result<(Vec<Binding>, ops::ShardRef)> {
    let flow::MaterializationSpec {
        bindings: _,
        config_json: _,
        connector_type: _,
        name,
        network_ports: _,
        recovery_log_template: _,
        shard_template: _,
        inactive_bindings: _,
        triggers_json: _,
        created_at: _,
        sync_schedule_json: _,
        linked_collections: _,
    } = spec;

    let ops::proto::ShardLabeling {
        range,
        build: version,
        ..
    } = shard;

    let range = range.context("missing range")?;

    if range.r_clock_begin != 0 || range.r_clock_end != u32::MAX {
        anyhow::bail!("materialization cannot split on r-clock: {range:?}");
    }

    // `doc::combine` packs its binding index into a u16. This guards the
    // *format* limit and deliberately shares no constant with
    // `validation::MAX_BINDINGS`, which gates published tasks far below it:
    // tripping this means an unvalidated spec reached the runtime.
    if spec.bindings.len() > u16::MAX as usize {
        anyhow::bail!(
            "materialization has {} bindings, which exceeds the combiner limit of {}",
            spec.bindings.len(),
            u16::MAX,
        );
    }

    let bindings = spec
        .resolved_bindings()
        .enumerate()
        .map(|(index, (binding, resolved))| {
            let (collection, identity) = resolved.context("missing collection").context(index)?;
            build_binding(binding, collection, identity).context(index)
        })
        .collect::<Result<Vec<_>, _>>()?;

    let shard_ref = ops::ShardRef {
        kind: ops::TaskType::Materialization as i32,
        name: name.clone(),
        key_begin: format!("{:08x}", range.key_begin),
        r_clock_begin: format!("{:08x}", range.r_clock_begin),
        build: version.clone(),
    };

    Ok((bindings, shard_ref))
}

// Build the runtime structure for a single binding.
fn build_binding(
    spec: &flow::materialization_spec::Binding,
    collection: &flow::CollectionSpec,
    collection_index: Option<u32>,
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
        state_key,
        ser_policy: binding_ser_policy,
    } = spec;

    let flow::FieldSelection {
        document: selected_root,
        field_config_json_map: _,
        keys: selected_key,
        values: selected_values,
    } = field_selection
        .as_ref()
        .context("missing field selection")?;

    let flow::CollectionSpec {
        ack_template_json: _,
        derivation: _,
        key: _,
        name: collection_name,
        partition_fields: _,
        partition_template: _,
        projections,
        read_schema_json,
        uuid_ptr,
        write_schema_json,
    } = collection;

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
    let key_extractors =
        extractors::for_fields(selected_key, projections, &doc::SerPolicy::noop())?;
    let value_plan = doc::ExtractorPlan::new(&extractors::for_fields(
        selected_values,
        projections,
        &ser_policy,
    )?);

    let read_schema_json = if read_schema_json.is_empty() {
        write_schema_json
    } else {
        read_schema_json
    }
    .clone();

    Ok(Binding {
        collection_name: collection_name.clone(),
        collection_index,
        delta_updates: *delta_updates,
        document_uuid_ptr: json::Pointer::from(uuid_ptr.as_str()),
        key_extractors,
        read_schema_json,
        ser_policy,
        state_key: state_key.clone(),
        store_document: !selected_root.is_empty(),
        value_plan,
    })
}

/// Build the combiner Spec of a materialization: one validator slot per distinct
/// source collection, shared by every binding which reads it.
///
/// Slots group on `collection_index` -- a proof of full `CollectionSpec` value
/// equality, and therefore of an equal `read_schema_json` -- so a binding of an
/// inline-form spec, which carries no identity, is its own slot. Keys and
/// full-reduction flags stay per-binding: they follow the binding's field
/// selection and `delta_updates`, not its collection.
pub fn combine_spec(bindings: &[Binding]) -> anyhow::Result<doc::combine::Spec> {
    let mut slots = Vec::new();
    let mut slots_by_identity = std::collections::BTreeMap::<u32, u32>::new();
    let mut spec_bindings = Vec::with_capacity(bindings.len());

    for Binding {
        state_key,
        read_schema_json,
        delta_updates,
        key_extractors,
        collection_index,
        collection_name,
        ..
    } in bindings
    {
        let slot = match collection_index.and_then(|i| slots_by_identity.get(&i)) {
            Some(slot) => *slot,
            None => {
                let built_schema = doc::validation::build_bundle(read_schema_json)
                    .context("collection read_schema_json is not a JSON schema")?;
                let validator = doc::Validator::new(built_schema).with_context(|| {
                    format!("could not build a schema validator for binding {state_key}",)
                })?;

                slots.push((
                    format!("materialized collection {collection_name}"),
                    validator,
                ));
                let slot = slots.len() as u32 - 1;

                if let Some(collection_index) = collection_index {
                    slots_by_identity.insert(*collection_index, slot);
                }
                slot
            }
        };
        spec_bindings.push((!delta_updates, key_extractors.clone(), slot));
    }

    Ok(doc::combine::Spec::with_bindings(
        spec_bindings,
        slots,
        Vec::new(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Bindings reading one collection share a combiner validator, and bindings
    /// of a spec which carries no identity keep one validator each.
    #[test]
    fn combine_spec_groups_validators_by_collection() {
        let binding = |collection_index| Binding {
            collection_name: "acmeCo/collection".to_string(),
            collection_index,
            delta_updates: false,
            document_uuid_ptr: json::Pointer::from("/_meta/uuid"),
            key_extractors: Vec::new(),
            read_schema_json: bytes::Bytes::from_static(br#"{"type":"object"}"#),
            ser_policy: doc::SerPolicy::noop(),
            state_key: "state".to_string(),
            store_document: false,
            value_plan: doc::ExtractorPlan::new(&[]),
        };

        // Indirect form: five bindings over two distinct collection values.
        let bindings: Vec<Binding> = [0, 1, 0, 1, 0]
            .into_iter()
            .map(|i| binding(Some(i)))
            .collect();
        let spec = combine_spec(&bindings).unwrap();
        assert_eq!(spec.binding_count(), 5);
        assert_eq!(spec.validator_count(), 2);

        // Inline form: identity, even though every binding names one collection.
        let bindings: Vec<Binding> = (0..5).map(|_| binding(None)).collect();
        let spec = combine_spec(&bindings).unwrap();
        assert_eq!(spec.binding_count(), 5);
        assert_eq!(spec.validator_count(), 5);
    }
}
