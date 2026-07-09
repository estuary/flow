use super::Binding;
use anyhow::Context;
use proto_flow::flow;

/// Build binding structures and shard_ref for a materialization task.
pub fn build_bindings(
    spec: &flow::MaterializationSpec,
    shard: &ops::proto::ShardLabeling,
) -> anyhow::Result<(Vec<Binding>, ops::ShardRef)> {
    let flow::MaterializationSpec {
        bindings,
        config_json: _,
        connector_type: _,
        name,
        network_ports: _,
        recovery_log_template: _,
        shard_template: _,
        inactive_bindings: _,
        triggers_json: _,
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

    let bindings = bindings
        .into_iter()
        .enumerate()
        .map(|(index, spec)| build_binding(spec).context(index))
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
fn build_binding(spec: &flow::materialization_spec::Binding) -> anyhow::Result<Binding> {
    let flow::materialization_spec::Binding {
        backfill: _,
        collection,
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
        uuid_ptr: _,
        write_schema_json,
    } = collection.as_ref().context("missing collection")?;

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
        delta_updates: *delta_updates,
        key_extractors,
        read_schema_json,
        ser_policy,
        state_key: state_key.clone(),
        store_document: !selected_root.is_empty(),
        value_plan,
    })
}

pub fn combine_spec(bindings: &[Binding]) -> anyhow::Result<doc::combine::Spec> {
    let mut combiner_specs = Vec::with_capacity(bindings.len());

    for Binding {
        state_key,
        read_schema_json,
        delta_updates,
        key_extractors,
        collection_name,
        ..
    } in bindings
    {
        let built_schema = doc::validation::build_bundle(read_schema_json)
            .context("collection read_schema_json is not a JSON schema")?;
        let validator = doc::Validator::new(built_schema).with_context(|| {
            format!("could not build a schema validator for binding {state_key}",)
        })?;

        combiner_specs.push((
            !delta_updates,
            key_extractors.clone(),
            format!("materialized collection {collection_name}"),
            validator,
        ));
    }

    // Build combiner Spec with all bindings, plus one extra for state reductions.
    Ok(doc::combine::Spec::with_bindings(
        combiner_specs,
        Vec::new(),
    ))
}
