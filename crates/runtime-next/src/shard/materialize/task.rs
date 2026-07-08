use super::Binding;
use crate::shard::task_schema::relax_inferred_datetime_formats;
use anyhow::Context;
use proto_flow::flow;

/// Build binding structures and shard_ref for a materialization task.
pub fn build_bindings(
    spec: &flow::MaterializationSpec,
    shard: &ops::proto::ShardLabeling,
) -> anyhow::Result<(Vec<Binding>, ops::ShardRef)> {
    let flow::MaterializationSpec {
        bindings,
        config_json,
        connector_type: _,
        name,
        network_ports: _,
        recovery_log_template: _,
        shard_template,
        inactive_bindings: _,
        triggers_json: _,
    } = spec;

    // Opt-in, per-task relaxation of read-side date-time `format` enforcement
    // inherited from the collection's inferred schema. See build_binding and
    // estuary/flow#3133.
    let relax_inferred_datetime = labels::shard_flag_enabled(
        shard_template.as_ref(),
        labels::RELAX_INFERRED_DATETIME_FLAG,
    );

    let ops::proto::ShardLabeling {
        range,
        build: version,
        ..
    } = shard;

    let range = range.context("missing range")?;

    if range.r_clock_begin != 0 || range.r_clock_end != u32::MAX {
        anyhow::bail!("materialization cannot split on r-clock: {range:?}");
    }

    // TODO(johnny): Hack to limit serialized value sizes for these common materialization connectors
    // that don't handle large strings very well. This should be negotiated via connector protocol.
    // See go/runtime/materialize.go:135
    let ser_policy = if let Some(limit) = [
        ("ghcr.io/estuary/materialize-azure-fabric-warehouse", 1000),
        ("ghcr.io/estuary/materialize-bigquery", 1500),
        ("ghcr.io/estuary/materialize-kafka", 1000),
        ("ghcr.io/estuary/materialize-snowflake", 1000),
        ("ghcr.io/estuary/materialize-redshift", 1000),
        ("ghcr.io/estuary/materialize-sqlite", 1000),
    ]
    .iter()
    .filter_map(|(image, limit)| {
        config_json
            .windows(image.len())
            .any(|window| window == image.as_bytes())
            .then_some(*limit)
    })
    .next()
    {
        doc::SerPolicy {
            str_truncate_after: 1 << 16, // Truncate at 64KB.
            nested_obj_truncate_after: limit,
            array_truncate_after: limit,
            ..doc::SerPolicy::noop()
        }
    } else {
        doc::SerPolicy::noop()
    };

    let bindings = bindings
        .into_iter()
        .enumerate()
        .map(|(index, spec)| {
            build_binding(spec, &ser_policy, relax_inferred_datetime).context(index)
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
    default_ser_policy: &doc::SerPolicy,
    relax_inferred_datetime: bool,
) -> anyhow::Result<Binding> {
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

    // TODO(whb): At some point once all built materialization specs have
    // been updated we can get rid of the `default_ser_policy` parameter and
    // just default to doc::SerPolicy::noop() with overrides from the
    // specific binding serialization policy.
    let ser_policy = if let Some(binding_ser_policy) = binding_ser_policy {
        let mut base = doc::SerPolicy::noop();
        if binding_ser_policy.str_truncate_after > 0 {
            base.str_truncate_after = binding_ser_policy.str_truncate_after as usize;
        };
        if binding_ser_policy.nested_obj_truncate_after > 0 {
            base.nested_obj_truncate_after = binding_ser_policy.nested_obj_truncate_after as usize;
        };
        if binding_ser_policy.array_truncate_after > 0 {
            base.array_truncate_after = binding_ser_policy.array_truncate_after as usize;
        };
        base
    } else {
        default_ser_policy.clone()
    };

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

    // When enabled for this task, strip `date`/`date-time`/`time` `format`
    // keywords contributed by the collection's inferred schema so that
    // historical, non-conforming values are not retroactively rejected on read.
    // Capture-time write-schema validation is unaffected.
    let read_schema_json = if relax_inferred_datetime {
        relax_inferred_datetime_formats(&read_schema_json)
            .context("relaxing inferred date-time formats of read schema")?
    } else {
        read_schema_json
    };

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

#[cfg(test)]
mod test {
    use super::*;

    // A read schema whose inlined inferred schema tags a field
    // `format: date-time`, as the control plane assembles it.
    const READ_SCHEMA: &str = r#"{
        "$defs": {
            "flow://inferred-schema": {
                "$id": "flow://inferred-schema",
                "type": "object",
                "properties": { "ts": { "type": "string", "format": "date-time" } }
            }
        },
        "allOf": [ { "$ref": "flow://inferred-schema" } ]
    }"#;

    fn binding_accepts(relax_inferred_datetime: bool, doc: &str) -> bool {
        let spec = flow::materialization_spec::Binding {
            collection: Some(flow::CollectionSpec {
                read_schema_json: bytes::Bytes::from(READ_SCHEMA),
                ..Default::default()
            }),
            field_selection: Some(flow::FieldSelection::default()),
            ..Default::default()
        };
        let binding =
            build_binding(&spec, &doc::SerPolicy::noop(), relax_inferred_datetime).unwrap();

        let mut validator =
            doc::Validator::new(doc::validation::build_bundle(&binding.read_schema_json).unwrap())
                .unwrap();

        let alloc = doc::HeapNode::new_allocator();
        let mut de = serde_json::Deserializer::from_str(doc);
        let node = doc::HeapNode::from_serde(&mut de, &alloc).unwrap();

        validator.is_valid(&node)
    }

    #[test]
    fn test_v2_binding_relaxes_inferred_datetime_when_flagged() {
        let legacy = r#"{"ts": "2026-06-17 12:46:17.375663+00:00"}"#;
        let conforming = r#"{"ts": "2026-06-17T12:46:17.375663+00:00"}"#;

        // Flag OFF: the read validator rejects the legacy value.
        assert!(!binding_accepts(false, legacy));
        assert!(binding_accepts(false, conforming));

        // Flag ON: the legacy value is tolerated; conforming values still pass.
        assert!(binding_accepts(true, legacy));
        assert!(binding_accepts(true, conforming));
    }
}
