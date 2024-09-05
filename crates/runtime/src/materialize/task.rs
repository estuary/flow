use super::{Binding, Task};
use anyhow::Context;
use proto_flow::flow;
use proto_flow::materialize::{request, Request};

impl Task {
    pub fn new(open: &Request) -> anyhow::Result<Self> {
        let request::Open {
            materialization: spec,
            range,
            state_json: _,
            version: _,
        } = open.clone().open.context("expected Open")?;

        let flow::MaterializationSpec {
            bindings,
            config_json,
            connector_type: _,
            name,
            network_ports: _,
            recovery_log_template: _,
            shard_template: _,
        } = spec.as_ref().context("missing materialization")?;
        let range = range.context("missing range")?;

        if range.r_clock_begin != 0 || range.r_clock_end != u32::MAX {
            anyhow::bail!("materialization cannot split on r-clock: {range:?}");
        }

        // TODO(johnny): Hack to limit serialized value sizes for these common materialization connectors
        // that don't handle large strings very well. This should be negotiated via connector protocol.
        // See go/runtime/materialize.go:135
        let ser_policy = if [
            "ghcr.io/estuary/materialize-bigquery",
            "ghcr.io/estuary/materialize-snowflake",
            "ghcr.io/estuary/materialize-redshift",
            "ghcr.io/estuary/materialize-sqlite",
        ]
        .iter()
        .any(|image| config_json.contains(image))
        {
            doc::SerPolicy {
                str_truncate_after: 1 << 16, // Truncate at 64KB.
                nested_obj_truncate_after: 1000,
                array_truncate_after: 1000,
                ..doc::SerPolicy::noop()
            }
        } else {
            doc::SerPolicy::noop()
        };

        let bindings = bindings
            .into_iter()
            .enumerate()
            .map(|(index, spec)| Binding::new(spec, &ser_policy).context(index))
            .collect::<Result<Vec<_>, _>>()?;

        let shard_ref = ops::ShardRef {
            kind: ops::TaskType::Materialization as i32,
            name: name.clone(),
            key_begin: format!("{:08x}", range.key_begin),
            r_clock_begin: format!("{:08x}", range.r_clock_begin),
        };

        Ok(Self {
            bindings,
            shard_ref,
        })
    }

    pub fn combine_spec(&self) -> anyhow::Result<doc::combine::Spec> {
        let combiner_spec = self
            .bindings
            .iter()
            .enumerate()
            .map(|(index, binding)| binding.combiner_spec().context(index))
            .collect::<Result<Vec<_>, _>>()?;

        // Build combiner Spec with all bindings, plus one extra for state reductions.
        let combiner_spec = doc::combine::Spec::with_bindings(
            combiner_spec
                .into_iter()
                .map(|(is_full, key, name, validator)| (is_full, key, name, None, validator)),
        );

        Ok(combiner_spec)
    }
}

impl Binding {
    pub fn new(
        spec: &flow::materialization_spec::Binding,
        ser_policy: &doc::SerPolicy,
    ) -> anyhow::Result<Self> {
        let flow::materialization_spec::Binding {
            backfill: _,
            collection,
            delta_updates,
            deprecated_shuffle: _,
            field_selection,
            journal_read_suffix,
            not_after: _,
            not_before: _,
            partition_selector: _,
            priority: _,
            resource_config_json: _,
            resource_path: _,
            state_key,
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

        let key_extractors = extractors::for_fields(selected_key, projections, ser_policy)?;
        let value_extractors = extractors::for_fields(selected_values, projections, ser_policy)?;

        let read_schema_json = if read_schema_json.is_empty() {
            write_schema_json
        } else {
            read_schema_json
        }
        .clone();

        Ok(Self {
            collection_name: collection_name.clone(),
            delta_updates: *delta_updates,
            journal_read_suffix: journal_read_suffix.clone(),
            key_extractors,
            read_schema_json,
            ser_policy: ser_policy.clone(),
            state_key: state_key.clone(),
            store_document: !selected_root.is_empty(),
            value_extractors,
        })
    }

    pub fn combiner_spec(
        &self,
    ) -> anyhow::Result<(bool, Vec<doc::Extractor>, String, doc::Validator)> {
        let built_schema = doc::validation::build_bundle(&self.read_schema_json)
            .context("collection read_schema_json is not a JSON schema")?;
        let validator =
            doc::Validator::new(built_schema).context("could not build a schema validator")?;

        Ok((
            !self.delta_updates,
            self.key_extractors.clone(),
            format!("materialized collection {}", self.collection_name),
            validator,
        ))
    }
}
