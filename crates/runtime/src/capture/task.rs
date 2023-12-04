use super::{Binding, Task};
use anyhow::Context;
use proto_flow::capture::{request, response, Request, Response};
use proto_flow::flow;
use std::collections::BTreeMap;

impl Task {
    pub fn new(open: &Request, opened: &Response) -> anyhow::Result<Self> {
        let request::Open {
            capture: spec,
            range,
            state_json: _,
            version: _,
        } = open.clone().open.context("expected Open")?;

        let response::Opened {
            explicit_acknowledgements,
        } = opened.clone().opened.context("expected Opened")?;

        let flow::CaptureSpec {
            bindings,
            config_json: _,
            connector_type: _,
            interval_seconds,
            name,
            network_ports: _,
            recovery_log_template: _,
            shard_template: _,
        } = spec.as_ref().context("missing capture")?;
        let range = range.context("missing range")?;

        if range.r_clock_begin != 0 || range.r_clock_end != u32::MAX {
            anyhow::bail!("captures cannot split on r-clock: {range:?}");
        }

        let ser_policy = doc::SerPolicy::default();

        let bindings = bindings
            .into_iter()
            .enumerate()
            .map(|(index, spec)| Binding::new(spec, ser_policy.clone()).context(index))
            .collect::<Result<Vec<_>, _>>()?;

        let restart = std::time::Duration::from_secs(*interval_seconds as u64);
        let restart = tokio::time::Instant::now().checked_add(restart).unwrap();

        let shard_ref = ops::ShardRef {
            kind: ops::TaskType::Capture as i32,
            name: name.clone(),
            key_begin: format!("{:08x}", range.key_begin),
            r_clock_begin: format!("{:08x}", range.r_clock_begin),
        };

        Ok(Self {
            bindings,
            explicit_acknowledgements,
            restart,
            shard_ref,
        })
    }

    pub fn binding_shapes_by_index(
        &self,
        mut by_key: BTreeMap<String, doc::Shape>,
    ) -> Vec<doc::Shape> {
        let mut by_index = Vec::new();
        by_index.resize_with(self.bindings.len(), || doc::shape::Shape::nothing());

        for (index, binding) in self.bindings.iter().enumerate() {
            let key = binding.resource_path.join("\t"); // TODO(johnny): bindings PRD.

            if let Some(shape) = by_key.remove(&key) {
                by_index[index] = shape;
            }
        }

        by_index
    }

    pub fn binding_shapes_by_key(&self, by_index: Vec<doc::Shape>) -> BTreeMap<String, doc::Shape> {
        let mut by_key = BTreeMap::new();

        for (index, shape) in by_index.into_iter().enumerate() {
            let key = self.bindings[index].resource_path.join("\t"); // TODO(johnny): bindings PRD.
            by_key.insert(key, shape);
        }
        by_key
    }

    pub fn combine_spec(&self) -> anyhow::Result<doc::combine::Spec> {
        let combiner_spec = self
            .bindings
            .iter()
            .enumerate()
            .map(|(index, binding)| binding.combiner_spec().context(index))
            .collect::<Result<Vec<_>, _>>()?;

        let state_schema = doc::reduce::merge_patch_schema().to_string();
        let state_schema = doc::validation::build_bundle(&state_schema).unwrap();
        let state_validator = doc::Validator::new(state_schema).unwrap();

        // Build combiner Spec with all bindings, plus one extra for state reductions.
        let combiner_spec = doc::combine::Spec::with_bindings(
            combiner_spec
                .into_iter()
                .map(|(is_full, key, validator)| (is_full, key, None, validator))
                .chain(std::iter::once((false, Vec::new(), None, state_validator))),
        );

        Ok(combiner_spec)
    }
}

impl Binding {
    pub fn new(
        spec: &flow::capture_spec::Binding,
        ser_policy: doc::SerPolicy,
    ) -> anyhow::Result<Self> {
        let flow::capture_spec::Binding {
            backfill: _,
            collection,
            resource_config_json: _,
            resource_path,
            state_key: _,
        } = spec;

        let flow::CollectionSpec {
            ack_template_json: _,
            derivation: _,
            key,
            name,
            partition_fields,
            partition_template: _,
            projections,
            read_schema_json: _,
            uuid_ptr,
            write_schema_json,
        } = collection.as_ref().context("missing collection")?;

        let document_uuid_ptr = doc::Pointer::from(uuid_ptr);
        let key_extractors = extractors::for_key(&key, &projections, &ser_policy)?;
        let partition_extractors =
            extractors::for_fields(&partition_fields, &projections, &ser_policy)?;

        Ok(Self {
            collection_name: name.clone(),
            document_uuid_ptr,
            key_extractors,
            partition_extractors,
            resource_path: resource_path.clone(),
            ser_policy,
            write_schema_json: write_schema_json.clone(),
        })
    }

    pub fn combiner_spec(&self) -> anyhow::Result<(bool, Vec<doc::Extractor>, doc::Validator)> {
        let built_schema = doc::validation::build_bundle(&self.write_schema_json)
            .context("collection write_schema_json is not a JSON schema")?;
        let validator =
            doc::Validator::new(built_schema).context("could not build a schema validator")?;

        Ok((false, self.key_extractors.clone(), validator))
    }
}
