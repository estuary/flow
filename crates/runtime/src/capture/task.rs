use super::{Binding, Task};
use anyhow::Context;
use proto_flow::capture::{Request, Response, request, response};
use proto_flow::flow;
use std::collections::BTreeMap;

impl Task {
    pub fn new(open: &Request, opened: &Response) -> anyhow::Result<Self> {
        let request::Open {
            capture: spec,
            range,
            state_json: _,
            version,
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
            inactive_bindings: _,
            redact_salt,
        } = spec.as_ref().context("missing capture")?;
        let range = range.context("missing range")?;

        if range.r_clock_begin != 0 || range.r_clock_end != u32::MAX {
            anyhow::bail!("captures cannot split on r-clock: {range:?}");
        }

        let ser_policy = doc::SerPolicy::noop();

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
            build: version.clone(),
        };

        Ok(Self {
            bindings,
            explicit_acknowledgements,
            redact_salt: redact_salt.clone(),
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
            // partition_template_name embeds the collection name and generation ID,
            // while state_key is unique if a single target collection is bound
            // to multiple endpoint resources.
            let key = format!("{};{}", binding.partition_template_name, binding.state_key);

            if let Some(shape) = by_key.remove(&key) {
                by_index[index] = shape;
            }

            by_index[index].annotations.insert(
                crate::X_GENERATION_ID.to_string(),
                serde_json::Value::String(binding.collection_generation_id.to_string()),
            );
        }
        by_index
    }

    pub fn binding_shapes_by_key(&self, by_index: Vec<doc::Shape>) -> BTreeMap<String, doc::Shape> {
        let mut by_key = BTreeMap::new();

        for (index, shape) in by_index.into_iter().enumerate() {
            let binding = &self.bindings[index];
            let key = format!("{};{}", binding.partition_template_name, binding.state_key);
            by_key.insert(key, shape);
        }
        by_key
    }

    pub fn combine_spec(&self) -> anyhow::Result<doc::combine::Spec> {
        let combiner_spec = self
            .bindings
            .iter()
            .map(|binding| binding.combiner_spec())
            .collect::<Vec<_>>();

        let state_schema = doc::reduce::merge_patch_schema().to_string();
        let state_schema = doc::validation::build_bundle(state_schema.as_bytes()).unwrap();
        let state_validator = doc::Validator::new(state_schema).unwrap();

        // Build combiner Spec with all bindings, plus one extra for state reductions.
        let combiner_spec = doc::combine::Spec::with_bindings(
            combiner_spec
                .into_iter()
                .map(|(is_full, key, name, validator)| (is_full, key, name, None, validator))
                .chain(std::iter::once((
                    false,
                    Vec::new(),
                    "connector state".to_string(),
                    None,
                    state_validator,
                ))),
            self.redact_salt.to_vec(),
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
            resource_path: _,
            state_key,
        } = spec;

        let flow::CollectionSpec {
            ack_template_json: _,
            derivation: _,
            key,
            name,
            partition_fields,
            partition_template,
            projections,
            read_schema_json: _,
            uuid_ptr,
            write_schema_json,
        } = collection.as_ref().context("missing collection")?;

        let partition_template = partition_template
            .as_ref()
            .context("missing partition template")?;

        let collection_generation_id =
            assemble::extract_generation_id_suffix(&partition_template.name);

        let document_uuid_ptr = doc::Pointer::from(uuid_ptr);
        let key_extractors = extractors::for_key(&key, &projections, &ser_policy)?;
        let partition_extractors =
            extractors::for_fields(&partition_fields, &projections, &ser_policy)?;

        let built_schema = doc::validation::build_bundle(&write_schema_json)
            .context("collection write_schema_json is not a JSON schema")?;
        let validator =
            doc::Validator::new(built_schema).context("could not build a schema validator")?;
        let write_shape = doc::Shape::infer(&validator.schemas()[0], validator.schema_index());

        Ok(Self {
            collection_name: name.clone(),
            collection_generation_id,
            document_uuid_ptr,
            key_extractors,
            partition_extractors,
            partition_template_name: partition_template.name.clone(),
            ser_policy,
            state_key: state_key.clone(),
            write_schema_json: write_schema_json.clone(),
            write_shape,
        })
    }

    pub fn combiner_spec(&self) -> (bool, Vec<doc::Extractor>, String, doc::Validator) {
        // These are safe to unwrap() because they were previously run over
        // `self.write_schema_json` by Binding::new().
        let built_schema = doc::validation::build_bundle(&self.write_schema_json).unwrap();
        let validator = doc::Validator::new(built_schema).unwrap();

        (
            false,
            self.key_extractors.clone(),
            format!("captured collection {}", self.collection_name),
            validator,
        )
    }
}
