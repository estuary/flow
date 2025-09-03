use super::{Task, Transform};
use anyhow::Context;
use proto_flow::derive::{Request, Response, request, response};
use proto_flow::flow;

impl Task {
    pub fn new(open: &Request, opened: &Response) -> anyhow::Result<Self> {
        let request::Open {
            collection,
            range,
            state_json: _,
            version,
        } = open.clone().open.context("expected Open")?;

        let response::Opened {} = opened.opened.as_ref().context("expected Opened")?;

        let flow::CollectionSpec {
            ack_template_json: _,
            derivation,
            key,
            name: collection_name,
            partition_fields,
            partition_template,
            projections,
            read_schema_json: _,
            uuid_ptr,
            write_schema_json,
        } = collection.context("missing collection")?;

        let partition_template = partition_template
            .as_ref()
            .context("missing partition template")?;

        let collection_generation_id =
            assemble::extract_generation_id_suffix(&partition_template.name);

        let flow::collection_spec::Derivation {
            config_json: _,
            connector_type: _,
            network_ports: _,
            recovery_log_template: _,
            shard_template: _,
            shuffle_key_types: _,
            transforms,
            inactive_transforms: _,
            redact_salt,
        } = derivation.as_ref().context("missing derivation")?;

        if key.is_empty() {
            anyhow::bail!("collection key cannot be empty");
        }

        let range = range.context("missing range")?;
        let ser_policy = doc::SerPolicy::noop();

        let document_uuid_ptr = doc::Pointer::from(uuid_ptr);
        let key_extractors = extractors::for_key(&key, &projections, &ser_policy)?;
        let partition_extractors =
            extractors::for_fields(&partition_fields, &projections, &ser_policy)?;

        let shard_ref = ops::ShardRef {
            kind: ops::TaskType::Derivation as i32,
            name: collection_name.clone(),
            key_begin: format!("{:08x}", range.key_begin),
            r_clock_begin: format!("{:08x}", range.r_clock_begin),
            build: version.clone(),
        };

        let transforms = transforms
            .into_iter()
            .enumerate()
            .map(|(index, spec)| Transform::new(spec).context(index))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            collection_name,
            collection_generation_id,
            document_uuid_ptr,
            key_extractors,
            partition_extractors,
            redact_salt: redact_salt.clone(),
            ser_policy,
            shard_ref,
            transforms,
            write_schema_json,
        })
    }

    pub fn combine_spec(&self) -> anyhow::Result<doc::combine::Spec> {
        let built_schema = doc::validation::build_bundle(&self.write_schema_json)
            .context("collection write_schema_json is not a JSON schema")?;
        let validator =
            doc::Validator::new(built_schema).context("could not build a schema validator")?;

        Ok(doc::combine::Spec::with_one_binding(
            false,
            self.key_extractors.clone(),
            "derived",
            self.redact_salt.to_vec(),
            None,
            validator,
        ))
    }

    pub fn validators(&self) -> anyhow::Result<Vec<doc::Validator>> {
        self.transforms
            .iter()
            .map(|transform| {
                transform.validator().with_context(|| {
                    format!("failed to build validator for transform {}", transform.name)
                })
            })
            .collect::<Result<Vec<_>, _>>()
    }
}

impl Transform {
    pub fn new(spec: &flow::collection_spec::derivation::Transform) -> anyhow::Result<Self> {
        let flow::collection_spec::derivation::Transform {
            backfill: _,
            collection,
            journal_read_suffix: _,
            lambda_config_json: _,
            name,
            not_after: _,
            not_before: _,
            partition_selector: _,
            priority: _,
            read_delay_seconds: _,
            read_only: _,
            shuffle_key: _,
            shuffle_lambda_config_json: _,
        } = spec;

        let flow::CollectionSpec {
            ack_template_json: _,
            derivation: _,
            key: _,
            name: collection_name,
            partition_fields: _,
            partition_template: _,
            projections: _,
            read_schema_json,
            uuid_ptr: _,
            write_schema_json,
        } = collection.as_ref().context("missing collection")?;

        let read_schema_json = if read_schema_json.is_empty() {
            write_schema_json
        } else {
            read_schema_json
        }
        .clone();

        Ok(Self {
            collection_name: collection_name.clone(),
            name: name.clone(),
            read_schema_json,
        })
    }

    pub fn validator(&self) -> anyhow::Result<doc::Validator> {
        let built_schema = doc::validation::build_bundle(&self.read_schema_json)
            .context("collection read_schema_json is not a JSON schema")?;
        let validator =
            doc::Validator::new(built_schema).context("could not build a schema validator")?;
        Ok(validator)
    }
}
