use super::{Task, Transform};
use crate::task_schema::{relax_inferred_datetime_formats, shard_flag_enabled};
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

        let response::Opened { .. } = opened.opened.as_ref().context("expected Opened")?;

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
            shard_template,
            shuffle_key_types: _,
            transforms,
            inactive_transforms: _,
            redact_salt,
        } = derivation.as_ref().context("missing derivation")?;

        // Opt-in, per-task relaxation of read-side date-time `format`
        // enforcement inherited from each source collection's inferred schema.
        // See Transform::new and estuary/flow#3133.
        let relax_inferred_datetime = shard_flag_enabled(
            shard_template.as_ref(),
            labels::RELAX_INFERRED_DATETIME_FLAG,
        );

        if key.is_empty() {
            anyhow::bail!("collection key cannot be empty");
        }

        let range = range.context("missing range")?;
        let ser_policy = doc::SerPolicy::noop();

        let document_uuid_ptr = json::Pointer::from(uuid_ptr);
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
            .map(|(index, spec)| Transform::new(spec, relax_inferred_datetime).context(index))
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
    pub fn new(
        spec: &flow::collection_spec::derivation::Transform,
        relax_inferred_datetime: bool,
    ) -> anyhow::Result<Self> {
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
            state_key: _,
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

        // When enabled for this task, strip `date`/`date-time`/`time` `format`
        // keywords contributed by the source collection's inferred schema so
        // that historical, non-conforming values are not retroactively rejected
        // when read into the derivation. Capture-time write-schema validation of
        // the source is unaffected.
        let read_schema_json = if relax_inferred_datetime {
            relax_inferred_datetime_formats(&read_schema_json)
                .context("relaxing inferred date-time formats of read schema")?
        } else {
            read_schema_json
        };

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

#[cfg(test)]
mod test {
    use super::*;

    // A source-collection read schema whose inlined inferred schema tags a
    // field `format: date-time`, as the control plane assembles it.
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

    fn transform_accepts(relax_inferred_datetime: bool, doc: &str) -> bool {
        let spec = flow::collection_spec::derivation::Transform {
            collection: Some(flow::CollectionSpec {
                read_schema_json: bytes::Bytes::from(READ_SCHEMA),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut validator = Transform::new(&spec, relax_inferred_datetime)
            .unwrap()
            .validator()
            .unwrap();

        let alloc = doc::HeapNode::new_allocator();
        let mut de = serde_json::Deserializer::from_str(doc);
        let node = doc::HeapNode::from_serde(&mut de, &alloc).unwrap();

        validator.is_valid(&node)
    }

    #[test]
    fn test_transform_relaxes_inferred_datetime_when_flagged() {
        // A space-separated (non-RFC3339) timestamp — the historical shape from
        // #3133 — read from a source collection into a derivation.
        let legacy = r#"{"ts": "2026-06-17 12:46:17.375663+00:00"}"#;
        let conforming = r#"{"ts": "2026-06-17T12:46:17.375663+00:00"}"#;

        // Flag OFF: the source read validator rejects the legacy value.
        assert!(!transform_accepts(false, legacy));
        assert!(transform_accepts(false, conforming));

        // Flag ON: the legacy value is tolerated; conforming values still pass.
        assert!(transform_accepts(true, legacy));
        assert!(transform_accepts(true, conforming));
    }
}
