use crate::shard::task_schema::relax_inferred_datetime_formats;
use anyhow::Context;
use proto_flow::flow;

/// Task configuration for a derivation shard.
///
/// A derivation is a single output collection (the derived collection) fed by
/// one-or-more transforms (input bindings). The shard forwards source documents
/// to the connector as `C:Read` and combines `C:Published` documents by the
/// derived collection's key before publishing them to its journals.
pub(super) struct Task {
    /// Derived collection name.
    pub collection_name: String,
    /// JSON pointer at which document UUIDs are added.
    pub document_uuid_ptr: json::Pointer,
    /// Key components extracted from derived (published) documents.
    pub key_extractors: Vec<doc::Extractor>,
    /// Salt used for redacting sensitive fields when combining.
    pub redact_salt: bytes::Bytes,
    /// Source metadata of each transform (input binding), indexed by transform
    /// index. Used to bound `C:Read` indices and to re-validate source documents.
    pub transforms: Vec<Transform>,
    /// Stable RocksDB `state_key` of each transform, indexed by binding index.
    /// Used to map the leader's frontier binding indices to the `FC:`/`FH:`
    /// key layout.
    pub binding_state_keys: Vec<String>,
    /// Write JSON-Schema of the derived collection.
    pub write_schema_json: bytes::Bytes,
    /// Inferred Shape of written documents, seeded from `write_schema_json`.
    pub write_shape: doc::Shape,
}

/// Transform configuration for a derivation shard.
pub(super) struct Transform {
    /// Name of this transform.
    pub transform: String,
    /// Source collection name.
    pub collection: String,
    /// Schema the shuffle read pipeline validates source documents against:
    /// the source collection's read schema, or its write schema when no read
    /// schema is defined (mirroring `shuffle::binding::build_schema`).
    pub schema_json: bytes::Bytes,
    /// Extractors of this transform's shuffle key, applied to source documents
    /// to populate `Read.shuffle.key_json` for JSON connectors. Empty for a
    /// lambda-computed key.
    pub shuffle_key_extractors: Vec<doc::Extractor>,
}

/// Build the runtime [`Transform`] for a single derivation transform (input binding).
fn build_transform(
    t: &flow::collection_spec::derivation::Transform,
    ser_policy: &doc::SerPolicy,
    relax_inferred_datetime: bool,
) -> anyhow::Result<Transform> {
    let collection = t
        .collection
        .as_ref()
        .context("transform missing source collection")?;

    // Prefer the read schema, falling back to the write schema, so
    // re-validation uses the same schema the shuffle read pipeline
    // validated against when it set `FLAGS_SCHEMA_VALID`.
    let schema_json = if collection.read_schema_json.is_empty() {
        collection.write_schema_json.clone()
    } else {
        collection.read_schema_json.clone()
    };

    // When enabled for this task, strip `date`/`date-time`/`time` `format`
    // keywords contributed by the source collection's inferred schema so that
    // historical, non-conforming values are not retroactively rejected when read
    // into the derivation. Capture-time write-schema validation is unaffected.
    let schema_json = if relax_inferred_datetime {
        relax_inferred_datetime_formats(&schema_json)
            .context("relaxing inferred date-time formats of read schema")?
    } else {
        schema_json
    };

    // Resolve the extractors of a transform's shuffle key, applied to source
    // documents to populate `Read.shuffle.key_json` for JSON connectors.
    //
    // Mirrors the key selection of `shuffle::binding::from_derivation_transform`.
    let shuffle_key_extractors = if !t.shuffle_key.is_empty() {
        extractors::for_key(&t.shuffle_key, &collection.projections, ser_policy)
            .with_context(|| format!("building shuffle key extractors for transform {}", t.name))?
    } else if !t.shuffle_lambda_config_json.is_empty() {
        Vec::new() // Lambda-computed (no extractors).
    } else {
        extractors::for_key(&collection.key, &collection.projections, ser_policy)
            .with_context(|| format!("building source key extractors for transform {}", t.name))?
    };

    Ok(Transform {
        transform: t.name.clone(),
        collection: collection.name.clone(),
        schema_json,
        shuffle_key_extractors,
    })
}

impl Task {
    pub fn new(spec: &flow::CollectionSpec) -> anyhow::Result<Self> {
        let flow::CollectionSpec {
            derivation,
            key,
            name: collection_name,
            partition_template,
            projections,
            uuid_ptr,
            write_schema_json,
            ..
        } = spec;

        if key.is_empty() {
            anyhow::bail!("derived collection key cannot be empty");
        }

        let flow::collection_spec::Derivation {
            transforms,
            redact_salt,
            shard_template,
            ..
        } = derivation.as_ref().context("missing derivation")?;

        // Opt-in, per-task relaxation of read-side date-time `format`
        // enforcement inherited from each source collection's inferred schema.
        // See build_transform and estuary/flow#3133.
        let relax_inferred_datetime = labels::shard_flag_enabled(
            shard_template.as_ref(),
            labels::RELAX_INFERRED_DATETIME_FLAG,
        );

        // The built `Transform.state_key` is intentionally left unpopulated until the
        // V2 derivation migration completes (the frozen V1 derive connectors reject the
        // unknown `stateKey` JSON field). Recompute it on-demand from the transform name
        // and backfill, exactly as `validation` does, so the RocksDB `FC:`/`FH:` key
        // layout is identical regardless of whether the spec carries the field.
        let binding_state_keys = transforms
            .iter()
            .map(|t| assemble::encode_state_key(&[&t.name], t.backfill))
            .collect::<Vec<String>>();

        let ser_policy = doc::SerPolicy::noop();

        let sources = transforms
            .iter()
            .map(|t| build_transform(t, &ser_policy, relax_inferred_datetime))
            .collect::<anyhow::Result<Vec<Transform>>>()?;

        let partition_template = partition_template
            .as_ref()
            .context("missing partition template")?;
        let collection_generation_id =
            assemble::extract_generation_id_suffix(&partition_template.name);

        let document_uuid_ptr = json::Pointer::from(uuid_ptr);
        let key_extractors = extractors::for_key(key, projections, &ser_policy)?;

        let built_schema = doc::validation::build_bundle(write_schema_json)
            .context("derived collection write_schema_json is not a JSON schema")?;
        let validator = doc::Validator::new(built_schema)
            .context("could not build a derived collection schema validator")?;
        let mut write_shape = doc::Shape::infer(validator.schema(), validator.schema_index());
        // Stamp the generation id so inferred-schema updates carry it (mirrors
        // capture `Task::binding_shapes_by_index`).
        write_shape.annotations.insert(
            crate::X_GENERATION_ID.to_string(),
            serde_json::Value::String(collection_generation_id.to_string()),
        );

        Ok(Self {
            collection_name: collection_name.clone(),
            document_uuid_ptr,
            key_extractors,
            redact_salt: redact_salt.clone(),
            transforms: sources,
            binding_state_keys,
            write_schema_json: write_schema_json.clone(),
            write_shape,
        })
    }

    /// Build a source-document validator per transform.
    pub fn source_validators(&self) -> anyhow::Result<Vec<doc::Validator>> {
        self.transforms
            .iter()
            .map(
                |Transform {
                     collection: collection_name,
                     transform: transform_name,
                     schema_json,
                     shuffle_key_extractors: _,
                 }| {
                    let built_schema =
                        doc::validation::build_bundle(schema_json).with_context(|| {
                            format!(
                                "source collection {collection_name} schema is not a JSON schema",
                            )
                        })?;
                    doc::Validator::new(built_schema).with_context(|| {
                        format!(
                            "could not build a schema validator for transform {transform_name}",
                        )
                    })
                },
            )
            .collect()
    }

    /// Combiner over the single derived-collection output binding. Connector
    /// state arrives via `C:Flushed.state` (not the combiner), so unlike capture
    /// there is no extra connector-state binding.
    pub fn combine_spec(&self) -> anyhow::Result<doc::combine::Spec> {
        let built_schema = doc::validation::build_bundle(&self.write_schema_json)
            .context("derived collection write_schema_json is not a JSON schema")?;
        let validator = doc::Validator::new(built_schema)
            .context("could not build a derived collection schema validator")?;

        Ok(doc::combine::Spec::with_one_binding(
            false, // Associative combine, matching the V1 derive runtime.
            self.key_extractors.clone(),
            format!("derived collection {}", self.collection_name),
            self.redact_salt.to_vec(),
            validator,
        ))
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
        let transform =
            build_transform(&spec, &doc::SerPolicy::noop(), relax_inferred_datetime).unwrap();

        let mut validator =
            doc::Validator::new(doc::validation::build_bundle(&transform.schema_json).unwrap())
                .unwrap();

        let alloc = doc::HeapNode::new_allocator();
        let mut de = serde_json::Deserializer::from_str(doc);
        let node = doc::HeapNode::from_serde(&mut de, &alloc).unwrap();

        validator.is_valid(&node)
    }

    #[test]
    fn test_v2_transform_relaxes_inferred_datetime_when_flagged() {
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
