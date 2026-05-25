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
    /// Number of transforms (input bindings), for `C:Read` index validation.
    pub n_transforms: usize,
    /// Stable RocksDB `state_key` of each transform, indexed by binding index.
    /// Used to map the leader's frontier binding indices to the `FC:`/`FH:`
    /// key layout.
    pub binding_state_keys: Vec<String>,
    /// Write JSON-Schema of the derived collection.
    pub write_schema_json: bytes::Bytes,
    /// Inferred Shape of written documents, seeded from `write_schema_json`.
    pub write_shape: doc::Shape,
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
            ..
        } = derivation.as_ref().context("missing derivation")?;

        // The built `Transform.state_key` is intentionally left unpopulated until the
        // V2 derivation migration completes (the frozen V1 derive connectors reject the
        // unknown `stateKey` JSON field). Recompute it on-demand from the transform name
        // and backfill, exactly as `validation` does, so the RocksDB `FC:`/`FH:` key
        // layout is identical regardless of whether the spec carries the field.
        let binding_state_keys = transforms
            .iter()
            .map(|t| assemble::encode_state_key(&[&t.name], t.backfill))
            .collect::<Vec<String>>();

        let partition_template = partition_template
            .as_ref()
            .context("missing partition template")?;
        let collection_generation_id =
            assemble::extract_generation_id_suffix(&partition_template.name);

        let ser_policy = doc::SerPolicy::noop();
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
            n_transforms: transforms.len(),
            binding_state_keys,
            write_schema_json: write_schema_json.clone(),
            write_shape,
        })
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
