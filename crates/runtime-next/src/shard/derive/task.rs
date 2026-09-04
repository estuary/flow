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
    /// Transforms of the derivation.
    pub transforms: Vec<Transform>,
    /// Source collections read by the task's transforms.
    pub sources: Vec<Source>,
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
    /// Index of this transform's source collection within [`Task::sources`].
    pub source: u32,
    /// Extractors of this transform's shuffle key, applied to source documents
    /// to populate `Read.shuffle.key_json` for JSON connectors. Empty for a
    /// lambda-computed key.
    pub shuffle_key_extractors: Vec<doc::Extractor>,
}

/// A source collection, read by one or more transforms of the derivation.
///
/// Sources follow the spec's declared indirection: transforms sharing a
/// `collection_index` share one Source, while a transform of an inline-form
/// spec carries no index and is its own Source.
pub(super) struct Source {
    /// Source collection name.
    pub collection_name: String,
    /// Schema the shuffle read pipeline validates source documents against:
    /// the source collection's read schema, or its write schema when no read
    /// schema is defined (mirroring `shuffle::Source::new`).
    pub read_schema_json: bytes::Bytes,
}

/// Build the runtime [`Transform`] for a single derivation transform (input binding).
fn build_transform(
    t: &flow::collection_spec::derivation::Transform,
    collection: &flow::CollectionSpec,
    source: u32,
    ser_policy: &doc::SerPolicy,
) -> anyhow::Result<Transform> {
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
        source,
        shuffle_key_extractors,
    })
}

/// Build the runtime [`Source`] for a source collection.
fn build_source(collection: &flow::CollectionSpec) -> Source {
    // Prefer the read schema, falling back to the write schema, so
    // re-validation uses the same schema the shuffle read pipeline
    // validated against when it set `FLAGS_SCHEMA_VALID`.
    let read_schema_json = if collection.read_schema_json.is_empty() {
        collection.write_schema_json.clone()
    } else {
        collection.read_schema_json.clone()
    };

    Source {
        collection_name: collection.name.clone(),
        read_schema_json,
    }
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

        let derivation = derivation.as_deref().context("missing derivation")?;

        let flow::collection_spec::Derivation {
            transforms,
            redact_salt,
            ..
        } = derivation;

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

        let mut transforms = Vec::new();
        let mut sources = Vec::<Source>::new();
        let mut sources_by_identity = std::collections::BTreeMap::<u32, u32>::new();

        for (t, resolved) in derivation.resolved_transforms() {
            let (collection, identity) = resolved.context("transform missing source collection")?;

            let source = match identity.and_then(|i| sources_by_identity.get(&i)) {
                Some(&source) => source,
                None => {
                    let source = sources.len() as u32;
                    sources.push(build_source(collection));

                    if let Some(identity) = identity {
                        sources_by_identity.insert(identity, source);
                    }
                    source
                }
            };
            transforms.push(build_transform(t, collection, source, &ser_policy)?);
        }

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
        // capture `Task::shapes_by_target`).
        write_shape.annotations.insert(
            crate::X_GENERATION_ID.to_string(),
            serde_json::Value::String(collection_generation_id.to_string()),
        );

        Ok(Self {
            collection_name: collection_name.clone(),
            document_uuid_ptr,
            key_extractors,
            redact_salt: redact_salt.clone(),
            transforms,
            sources,
            binding_state_keys,
            write_schema_json: write_schema_json.clone(),
            write_shape,
        })
    }

    /// Build a source-document validator per Source.
    pub fn source_validators(&self) -> anyhow::Result<Vec<doc::Validator>> {
        self.sources
            .iter()
            .map(
                |Source {
                     collection_name,
                     read_schema_json,
                     ..
                 }| {
                    let built_schema = doc::validation::build_bundle(read_schema_json)
                        .with_context(|| {
                            format!(
                                "source collection {collection_name} schema is not a JSON schema",
                            )
                        })?;
                    doc::Validator::new(built_schema).with_context(|| {
                        format!(
                            "could not build a schema validator for collection {collection_name}",
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
