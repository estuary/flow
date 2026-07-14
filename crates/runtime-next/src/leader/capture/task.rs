use crate::leader::close_policy;
use anyhow::Context;
use proto_flow::capture::{Request, Response, request, response};
use proto_flow::flow;
use proto_gazette::{consumer, uuid};
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct Task {
    /// Bindings of this Task.
    pub bindings: Vec<Binding>,
    /// Policy for how transactions close.
    pub close_policy: close_policy::Policy,
    /// Does the capture connector want explicit acknowledgements?
    pub explicit_acknowledgements: bool,
    /// Transactions to complete before stopping, or zero for unbounded.
    /// Set only by the preview / test harness; production leaves it zero.
    pub max_transactions: u32,
    /// Salt used for redacting sensitive fields.
    pub redact_salt: bytes::Bytes,
    /// Clock at which this Task is eligible for restart.
    pub restart: uuid::Clock,
    /// Bound on the number of captured document bytes in a single connector
    /// checkpoint sequence. Injects a synthetic checkpoint upon breach.
    pub sequence_bytes_limit: u64,
    /// ShardRef of this task.
    pub shard_ref: ops::ShardRef,
}

#[derive(Debug, Clone)]
pub struct Binding {
    // Target collection.
    pub collection_name: String,
    // Generation id of the collection, which must be output as part of updating inferred schemas.
    pub collection_generation_id: models::Id,
    // JSON pointer at which document UUIDs are added.
    pub document_uuid_ptr: json::Pointer,
    // Key components which are extracted from written documents.
    pub key_extractors: Vec<doc::Extractor>,
    // Partition template name for journals of the target collection.
    pub partition_template_name: String,
    // Encoded resource path + backfill state key of this binding.
    pub state_key: String,
    // Write schema of the target collection.
    pub write_schema_json: bytes::Bytes,
    // Inferred Shape of written documents.
    pub write_shape: doc::Shape,
}

impl Task {
    pub fn new(open: &Request, opened: &Response, max_transactions: u32) -> anyhow::Result<Self> {
        let request::Open {
            capture: spec,
            range,
            state_json: _,
            sealed_config_json: _,
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
            shard_template,
            inactive_bindings: _,
            redact_salt,
        } = spec.as_ref().context("missing capture")?;
        let range = range.context("missing range")?;

        if range.r_clock_begin != 0 || range.r_clock_end != u32::MAX {
            anyhow::bail!("captures cannot split on r-clock: {range:?}");
        }

        // Min/max transaction duration come from the shard template.
        let consumer::ShardSpec {
            min_txn_duration,
            max_txn_duration,
            ..
        } = shard_template.as_ref().context("missing shard template")?;

        let min_txn_duration = min_txn_duration
            .context("missing min_txn_duration")?
            .try_into()?;
        let max_txn_duration = max_txn_duration
            .context("missing max_txn_duration")?
            .try_into()?;

        let ser_policy = doc::SerPolicy::noop();
        let bindings = bindings
            .iter()
            .enumerate()
            .map(|(index, spec)| Binding::new(spec, ser_policy.clone()).context(index))
            .collect::<Result<Vec<_>, _>>()?;

        // A Clock one poll `interval` from now, on the same wall-clock base as
        // the actor's monotonic `now`, so the HeadFSM computes the restart wait
        // directly. `tokens::now()` is tokio test-time aware.
        let now = tokens::now();
        let restart = uuid::Clock::from_unix(
            now.timestamp() as u64 + *interval_seconds as u64,
            now.timestamp_subsec_nanos(),
        );

        let shard_ref = ops::ShardRef {
            kind: ops::TaskType::Capture as i32,
            name: name.clone(),
            key_begin: format!("{:08x}", range.key_begin),
            r_clock_begin: format!("{:08x}", range.r_clock_begin),
            build: version.clone(),
        };

        let mut close_policy = close_policy::Policy::new(min_txn_duration, max_txn_duration);
        // Cap combiner usage at 64MB to favor small transactions.
        close_policy.combiner_usage_bytes = 0..(64 * 1024 * 1024);

        Ok(Self {
            bindings,
            close_policy,
            explicit_acknowledgements,
            max_transactions,
            redact_salt: redact_salt.clone(),
            restart,
            sequence_bytes_limit: 512 * 1024 * 1024, // 512 MB.
            shard_ref,
        })
    }

    pub fn binding_shapes_by_index(
        &self,
        mut by_key: BTreeMap<String, doc::Shape>,
    ) -> Vec<doc::Shape> {
        let mut by_index = Vec::new();
        by_index.resize_with(self.bindings.len(), doc::shape::Shape::nothing);

        for (index, binding) in self.bindings.iter().enumerate() {
            // `partition_template_name` embeds the collection name and generation ID,
            // while `state_key` is unique if a single target collection is bound
            // to multiple endpoint resources.
            let key = format!("{};{}", binding.partition_template_name, binding.state_key);

            // Seed inference annotations only when the binding is net-new.
            // In particular, carry forward an x-complexity-limit that may have
            // been elevated by the presence of a SourcedSchema.
            if let Some(shape) = by_key.remove(&key) {
                by_index[index] = shape;
            } else {
                let annotations = &mut by_index[index].annotations;
                annotations.insert(
                    crate::X_GENERATION_ID.to_string(),
                    serde_json::Value::String(binding.collection_generation_id.to_string()),
                );
                annotations.insert(
                    doc::shape::X_COMPLEXITY_LIMIT.to_string(),
                    serde_json::json!(doc::shape::limits::DEFAULT_SCHEMA_COMPLEXITY_LIMIT),
                );
            }
        }
        by_index
    }

    /// Invert `binding_shapes_by_index`, re-keying inferred shapes by their
    /// stable `partition_template_name;state_key` identity.
    ///
    /// Inferred shapes are held only in memory and accumulate across the many
    /// connector sessions of a shard's lifetime (a capture re-Opens every poll
    /// `interval`). Binding *indices* are not stable across spec updates but the
    /// key is, so the shard stows shapes by key between sessions and
    /// `binding_shapes_by_index` restores them into the next session's layout.
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
        let state_schema = doc::reduce::merge_patch_schema().to_string();
        let state_schema = doc::validation::build_bundle(state_schema.as_bytes()).unwrap();
        let state_validator = doc::Validator::new(state_schema).unwrap();

        Ok(doc::combine::Spec::with_bindings(
            self.bindings
                .iter()
                .map(|binding| binding.combiner_spec())
                .chain(std::iter::once((
                    false,
                    Vec::new(),
                    "connector state".to_string(),
                    state_validator,
                ))),
            self.redact_salt.to_vec(),
        ))
    }
}

impl Binding {
    fn new(spec: &flow::capture_spec::Binding, ser_policy: doc::SerPolicy) -> anyhow::Result<Self> {
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
            partition_fields: _,
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

        let document_uuid_ptr = json::Pointer::from(uuid_ptr);
        let key_extractors = extractors::for_key(key, projections, &ser_policy)?;

        let built_schema = doc::validation::build_bundle(write_schema_json)
            .context("collection write_schema_json is not a JSON schema")?;
        let validator =
            doc::Validator::new(built_schema).context("could not build a schema validator")?;
        let write_shape = doc::Shape::infer(&validator.schema(), validator.schema_index());

        Ok(Self {
            collection_name: name.clone(),
            collection_generation_id,
            document_uuid_ptr,
            key_extractors,
            partition_template_name: partition_template.name.clone(),
            state_key: state_key.clone(),
            write_schema_json: write_schema_json.clone(),
            write_shape,
        })
    }

    fn combiner_spec(&self) -> (bool, Vec<doc::Extractor>, String, doc::Validator) {
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
