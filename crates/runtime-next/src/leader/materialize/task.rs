use super::{Task, close_policy, triggers};
use anyhow::Context;
use proto_flow::flow;
use proto_gazette::consumer;

impl Task {
    pub async fn new(
        build: String,
        spec: &flow::MaterializationSpec,
        max_transactions: u32,
        peers: Vec<String>,
    ) -> anyhow::Result<Self> {
        let flow::MaterializationSpec {
            name,
            shard_template,
            bindings,
            triggers_json,
            connector_type,
            config_json,
            sync_schedule_json,
            ..
        } = spec;

        let consumer::ShardSpec {
            min_txn_duration,
            max_txn_duration,
            ..
        } = shard_template.as_ref().context("missing shard template")?;

        let mut binding_collection_names = Vec::with_capacity(bindings.len());
        let mut binding_journal_read_suffixes = Vec::with_capacity(bindings.len());

        for (binding, resolved) in spec.resolved_bindings() {
            let flow::materialization_spec::Binding {
                journal_read_suffix,
                ..
            } = binding;

            let (collection, _identity) = resolved.context("missing collection")?;

            binding_collection_names.push(collection.name.clone());
            binding_journal_read_suffixes.push(journal_read_suffix.clone());
        }

        // Extract the connector image from the config, if applicable.
        let connector_image =
            if *connector_type == flow::materialization_spec::ConnectorType::Image as i32 {
                serde_json::from_slice::<models::ConnectorConfig>(&config_json)
                    .context("parsing connector config")?
                    .image
            } else {
                String::new()
            };

        let triggers = if triggers_json.is_empty() {
            None
        } else {
            Some(std::sync::Arc::new(
                decrypt_and_compile_triggers(&triggers_json).await?,
            ))
        };

        // Sync schedule (materialization-only, no secrets, so no decryption).
        // Compilation re-runs full validation, so a malformed spec that
        // somehow slipped past the build fails the task cleanly here.
        let sync_schedule = if sync_schedule_json.is_empty() {
            None
        } else {
            let model: models::SyncSchedule = serde_json::from_slice(&sync_schedule_json)
                .context("parsing sync_schedule_json")?;
            Some(
                super::sync_schedule::CompiledSchedule::new(model)
                    .map_err(anyhow::Error::msg)
                    .context("invalid sync schedule")?,
            )
        };
        // Deterministic jitter seed, derived from the task's tenant.
        let sync_seed = if sync_schedule.is_some() {
            xxhash_rust::xxh3::xxh3_64(models::tenant_from(name).as_bytes())
        } else {
            0
        };

        let min_txn_duration = min_txn_duration
            .context("missing min_txn_duration")?
            .try_into()?;
        let max_txn_duration = max_txn_duration
            .context("missing max_txn_duration")?
            .try_into()?;

        let shard_ref = ops::ShardRef {
            kind: ops::TaskType::Materialization as i32,
            name: name.clone(),
            key_begin: labels::KEY_BEGIN_MIN.to_string(),
            r_clock_begin: labels::RCLOCK_BEGIN_MIN.to_string(),
            build,
        };

        // When a sync schedule is configured, the FSM modulates this policy's
        // open-duration band per evaluation (see `compute_open_duration`), so
        // the schedule governs commit timing in place of min/max_txn_duration.
        let close_policy = close_policy::Policy::new(min_txn_duration, max_txn_duration);

        Ok(Self {
            binding_collection_names,
            binding_journal_read_suffixes,
            close_policy,
            connector_image,
            max_transactions,
            n_shards: peers.len(),
            peers,
            shard_ref,
            triggers,
            sync_schedule,
            sync_seed,
        })
    }
}

async fn decrypt_and_compile_triggers(
    triggers: &[u8],
) -> anyhow::Result<triggers::CompiledTriggers> {
    let mut triggers: models::Triggers =
        serde_json::from_slice(triggers).context("parsing triggers JSON")?;

    // Strip HMAC-excluded fields before decryption (they were stripped
    // during encryption so SOPS HMAC doesn't cover them), then restore.
    let originals = models::triggers::strip_hmac_excluded_fields(&mut triggers);

    let stripped = models::RawValue::from_string(
        serde_json::to_string(&triggers).expect("triggers always serialize"),
    )
    .expect("trigger serialization is JSON");

    let mut decrypted: models::Triggers = serde_json::from_str(
        unseal::decrypt_sops(&stripped)
            .await
            .context("decrypting triggers_json")?
            .get(),
    )
    .context("parsing decrypted triggers JSON")?;

    models::triggers::restore_hmac_excluded_fields(&mut decrypted, originals);

    let compiled =
        triggers::CompiledTriggers::compile(decrypted).context("compiling trigger templates")?;

    Ok(compiled)
}
