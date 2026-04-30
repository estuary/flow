use super::{Task, triggers};
use anyhow::Context;
use proto_flow::flow;
use proto_gazette::consumer;

impl Task {
    pub async fn new(
        build: String,
        spec: &flow::MaterializationSpec,
        peers: Vec<String>,
    ) -> anyhow::Result<Self> {
        let flow::MaterializationSpec {
            name,
            shard_template,
            bindings,
            triggers_json,
            connector_type,
            config_json,
            ..
        } = spec;

        let consumer::ShardSpec {
            min_txn_duration,
            max_txn_duration,
            ..
        } = shard_template.as_ref().context("missing shard template")?;

        let mut binding_collection_names = Vec::with_capacity(bindings.len());
        let mut binding_journal_read_suffixes = Vec::with_capacity(bindings.len());

        for binding in bindings {
            let flow::materialization_spec::Binding {
                collection,
                journal_read_suffix,
                ..
            } = binding;

            let flow::CollectionSpec {
                name: collection_name,
                ..
            } = collection.as_ref().context("missing collection")?;

            binding_collection_names.push(collection_name.clone());
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

        // Close-policy thresholds, many with placeholder defaults.
        // TODO: thread these through from the spec once they're supported there.
        let open_duration: std::ops::Range<std::time::Duration> =
            min_txn_duration..max_txn_duration;
        let last_commit_age =
            std::time::Duration::from_secs(0)..std::time::Duration::from_secs(300);
        let combiner_usage_bytes = (4 * 1024 * 1024)..(256 * 1024 * 1024);
        let read_docs = 1_000..1_000_000;
        let read_bytes = (1 << 20)..(1 << 30);

        Ok(Self {
            binding_collection_names,
            binding_journal_read_suffixes,
            combiner_usage_bytes,
            connector_image,
            last_close_age: last_commit_age,
            n_shards: peers.len(),
            open_duration,
            peers,
            read_bytes,
            read_docs,
            shard_ref,
            triggers,
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

    let compiled = triggers::CompiledTriggers::compile(decrypted.config)
        .context("compiling trigger templates")?;

    Ok(compiled)
}
