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
        })
    }
}

async fn decrypt_and_compile_triggers(
    triggers: &[u8],
) -> anyhow::Result<triggers::CompiledTriggers> {
    let decrypted = decrypt_triggers(triggers).await?;
    triggers::CompiledTriggers::compile(decrypted).context("compiling trigger templates")
}

/// Decrypt a sealed `triggers_json` into its plaintext model.
///
/// New configs decrypt through the `sops.overlay` path. Legacy (pre-overlay)
/// configs — recognized by a list-shaped `config` — are decrypted through the
/// old HMAC-exclusion path instead, since their MAC was computed over a
/// placeholder-stripped document. This compatibility branch is removed once no
/// legacy configs remain.
async fn decrypt_triggers(triggers: &[u8]) -> anyhow::Result<models::Triggers> {
    let probe: serde_json::Value =
        serde_json::from_slice(triggers).context("parsing triggers JSON")?;

    if probe.get("config").is_some_and(|c| c.is_array()) {
        let mut legacy: models::triggers::LegacyTriggers =
            serde_json::from_value(probe).context("parsing legacy triggers JSON")?;
        let originals = models::triggers::strip_hmac_excluded_fields(&mut legacy);
        let stripped = models::RawValue::from_string(
            serde_json::to_string(&legacy).context("serializing stripped triggers")?,
        )
        .expect("stripped triggers serialize to JSON");

        let mut decrypted: models::triggers::LegacyTriggers = serde_json::from_str(
            unseal::decrypt_sops(&stripped)
                .await
                .context("decrypting legacy triggers_json")?
                .get(),
        )
        .context("parsing decrypted legacy triggers JSON")?;
        models::triggers::restore_hmac_excluded_fields(&mut decrypted, originals);
        return Ok(decrypted.into_triggers());
    }

    let sealed: Box<models::RawValue> =
        serde_json::from_slice(triggers).context("parsing triggers JSON")?;
    let schema =
        serde_json::to_vec(&models::triggers_schema()).expect("triggers schema must serialize");

    serde_json::from_str(
        unseal::overlay::decrypt_with_overlay(&sealed, &schema)
            .await
            .context("decrypting triggers_json")?
            .get(),
    )
    .context("parsing decrypted triggers JSON")
}
