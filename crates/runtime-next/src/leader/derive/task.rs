use super::Task;
use anyhow::Context;
use proto_flow::flow;
use proto_gazette::consumer;

impl Task {
    pub async fn new(
        build: String,
        spec: &flow::CollectionSpec,
        max_transactions: u32,
        peers: Vec<String>,
    ) -> anyhow::Result<Self> {
        let derivation = spec
            .derivation
            .as_ref()
            .context("CollectionSpec missing derivation")?;

        let flow::collection_spec::Derivation {
            transforms,
            shard_template,
            ..
        } = derivation;

        let consumer::ShardSpec {
            min_txn_duration,
            max_txn_duration,
            ..
        } = shard_template.as_ref().context("missing shard template")?;

        let mut binding_collection_names = Vec::with_capacity(transforms.len());
        let mut binding_journal_read_suffixes = Vec::with_capacity(transforms.len());
        let mut binding_transform_names = Vec::with_capacity(transforms.len());

        for transform in transforms {
            let flow::collection_spec::derivation::Transform {
                name,
                collection,
                journal_read_suffix,
                ..
            } = transform;

            let flow::CollectionSpec {
                name: collection_name,
                ..
            } = collection
                .as_ref()
                .context("missing transform collection")?;

            binding_collection_names.push(collection_name.clone());
            binding_journal_read_suffixes.push(journal_read_suffix.clone());
            binding_transform_names.push(name.clone());
        }

        let min_txn_duration = min_txn_duration
            .context("missing min_txn_duration")?
            .try_into()?;
        let max_txn_duration = max_txn_duration
            .context("missing max_txn_duration")?
            .try_into()?;

        let shard_ref = ops::ShardRef {
            kind: ops::TaskType::Derivation as i32,
            name: spec.name.clone(),
            key_begin: labels::KEY_BEGIN_MIN.to_string(),
            r_clock_begin: labels::RCLOCK_BEGIN_MIN.to_string(),
            build,
        };

        let close_policy = super::close_policy::Policy::new(min_txn_duration, max_txn_duration);

        Ok(Self {
            binding_collection_names,
            binding_journal_read_suffixes,
            binding_transform_names,
            close_policy,
            max_transactions,
            n_shards: peers.len(),
            peers,
            remote_authoritative: false, // Set after the Opened fan-in.
            shard_ref,
        })
    }
}
