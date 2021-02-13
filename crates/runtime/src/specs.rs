use labels::{keys as label_keys, label_set, values as label_values};
use models::tables;
use protocol::consumer;
use std::collections::BTreeMap;
use std::fmt::{Display, Write};

#[derive(Debug)]
pub struct DerivationSet(BTreeMap<String, ()>);

impl std::convert::TryFrom<consumer::ListResponse> for DerivationSet {
    type Error = ();

    // Eventually we'll want to hoist existing shards into a DerivationSet
    // that knows about current Etcd revisions and splits.
    // For now, we assume a single shard and over-write it each time.
    fn try_from(_: consumer::ListResponse) -> Result<Self, Self::Error> {
        Ok(DerivationSet(BTreeMap::new()))
    }
}

fn derivation_shard_id(
    collection: impl Display,
    key_range_begin: impl Display,
    rclock_range_begin: impl Display,
) -> String {
    format!(
        "derivation/{}/{}-{}",
        collection, key_range_begin, rclock_range_begin
    )
}

impl DerivationSet {
    pub fn update_from_catalog(&mut self, derivations: &[tables::BuiltDerivation]) {
        self.0
            .extend(derivations.iter().map(|d| (d.derivation.to_string(), ())));
    }

    pub fn build_shard_apply_request(&self, catalog_url: &str) -> consumer::ApplyRequest {
        let changes = self
            .0
            .iter()
            .map(|(collection, _)| {
                let labels = label_set![
                        label_keys::MANAGED_BY => label_values::FLOW,
                        label_keys::CATALOG_URL => catalog_url,
                        label_keys::DERIVATION => collection.as_str(),
                        label_keys::KEY_BEGIN => label_values::DEFAULT_KEY_BEGIN,
                        label_keys::KEY_END => label_values::DEFAULT_KEY_END,
                        label_keys::RCLOCK_BEGIN => label_values::DEFAULT_RCLOCK_BEGIN,
                        label_keys::RCLOCK_END => label_values::DEFAULT_RCLOCK_END,
                ];

                consumer::apply_request::Change {
                    upsert: Some(consumer::ShardSpec {
                        id: derivation_shard_id(
                            collection,
                            label_values::DEFAULT_KEY_BEGIN,
                            label_values::DEFAULT_RCLOCK_BEGIN,
                        ),
                        sources: Vec::new(),
                        recovery_log_prefix: "recovery".to_owned(),
                        hint_prefix: "/estuary/flow/hints".to_owned(),
                        hint_backups: 2,
                        max_txn_duration: Some(prost_types::Duration {
                            seconds: 1,
                            nanos: 0,
                        }),
                        min_txn_duration: None,
                        disable: false,
                        hot_standbys: 0, // TODO
                        disable_wait_for_ack: false,
                        labels: Some(labels),
                    }),
                    expect_mod_revision: -1, // TODO (always update).
                    delete: String::new(),
                }
            })
            .collect::<Vec<_>>();

        consumer::ApplyRequest {
            changes,
            ..Default::default()
        }
    }
}

fn _hex_key(key: &[u8]) -> String {
    let mut s = String::with_capacity(2 * key.len());
    for byte in key {
        write!(s, "{:02X}", byte).unwrap();
    }
    s
}

fn _hex_rc(rc: u32) -> String {
    format!("{:08x}", rc)
}
