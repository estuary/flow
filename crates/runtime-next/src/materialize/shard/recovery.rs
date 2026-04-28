//! POD codec helpers for the shard's session-startup `Recover` stream.
//!
//! These functions translate persisted RocksDB state (decoded by
//! `crate::recovery::codec`) into the wire `Recover` messages we send to
//! the leader, plus inverse helpers (state-patch reduction, append).
//! IO orchestration lives in `startup.rs`.

use crate::proto;
use bytes::Bytes;
use shuffle::frontier::{Drain, Frontier, JournalFrontier};

/// Build a `shuffle::Frontier` from a `recovery_codec::State`'s
/// `JournalFrontier` accumulator.
fn frontier_from_groups(
    mut groups: Vec<JournalFrontier>,
) -> Result<Frontier, shuffle::frontier::Error> {
    groups.sort_by(|a, b| a.journal.cmp(&b.journal).then(a.binding.cmp(&b.binding)));
    Frontier::new(groups, Vec::new())
}

/// Build the L:Recover stream from a recovered RocksDB `State`. Consumes
/// `state` so the JournalFrontier accumulators move directly into
/// `Frontier::new` without copying.
///
/// Emits frontier chunks for hinted and committed frontiers, a singleton
/// Recover carrying ack_intents/connector_patches/last_applied/
/// max_keys/trigger_params, and an empty terminator.
pub fn recover_stream_from_state(
    state: crate::recovery::codec::State,
) -> Result<Vec<proto::Recover>, shuffle::frontier::Error> {
    let crate::recovery::codec::State {
        hinted,
        committed,
        ack_intents,
        max_keys,
        connector_state,
        trigger_params,
        last_applied,
        unknown: _,
    } = state;

    let mut out = Vec::new();

    drain_into(&mut out, frontier_from_groups(hinted)?, true);
    drain_into(&mut out, frontier_from_groups(committed)?, false);

    // Singleton fields packed into one Recover.
    let mut singleton = proto::Recover::default();
    let mut any = false;
    if !ack_intents.is_empty() {
        singleton.ack_intents = ack_intents;
        any = true;
    }
    if let Some(patches) = connector_state {
        singleton.connector_patches_json = patches;
        any = true;
    }
    if let Some(last_applied) = last_applied {
        singleton.last_applied = last_applied;
        any = true;
    }
    if !max_keys.is_empty() {
        singleton.max_keys = max_keys;
        any = true;
    }
    if let Some(trigger_params) = trigger_params {
        singleton.trigger_params_json = trigger_params;
        any = true;
    }
    if any {
        out.push(singleton);
    }

    out.push(proto::Recover::default()); // Empty terminator.
    Ok(out)
}

fn drain_into(out: &mut Vec<proto::Recover>, frontier: Frontier, hinted: bool) {
    if frontier.journals.is_empty() && frontier.flushed_lsn.is_empty() {
        return;
    }
    let mut drain = Drain::new();
    drain.start(frontier);
    while let Some(chunk) = drain.next_chunk() {
        let mut r = proto::Recover::default();
        if hinted {
            r.hinted_frontier = Some(chunk);
        } else {
            r.committed_frontier = Some(chunk);
        }
        out.push(r);
    }
}

/// Reduce a State Update Wire Format payload into a single JSON document
/// by applying each patch via RFC 7396 merge-patch on top of `{}`.
pub fn reduce_state_patches(payload: &Bytes) -> anyhow::Result<Bytes> {
    use anyhow::Context;

    if payload.is_empty() {
        return Ok(Bytes::new());
    }
    let patches =
        crate::recovery::codec::split_state_patches(payload).context("splitting state patches")?;
    let mut doc: serde_json::Value = serde_json::Value::Object(Default::default());
    for patch in patches {
        let patch_value: serde_json::Value =
            serde_json::from_slice(&patch).context("parsing state patch as JSON")?;
        json_patch::merge(&mut doc, &patch_value);
    }
    Ok(Bytes::from(serde_json::to_vec(&doc)?))
}

/// Append `b` (a State Update Wire Format payload) to `a` (also wire format),
/// producing a single combined payload.
pub fn append_patch(a: &Bytes, b: &Bytes) -> Bytes {
    if a.is_empty() {
        return b.clone();
    }
    if b.is_empty() {
        return a.clone();
    }
    let mut buf = Vec::with_capacity(a.len() + b.len());
    buf.extend_from_slice(a);
    buf.truncate(buf.len() - 1); // strip trailing ']'
    buf.push(b',');
    buf.extend_from_slice(&b[1..]); // strip leading '['
    Bytes::from(buf)
}

/// Decode the build label from a MaterializationSpec's shard template.
pub fn labels_build_for(spec: &proto_flow::flow::MaterializationSpec) -> String {
    let Some(template) = spec.shard_template.as_ref() else {
        return String::new();
    };
    let Some(set) = template.labels.as_ref() else {
        return String::new();
    };
    labels::shard::decode_labeling(set)
        .map(|l| l.build)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto_gazette::uuid::{Clock, Producer};

    /// Producer IDs must have the multicast bit (LSB of byte 0) set; `tag`
    /// goes in byte 1.
    fn producer_id(tag: u8) -> [u8; 6] {
        [0x01, tag, 0, 0, 0, 0]
    }

    fn p(tag: u8, last_commit: u64) -> shuffle::ProducerFrontier {
        shuffle::ProducerFrontier {
            producer: Producer::from_bytes(producer_id(tag)),
            last_commit: Clock::from_unix(last_commit, 0),
            hinted_commit: Clock::default(),
            offset: 0,
        }
    }

    fn jf(
        journal: &str,
        binding: u16,
        producers: Vec<shuffle::ProducerFrontier>,
    ) -> JournalFrontier {
        JournalFrontier {
            journal: journal.into(),
            binding,
            producers,
            bytes_read_delta: 0,
            bytes_behind_delta: 0,
        }
    }

    #[test]
    fn empty_groups_yield_empty_frontier() {
        let f = frontier_from_groups(Vec::new()).unwrap();
        assert!(f.journals.is_empty());
        assert!(f.flushed_lsn.is_empty());
    }

    #[test]
    fn frontier_from_groups_sorts_unsorted_input() {
        // Out-of-order on (journal, binding) — exercises the defensive sort.
        let groups = vec![
            jf("journal/y", 1, vec![p(1, 75)]),
            jf("journal/x", 1, vec![p(1, 50)]),
            jf("journal/x", 0, vec![p(1, 100), p(2, 200)]),
        ];

        let f = frontier_from_groups(groups).unwrap();
        assert_eq!(f.journals.len(), 3);
        assert_eq!(f.journals[0].journal.as_ref(), "journal/x");
        assert_eq!(f.journals[0].binding, 0);
        assert_eq!(f.journals[0].producers.len(), 2);
        assert!(f.journals[0].producers[0].producer < f.journals[0].producers[1].producer);
        assert_eq!(f.journals[1].journal.as_ref(), "journal/x");
        assert_eq!(f.journals[1].binding, 1);
        assert_eq!(f.journals[2].journal.as_ref(), "journal/y");
        assert_eq!(f.journals[2].binding, 1);
    }

    #[test]
    fn recover_stream_emits_chunks_singletons_and_terminator() {
        let mut state = crate::recovery::codec::State::default();
        state.hinted.push(jf("journal/x", 0, vec![p(1, 100)]));
        state.max_keys.insert(0, Bytes::from_static(b"pk1"));
        state.connector_state = Some(Bytes::from_static(b"[{}\n]"));

        let recovers = recover_stream_from_state(state).unwrap();

        // Expect: 1 hinted chunk + 1 terminator-chunk-from-Drain + 1 singleton + 1 empty terminator.
        assert!(recovers.len() >= 3);
        assert!(recovers[0].hinted_frontier.is_some());
        let last = recovers.last().unwrap();
        assert_eq!(*last, proto::Recover::default());
        let has_singleton = recovers
            .iter()
            .any(|r| r.connector_patches_json.as_ref() == b"[{}\n]" && !r.max_keys.is_empty());
        assert!(has_singleton, "singleton fields should appear in stream");
    }

    /// Round-trip: take a RocksDB Persist that writes hinted_frontier, scan
    /// the resulting state, reconstruct via frontier_from_groups — the
    /// entries we get back must match what we put in.
    #[tokio::test]
    async fn persist_scan_state_to_frontier_round_trip() {
        use crate::proto::Persist;

        let keys = vec!["binding-A".to_string(), "binding-B".to_string()];

        let original = Frontier::new(
            vec![
                jf("journal/x", 0, vec![p(1, 100), p(2, 200)]),
                jf("journal/y", 1, vec![p(3, 300)]),
            ],
            Vec::new(),
        )
        .unwrap();

        let mut drain = shuffle::frontier::Drain::with_journals_per_chunk(64);
        drain.start(original.clone());
        let chunk = drain.next_chunk().unwrap();
        assert!(!chunk.journals.is_empty());
        let _terminator = drain.next_chunk().unwrap();

        let db = crate::rocksdb::RocksDB::open(None).await.unwrap();

        let mut wb = rocksdb::WriteBatch::default();
        crate::rocksdb::extend_write_batch(
            &mut wb,
            &Persist {
                nonce: 1,
                hinted_frontier: Some(chunk),
                ..Default::default()
            },
            keys.as_slice(),
        )
        .unwrap();
        let db = db.write_opt(wb, Default::default()).await.unwrap();

        // Sorted (state_key, binding_index) mapping for the scan.
        let mapping: Vec<(String, u32)> = vec![("binding-A".into(), 0), ("binding-B".into(), 1)];
        let (_db, state) = db.scan(mapping).await.unwrap();
        let rebuilt = frontier_from_groups(state.hinted).unwrap();

        assert_eq!(rebuilt.journals.len(), original.journals.len());
        for (got, want) in rebuilt.journals.iter().zip(original.journals.iter()) {
            assert_eq!(got.journal, want.journal);
            assert_eq!(got.binding, want.binding);
            assert_eq!(got.producers.len(), want.producers.len());
            for (gp, wp) in got.producers.iter().zip(want.producers.iter()) {
                assert_eq!(gp.producer, wp.producer);
                assert_eq!(gp.last_commit, wp.last_commit);
                assert_eq!(gp.hinted_commit, wp.hinted_commit);
            }
        }
    }
}
