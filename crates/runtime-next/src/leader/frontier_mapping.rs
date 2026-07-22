//! Mapping between legacy `consumer.Checkpoint` and `shuffle::Frontier`,
//! plus the `Persist` that adopts an authoritative checkpoint wholesale
//! during startup reconciliation.
use proto_gazette::consumer;
use proto_gazette::uuid;
use std::collections::BTreeMap;

/// Errors produced by `checkpoint_to_frontier`.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Checkpoint source key {source_key:?} has no ';' suffix separator")]
    MissingSourceKeySuffix { source_key: String },
    #[error("Checkpoint producer id for source {source_key:?} is {got} bytes, want 6")]
    InvalidProducerIdLength { source_key: String, got: usize },
    #[error("Checkpoint producer begin for source {source_key:?} is {begin} (must be >= -1)")]
    InvalidProducerBegin { source_key: String, begin: i64 },
    #[error("Frontier validation failed")]
    FrontierValidation(#[from] shuffle::frontier::Error),
}

/// Convert `Checkpoint.sources` to `shuffle::Frontier` journal entries.
///
/// `binding_suffixes` indexes binding index → `journal_read_suffix`.
/// Source keys without a `;` separator are an error. The returned
/// `Frontier` has empty `flushed_lsn` — the legacy checkpoint has no
/// analogous per-shard log barrier.
pub fn checkpoint_to_frontier(
    sources: &BTreeMap<String, consumer::checkpoint::Source>,
    journal_read_suffix_index: &[(&str, usize)],
) -> Result<shuffle::Frontier, Error> {
    let mut journals: Vec<shuffle::JournalFrontier> = Vec::with_capacity(sources.len());

    for (source_key, source) in sources {
        // Skip the synthetic committed-close source (`encode_committed_close`):
        // it's not a journal source, and connectors round-trip it verbatim
        // within their stored checkpoints.
        if source_key.as_bytes() == crate::shard::recovery::KEY_COMMITTED_CLOSE {
            continue;
        }
        let Some((journal, suffix)) = source_key.split_once(';') else {
            return Err(Error::MissingSourceKeySuffix {
                source_key: source_key.clone(),
            });
        };

        let Ok(index) =
            journal_read_suffix_index.binary_search_by(|(cursor, _)| (*cursor).cmp(suffix))
        else {
            continue;
        };
        let binding = journal_read_suffix_index[index].1 as u16;

        let mut producers: Vec<shuffle::ProducerFrontier> =
            Vec::with_capacity(source.producers.len());
        for entry in &source.producers {
            let id: [u8; 6] =
                entry
                    .id
                    .as_ref()
                    .try_into()
                    .map_err(|_| Error::InvalidProducerIdLength {
                        source_key: source_key.clone(),
                        got: entry.id.len(),
                    })?;
            let state = entry.state.unwrap_or_default();

            let offset = if state.begin >= 0 {
                state.begin
            } else if state.begin == -1 {
                -source.read_through
            } else {
                return Err(Error::InvalidProducerBegin {
                    source_key: source_key.clone(),
                    begin: state.begin,
                });
            };

            producers.push(shuffle::ProducerFrontier {
                producer: uuid::Producer(id),
                last_commit: uuid::Clock::from_u64(state.last_ack),
                hinted_commit: uuid::Clock::zero(),
                offset,
            });
        }
        producers.sort_by_key(|p| p.producer);

        journals.push(shuffle::JournalFrontier {
            journal: journal.into(),
            binding,
            producers,
            bytes_read_delta: 0,
            bytes_behind_delta: 0,
        });
    }

    journals.sort_by(|a, b| {
        a.journal
            .as_ref()
            .cmp(b.journal.as_ref())
            .then(a.binding.cmp(&b.binding))
    });

    Ok(shuffle::Frontier::new(journals, vec![])?)
}

/// Merge `Frontier` journal entries into `Checkpoint.sources`, keyed by
/// `"{journal};{suffix}"`, while minimizing re-allocation. Producer entries of
/// both `frontier` and `checkpoint` are expected to already be sorted on
/// producer ID. Any journals or producers in `checkpoint.sources` which are not
/// in `frontier` are left alone.
pub fn merge_frontier_into_checkpoint<S: AsRef<str>>(
    frontier: &shuffle::Frontier,
    checkpoint: &mut consumer::Checkpoint,
    binding_journal_read_suffixes: &[S],
) {
    let mut scratch = Vec::new();

    for jf in &frontier.journals {
        let Some(suffix) = binding_journal_read_suffixes.get(jf.binding as usize) else {
            continue; // Reachable only if shuffle service provides bad binding indices.
        };
        let source_key = format!("{};{}", jf.journal, suffix.as_ref());

        let mut source = checkpoint.sources.remove(&source_key).unwrap_or_default();
        scratch.append(&mut source.producers); // Move via copy; source.producers is now empty.

        let mut existing = scratch.drain(..).peekable();
        let mut frontier = jf.producers.iter().peekable();

        fn pf_to_pe(
            pf: &shuffle::ProducerFrontier,
            read_through: &mut i64,
        ) -> consumer::checkpoint::source::ProducerEntry {
            consumer::checkpoint::source::ProducerEntry {
                id: bytes::Bytes::copy_from_slice(pf.producer.as_bytes()),
                state: Some(pf_to_ps(pf, read_through)),
            }
        }

        fn update_pe(
            entry: &mut consumer::checkpoint::source::ProducerEntry,
            pf: &shuffle::ProducerFrontier,
            read_through: &mut i64,
        ) {
            entry.state = Some(pf_to_ps(pf, read_through));
        }

        fn pf_to_ps(
            pf: &shuffle::ProducerFrontier,
            read_through: &mut i64,
        ) -> consumer::checkpoint::ProducerState {
            let offset = pf.offset.checked_abs().unwrap_or_default();
            *read_through = (*read_through).max(offset);

            consumer::checkpoint::ProducerState {
                last_ack: pf.last_commit.as_u64(),
                begin: if pf.offset >= 0 { pf.offset } else { -1 },
            }
        }

        loop {
            match (existing.peek(), frontier.peek()) {
                (Some(a), Some(b)) => match a.id.as_ref().cmp(b.producer.as_bytes()) {
                    std::cmp::Ordering::Less => source.producers.push(existing.next().unwrap()),
                    std::cmp::Ordering::Greater => {
                        source
                            .producers
                            .push(pf_to_pe(frontier.next().unwrap(), &mut source.read_through));
                    }
                    std::cmp::Ordering::Equal => {
                        let mut entry = existing.next().unwrap();
                        let pf = frontier.next().unwrap();
                        update_pe(&mut entry, pf, &mut source.read_through);
                        source.producers.push(entry);
                    }
                },
                (Some(_), None) => {
                    source.producers.extend(existing);
                    break;
                }
                (None, Some(_)) => {
                    for pf in frontier {
                        let entry = pf_to_pe(pf, &mut source.read_through);
                        source.producers.push(entry);
                    }
                    break;
                }
                (None, None) => break,
            }
        }

        checkpoint.sources.insert(source_key, source);
    }
}

/// Project a verbatim `FH:` Frontier into hinted form: each producer's
/// `last_commit` is promoted to `hinted_commit`, and `last_commit` /
/// `offset` are zeroed. The result is reduced with the recovered
/// committed Frontier when composing a session's resume Frontier.
pub fn project_hinted(mut frontier: shuffle::Frontier) -> shuffle::Frontier {
    for jf in &mut frontier.journals {
        for pf in &mut jf.producers {
            pf.hinted_commit = pf.last_commit;
            pf.last_commit = uuid::Clock::zero();
            pf.offset = 0;
        }
    }
    frontier
}

/// Synthetic producer ID under which `encode_committed_close` records the
/// close Clock. Coexisting with real producer IDs in the same source is
/// safe in practice: real IDs are randomly drawn 6-byte values, and a
/// collision is vanishingly unlikely. This is a hack — but an acceptable one.
const COMMITTED_CLOSE_PRODUCER_ID: &[u8; 6] = b"\x01\x00\x00\x00\x00\x00";

/// Encode the close Clock of a committing transaction for inclusion in
/// consumer::Checkpoint::sources.
///
/// This is a re-purpose of the Checkpoint structure (pervasive among Estuary
/// connectors and the Gazette ecosystem), using it to convey a single close
/// Clock which can later be inspected to determine a commit outcome.
pub fn encode_committed_close(clock: uuid::Clock) -> (String, consumer::checkpoint::Source) {
    let key = str::from_utf8(crate::shard::recovery::KEY_COMMITTED_CLOSE).unwrap();

    (
        key.to_string(),
        consumer::checkpoint::Source {
            read_through: 0,
            producers: vec![consumer::checkpoint::source::ProducerEntry {
                id: bytes::Bytes::from_static(COMMITTED_CLOSE_PRODUCER_ID),
                state: Some(consumer::checkpoint::ProducerState {
                    last_ack: clock.as_u64(),
                    begin: 0,
                }),
            }],
        },
    )
}

/// Extract a close Clock of a committed transaction from a consumer::Checkpoint, if present.
/// This is the inverse of `encode_committed_close`.
pub fn extract_committed_close(checkpoint: &consumer::Checkpoint) -> Option<uuid::Clock> {
    let key = str::from_utf8(crate::shard::recovery::KEY_COMMITTED_CLOSE).unwrap();

    let Some(source) = checkpoint.sources.get(key) else {
        return None;
    };
    let Some(producer) = source.producers.get(0) else {
        return None;
    };
    let Some(state) = &producer.state else {
        return None;
    };
    Some(uuid::Clock::from_u64(state.last_ack))
}

/// Committed-close Clock seeded when reconciliation adopts an authoritative
/// checkpoint carrying no embedded close Clock (one last written by the V1
/// runtime). It's shuffle's `OBSERVED_COMMIT_FLOOR`: greater than zero (a
/// never-reconciled RocksDB) yet strictly below every real Clock, so a
/// committed-close equal to this floor durably means "a consistent V2
/// baseline was adopted, and no V2 transaction has committed atop it yet".
/// Because each `Persist` applies as one atomic WriteBatch, the committed
/// close doubles as the marker proving all of an adoption's effects landed.
pub const COMMITTED_CLOSE_FLOOR: uuid::Clock = uuid::Clock::from_u64(1);

/// Build the `Persist` that adopts an authoritative `checkpoint` wholesale:
/// its mapped sources replace the committed Frontier, its ACK intents replace
/// `AI:`, and `close` is stamped as the committed close Clock.
///
/// When `maintain_rollback`, the legacy checkpoint is replaced by `checkpoint`
/// with `close` embedded — preserving the invariant that a V2-written legacy
/// checkpoint always matches committed-close. Otherwise any legacy checkpoint
/// is deleted (vacuously, when none exists).
///
/// When `discard_hints` (materializations), hinted state is discarded rather
/// than replayed. A hint is a read-ahead of the committed Frontier:
/// `project_hinted` zeroes each hinted producer's read offset and relies on
/// that producer's committed entry to restore it. An adopted checkpoint is a
/// fresh mapping that won't carry producers the checkpoint has dropped (e.g.
/// the V1 runtime prunes a producer that's been silent for >24h), and an
/// orphaned hint would resolve to offset zero, forcing a full journal re-read
/// to replay a hint whose data the checkpoint already reflects. A `Persist`
/// cannot express clearing the hinted-close Clock, so it's overwritten with
/// `close`; a hinted close equal to committed-close is inert.
///
/// The mapped Frontier is deliberately NOT pruned: the shard's recovery scan
/// is the sole prune point, and it both drops stale producers from its
/// `Recover` and deletes them from RocksDB — so any stale entries this
/// adoption persists live for exactly one persist/re-scan round-trip, and the
/// leader's converged baseline never contains them. Reconciliation termination
/// doesn't depend on pruning: adoption steps trigger on the committed-close
/// Clock, never on Frontier content.
pub fn adopt_checkpoint(
    checkpoint: &consumer::Checkpoint,
    close: uuid::Clock,
    maintain_rollback: bool,
    discard_hints: bool,
    journal_read_suffix_index: &[(&str, usize)],
) -> Result<crate::proto::Persist, Error> {
    let frontier = checkpoint_to_frontier(&checkpoint.sources, journal_read_suffix_index)?;

    let legacy_checkpoint = maintain_rollback.then(|| {
        let mut refresh = checkpoint.clone();
        let (key, source) = encode_committed_close(close);
        refresh.sources.insert(key, source);
        refresh
    });

    Ok(crate::proto::Persist {
        committed_close_clock: close.as_u64(),
        delete_committed_frontier: true,
        committed_frontier: Some(shuffle::JournalFrontier::encode(&frontier.journals)),
        delete_ack_intents: true,
        ack_intents: checkpoint.ack_intents.clone(),
        hinted_close_clock: if discard_hints { close.as_u64() } else { 0 },
        delete_hinted_frontier: discard_hints,
        delete_legacy_checkpoint: !maintain_rollback,
        legacy_checkpoint,
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::leader::fixtures::{journal_frontier, pf, producer_entry, source};
    use bytes::Bytes;

    // Checkpoint mapping never reads `hinted_commit`, so these fixtures fix it
    // at zero and take `last_commit` as a bare u64.
    fn producer_frontier(tag: u8, last_commit: u64, offset: i64) -> shuffle::ProducerFrontier {
        pf(
            tag,
            uuid::Clock::from_u64(last_commit),
            uuid::Clock::zero(),
            offset,
        )
    }

    #[test]
    fn merge_frontier_preserves_sources_and_sorted_merges_producers() {
        let mut checkpoint = consumer::Checkpoint::default();
        let mut source_a_producers = Vec::with_capacity(4);
        source_a_producers.push(producer_entry(0x10, 1, 11));
        source_a_producers.push(producer_entry(0x30, 3, -1));
        let source_a_vec_ptr = source_a_producers.as_ptr();
        let producer_30_id_ptr = source_a_producers[1].id.as_ptr();

        checkpoint.sources.insert(
            "journal/a;suffix-a".to_string(),
            source(90, source_a_producers),
        );
        checkpoint.sources.insert(
            "unrelated;suffix-z".to_string(),
            source(7, vec![producer_entry(0x50, 5, -1)]),
        );
        checkpoint
            .ack_intents
            .insert("ack/journal".to_string(), Bytes::from_static(b"ack"));

        let frontier = shuffle::Frontier::new(
            vec![
                journal_frontier(
                    "journal/a",
                    0,
                    vec![
                        producer_frontier(0x20, 20, 70),
                        producer_frontier(0x30, 30, -150),
                    ],
                ),
                journal_frontier("journal/b", 1, vec![producer_frontier(0x40, 40, -10)]),
            ],
            vec![],
        )
        .unwrap();

        merge_frontier_into_checkpoint(
            &frontier,
            &mut checkpoint,
            &["suffix-a".to_string(), "suffix-b".to_string()],
        );

        assert_eq!(
            checkpoint.ack_intents.get("ack/journal").unwrap().as_ref(),
            b"ack"
        );
        assert!(checkpoint.sources.contains_key("unrelated;suffix-z"));

        let source_a = checkpoint.sources.get("journal/a;suffix-a").unwrap();
        assert_eq!(source_a.producers.as_ptr(), source_a_vec_ptr);
        assert_eq!(source_a.read_through, 150);
        let ids: Vec<_> = source_a
            .producers
            .iter()
            .map(|p| p.id.as_ref()[1])
            .collect();
        assert_eq!(ids, vec![0x10, 0x20, 0x30]);
        assert_eq!(source_a.producers[0].state.unwrap().last_ack, 1);
        assert_eq!(source_a.producers[1].state.unwrap().last_ack, 20);
        assert_eq!(source_a.producers[1].state.unwrap().begin, 70);
        assert_eq!(source_a.producers[2].id.as_ptr(), producer_30_id_ptr);
        assert_eq!(source_a.producers[2].state.unwrap().last_ack, 30);
        assert_eq!(source_a.producers[2].state.unwrap().begin, -1);

        let source_b = checkpoint.sources.get("journal/b;suffix-b").unwrap();
        assert_eq!(source_b.read_through, 10);
        assert_eq!(source_b.producers[0].state.unwrap().last_ack, 40);
        assert_eq!(source_b.producers[0].state.unwrap().begin, -1);
    }

    #[test]
    fn merge_frontier_round_trips_through_checkpoint_mapping() {
        let frontier = shuffle::Frontier::new(
            vec![
                journal_frontier("journal/a", 0, vec![producer_frontier(0x10, 10, 25)]),
                journal_frontier("journal/b", 1, vec![producer_frontier(0x20, 20, -50)]),
            ],
            vec![],
        )
        .unwrap();
        let mut checkpoint = consumer::Checkpoint::default();

        merge_frontier_into_checkpoint(
            &frontier,
            &mut checkpoint,
            &["suffix-a".to_string(), "suffix-b".to_string()],
        );

        let recovered =
            checkpoint_to_frontier(&checkpoint.sources, &[("suffix-a", 0), ("suffix-b", 1)])
                .unwrap();

        assert_eq!(recovered.journals.len(), frontier.journals.len());
        for (got, want) in recovered.journals.iter().zip(frontier.journals.iter()) {
            assert_eq!(got.journal, want.journal);
            assert_eq!(got.binding, want.binding);
            assert_eq!(got.producers.len(), want.producers.len());

            for (got, want) in got.producers.iter().zip(want.producers.iter()) {
                assert_eq!(got.producer, want.producer);
                assert_eq!(got.last_commit, want.last_commit);
                assert_eq!(got.hinted_commit, want.hinted_commit);
                assert_eq!(got.offset, want.offset);
            }
        }
    }
}
