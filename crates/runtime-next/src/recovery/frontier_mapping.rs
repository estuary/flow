//! Mapping between legacy `consumer.Checkpoint` and `shuffle::Frontier`.
//!
//! Single source of truth shared by forward migration (legacy → Frontier)
//! and rollback (Frontier → legacy). Both directions must round-trip
//! byte-identically for unchanged fields.
//!
//! In either direction, a source/journal whose binding cannot be resolved
//! against `binding_suffixes` is dropped — it represents a binding that
//! has been removed or backfilled since the other side was written.
//!
//! `ProducerFrontier.offset` packs a journal position with a sign:
//! non-negative is the begin offset of the first pending (uncommitted)
//! `CONTINUE_TXN` from this producer (legacy `ProducerState.begin`);
//! negative is the negation of the end offset of the last committing
//! `ACK_TXN` / `OUTSIDE_TXN` (legacy `begin == -1` combined with
//! `Source.read_through`). On the reverse mapping,
//! `read_through = max|offset|` across producers.
//!
//! The reverse mapping is **lossy** when a producer has
//! `0 <= begin < read_through`: the Frontier forgets read-ahead bytes
//! past `begin`. Round-trip fidelity requires either a peer producer
//! with `begin = -1` (whose `-read_through` carries the full
//! `read_through`) or `begin == read_through`.
//!
//! ACK intents are not handled here — they move out of `Checkpoint` into
//! their own persistence range; callers attach/detach them directly.

use proto_gazette::consumer;
use proto_gazette::uuid::{Clock, Producer};
use shuffle::{Frontier, JournalFrontier, ProducerFrontier, frontier};

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
    FrontierValidation(#[from] frontier::Error),
}

/// Convert `Checkpoint.sources` to `Frontier` journal entries.
///
/// `binding_suffixes` indexes binding index → `journal_read_suffix`.
/// Source keys without a `;` separator are an error. The returned
/// `Frontier` has empty `flushed_lsn` — the legacy checkpoint has no
/// analogous per-shard log barrier.
pub fn checkpoint_to_frontier(
    sources: &std::collections::HashMap<String, consumer::checkpoint::Source>,
    binding_suffixes: &[&str],
) -> Result<Frontier, Error> {
    let mut journals: Vec<JournalFrontier> = Vec::with_capacity(sources.len());

    for (source_key, source) in sources {
        let Some((journal, suffix)) = source_key.split_once(';') else {
            return Err(Error::MissingSourceKeySuffix {
                source_key: source_key.clone(),
            });
        };

        let Some(binding) = binding_suffixes
            .iter()
            .position(|s| *s == suffix)
            .map(|i| i as u16)
        else {
            continue;
        };

        let mut producers: Vec<ProducerFrontier> = Vec::with_capacity(source.producers.len());
        for entry in &source.producers {
            let id: [u8; 6] =
                entry
                    .id
                    .as_slice()
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

            producers.push(ProducerFrontier {
                producer: Producer(id),
                last_commit: Clock::from_u64(state.last_ack),
                hinted_commit: Clock::zero(),
                offset,
            });
        }
        producers.sort_by_key(|p| p.producer);

        journals.push(JournalFrontier {
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

    Ok(Frontier::new(journals, vec![])?)
}

/// Convert `Frontier` journal entries back to `Checkpoint.sources`,
/// keyed by `"{journal};{suffix}"`.
pub fn frontier_to_checkpoint(
    frontier: &Frontier,
    binding_suffixes: &[&str],
) -> std::collections::HashMap<String, consumer::checkpoint::Source> {
    let mut sources = std::collections::HashMap::with_capacity(frontier.journals.len());

    for jf in &frontier.journals {
        let Some(suffix) = binding_suffixes.get(jf.binding as usize).copied() else {
            continue;
        };
        let source_key = format!("{};{suffix}", jf.journal);

        let read_through = jf
            .producers
            .iter()
            .map(|p| p.offset.unsigned_abs())
            .max()
            .unwrap_or(0) as i64;

        let producers = jf
            .producers
            .iter()
            .map(|p| consumer::checkpoint::source::ProducerEntry {
                id: p.producer.as_bytes().to_vec(),
                state: Some(consumer::checkpoint::ProducerState {
                    last_ack: p.last_commit.as_u64(),
                    begin: if p.offset >= 0 { p.offset } else { -1 },
                }),
            })
            .collect();

        sources.insert(
            source_key,
            consumer::checkpoint::Source {
                read_through,
                producers,
            },
        );
    }

    sources
}

/// Project a verbatim `FH:` Frontier into hinted form: each producer's
/// `last_commit` is promoted to `hinted_commit`, and `last_commit` /
/// `offset` are zeroed. The result is reduced with the recovered
/// committed Frontier when composing a session's resume Frontier.
pub fn project_hinted(mut frontier: Frontier) -> Frontier {
    for jf in &mut frontier.journals {
        for pf in &mut jf.producers {
            pf.hinted_commit = pf.last_commit;
            pf.last_commit = Clock::zero();
            pf.offset = 0;
        }
    }
    frontier
}

#[cfg(test)]
mod test {
    use super::*;
    use consumer::checkpoint::{ProducerState, Source, source::ProducerEntry};
    use std::collections::HashMap;

    // Build a 6-byte producer id. The low bit of byte 0 is the RFC 4122
    // multicast bit, required by `Producer::from_bytes`.
    fn producer(tag: u8) -> [u8; 6] {
        [0x01, tag, 0x00, 0x00, 0x00, 0x00]
    }

    // Clock values are debug-printed as "{unix_s}s {nanos}ns". Building from
    // from_unix() keeps snapshots stable and human-readable; raw u64 values
    // lose precision in to_unix().
    fn clock(unix_s: u64) -> u64 {
        Clock::from_unix(unix_s, 0).as_u64()
    }

    fn source(read_through: i64, producers: Vec<ProducerEntry>) -> Source {
        Source {
            read_through,
            producers,
        }
    }

    fn entry(id: [u8; 6], unix_s: u64, begin: i64) -> ProducerEntry {
        ProducerEntry {
            id: id.to_vec(),
            state: Some(ProducerState {
                last_ack: clock(unix_s),
                begin,
            }),
        }
    }

    // Compact, sorted formatting of `Checkpoint.sources` for snapshots.
    // The default `Source` debug prints producer IDs as raw byte arrays,
    // which dwarfs the actual signal under test. We surface only byte 1
    // of each producer id (set by `producer()` as the tag) and last_ack
    // in unix seconds.
    fn dump_sources(m: &HashMap<String, Source>) -> Vec<String> {
        let mut v: Vec<_> = m
            .iter()
            .map(|(k, s)| {
                let producers: Vec<_> = s
                    .producers
                    .iter()
                    .map(|p| {
                        let st = p.state.as_ref().unwrap();
                        format!(
                            "id={:02x} last_ack={}s begin={}",
                            p.id[1],
                            Clock::from_u64(st.last_ack).to_unix().0,
                            st.begin,
                        )
                    })
                    .collect();
                format!(
                    "{k} read_through={} [{}]",
                    s.read_through,
                    producers.join(", "),
                )
            })
            .collect();
        v.sort();
        v
    }

    #[test]
    fn checkpoint_to_frontier_basic() {
        let mut sources = HashMap::new();
        sources.insert(
            "foo/000;derive/der/t1".to_string(),
            source(
                200,
                vec![
                    entry(producer(0xaa), 100, 150), // uncommitted-begin
                    entry(producer(0xbb), 90, -1),   // no pending → -read_through
                ],
            ),
        );
        sources.insert(
            "foo/001;derive/der/t2".to_string(),
            source(50, vec![entry(producer(0xcc), 50, -1)]),
        );
        // Dropped: suffix not in binding list.
        sources.insert(
            "foo/001;derive/der/gone".to_string(),
            source(10, vec![entry(producer(0xdd), 1, -1)]),
        );
        // Retained as an empty-producer JournalFrontier (source-level
        // read_through is dropped, but the (journal, binding) presence
        // flows through).
        sources.insert("foo/003;derive/der/t1".to_string(), source(10, vec![]));

        let frontier =
            checkpoint_to_frontier(&sources, &["derive/der/t1", "derive/der/t2"]).unwrap();

        insta::assert_debug_snapshot!(frontier, @r#"
        Frontier {
            journals: [
                JournalFrontier {
                    journal: "foo/000",
                    binding: 0,
                    producers: [
                        ProducerFrontier {
                            producer: Producer(01:aa:00:00:00:00),
                            last_commit: Clock(100s 0ns),
                            hinted_commit: Clock(0s 0ns),
                            offset: 150,
                        },
                        ProducerFrontier {
                            producer: Producer(01:bb:00:00:00:00),
                            last_commit: Clock(90s 0ns),
                            hinted_commit: Clock(0s 0ns),
                            offset: -200,
                        },
                    ],
                    bytes_read_delta: 0,
                    bytes_behind_delta: 0,
                },
                JournalFrontier {
                    journal: "foo/001",
                    binding: 1,
                    producers: [
                        ProducerFrontier {
                            producer: Producer(01:cc:00:00:00:00),
                            last_commit: Clock(50s 0ns),
                            hinted_commit: Clock(0s 0ns),
                            offset: -50,
                        },
                    ],
                    bytes_read_delta: 0,
                    bytes_behind_delta: 0,
                },
                JournalFrontier {
                    journal: "foo/003",
                    binding: 0,
                    producers: [],
                    bytes_read_delta: 0,
                    bytes_behind_delta: 0,
                },
            ],
            flushed_lsn: [],
            unresolved_hints: 0,
        }
        "#);
    }

    #[test]
    fn frontier_to_checkpoint_basic() {
        // Covers: mixed sign offsets; out-of-range binding drop; silent
        // drop of frontier-only fields with no legacy slot
        // (`hinted_commit`, `bytes_*_delta`, `flushed_lsn`).
        let frontier = Frontier::new(
            vec![
                JournalFrontier {
                    journal: "foo/000".into(),
                    binding: 0,
                    producers: vec![
                        ProducerFrontier {
                            producer: Producer(producer(0xaa)),
                            last_commit: Clock::from_unix(100, 0),
                            hinted_commit: Clock::from_unix(999, 0), // dropped
                            offset: 150,                             // uncommitted-begin
                        },
                        ProducerFrontier {
                            producer: Producer(producer(0xbb)),
                            last_commit: Clock::from_unix(90, 0),
                            hinted_commit: Clock::zero(),
                            offset: -200, // committed-end
                        },
                    ],
                    bytes_read_delta: 12345,   // dropped
                    bytes_behind_delta: 67890, // dropped
                },
                JournalFrontier {
                    journal: "foo/001".into(),
                    binding: 0,
                    producers: vec![ProducerFrontier {
                        producer: Producer(producer(0xcc)),
                        last_commit: Clock::from_unix(50, 0),
                        hinted_commit: Clock::zero(),
                        offset: -50,
                    }],
                    bytes_read_delta: 0,
                    bytes_behind_delta: 0,
                },
                JournalFrontier {
                    // Dropped: binding index out of range.
                    journal: "foo/002".into(),
                    binding: 99,
                    producers: vec![ProducerFrontier {
                        producer: Producer(producer(0xdd)),
                        last_commit: Clock::from_unix(1, 0),
                        hinted_commit: Clock::zero(),
                        offset: -10,
                    }],
                    bytes_read_delta: 0,
                    bytes_behind_delta: 0,
                },
            ],
            vec![7777], // flushed_lsn — dropped
        )
        .unwrap();

        insta::assert_debug_snapshot!(
            dump_sources(&frontier_to_checkpoint(&frontier, &["materialize/mat/r1"])),
            @r#"
        [
            "foo/000;materialize/mat/r1 read_through=200 [id=aa last_ack=100s begin=150, id=bb last_ack=90s begin=-1]",
            "foo/001;materialize/mat/r1 read_through=50 [id=cc last_ack=50s begin=-1]",
        ]
        "#
        );
    }

    #[test]
    fn round_trip() {
        // Cases avoid the lossy condition (see module docs): each positive
        // `begin` is paired with a peer `begin = -1`, or has
        // `begin == read_through`.
        let mut sources = HashMap::new();
        sources.insert(
            "stream/a;materialize/mat/r1".to_string(),
            source(
                500,
                vec![
                    entry(producer(0x01), 7777, 400),
                    entry(producer(0x03), 6666, -1),
                ],
            ),
        );
        sources.insert(
            "stream/a;materialize/mat/r2".to_string(),
            source(1100, vec![entry(producer(0x05), 8888, 1100)]),
        );
        sources.insert(
            "stream/b;materialize/mat/r1".to_string(),
            source(300, vec![entry(producer(0x07), 9999, -1)]),
        );

        let binding_suffixes = &["materialize/mat/r1", "materialize/mat/r2"];
        let frontier = checkpoint_to_frontier(&sources, binding_suffixes).unwrap();
        let round_tripped = frontier_to_checkpoint(&frontier, binding_suffixes);

        let normalize = |m: HashMap<String, Source>| -> Vec<(String, Source)> {
            let mut v: Vec<_> = m
                .into_iter()
                .map(|(k, mut s)| {
                    s.producers.sort_by(|a, b| a.id.cmp(&b.id));
                    (k, s)
                })
                .collect();
            v.sort_by(|a, b| a.0.cmp(&b.0));
            v
        };

        assert_eq!(normalize(sources), normalize(round_tripped));
    }

    #[test]
    fn checkpoint_to_frontier_errors() {
        let cases: Vec<(&str, HashMap<String, Source>)> = vec![
            (
                "missing suffix separator",
                HashMap::from([(
                    "foo/002".to_string(),
                    source(10, vec![entry(producer(0xee), 1, -1)]),
                )]),
            ),
            (
                "producer id wrong length",
                HashMap::from([(
                    "foo;derive/der/t1".to_string(),
                    Source {
                        read_through: 10,
                        producers: vec![ProducerEntry {
                            id: vec![0x01, 0x02, 0x03],
                            state: Some(ProducerState {
                                last_ack: 1,
                                begin: -1,
                            }),
                        }],
                    },
                )]),
            ),
            (
                "begin below -1",
                HashMap::from([(
                    "foo;derive/der/t1".to_string(),
                    source(10, vec![entry(producer(0xaa), 1, -5)]),
                )]),
            ),
        ];

        let results: Vec<String> = cases
            .iter()
            .map(|(name, sources)| {
                let err = checkpoint_to_frontier(sources, &["derive/der/t1"]).unwrap_err();
                format!("{name}: {err}")
            })
            .collect();

        insta::assert_debug_snapshot!(results, @r#"
        [
            "missing suffix separator: Checkpoint source key \"foo/002\" has no ';' suffix separator",
            "producer id wrong length: Checkpoint producer id for source \"foo;derive/der/t1\" is 3 bytes, want 6",
            "begin below -1: Checkpoint producer begin for source \"foo;derive/der/t1\" is -5 (must be >= -1)",
        ]
        "#);
    }

    #[test]
    fn project_hinted_basic() {
        let frontier = Frontier::new(
            vec![JournalFrontier {
                journal: "foo/000".into(),
                binding: 0,
                producers: vec![
                    ProducerFrontier {
                        producer: Producer(producer(0xaa)),
                        last_commit: Clock::from_unix(100, 0),
                        hinted_commit: Clock::zero(),
                        offset: 150,
                    },
                    ProducerFrontier {
                        producer: Producer(producer(0xbb)),
                        last_commit: Clock::from_unix(90, 0),
                        hinted_commit: Clock::from_unix(85, 0), // overwritten
                        offset: -200,
                    },
                ],
                bytes_read_delta: 0,
                bytes_behind_delta: 0,
            }],
            vec![],
        )
        .unwrap();

        insta::assert_debug_snapshot!(project_hinted(frontier), @r#"
        Frontier {
            journals: [
                JournalFrontier {
                    journal: "foo/000",
                    binding: 0,
                    producers: [
                        ProducerFrontier {
                            producer: Producer(01:aa:00:00:00:00),
                            last_commit: Clock(0s 0ns),
                            hinted_commit: Clock(100s 0ns),
                            offset: 0,
                        },
                        ProducerFrontier {
                            producer: Producer(01:bb:00:00:00:00),
                            last_commit: Clock(0s 0ns),
                            hinted_commit: Clock(90s 0ns),
                            offset: 0,
                        },
                    ],
                    bytes_read_delta: 0,
                    bytes_behind_delta: 0,
                },
            ],
            flushed_lsn: [],
            unresolved_hints: 0,
        }
        "#);
    }

    #[test]
    fn round_trip_lossy_without_committed_peer() {
        // Single producer with `begin < read_through`: there is no peer
        // carrying `-read_through`, so the Frontier has no slot for the
        // 100 bytes read past `begin`. The reverse mapping returns
        // `read_through == begin == 400`, losing the original 500.
        let mut sources = HashMap::new();
        sources.insert(
            "j;s".to_string(),
            source(500, vec![entry(producer(0x01), 1, 400)]),
        );

        let frontier = checkpoint_to_frontier(&sources, &["s"]).unwrap();
        let back = frontier_to_checkpoint(&frontier, &["s"]);

        let s = &back["j;s"];
        assert_eq!(s.read_through, 400);
        assert_eq!(s.producers[0].state.as_ref().unwrap().begin, 400);
    }
}
