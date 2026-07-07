//! Shuffle-log segment writer and the channel-fed [`ShuffleSessionFactory`].
//!
//! This is the shared, durable-before-frontier writer feeding both `flowctl
//! preview --fixture` and the catalog-test runner. Given a parsed transaction
//! (documents tagged with their source collection), it writes the transaction's
//! documents directly as [`shuffle::log`] segments — stamping a synthetic UUID
//! from a monotonic [`uuid::Clock`], validating against the source schema,
//! packing the shuffle key, and building a checkpoint [`shuffle::Frontier`]
//! whose per-(journal, binding) producer `last_commit` is the transaction's max
//! clock, so all of its documents become visible at the checkpoint.
//!
//! The matching [`FixtureOpener`] is a channel-fed [`ShuffleSessionFactory`]:
//! it hands the runtime-next leader one relayed [`shuffle::Frontier`] per
//! checkpoint request, so the consumer reads these documents exactly as if they
//! came from live journals. No `shuffle::Service` is constructed — the shuffle
//! crate is unaware of the source.
//!
//! Publication clocks and per-(journal, binding) committed offsets advance
//! globally across sessions so prior documents are never re-admitted; segment
//! LSNs restart per session (each session gets its own log directory, matching
//! the runtime's ephemeral per-session `Reader`).

use anyhow::Context;
use proto_gazette::uuid;
use runtime_next::{ShuffleSession, ShuffleSessionFactory};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};

/// Fixed synthetic producer for all fixture / test documents (matches legacy
/// `flowctl preview`).
pub const FIXTURE_PRODUCER: uuid::Producer = uuid::Producer([7, 19, 83, 3, 3, 17]);

/// A parsed transaction: documents and their source collection names.
pub type Transaction = Vec<(String, serde_json::Value)>;

/// One queued item of a fixture-replay opener's channel.
pub enum FixtureItem {
    /// A synthetic checkpoint Frontier, relayed to the consumer one per
    /// `NextCheckpoint` request.
    Frontier(shuffle::Frontier),
    /// A session boundary: the current source stops delivering frontiers.
    /// Because the channel is FIFO and frontiers relay one-per-request, the
    /// boundary is received only after every prior Frontier has been delivered;
    /// `reached` (when Some) fires at that moment, letting a streaming producer
    /// trigger a graceful consumer stop without truncating queued transactions.
    Boundary {
        reached: Option<tokio::sync::oneshot::Sender<()>>,
    },
}

/// Build a fixture [`ShuffleSessionFactory`] that replays the [`FixtureItem`]s
/// sent on the returned channel, reading no journals. No `shuffle::Service` is
/// constructed: the caller writes the log segments itself (via
/// [`write_transaction`]) and feeds the matching checkpoint Frontiers here. The
/// caller pushes one `Frontier` per transaction, then a `Boundary` per session
/// boundary; dropping the sender signals end-of-fixtures.
pub fn fixture_opener() -> (FixtureOpener, mpsc::UnboundedSender<FixtureItem>) {
    let (frontier_tx, frontier_rx) = mpsc::unbounded_channel::<FixtureItem>();
    let opener = FixtureOpener {
        frontier_rx: Arc::new(Mutex::new(frontier_rx)),
    };
    (opener, frontier_tx)
}

/// A [`ShuffleSessionFactory`] that yields fixture-replay [`ShuffleSession`]s.
///
/// Sessions are driven strictly sequentially (one `--sessions` iteration at a
/// time), so the single frontier receiver is shared behind a mutex: each `open`
/// acquires it for that session's lifetime, and the next session blocks until
/// the prior [`ShuffleSession`] is closed (or dropped). The journal-reading
/// Session logic in the shuffle crate is bypassed entirely.
pub struct FixtureOpener {
    frontier_rx: Arc<Mutex<mpsc::UnboundedReceiver<FixtureItem>>>,
}

impl ShuffleSessionFactory for FixtureOpener {
    type Session = FixtureCheckpoints;

    async fn open(
        &self,
        _task: shuffle::proto::Task,
        _shards: Vec<shuffle::proto::Shard>,
        _resume: shuffle::Frontier,
    ) -> anyhow::Result<FixtureCheckpoints> {
        // Acquire the shared frontier stream for this session's lifetime; a
        // following session blocks here until the prior source releases it. The
        // caller trusts its own write cursor, so the task spec, topology, and
        // resume Frontier are unused.
        let frontier_rx = self.frontier_rx.clone().lock_owned().await;
        Ok(FixtureCheckpoints {
            frontier_rx,
            boundary_reached: false,
        })
    }
}

/// A fixture-replay [`ShuffleSession`]: yields one queued [`FixtureItem::
/// Frontier`] per checkpoint request. A [`FixtureItem::Boundary`] (or a dropped
/// sender) ends this session's frontiers — the request is left unanswered (the
/// leader stops via its `max_transactions` limit or an external Stop), and every
/// subsequent request parks, so a stopping leader's speculative checkpoint can't
/// pop into the next session's frontiers.
pub struct FixtureCheckpoints {
    frontier_rx: tokio::sync::OwnedMutexGuard<mpsc::UnboundedReceiver<FixtureItem>>,
    /// Set once a Boundary (or end-of-fixtures) is observed; latches every
    /// further `recv_checkpoint` into an unresolving park.
    boundary_reached: bool,
}

impl ShuffleSession for FixtureCheckpoints {
    fn request_checkpoint(&self) {
        // No request protocol: `recv_checkpoint` pops the next queued frontier.
    }

    async fn recv_checkpoint(&mut self) -> anyhow::Result<shuffle::Frontier> {
        // Once the boundary is reached, never touch the channel again: a
        // re-issued request must not pop the next session's first frontier.
        if self.boundary_reached {
            return std::future::pending().await;
        }
        match self.frontier_rx.recv().await {
            Some(FixtureItem::Frontier(frontier)) => Ok(frontier),
            Some(FixtureItem::Boundary { reached }) => {
                self.boundary_reached = true;
                // Every prior frontier has been delivered (the channel is FIFO);
                // tell a streaming producer it may now request a graceful stop.
                if let Some(reached) = reached {
                    let _ = reached.send(());
                }
                std::future::pending().await
            }
            None => {
                self.boundary_reached = true;
                std::future::pending().await
            }
        }
    }

    async fn close(self) -> anyhow::Result<()> {
        // Dropping releases the shared frontier stream for the next session.
        Ok(())
    }
}

/// Build shuffle bindings and validators for `task`, plus a map from each
/// source collection name to the binding indices it feeds (a collection may be
/// read by multiple derivation transforms).
pub fn task_bindings(
    task: &shuffle::proto::Task,
) -> anyhow::Result<(
    Vec<shuffle::Binding>,
    Vec<doc::Validator>,
    HashMap<String, Vec<usize>>,
)> {
    let (bindings, validators) =
        shuffle::Binding::from_task(task).context("building shuffle bindings from task")?;

    let mut collection_bindings: HashMap<String, Vec<usize>> = HashMap::new();
    for (index, binding) in bindings.iter().enumerate() {
        collection_bindings
            .entry(binding.collection.to_string())
            .or_default()
            .push(index);
    }
    Ok((bindings, validators, collection_bindings))
}

/// A single shard's shuffle-log writer and its running (session-local) read
/// barrier. One per shard; the transaction writer advances the `last_lsn` of
/// each shard it appends a block to, and the checkpoint frontier reports all
/// shards' barriers as its per-shard `flushed_lsn`.
pub struct ShardWriter {
    pub writer: shuffle::log::Writer,
    pub last_lsn: shuffle::log::Lsn,
}

impl ShardWriter {
    /// Open a fresh shuffle-log writer for shard `shard_index` in `dir` (segments
    /// numbered from one). `shard_index` is the segment author: each shard's
    /// reader reconstructs its segment filenames as `mem-{shard_index:03}-seg-…`
    /// (see [`shuffle::log::segment_path`]), so a multi-shard writer MUST author
    /// its segments with the matching index or the reader can't find them.
    pub fn new(dir: &std::path::Path, shard_index: u32) -> anyhow::Result<Self> {
        Ok(Self {
            writer: shuffle::log::Writer::new(dir, shard_index)
                .context("opening shuffle-log writer")?,
            last_lsn: shuffle::log::Lsn::ZERO,
        })
    }
}

/// Build `count` shuffle shards with even key-range splits over the full `u32`
/// key space and a full r-clock range — the routing topology for
/// [`write_transaction`]. The r-clock range is full because the synthetic
/// topology splits on key only (matching the drive layer's join shards), so a
/// keyed document routes to exactly the one shard whose key range contains its
/// key hash. `count == 1` yields the single full-range shard preview uses.
pub fn full_range_shards(count: u32) -> Vec<shuffle::proto::Shard> {
    (0..count)
        .map(|i| {
            let (key_begin, key_end) = crate::drive::shards::key_range(i, count);
            shuffle::proto::Shard {
                range: Some(proto_flow::flow::RangeSpec {
                    key_begin,
                    key_end,
                    r_clock_begin: 0,
                    r_clock_end: u32::MAX,
                }),
                ..Default::default()
            }
        })
        .collect()
}

/// The shard(s) that should receive `packed_key`'s document, mirroring the
/// shuffle slice actor's routing (`crates/shuffle/src/slice/actor.rs`): hash the
/// packed shuffle key, rotate the document clock into an r-clock, and select by
/// `binding.filter_r_clocks`. With the key-split [`full_range_shards`] topology
/// this is exactly one shard.
///
/// A binding with no shuffle key (a `shuffle: lambda`, whose key the connector
/// computes and which we cannot reproduce here) packs an empty key that would
/// pin every document to shard zero; such bindings are round-robined instead,
/// per `round_robin`, so their documents still spread across shards.
fn route_targets(
    binding: &shuffle::Binding,
    packed_key: &[u8],
    doc_clock: uuid::Clock,
    shards: &[shuffle::proto::Shard],
    round_robin: &mut HashMap<u16, usize>,
) -> Vec<usize> {
    if shards.len() == 1 {
        return vec![0];
    }
    if binding.key_extractors.is_empty() {
        let cursor = round_robin.entry(binding.index).or_insert(0);
        let shard = *cursor % shards.len();
        *cursor += 1;
        return vec![shard];
    }
    let key_hash = doc::Extractor::packed_hash(packed_key);
    let r_clock = shuffle::slice::routing::rotate_clock(doc_clock);
    let targets: Vec<usize> = shuffle::slice::routing::route_to_shards(
        key_hash,
        r_clock,
        binding.filter_r_clocks,
        shards,
    )
    .collect();
    // The key-split topology covers the whole key space, so a match is
    // guaranteed; fall back to shard zero rather than dropping a document.
    if targets.is_empty() { vec![0] } else { targets }
}

/// Write one transaction, key-routing each document to its owning shard(s), and
/// return the checkpoint frontier. Each source document is tagged with the
/// **specific binding** it feeds; the collection-based [`write_transaction`]
/// fans a fixture document out to every binding of its collection (preview),
/// while the runner feeds one binding at a time so per-transform read progress —
/// and thus read delays — are honored independently.
///
/// Each shard that receives documents gets a single log block appended to its
/// own writer; the frontier's per-shard `flushed_lsn` reports every shard's read
/// barrier (advanced shards and idle shards alike). The producer's `last_commit`
/// is the transaction's max clock, so all of its documents become visible at the
/// checkpoint.
///
/// State threaded across calls: `clock`, `journal_offsets`, and `round_robin`
/// advance globally (so recovered frontiers never re-admit prior documents, and
/// round-robin distribution is stable across the run); `writers` and `sealed`
/// are session-local (each session opens fresh per-shard writers). `shards` and
/// `writers` are parallel and index-aligned. Preview passes a single-element
/// pair ([`full_range_shards(1)`](full_range_shards)); the test runner passes N.
#[allow(clippy::too_many_arguments)]
pub fn write_transaction_for_bindings(
    items: &[(usize, &serde_json::Value)],
    bindings: &[shuffle::Binding],
    validators: &mut [doc::Validator],
    shards: &[shuffle::proto::Shard],
    writers: &mut [ShardWriter],
    sealed: &mut Vec<shuffle::log::writer::SealedSegment>,
    clock: &mut uuid::Clock,
    journal_offsets: &mut HashMap<(String, u16), i64>,
    round_robin: &mut HashMap<u16, usize>,
    packed_key: &mut bytes::BytesMut,
) -> anyhow::Result<shuffle::Frontier> {
    assert_eq!(
        shards.len(),
        writers.len(),
        "shards and writers must be index-aligned",
    );
    let n_shards = writers.len();

    // Per-shard block entries and journal→bid maps accumulated this transaction.
    let mut entries: Vec<Vec<(shuffle::log::BlockMeta, u32, bytes::Bytes, bytes::Bytes)>> =
        (0..n_shards).map(|_| Vec::new()).collect();
    let mut block_journals: Vec<HashMap<String, u16>> =
        (0..n_shards).map(|_| HashMap::new()).collect();
    // (journal, binding) => (max committed clock, source bytes this txn). Global
    // across shards: the journal is logical and read once, regardless of routing.
    let mut frontier_acc: BTreeMap<(String, u16), (uuid::Clock, i64)> = BTreeMap::new();

    for &(bi, doc) in items {
        let binding = &bindings[bi];
        let journal = fixture_journal(&binding.collection);
        let doc_clock = clock.tick();

        // Inject a synthetic UUID at the collection's UUID pointer.
        let mut doc = doc.clone();
        let synthetic_uuid = uuid::build(FIXTURE_PRODUCER, doc_clock, uuid::Flags::OUTSIDE_TXN);
        *json::ptr::create_value(&binding.source_uuid_ptr, &mut doc)
            .context("creating fixture UUID location in document")? =
            serde_json::json!(synthetic_uuid.as_hyphenated().to_string());

        let alloc = doc::HeapNode::new_allocator();
        let heap =
            doc::HeapNode::from_serde(&doc, &alloc).context("allocating fixture document")?;
        let archive = heap.to_archive();
        let archived = doc::ArchivedNode::from_archive(archive.as_slice());

        // Mirror the slice: set the schema-valid flag from validation and
        // pack the shuffle key from the archived document.
        let mut flags = uuid::Flags::OUTSIDE_TXN.0;
        if validators[bi].is_valid(archived) {
            flags |= shuffle::FLAGS_SCHEMA_VALID;
        }

        packed_key.clear();
        doc::Extractor::extract_all(
            archived,
            &binding.key_extractors,
            doc::Encoding::Packed,
            packed_key,
            None,
        );

        let targets = route_targets(binding, packed_key, doc_clock, shards, round_robin);

        let doc_bytes = bytes::Bytes::from(archive.to_vec());
        let source_len = doc_bytes.len() as u32;
        let key_bytes = packed_key.split().freeze();

        for &shard in &targets {
            let journal_bid = {
                let block_journals = &mut block_journals[shard];
                let next = block_journals.len() as u16;
                *block_journals.entry(journal.clone()).or_insert(next)
            };
            entries[shard].push((
                shuffle::log::BlockMeta {
                    binding: binding.index,
                    journal_bid,
                    producer_bid: 0,
                    flags,
                    clock: doc_clock.as_u64(),
                },
                source_len,
                key_bytes.clone(),
                doc_bytes.clone(),
            ));
        }

        // The source journal is read once regardless of routing: advance its
        // offset and frontier clock once per document, not per target shard.
        let acc = frontier_acc
            .entry((journal.clone(), binding.index))
            .or_insert((uuid::Clock::from_u64(0), 0));
        acc.0 = acc.0.max(doc_clock);
        acc.1 += source_len as i64;
        *journal_offsets.entry((journal, binding.index)).or_insert(0) += source_len as i64;
    }

    // Write each shard's block (if any), advancing that shard's read barrier.
    for shard in 0..n_shards {
        if entries[shard].is_empty() {
            continue;
        }
        let producers: HashMap<uuid::Producer, u16> = [(FIXTURE_PRODUCER, 0)].into();
        let (lsn, rolled) = writers[shard]
            .writer
            .append_block(
                std::mem::take(&mut block_journals[shard]),
                producers,
                std::mem::take(&mut entries[shard]),
            )
            .context("writing fixture log block")?;
        if let Some(rolled) = rolled {
            sealed.push(rolled);
        }
        writers[shard].last_lsn = lsn;
    }

    // `frontier_acc` iterates sorted by (journal, binding), satisfying Frontier
    // ordering invariants.
    let journals: Vec<shuffle::JournalFrontier> = frontier_acc
        .into_iter()
        .map(|((journal, binding), (last_commit, bytes_read))| {
            let offset = -journal_offsets
                .get(&(journal.clone(), binding))
                .copied()
                .unwrap_or(0);
            shuffle::JournalFrontier {
                journal: journal.into(),
                binding,
                producers: vec![shuffle::ProducerFrontier {
                    producer: FIXTURE_PRODUCER,
                    last_commit,
                    hinted_commit: uuid::Clock::from_u64(0),
                    offset,
                }],
                bytes_read_delta: bytes_read,
                bytes_behind_delta: 0,
            }
        })
        .collect();

    let flushed_lsn: Vec<u64> = writers.iter().map(|w| w.last_lsn.as_u64()).collect();
    shuffle::Frontier::new(journals, flushed_lsn).context("building fixture checkpoint frontier")
}

/// Write one collection-tagged transaction, fanning each document out to **every**
/// binding of its source collection. This is the preview feed path: a fixture
/// document `["collection", {…}]` is delivered to all transforms reading that
/// collection at once (preview does not simulate per-transform read delays).
/// Documents whose collection sources no binding are skipped. Delegates to
/// [`write_transaction_for_bindings`], the shared writer.
#[allow(clippy::too_many_arguments)]
pub fn write_transaction(
    transaction: &Transaction,
    bindings: &[shuffle::Binding],
    validators: &mut [doc::Validator],
    collection_bindings: &HashMap<String, Vec<usize>>,
    shards: &[shuffle::proto::Shard],
    writers: &mut [ShardWriter],
    sealed: &mut Vec<shuffle::log::writer::SealedSegment>,
    clock: &mut uuid::Clock,
    journal_offsets: &mut HashMap<(String, u16), i64>,
    round_robin: &mut HashMap<u16, usize>,
    packed_key: &mut bytes::BytesMut,
) -> anyhow::Result<shuffle::Frontier> {
    let items: Vec<(usize, &serde_json::Value)> = transaction
        .iter()
        .flat_map(|(collection, doc)| {
            collection_bindings
                .get(collection.as_str())
                .into_iter()
                .flatten()
                .map(move |&bi| (bi, doc))
        })
        .collect();

    write_transaction_for_bindings(
        &items,
        bindings,
        validators,
        shards,
        writers,
        sealed,
        clock,
        journal_offsets,
        round_robin,
        packed_key,
    )
}

/// Synthetic journal name for a collection's fixture documents. The runtime-next
/// consumer ignores the journal name during processing; it is carried only in the
/// checkpoint frontier (where it must match the block's journal for visibility).
pub fn fixture_journal(collection: &models::Collection) -> String {
    format!("{}/fixture", collection.as_str())
}

#[cfg(test)]
mod test {
    use super::*;

    /// A checkpoint Frontier carrying `lsn` as its single `flushed_lsn`.
    fn frontier(lsn: u64) -> shuffle::Frontier {
        shuffle::Frontier::new(
            vec![shuffle::JournalFrontier {
                journal: "fixture/test/coll".into(),
                binding: 0,
                producers: vec![shuffle::ProducerFrontier {
                    producer: uuid::Producer::from_bytes([0x01, 0, 0, 0, 0, 0]),
                    last_commit: uuid::Clock::from_unix(lsn, 0),
                    hinted_commit: uuid::Clock::from_u64(0),
                    offset: -(lsn as i64),
                }],
                bytes_read_delta: 0,
                bytes_behind_delta: 0,
            }],
            vec![lsn],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn relays_one_frontier_per_checkpoint() {
        let (opener, frontier_tx) = fixture_opener();

        // Open a source for the first session; task/topology/resume are unused.
        let mut src = opener
            .open(Default::default(), Vec::new(), shuffle::Frontier::default())
            .await
            .unwrap();

        // Each request yields the next queued frontier, in order.
        frontier_tx
            .send(FixtureItem::Frontier(frontier(1)))
            .unwrap();
        frontier_tx
            .send(FixtureItem::Frontier(frontier(2)))
            .unwrap();
        for expect_lsn in [1u64, 2] {
            src.request_checkpoint();
            let frontier = src.recv_checkpoint().await.unwrap();
            assert_eq!(frontier.encode().flushed_lsn, vec![expect_lsn]);
        }

        // A Boundary leaves this request unanswered (the leader stops via
        // max_transactions). Its `reached` ack fires only now — after both
        // frontiers were delivered.
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
        frontier_tx
            .send(FixtureItem::Boundary {
                reached: Some(reached_tx),
            })
            .unwrap();
        // A frontier queued *after* the boundary belongs to the next session.
        frontier_tx
            .send(FixtureItem::Frontier(frontier(3)))
            .unwrap();

        tokio::select! {
            _ = src.recv_checkpoint() => panic!("recv_checkpoint must park on a Boundary"),
            r = reached_rx => r.expect("boundary ack fires"),
        }

        // The boundary latches: a re-issued request parks rather than popping
        // the next session's frontier.
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), src.recv_checkpoint())
                .await
                .is_err(),
            "a post-boundary request must not steal the next session's frontier",
        );

        // Closing releases the shared stream; the next session resumes at the
        // frontier queued after the boundary.
        src.close().await.unwrap();
        let mut next = opener
            .open(Default::default(), Vec::new(), shuffle::Frontier::default())
            .await
            .unwrap();
        next.request_checkpoint();
        let frontier = next.recv_checkpoint().await.unwrap();
        assert_eq!(frontier.encode().flushed_lsn, vec![3]);
    }
}
