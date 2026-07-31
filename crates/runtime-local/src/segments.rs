//! Shuffle-log segment writer, and the channel-fed [`ShuffleSessionFactory`]
//! which makes what it writes visible to the runtime.
//!
//! Given documents tagged with the binding they feed, [`write_transaction`]
//! writes them directly as [`shuffle::log`] segments — stamping a synthetic UUID
//! from a caller-supplied [`uuid::Clock`], validating against the source schema,
//! packing the shuffle key, routing by that key's hash exactly as the live
//! shuffle slice does — and returns a checkpoint [`shuffle::Frontier`] whose
//! per-(journal, binding) producer `last_commit` is the transaction's maximum
//! clock, so all of its documents become visible at that checkpoint.
//!
//! [`fixture_opener`] builds the matching [`ShuffleSessionFactory`]: it hands the
//! leader one relayed `Frontier` per checkpoint request, so the consumer reads
//! these documents exactly as if they had come from live journals. No
//! `shuffle::Service` is constructed and the `shuffle` crate is unaware of the
//! source.
//!
//! State that must advance globally across sessions (the document clock and
//! per-(journal, binding) committed offsets) is threaded by the caller, so a
//! recovered frontier never re-admits an earlier session's documents. Segment
//! LSNs, by contrast, restart per session: each session opens its own log
//! directory, matching the runtime's ephemeral per-session `Reader`.

use anyhow::Context;
use proto_gazette::uuid;
use runtime_next::{ShuffleSession, ShuffleSessionFactory};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};

/// Fixed synthetic producer for all locally-written documents (matches legacy
/// `flowctl preview`).
pub const FIXTURE_PRODUCER: uuid::Producer = uuid::Producer([7, 19, 83, 3, 3, 17]);

/// A transaction of documents tagged with their source collection names.
pub type Transaction = Vec<(String, serde_json::Value)>;

/// One queued item of a replay opener's channel.
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

/// Build a [`ShuffleSessionFactory`] that replays the [`FixtureItem`]s sent on
/// the returned channel, reading no journals. No `shuffle::Service` is
/// constructed: the caller writes the log segments itself (via
/// [`write_transaction`]) and feeds the matching checkpoint Frontiers here. Push
/// one `Frontier` per transaction, then a `Boundary` per session boundary;
/// dropping the sender signals end-of-input.
pub fn fixture_opener() -> (FixtureOpener, mpsc::UnboundedSender<FixtureItem>) {
    let (frontier_tx, frontier_rx) = mpsc::unbounded_channel::<FixtureItem>();
    let opener = FixtureOpener {
        frontier_rx: Arc::new(Mutex::new(frontier_rx)),
    };
    (opener, frontier_tx)
}

/// A [`ShuffleSessionFactory`] that yields replay [`ShuffleSession`]s.
///
/// Sessions run strictly sequentially (one at a time), so the single frontier
/// receiver is shared behind a mutex: each `open` acquires it for that session's
/// lifetime, and the next session blocks until the prior [`ShuffleSession`] is
/// closed (or dropped). The journal-reading Session logic in the shuffle crate
/// is bypassed entirely.
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
        // caller owns its own write cursor, so the task spec, topology, and
        // resume Frontier are unused.
        let frontier_rx = self.frontier_rx.clone().lock_owned().await;
        Ok(FixtureCheckpoints {
            frontier_rx,
            boundary_reached: false,
        })
    }
}

/// A replay [`ShuffleSession`]: yields one queued [`FixtureItem::Frontier`] per
/// checkpoint request. A [`FixtureItem::Boundary`] (or a dropped sender) ends
/// this session's frontiers — the request is left unanswered (the leader stops
/// via its `max_transactions` limit or an external Stop), and every subsequent
/// request parks, so a stopping leader's speculative checkpoint can't pop into
/// the next session's frontiers.
pub struct FixtureCheckpoints {
    frontier_rx: tokio::sync::OwnedMutexGuard<mpsc::UnboundedReceiver<FixtureItem>>,
    /// Set once a Boundary (or end-of-input) is observed; latches every further
    /// `recv_checkpoint` into an unresolving park.
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

/// Build shuffle bindings and validators for `task`, plus a map from each source
/// collection name to the binding indices it feeds (a collection may be read by
/// multiple derivation transforms).
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

/// One shard's shuffle-log writer paired with its session-local read barrier.
/// The transaction writer advances the `last_lsn` of each shard it appends a
/// block to, and reports every shard's barrier — advanced and idle alike — as the
/// checkpoint frontier's per-shard `flushed_lsn`.
pub struct ShardWriter {
    pub writer: shuffle::log::Writer,
    pub last_lsn: shuffle::log::Lsn,
}

impl ShardWriter {
    /// Open a fresh shuffle-log writer for shard `shard_index` in `dir`, with
    /// segments numbered from one.
    ///
    /// `shard_index` is the segment *author*: a shard's reader reconstructs its
    /// segment filenames from its own index, so a multi-shard writer must author
    /// each stream under the matching index or that shard's reader will not find
    /// its segments.
    pub fn new(dir: &std::path::Path, shard_index: u32) -> anyhow::Result<Self> {
        Ok(Self {
            writer: shuffle::log::Writer::new(dir, shard_index)
                .context("opening shuffle-log writer")?,
            last_lsn: shuffle::log::Lsn::ZERO,
        })
    }
}

/// Open one [`ShardWriter`] per shard, all within one session directory: a
/// shard's stream is distinguished by its segment file names, and each shard's
/// consumer reads only its own stream.
pub fn open_shard_writers(
    dir: &std::path::Path,
    n_shards: u32,
) -> anyhow::Result<Vec<ShardWriter>> {
    (0..n_shards).map(|i| ShardWriter::new(dir, i)).collect()
}

/// Build `count` shuffle shards evenly splitting the full `u32` key space, each
/// with a full r-clock range — the routing topology [`write_transaction`] expects,
/// matching the drivers' join shards. The r-clock range is full because this
/// synthetic topology splits on key alone, so a keyed document routes to exactly
/// the one shard whose key range contains its key hash.
pub fn full_range_shards(count: u32) -> Vec<shuffle::proto::Shard> {
    (0..count)
        .map(|i| {
            let (key_begin, key_end) = crate::shards::key_range(i, count);
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

/// Write one transaction of documents, each tagged with the **specific binding**
/// it feeds and the clock to stamp it with, and return the checkpoint frontier
/// which makes them visible.
///
/// Tagging per binding (rather than per collection) is what lets a caller feed
/// one transform at a time, so per-transform read progress — and therefore read
/// delays — are honored independently. [`write_transaction`] is the
/// collection-tagged wrapper, which fans each document out to every binding of
/// its collection at once.
///
/// Documents route to shards by their packed shuffle-key hash, exactly as the
/// live slice routes them; each shard receiving documents gets a single log block
/// appended to its own writer.
///
/// `shards` and `writers` are parallel and index-aligned. `journal_offsets`
/// advances globally across the run (so a recovered frontier never re-admits
/// earlier documents), while `writers` and `sealed` are session-local.
#[allow(clippy::too_many_arguments)]
pub fn write_transaction_for_bindings(
    items: &[(usize, uuid::Clock, &serde_json::Value)],
    bindings: &[shuffle::Binding],
    validators: &mut [doc::Validator],
    shards: &[shuffle::proto::Shard],
    writers: &mut [ShardWriter],
    sealed: &mut Vec<shuffle::log::writer::SealedSegment>,
    journal_offsets: &mut HashMap<(String, u16), i64>,
    packed_key: &mut bytes::BytesMut,
) -> anyhow::Result<shuffle::Frontier> {
    assert_eq!(
        shards.len(),
        writers.len(),
        "shards and writers must be index-aligned",
    );

    let mut entries: Vec<Vec<(shuffle::log::BlockMeta, u32, bytes::Bytes, bytes::Bytes)>> =
        vec![Vec::new(); writers.len()];
    let mut block_journals: HashMap<String, u16> = HashMap::new();
    // (journal, binding) => (max committed clock, source bytes this txn).
    let mut frontier_acc: BTreeMap<(String, u16), (uuid::Clock, i64)> = BTreeMap::new();

    for &(bi, doc_clock, doc) in items {
        let binding = &bindings[bi];
        let journal = fixture_journal(&binding.collection);

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

        let doc_bytes = bytes::Bytes::from(archive.to_vec());
        let source_len = doc_bytes.len() as u32;

        let journal_bid = {
            let next = block_journals.len() as u16;
            *block_journals.entry(journal.clone()).or_insert(next)
        };

        // Mirror the slice's routing: hash the packed key and write the
        // document to each shard whose range admits it.
        let key_hash = doc::Extractor::packed_hash(packed_key);
        let r_clock = shuffle::slice::routing::rotate_clock(doc_clock);
        let key = packed_key.split().freeze();

        let meta = shuffle::log::BlockMeta {
            binding: binding.index,
            journal_bid,
            producer_bid: 0,
            flags,
            clock: doc_clock.as_u64(),
        };
        for shard_index in shuffle::slice::routing::route_to_shards(
            key_hash,
            r_clock,
            binding.filter_r_clocks,
            shards,
        ) {
            entries[shard_index].push((meta, source_len, key.clone(), doc_bytes.clone()));
        }

        // The source journal is read once regardless of routing: advance its
        // offset and frontier clock per document, not per target shard.
        let acc = frontier_acc
            .entry((journal.clone(), binding.index))
            .or_insert((uuid::Clock::from_u64(0), 0));
        acc.0 = acc.0.max(doc_clock);
        acc.1 += source_len as i64;
        *journal_offsets.entry((journal, binding.index)).or_insert(0) += source_len as i64;
    }

    // Write each shard's documents as a single block (if any), advancing that
    // shard's session-local read barrier to its LSN.
    let producers: HashMap<uuid::Producer, u16> = [(FIXTURE_PRODUCER, 0)].into();
    for (shard_index, shard_entries) in entries.into_iter().enumerate() {
        if shard_entries.is_empty() {
            continue;
        }
        let (lsn, rolled) = writers[shard_index]
            .writer
            .append_block(block_journals.clone(), producers.clone(), shard_entries)
            .context("writing fixture log block")?;
        if let Some(rolled) = rolled {
            sealed.push(rolled);
        }
        writers[shard_index].last_lsn = lsn;
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

/// Write one collection-tagged transaction, fanning each document out to *every*
/// binding of its source collection, and return its checkpoint frontier.
/// Documents whose collection sources no binding are skipped.
///
/// Document clocks mirror the legacy preview fixture harness: the `ordinal`-th
/// transaction's documents are stamped `3600 * ordinal + <index>` seconds, so
/// fixture-driven outputs (e.g. `flow_published_at`) are identical between the
/// legacy and runtime-v2 preview stacks. Clocks must increase globally, which
/// holds for transactions of fewer than 3600 documents.
///
/// One clock is shared by every binding a line feeds, as a single published
/// document would be. Lines whose collection isn't sourced still consume a
/// clock, so a fixture yields identical document clocks for every task it
/// drives — again matching the legacy harness.
#[allow(clippy::too_many_arguments)]
pub fn write_transaction(
    transaction: &Transaction,
    bindings: &[shuffle::Binding],
    validators: &mut [doc::Validator],
    collection_bindings: &HashMap<String, Vec<usize>>,
    shards: &[shuffle::proto::Shard],
    writers: &mut [ShardWriter],
    sealed: &mut Vec<shuffle::log::writer::SealedSegment>,
    txn_ordinal: &mut u64,
    journal_offsets: &mut HashMap<(String, u16), i64>,
    packed_key: &mut bytes::BytesMut,
) -> anyhow::Result<shuffle::Frontier> {
    let mut doc_seconds = 3600 * *txn_ordinal;
    *txn_ordinal += 1;

    let items: Vec<(usize, uuid::Clock, &serde_json::Value)> = transaction
        .iter()
        .flat_map(|(collection, doc)| {
            let doc_clock = uuid::Clock::from_unix(doc_seconds, 0);
            doc_seconds += 1;

            collection_bindings
                .get(collection.as_str())
                .into_iter()
                .flatten()
                .map(move |&bi| (bi, doc_clock, doc))
        })
        .collect();

    write_transaction_for_bindings(
        &items,
        bindings,
        validators,
        shards,
        writers,
        sealed,
        journal_offsets,
        packed_key,
    )
}

/// Synthetic journal name for a collection's locally-written documents. The
/// runtime-next consumer ignores the journal name during processing; it is
/// carried only in the checkpoint frontier, where it must match the block's
/// journal for visibility.
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
