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
//!
//! [`write_transaction`] takes a whole transaction, but it is a wrapper: a
//! caller which does not have one yet — a streaming fixture reading a FIFO
//! line by line — instead pushes documents into a [`TxnState`] as it parses
//! them and closes it with [`finish_txn`]. Either way at most one block per
//! shard is resident, so peak memory tracks block size and not transaction
//! size.

use anyhow::Context;
use proto_gazette::uuid;
use runtime_next::{ShuffleSession, ShuffleSessionFactory};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};

/// Fixed synthetic producer for all locally-written documents (matches legacy
/// `flowctl preview`).
pub const FIXTURE_PRODUCER: uuid::Producer = uuid::Producer([7, 19, 83, 3, 3, 17]);

/// Flush a shard's buffered entries into a block once either threshold is met.
///
/// `block::encode` asserts that a block holds at most 65,536 entries, an
/// invariant the live actor keeps by consulting `BlockState::is_full()` every
/// iteration. We write blocks through `Writer::append_block`, which performs no
/// such check, so we must bound our own blocks. A benchmark generator feeding
/// millions of documents in one transaction otherwise trips the assert.
///
/// The byte threshold matters as much as the entry count: it bounds the encoded
/// block, and so the resident cost of a transaction whose documents are pushed
/// into a [`TxnState`] as they are parsed.
pub const FIXTURE_BLOCK_ENTRIES: usize = 32 * 1024;
pub const FIXTURE_BLOCK_BYTES: usize = 64 * 1024 * 1024;

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
        if !self.boundary_reached {
            match self.frontier_rx.recv().await {
                Some(FixtureItem::Frontier(frontier)) => return Ok(frontier),
                // Every prior frontier has been delivered (the channel is FIFO);
                // tell a streaming producer it may now request a graceful stop.
                Some(FixtureItem::Boundary { reached }) => _ = reached.map(|tx| tx.send(())),
                None => (),
            }
            self.boundary_reached = true;
        }
        // Having reached the boundary, never touch the channel again: a
        // re-issued request must not pop the next session's first frontier.
        std::future::pending().await
    }

    async fn close(self) -> anyhow::Result<()> {
        // Dropping releases the shared frontier stream for the next session.
        Ok(())
    }
}

/// Build shuffle bindings, sources, and per-source validators for `task`, plus
/// a map from each source collection name to the binding indices it feeds (a
/// collection may be read by multiple derivation transforms).
pub fn task_bindings(
    task: &shuffle::proto::Task,
) -> anyhow::Result<(
    Vec<shuffle::Binding>,
    Vec<shuffle::Source>,
    Vec<doc::Validator>,
    HashMap<String, Vec<usize>>,
)> {
    let (bindings, sources, validators) =
        shuffle::Binding::from_task(task).context("building shuffle bindings from task")?;

    let mut collection_bindings: HashMap<String, Vec<usize>> = HashMap::new();
    for (index, binding) in bindings.iter().enumerate() {
        collection_bindings
            .entry(sources[binding.source as usize].collection.to_string())
            .or_default()
            .push(index);
    }
    Ok((bindings, sources, validators, collection_bindings))
}

/// One buffered log entry: its block metadata, source length, packed shuffle
/// key, and archived document.
type Entry = (shuffle::log::BlockMeta, u32, bytes::Bytes, bytes::Bytes);

/// One shard's shuffle-log writer paired with its session-local read barrier.
/// The transaction writer advances the `last_lsn` of each shard it appends a
/// block to, and reports every shard's barrier — advanced and idle alike — as the
/// checkpoint frontier's per-shard `flushed_lsn`.
pub struct ShardWriter {
    pub writer: shuffle::log::Writer,
    pub last_lsn: shuffle::log::Lsn,
}

impl ShardWriter {
    /// Append `block` to this shard's log, advancing its read barrier to the
    /// resulting LSN and collecting a segment which the append may have sealed.
    fn append_block(
        &mut self,
        journals: &HashMap<String, u16>,
        block: Vec<Entry>,
        sealed: &mut Vec<shuffle::log::writer::SealedSegment>,
    ) -> anyhow::Result<()> {
        let (lsn, rolled) = self
            .writer
            .append_block(journals.clone(), [(FIXTURE_PRODUCER, 0)].into(), block)
            .context("writing fixture log block")?;

        sealed.extend(rolled);
        self.last_lsn = lsn;
        Ok(())
    }

    /// Open a fresh shuffle-log writer for shard `shard_index` in `dir`, with
    /// segments numbered from one.
    ///
    /// `shard_index` is the segment *author*: a shard's reader reconstructs its
    /// segment filenames from its own index, so a multi-shard writer must author
    /// each stream under the matching index or that shard's reader will not find
    /// its segments.
    pub fn new(dir: &std::path::Path, shard_index: u32) -> anyhow::Result<Self> {
        Self::with_segment_threshold(
            dir,
            shard_index,
            shuffle::log::writer::DEFAULT_SEGMENT_THRESHOLD,
        )
    }

    /// Open a shard writer whose segment files roll after
    /// `segment_threshold_bytes`. A caller which bounds its standing on-disk
    /// backlog shrinks this: sealed segments are the granularity that backlog is
    /// accounted (and reclaimed) at, so a coarse threshold overshoots the bound.
    pub fn with_segment_threshold(
        dir: &std::path::Path,
        shard_index: u32,
        segment_threshold_bytes: u64,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            // `usize::MAX` disables block compression, matching `Writer::new`.
            writer: shuffle::log::Writer::with_thresholds(
                dir,
                shard_index,
                usize::MAX,
                segment_threshold_bytes,
            )
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

/// Per-transaction accumulation state of the segment writer.
///
/// Documents are routed and buffered into per-shard blocks as they are pushed,
/// and each block is appended as soon as it meets a [`FIXTURE_BLOCK_ENTRIES`] or
/// [`FIXTURE_BLOCK_BYTES`] threshold — so at most one block per shard is
/// resident, however large a transaction is. A caller which does not yet hold a
/// whole transaction can therefore push documents as it parses them.
///
/// Close with [`finish_txn`], which appends each shard's remainder and returns
/// the checkpoint frontier making the transaction's documents visible.
pub struct TxnState {
    /// Block entries buffered per shard.
    entries: Vec<Vec<Entry>>,
    /// Buffered document bytes per shard, driving FIXTURE_BLOCK_BYTES.
    entries_bytes: Vec<usize>,
    /// Journal => block-internal ID, extended as journals are first seen.
    block_journals: HashMap<String, u16>,
    /// (journal, binding) => (max committed clock, source bytes this txn).
    frontier_acc: BTreeMap<(String, u16), (uuid::Clock, i64)>,
    /// Clock seconds stamped on the next document pushed by [`push_doc`], which
    /// advances it per document. Unused (and left zero) when the caller stamps
    /// each document itself via [`push_binding`].
    doc_seconds: u64,
}

impl TxnState {
    /// Begin a transaction over `n_shards` shards whose documents the caller
    /// stamps itself, pushing them through [`push_binding`].
    pub fn new(n_shards: usize) -> Self {
        Self {
            entries: vec![Vec::new(); n_shards],
            entries_bytes: vec![0; n_shards],
            block_journals: HashMap::new(),
            frontier_acc: BTreeMap::new(),
            doc_seconds: 0,
        }
    }

    /// Begin the `txn_ordinal`-th transaction of a run, whose [`push_doc`]
    /// documents are stamped `3600 * txn_ordinal + <index>` seconds.
    ///
    /// Those clocks mirror the legacy preview fixture harness, so fixture-driven
    /// outputs (e.g. `flow_published_at`) are identical between the legacy and
    /// runtime-v2 preview stacks.
    ///
    /// Transactions of more than 3600 documents overlap the next transaction's
    /// clock range. That matters only across session boundaries, where the
    /// recovered frontier re-admits documents by clock: within one session —
    /// including the entire streaming path — the read barrier is the LSN, not the
    /// clock, so overlap is harmless. Eager multi-session callers should keep
    /// transactions under 3600 documents.
    pub fn for_txn(n_shards: usize, txn_ordinal: u64) -> Self {
        Self {
            doc_seconds: 3600 * txn_ordinal,
            ..Self::new(n_shards)
        }
    }
}

/// Push one document, tagged with the **specific binding** it feeds and the
/// clock to stamp it with, into `state`'s per-shard blocks — appending any block
/// which has met its threshold.
///
/// Tagging per binding (rather than per collection) is what lets a caller feed
/// one transform at a time, so per-transform read progress — and therefore read
/// delays — are honored independently. [`push_doc`] is the collection-tagged
/// wrapper, which fans each document out to every binding of its collection.
///
/// Documents route to shards by their packed shuffle-key hash, exactly as the
/// live slice routes them. `shards` and `writers` are parallel and index-aligned.
/// `journal_offsets` advances globally across the run (so a recovered frontier
/// never re-admits earlier documents), while `writers` and `sealed` are
/// session-local.
#[allow(clippy::too_many_arguments)]
pub fn push_binding(
    state: &mut TxnState,
    bi: usize,
    doc_clock: uuid::Clock,
    doc: &serde_json::Value,
    bindings: &[shuffle::Binding],
    sources: &[shuffle::Source],
    validators: &mut [doc::Validator],
    shards: &[shuffle::proto::Shard],
    writers: &mut [ShardWriter],
    sealed: &mut Vec<shuffle::log::writer::SealedSegment>,
    journal_offsets: &mut HashMap<(String, u16), i64>,
    packed_key: &mut bytes::BytesMut,
) -> anyhow::Result<()> {
    assert_eq!(
        shards.len(),
        writers.len(),
        "shards and writers must be index-aligned"
    );

    let binding = &bindings[bi];
    let source = &sources[binding.source as usize];
    let journal = fixture_journal(&source.collection);

    // Inject a synthetic UUID at the collection's UUID pointer.
    let mut doc = doc.clone();
    let synthetic_uuid = uuid::build(FIXTURE_PRODUCER, doc_clock, uuid::Flags::OUTSIDE_TXN);
    *json::ptr::create_value(&source.uuid_ptr, &mut doc)
        .context("creating fixture UUID location in document")? =
        serde_json::json!(synthetic_uuid.as_hyphenated().to_string());

    let alloc = doc::HeapNode::new_allocator();
    let heap = doc::HeapNode::from_serde(&doc, &alloc).context("allocating fixture document")?;
    let archive = heap.to_archive();
    let archived = doc::ArchivedNode::from_archive(archive.as_slice());

    // Mirror the slice: set the schema-valid flag from validation and
    // pack the shuffle key from the archived document.
    let mut flags = uuid::Flags::OUTSIDE_TXN.0;
    if validators[binding.source as usize].is_valid(archived) {
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

    let next_bid = state.block_journals.len() as u16;
    let journal_bid = *state
        .block_journals
        .entry(journal.clone())
        .or_insert(next_bid);

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
    for shard_index in
        shuffle::slice::routing::route_to_shards(key_hash, r_clock, binding.filter_r_clocks, shards)
    {
        state.entries[shard_index].push((meta, source_len, key.clone(), doc_bytes.clone()));
        state.entries_bytes[shard_index] += doc_bytes.len();

        if state.entries[shard_index].len() >= FIXTURE_BLOCK_ENTRIES
            || state.entries_bytes[shard_index] >= FIXTURE_BLOCK_BYTES
        {
            // Every entry buffered for this shard carries a journal_bid already
            // present in `block_journals`, so the map as it stands resolves them
            // all. Later journals appearing in it are unreferenced by this block
            // and harmless.
            let block = std::mem::take(&mut state.entries[shard_index]);
            state.entries_bytes[shard_index] = 0;

            writers[shard_index].append_block(&state.block_journals, block, sealed)?;
        }
    }

    // The source journal is read once regardless of routing: advance its
    // offset and frontier clock per document, not per target shard.
    let acc = state
        .frontier_acc
        .entry((journal.clone(), binding.index))
        .or_insert((uuid::Clock::from_u64(0), 0));
    acc.0 = acc.0.max(doc_clock);
    acc.1 += source_len as i64;
    *journal_offsets.entry((journal, binding.index)).or_insert(0) += source_len as i64;

    Ok(())
}

/// Push one collection-tagged document into `state`, consuming one document
/// clock (see [`TxnState::for_txn`]) and fanning the document out to *every*
/// binding of its source collection.
///
/// One clock is shared by every binding a document feeds, as a single published
/// document would be. Documents whose collection sources no binding are skipped
/// but still consume a clock, so a fixture yields identical document clocks for
/// every task it drives — again matching the legacy harness.
#[allow(clippy::too_many_arguments)]
pub fn push_doc(
    state: &mut TxnState,
    collection: &str,
    doc: &serde_json::Value,
    bindings: &[shuffle::Binding],
    sources: &[shuffle::Source],
    validators: &mut [doc::Validator],
    collection_bindings: &HashMap<String, Vec<usize>>,
    shards: &[shuffle::proto::Shard],
    writers: &mut [ShardWriter],
    sealed: &mut Vec<shuffle::log::writer::SealedSegment>,
    journal_offsets: &mut HashMap<(String, u16), i64>,
    packed_key: &mut bytes::BytesMut,
) -> anyhow::Result<()> {
    let doc_clock = uuid::Clock::from_unix(state.doc_seconds, 0);
    state.doc_seconds += 1;

    let Some(binding_indices) = collection_bindings.get(collection) else {
        return Ok(()); // Collection isn't a source of this task.
    };

    for &bi in binding_indices {
        push_binding(
            state,
            bi,
            doc_clock,
            doc,
            bindings,
            sources,
            validators,
            shards,
            writers,
            sealed,
            journal_offsets,
            packed_key,
        )?;
    }
    Ok(())
}

/// Close a transaction: append each shard's remaining documents and return the
/// checkpoint frontier which makes the transaction's documents visible.
///
/// `flushed_lsn` carries every shard's session-local read barrier; a shard
/// receiving no documents carries its prior barrier forward. Block count doesn't
/// affect transaction boundaries: the frontier carries only each shard's final
/// LSN. Each per-(journal, binding) producer's `last_commit` is the
/// transaction's maximum clock, so all of its documents become visible at once.
pub fn finish_txn(
    state: TxnState,
    writers: &mut [ShardWriter],
    sealed: &mut Vec<shuffle::log::writer::SealedSegment>,
    journal_offsets: &HashMap<(String, u16), i64>,
) -> anyhow::Result<shuffle::Frontier> {
    let TxnState {
        entries,
        block_journals,
        frontier_acc,
        ..
    } = state;

    // Write each shard's remaining documents (if any), advancing that shard's
    // session-local read barrier to its LSN. Shards flushed mid-transaction and
    // left empty keep the barrier their last flush set.
    for (shard_index, block) in entries.into_iter().enumerate() {
        if !block.is_empty() {
            writers[shard_index].append_block(&block_journals, block, sealed)?;
        }
    }

    // `frontier_acc` iterates sorted by (journal, binding), satisfying Frontier
    // ordering invariants.
    let journals: Vec<shuffle::JournalFrontier> = frontier_acc
        .into_iter()
        .map(|(key, (last_commit, bytes_read))| {
            // Every accumulated (journal, binding) was offset by its pushes.
            let offset = -journal_offsets[&key];
            let (journal, binding) = key;

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

/// Write one whole binding-tagged transaction and return its checkpoint
/// frontier: a [`TxnState::new`] -> [`push_binding`] -> [`finish_txn`] sequence
/// over `items`, each of which carries the binding it feeds and its clock.
#[allow(clippy::too_many_arguments)]
pub fn write_transaction_for_bindings(
    items: &[(usize, uuid::Clock, &serde_json::Value)],
    bindings: &[shuffle::Binding],
    sources: &[shuffle::Source],
    validators: &mut [doc::Validator],
    shards: &[shuffle::proto::Shard],
    writers: &mut [ShardWriter],
    sealed: &mut Vec<shuffle::log::writer::SealedSegment>,
    journal_offsets: &mut HashMap<(String, u16), i64>,
    packed_key: &mut bytes::BytesMut,
) -> anyhow::Result<shuffle::Frontier> {
    let mut state = TxnState::new(writers.len());

    for &(bi, doc_clock, doc) in items {
        push_binding(
            &mut state,
            bi,
            doc_clock,
            doc,
            bindings,
            sources,
            validators,
            shards,
            writers,
            sealed,
            journal_offsets,
            packed_key,
        )?;
    }

    finish_txn(state, writers, sealed, journal_offsets)
}

/// Write one whole collection-tagged transaction — the `txn_ordinal`-th of the
/// run — and return its checkpoint frontier: a [`TxnState::for_txn`] ->
/// [`push_doc`] -> [`finish_txn`] sequence over its documents.
#[allow(clippy::too_many_arguments)]
pub fn write_transaction(
    transaction: &Transaction,
    bindings: &[shuffle::Binding],
    sources: &[shuffle::Source],
    validators: &mut [doc::Validator],
    collection_bindings: &HashMap<String, Vec<usize>>,
    shards: &[shuffle::proto::Shard],
    writers: &mut [ShardWriter],
    sealed: &mut Vec<shuffle::log::writer::SealedSegment>,
    txn_ordinal: &mut u64,
    journal_offsets: &mut HashMap<(String, u16), i64>,
    packed_key: &mut bytes::BytesMut,
) -> anyhow::Result<shuffle::Frontier> {
    let mut state = TxnState::for_txn(writers.len(), *txn_ordinal);
    *txn_ordinal += 1;

    for (collection, doc) in transaction {
        push_doc(
            &mut state,
            collection,
            doc,
            bindings,
            sources,
            validators,
            collection_bindings,
            shards,
            writers,
            sealed,
            journal_offsets,
            packed_key,
        )?;
    }

    finish_txn(state, writers, sealed, journal_offsets)
}

/// Synthetic journal name for a collection's locally-written documents. The
/// runtime-next consumer ignores the journal name during processing; it is
/// carried only in the checkpoint frontier, where it must match the block's
/// journal for visibility.
fn fixture_journal(collection: &models::Collection) -> String {
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

        let send = |item| frontier_tx.send(item).unwrap();

        // Each request yields the next queued frontier, in order.
        send(FixtureItem::Frontier(frontier(1)));
        send(FixtureItem::Frontier(frontier(2)));
        for expect_lsn in [1u64, 2] {
            src.request_checkpoint();
            let frontier = src.recv_checkpoint().await.unwrap();
            assert_eq!(frontier.encode().flushed_lsn, vec![expect_lsn]);
        }

        // A Boundary leaves this request unanswered (the leader stops via
        // max_transactions). Its `reached` ack fires only now — after both
        // frontiers were delivered.
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
        send(FixtureItem::Boundary {
            reached: Some(reached_tx),
        });
        // A frontier queued *after* the boundary belongs to the next session.
        send(FixtureItem::Frontier(frontier(3)));

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
