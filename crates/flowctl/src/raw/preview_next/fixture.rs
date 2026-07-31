//! Fixture input for `flowctl raw preview-next --fixture`.
//!
//! This module owns the fixture *file format* and the *session planning* around
//! it. Turning transactions into shuffle-log segments, and relaying the matching
//! checkpoint frontiers to the runtime, is [`runtime_local::segments`]'s job.
//!
//! Fixture format (one JSON value per line, matching legacy `flowctl preview`):
//! - a document:      `["collection/name", { ...document... }]`
//! - a commit marker: `{"commit": true}`
//!
//! Documents between commit markers form one transaction. Each transaction is
//! written as a single log block, and (paired with a collapsed transaction
//! duration window on the preview task spec) commits as exactly one runtime
//! transaction — preserving the 1:1 transaction boundaries of legacy fixture
//! preview. Empty transactions — consecutive commit markers — are deliberate
//! and preserved: connectors-repo fixtures lead with one to drive an initial
//! empty commit cycle, and apply-only tests use a fixture that is a single
//! bare `{"commit": true}` line.
//!
//! ## Per-session segments
//!
//! The runtime-next consumer's log `Reader` is ephemeral: it restarts at the
//! first segment each session and unlinks segments as it reads them — exactly as
//! in production, where each session re-derives its segments from the (durable)
//! source journals starting at the recovered offset. We mirror that: each
//! `--sessions` iteration gets its own directory holding only that session's
//! transactions, as fresh segments numbered from one. Publication clocks
//! increase globally across sessions so the runtime's recovered frontier doesn't
//! re-admit a prior session's documents, but each session's read barrier
//! (`flushed_lsn`) is session-local.
//!
//! ## Streaming fixtures
//!
//! A FIFO (or stdin `-`) fixture cannot be pre-planned: its transaction count
//! is unknown and reads block on the producing writer. [`start_streaming`]
//! instead runs a single unbounded session fed by a spawned feeder task, which
//! incrementally reads lines, writes each transaction as it commits, and relays
//! its frontier — the producer (e.g. a benchmark generator) paces the run. At
//! stream EOF the feeder sends a [`FixtureItem::Boundary`] whose ack
//! fires once every relayed frontier has been delivered, and only then triggers
//! a graceful stop: stopping any earlier would truncate transactions still
//! queued ahead of the consumer.

use anyhow::Context;
use runtime_local::segments::{
    self, FixtureItem, ShardWriter, Transaction, open_shard_writers, write_transaction,
};
use std::collections::HashMap;
use tokio::io::AsyncBufReadExt;

/// A materialized fixture, ready to drive a preview run.
pub struct FixturePlan {
    /// Per-session transaction budgets, bounded by the available fixtures.
    pub session_targets: Vec<u32>,
    /// Per-session shuffle-log directory; the shard for session `i` reads from
    /// `session_dirs[i]` (carried in its `Join.shuffle_directory`).
    pub session_dirs: Vec<String>,
    /// Per-session checkpoint frontiers, in order; fed one-per-NextCheckpoint.
    pub session_frontiers: Vec<Vec<shuffle::Frontier>>,
    /// Retained writers/segments: their files are unlinked on drop, so they must
    /// outlive the consumer's reads (i.e. the whole preview run).
    _keepalive: Keepalive,
}

struct Keepalive {
    _writers: Vec<ShardWriter>,
    _sealed: Vec<shuffle::log::writer::SealedSegment>,
}

/// Parse `path` and write its transactions as shuffle log segments, one
/// per-session directory under `base_dir`. `requested_targets` are the
/// `--sessions` budgets (`0` = unbounded); the returned plan bounds them by the
/// number of fixture transactions. `task` supplies the binding ↔ collection
/// mapping and shuffle key extractors.
pub fn build(
    task: &shuffle::proto::Task,
    path: &std::path::Path,
    base_dir: &std::path::Path,
    requested_targets: &[u32],
    n_shards: u32,
) -> anyhow::Result<FixturePlan> {
    let (bindings, mut validators, collection_bindings) = segments::task_bindings(task)?;

    let mut transactions = parse(path)?;
    // A session bounded by `max_transactions` can't run zero transactions, so
    // an empty fixture file becomes one empty transaction: the session still
    // runs the connector's Apply and one empty commit cycle before stopping.
    if transactions.is_empty() {
        transactions.push(Vec::new());
    }
    let session_targets = session_targets(requested_targets, transactions.len());

    let mut keepalive = Keepalive {
        _writers: Vec::new(),
        _sealed: Vec::new(),
    };
    let mut session_dirs = Vec::with_capacity(session_targets.len());
    let mut session_frontiers = Vec::with_capacity(session_targets.len());

    // Publication clock and per-(journal, binding) committed offsets advance
    // globally across sessions; segment LSNs restart per session. Offsets are
    // tracked per binding to mirror live reads, where each binding of a shared
    // journal independently observes that journal's (single) offset space.
    let shards = segments::full_range_shards(n_shards);
    let mut txn_ordinal = 0u64;
    let mut journal_offsets: HashMap<(String, u16), i64> = HashMap::new();
    let mut packed_key = bytes::BytesMut::new();
    let mut transactions = transactions.into_iter();

    for (session_index, &budget) in session_targets.iter().enumerate() {
        let dir = base_dir.join(format!("{session_index:03}"));
        std::fs::create_dir(&dir)
            .with_context(|| format!("creating fixture session directory {dir:?}"))?;
        let dir = dir.to_string_lossy().into_owned();

        let mut writers = open_shard_writers(std::path::Path::new(&dir), n_shards)?;
        let mut frontiers = Vec::with_capacity(budget as usize);

        for _ in 0..budget {
            let transaction = transactions
                .next()
                .expect("session_targets are bounded by the transaction count");

            frontiers.push(write_transaction(
                &transaction,
                &bindings,
                &mut validators,
                &collection_bindings,
                &shards,
                &mut writers,
                &mut keepalive._sealed,
                &mut txn_ordinal,
                &mut journal_offsets,
                &mut packed_key,
            )?);
        }

        keepalive._writers.extend(writers);
        session_dirs.push(dir);
        session_frontiers.push(frontiers);
    }

    Ok(FixturePlan {
        session_targets,
        session_dirs,
        session_frontiers,
        _keepalive: keepalive,
    })
}

/// Start a streaming fixture: spawn a feeder task that incrementally reads
/// newline-delimited fixture lines from `path` (or stdin, when `None`), writes
/// each transaction as it commits, and relays its frontier. Returns the single
/// session's shuffle-log directory and the feeder's join handle.
///
/// Feeder lifecycle:
/// - At stream EOF — or on an error, such as a malformed line — it sends a
///   `Boundary` whose ack fires once every relayed frontier has been delivered
///   to the consumer, and only then cancels `eof_stop`: a graceful stop which
///   cannot truncate still-queued transactions, nor land mid session-startup
///   (where a Stop is a protocol error). An error surfaces when the caller
///   joins the returned handle after the run.
/// - It retains the log writer and sealed segments until `hold` cancels (the
///   run ended): the consumer unlinks segment files as it reads, and the
///   writer/segment drops tolerate NotFound.
pub fn start_streaming(
    task: &shuffle::proto::Task,
    path: Option<std::path::PathBuf>,
    base_dir: &std::path::Path,
    n_shards: u32,
    frontier_tx: tokio::sync::mpsc::UnboundedSender<FixtureItem>,
    eof_stop: tokio_util::sync::CancellationToken,
    hold: tokio_util::sync::CancellationToken,
) -> anyhow::Result<(String, tokio::task::JoinHandle<anyhow::Result<()>>)> {
    let (bindings, validators, collection_bindings) = segments::task_bindings(task)?;

    // The session reads from its own directory, mirroring the eager per-session
    // layout.
    let dir = base_dir.join("000");
    std::fs::create_dir(&dir)
        .with_context(|| format!("creating fixture session directory {dir:?}"))?;
    let dir = dir.to_string_lossy().into_owned();

    let writers = open_shard_writers(std::path::Path::new(&dir), n_shards)?;

    let handle = tokio::spawn(feed_stream(
        bindings,
        validators,
        collection_bindings,
        path,
        segments::full_range_shards(n_shards),
        writers,
        frontier_tx,
        eof_stop,
        hold,
    ));
    Ok((dir, handle))
}

async fn feed_stream(
    bindings: Vec<shuffle::Binding>,
    mut validators: Vec<doc::Validator>,
    collection_bindings: HashMap<String, Vec<usize>>,
    path: Option<std::path::PathBuf>,
    shards: Vec<shuffle::proto::Shard>,
    mut writers: Vec<ShardWriter>,
    frontier_tx: tokio::sync::mpsc::UnboundedSender<FixtureItem>,
    eof_stop: tokio_util::sync::CancellationToken,
    hold: tokio_util::sync::CancellationToken,
) -> anyhow::Result<()> {
    let mut sealed = Vec::new();
    let result = feed_lines(
        &bindings,
        &mut validators,
        &collection_bindings,
        path,
        &shards,
        &mut writers,
        &mut sealed,
        &frontier_tx,
        &hold,
    )
    .await;

    // Request the graceful stop only once every relayed frontier has been
    // delivered (the Boundary ack): stopping earlier would truncate queued
    // transactions, or interrupt session startup (where a Stop is a protocol
    // error). This applies to errors too — transactions committed before a
    // malformed line still run, and the error surfaces at join time.
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    if frontier_tx
        .send(FixtureItem::Boundary {
            reached: Some(reached_tx),
        })
        .is_ok()
    {
        tokio::select! {
            _ = reached_rx => (),
            () = hold.cancelled() => (), // The run ended some other way.
        }
    }
    eof_stop.cancel();

    // The writer and sealed segments must outlive the consumer's reads.
    () = hold.cancelled().await;
    result
}

/// Incrementally read fixture lines, writing each transaction as it commits and
/// relaying its frontier. Returns at stream EOF, when the run ends (`hold`
/// cancels), or on a stream / fixture error.
async fn feed_lines(
    bindings: &[shuffle::Binding],
    validators: &mut [doc::Validator],
    collection_bindings: &HashMap<String, Vec<usize>>,
    path: Option<std::path::PathBuf>,
    shards: &[shuffle::proto::Shard],
    writers: &mut [ShardWriter],
    sealed: &mut Vec<shuffle::log::writer::SealedSegment>,
    frontier_tx: &tokio::sync::mpsc::UnboundedSender<FixtureItem>,
    hold: &tokio_util::sync::CancellationToken,
) -> anyhow::Result<()> {
    // Opening a FIFO blocks until its writer connects (stdin is immediate).
    let reader: std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>> = match &path {
        Some(path) => {
            let file = tokio::select! {
                biased;
                () = hold.cancelled() => return Ok(()),
                file = tokio::fs::File::open(path) => {
                    file.with_context(|| format!("opening fixture stream {path:?}"))?
                }
            };
            Box::pin(file)
        }
        None => Box::pin(tokio::io::stdin()),
    };
    let mut lines = tokio::io::BufReader::new(reader).lines();

    let mut txn_ordinal = 0u64;
    let mut journal_offsets: HashMap<(String, u16), i64> = HashMap::new();
    let mut packed_key = bytes::BytesMut::new();

    let mut current: Transaction = Vec::new();
    let mut committed = 0usize;
    let mut lineno = 0usize;

    loop {
        // `hold` cancelling means the run ended out from under us (Ctrl-C or
        // timeout): abandon the stream rather than waiting on its writer.
        let line = tokio::select! {
            biased;
            () = hold.cancelled() => return Ok(()),
            line = lines.next_line() => line.context("reading fixture stream")?,
        };
        let Some(line) = line else {
            break; // EOF: the stream's writer closed.
        };
        lineno += 1;

        match parse_line(&line, lineno)? {
            None => (),
            Some(Line::Doc(collection, doc)) => current.push((collection, doc)),
            Some(Line::Commit) => {
                let frontier = write_transaction(
                    &std::mem::take(&mut current),
                    bindings,
                    validators,
                    collection_bindings,
                    shards,
                    writers,
                    sealed,
                    &mut txn_ordinal,
                    &mut journal_offsets,
                    &mut packed_key,
                )?;
                committed += 1;
                if frontier_tx.send(FixtureItem::Frontier(frontier)).is_err() {
                    return Ok(()); // The consumer went away.
                }
            }
        }
    }

    // Trailing documents without a final commit marker form a final
    // transaction, and an entirely-empty stream still runs one empty
    // transaction (the connector's Apply and one empty commit cycle) — both
    // mirroring eager parsing.
    if !current.is_empty() || committed == 0 {
        let frontier = write_transaction(
            &current,
            bindings,
            validators,
            collection_bindings,
            shards,
            writers,
            sealed,
            &mut txn_ordinal,
            &mut journal_offsets,
            &mut packed_key,
        )?;
        let _ = frontier_tx.send(FixtureItem::Frontier(frontier));
    }
    Ok(())
}

/// Bound each requested session's transaction target by the fixtures still
/// unconsumed. An "unbounded" request (`0`) consumes the remainder; sessions past
/// exhaustion are dropped. Each session then ends cleanly via its
/// `max_transactions` limit once its fixtures are processed.
fn session_targets(requested: &[u32], txn_count: usize) -> Vec<u32> {
    let mut remaining = txn_count;
    let mut out = Vec::with_capacity(requested.len());
    for &target in requested {
        if remaining == 0 {
            break;
        }
        let take = if target == 0 {
            remaining
        } else {
            (target as usize).min(remaining)
        };
        out.push(take as u32);
        remaining -= take;
    }
    out
}

/// Read and parse a fixture file into transactions.
fn parse(path: &std::path::Path) -> anyhow::Result<Vec<Transaction>> {
    let content =
        std::fs::read_to_string(path).with_context(|| format!("reading fixture file {path:?}"))?;
    parse_content(&content)
}

/// Parse fixture content into transactions, splitting on `{"commit": true}`
/// lines. Trailing documents without a final commit marker form a final
/// transaction.
fn parse_content(content: &str) -> anyhow::Result<Vec<Transaction>> {
    let mut transactions: Vec<Transaction> = Vec::new();
    let mut current: Transaction = Vec::new();

    for (lineno, line) in content.lines().enumerate() {
        match parse_line(line, lineno + 1)? {
            None => continue,
            Some(Line::Commit) => transactions.push(std::mem::take(&mut current)),
            Some(Line::Doc(collection, doc)) => current.push((collection, doc)),
        }
    }

    if !current.is_empty() {
        transactions.push(current);
    }

    Ok(transactions)
}

/// One parsed fixture line: a transaction boundary or a sourced document.
enum Line {
    Commit,
    Doc(String, serde_json::Value),
}

/// Parse a single fixture line (`None` for blank lines); `lineno` is 1-based.
fn parse_line(line: &str, lineno: usize) -> anyhow::Result<Option<Line>> {
    let line = line.trim();
    if line.is_empty() {
        return Ok(None);
    }
    if is_commit_line(line) {
        return Ok(Some(Line::Commit));
    }
    let (collection, doc): (String, serde_json::Value) = serde_json::from_str(line)
        .with_context(|| format!("fixture line {lineno} is not [collection, document]: {line}"))?;
    Ok(Some(Line::Doc(collection, doc)))
}

/// True if `line` is a `{"commit": true}` transaction boundary marker.
fn is_commit_line(line: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(line)
        .ok()
        .as_ref()
        .and_then(|v| v.as_object())
        .and_then(|o| o.get("commit"))
        .and_then(|c| c.as_bool())
        .unwrap_or(false)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_session_targets() {
        // (requested, txn_count) => expected, where 0 means "unbounded".
        // Unbounded consumes the remainder.
        assert_eq!(session_targets(&[0], 3), vec![3]);
        // Bounded sessions pass through when they fit.
        assert_eq!(session_targets(&[2, 1], 3), vec![2, 1]);
        // A trailing unbounded session takes the remainder; exhausted ones drop.
        assert_eq!(session_targets(&[2, 1, 0], 3), vec![2, 1]);
        assert_eq!(session_targets(&[1, 0], 3), vec![1, 2]);
        // A bounded request larger than what's left is capped.
        assert_eq!(session_targets(&[5], 3), vec![3]);
        assert_eq!(session_targets(&[2, 5], 3), vec![2, 1]);
        // Sessions beyond exhaustion are dropped.
        assert_eq!(session_targets(&[1, 1, 1, 1], 2), vec![1, 1]);
        // No fixtures: no sessions.
        assert_eq!(session_targets(&[0], 0), Vec::<u32>::new());
    }

    #[test]
    fn test_parse_content() {
        let content = "\
[\"a/coll\", {\"k\": 1}]
[\"b/coll\", {\"k\": 2}]
{\"commit\": true}

[\"a/coll\", {\"k\": 3}]
{\"commit\": true}
[\"a/coll\", {\"k\": 4}]
";
        let txns = parse_content(content).unwrap();
        // Three transactions: two committed, plus a trailing un-committed one.
        assert_eq!(txns.len(), 3);
        assert_eq!(txns[0].len(), 2);
        assert_eq!(txns[0][0].0, "a/coll");
        assert_eq!(txns[1].len(), 1);
        assert_eq!(txns[2].len(), 1);
        assert_eq!(txns[2][0].1, serde_json::json!({"k": 4}));
    }

    /// A Task with no bindings: fixture documents are skipped (no collection is
    /// a source), but the streaming feeder's transaction cadence — frontiers,
    /// the EOF Boundary ack, and the graceful stop — is fully exercised.
    fn empty_task() -> shuffle::proto::Task {
        shuffle::proto::Task {
            task: Some(shuffle::proto::task::Task::Materialization(
                Default::default(),
            )),
        }
    }

    struct StreamHarness {
        _tmp: tempfile::TempDir,
        frontier_rx: tokio::sync::mpsc::UnboundedReceiver<FixtureItem>,
        eof_stop: tokio_util::sync::CancellationToken,
        hold: tokio_util::sync::CancellationToken,
        feeder: tokio::task::JoinHandle<anyhow::Result<()>>,
    }

    fn start_stream_harness(content: &str) -> StreamHarness {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("stream");
        std::fs::write(&path, content).unwrap();

        let (frontier_tx, frontier_rx) = tokio::sync::mpsc::unbounded_channel();
        let eof_stop = tokio_util::sync::CancellationToken::new();
        let hold = tokio_util::sync::CancellationToken::new();

        let (dir, feeder) = start_streaming(
            &empty_task(),
            Some(path),
            tmp.path(),
            1,
            frontier_tx,
            eof_stop.clone(),
            hold.clone(),
        )
        .unwrap();
        assert!(dir.ends_with("000"));

        StreamHarness {
            _tmp: tmp,
            frontier_rx,
            eof_stop,
            hold,
            feeder,
        }
    }

    #[tokio::test]
    async fn test_streaming_cadence_and_eof_stop() {
        let mut h = start_stream_harness(
            "[\"a/coll\", {\"k\": 1}]\n{\"commit\": true}\n{\"commit\": true}\n[\"a/coll\", {\"k\": 2}]\n",
        );

        // Two committed transactions, plus the trailing document's final one.
        for _ in 0..3 {
            let item = h.frontier_rx.recv().await.unwrap();
            assert!(matches!(item, FixtureItem::Frontier(_)));
        }

        // EOF: a Boundary whose ack (fired by the fixture source once every
        // prior frontier was delivered) triggers the graceful stop.
        let Some(FixtureItem::Boundary { reached: Some(ack) }) = h.frontier_rx.recv().await else {
            panic!("expected an acked Boundary at EOF");
        };
        assert!(!h.eof_stop.is_cancelled());
        ack.send(()).unwrap();
        h.eof_stop.cancelled().await;

        // Releasing the hold lets the feeder drop its writer and exit cleanly.
        h.hold.cancel();
        h.feeder.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_streaming_empty_stream() {
        let mut h = start_stream_harness("");

        // An entirely-empty stream still runs one empty transaction.
        let item = h.frontier_rx.recv().await.unwrap();
        assert!(matches!(item, FixtureItem::Frontier(_)));
        let item = h.frontier_rx.recv().await.unwrap();
        assert!(matches!(item, FixtureItem::Boundary { .. }));

        h.hold.cancel();
        h.feeder.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_streaming_malformed_line() {
        let mut h = start_stream_harness("not json\n");

        // An error still performs the Boundary handshake — so the stop can't
        // land mid session-startup — and surfaces its parse error when joined.
        let Some(FixtureItem::Boundary { reached: Some(ack) }) = h.frontier_rx.recv().await else {
            panic!("expected an acked Boundary on error");
        };
        ack.send(()).unwrap();
        h.eof_stop.cancelled().await;

        h.hold.cancel();
        let err = h.feeder.await.unwrap().unwrap_err();
        assert!(format!("{err:#}").contains("fixture line 1"), "{err:#}");
    }

    /// Drive the feeder through a real FIFO: a frontier arrives while the
    /// writer still holds the pipe open (proving incremental reads), and
    /// closing the writer produces EOF.
    #[cfg(unix)]
    #[tokio::test]
    async fn test_streaming_fifo() {
        use tokio::io::AsyncWriteExt;

        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("fifo");
        assert!(
            std::process::Command::new("mkfifo")
                .arg(&path)
                .status()
                .unwrap()
                .success()
        );

        let (frontier_tx, mut frontier_rx) = tokio::sync::mpsc::unbounded_channel();
        let eof_stop = tokio_util::sync::CancellationToken::new();
        let hold = tokio_util::sync::CancellationToken::new();

        let (_dir, feeder) = start_streaming(
            &empty_task(),
            Some(path.clone()),
            tmp.path(),
            1,
            frontier_tx,
            eof_stop.clone(),
            hold.clone(),
        )
        .unwrap();

        // Opening the write end rendezvouses with the feeder's read-end open.
        let mut pipe = tokio::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .await
            .unwrap();

        pipe.write_all(b"[\"a/coll\", {\"k\": 1}]\n{\"commit\": true}\n")
            .await
            .unwrap();
        pipe.flush().await.unwrap();

        // The transaction's frontier arrives while the pipe remains open.
        let item = frontier_rx.recv().await.unwrap();
        assert!(matches!(item, FixtureItem::Frontier(_)));

        drop(pipe); // EOF.

        let Some(FixtureItem::Boundary { reached: Some(ack) }) = frontier_rx.recv().await else {
            panic!("expected an acked Boundary at EOF");
        };
        ack.send(()).unwrap();
        eof_stop.cancelled().await;

        hold.cancel();
        feeder.await.unwrap().unwrap();
    }

    #[test]
    fn test_is_commit_line() {
        assert!(is_commit_line(r#"{"commit": true}"#));
        assert!(!is_commit_line(r#"{"commit": false}"#));
        assert!(!is_commit_line(r#"["a/coll", {"commit": true}]"#));
        assert!(!is_commit_line(r#"{"other": true}"#));
        assert!(!is_commit_line("not json"));
    }
}
