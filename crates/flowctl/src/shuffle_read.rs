//! Shared in-process `shuffle` reader for flowctl.
//!
//! Backs flowctl's ad-hoc collection reads (`collections read`, `logs`, and
//! `raw stats`) by hosting an ephemeral, single-shard `shuffle::Service` on a
//! loopback tonic server and draining a `shuffle::proto::Task` — invoking a
//! caller callback for each committed, non-ACK document and, after each
//! checkpoint, an optional checkpoint callback. (`raw preview-next` also uses
//! the `shuffle` crate, but drives its own Session directly rather than through
//! this module.)
//!
//! Reads are non-blocking by default. The Session always tails its journals, so
//! a non-following read decides for itself when "all currently-available content"
//! has been read by first snapshotting a *base set*: it lists the task's journals
//! and, for each, probes where a read would begin once fragments before the read's
//! `not_before` are fast-forwarded (see `list_base_set`). A journal whose probed
//! start is already at its write head has no content to read and is left out. The
//! read then stops once both:
//!  - the reduced Frontier has caught up to every journal's live write head
//!    with no unresolved causal hints (summed cumulative `bytes_behind == 0`),
//!    and
//!  - every base-set journal has been observed in the Frontier
//!
//! If the base set is empty (the task selects no journals, or none with any
//! content) there is nothing to read and the read returns immediately.

use anyhow::Context;
use proto_gazette::{broker, uuid};
use shuffle::log::reader::{Entry, FrontierScan, Reader, Remainder};
use std::collections::{BTreeSet, VecDeque};
use tokio_stream::wrappers::TcpListenerStream;

/// Shard ID and AuthZ subject of flowctl's single ephemeral read shard.
const SHARD_ID: &str = "flowctl-read/0";

/// Drain `task` and print each committed, non-ACK document as a JSON line to
/// stdout. Shared by `collections read`, `logs`, and `raw stats`.
pub async fn read_to_stdout(
    registry: service_kit::Registry,
    task: shuffle::proto::Task,
    factory: gazette::journal::ClientFactory,
    follow: bool,
) -> anyhow::Result<()> {
    use std::io::Write;

    let policy = doc::SerPolicy::noop();
    let mut stdout = std::io::stdout();

    drain_task(
        registry,
        task,
        factory,
        follow,
        |_binding, entry| {
            let mut v = serde_json::to_vec(&policy.on(entry.doc.doc.get()))?;
            v.push(b'\n');
            stdout.write_all(&v)?;
            Ok(())
        },
        |_delta| Ok(()),
    )
    .await
}

/// Drain `task` through an in-process shuffle Service.
///
/// `on_entry` is called for each committed, non-ACK document (with its binding
/// index). `on_checkpoint` is called after each Frontier delta's documents have
/// been drained. When `!follow`, the drain first snapshots the base set of
/// journals to read through (listing + write-head probe); if it's empty the read
/// returns immediately, otherwise the drain ends once the cumulative frontier has
/// caught up to every journal's live head with no unresolved hints AND every
/// base-set journal has been read through. In follow mode it tails until
/// interrupted by Ctrl-C.
pub async fn drain_task(
    registry: service_kit::Registry,
    task: shuffle::proto::Task,
    factory: gazette::journal::ClientFactory,
    follow: bool,
    mut on_entry: impl FnMut(u16, &Entry) -> anyhow::Result<()>,
    mut on_checkpoint: impl FnMut(&shuffle::Frontier) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    // For a non-following read, snapshot the journals (with content) that must be
    // read through. An empty snapshot means there's nothing to read, and no
    // Session to open — one would never checkpoint. `follow` reads have no base
    // set: they tail forever, picking up journals as they're created.
    let base_set = if follow {
        None
    } else {
        let base_set = list_base_set(&task, &factory)
            .await
            .context("listing source journals to read")?;

        if base_set.is_empty() {
            tracing::debug!("task selects no journals with content; nothing to read");
            return Ok(());
        }
        Some(base_set)
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("binding ephemeral shuffle listener")?;
    let peer_endpoint = format!("http://{}", listener.local_addr()?);

    let service = shuffle::Service::new_loopback(peer_endpoint.clone(), factory, registry);
    let server_task = tokio::spawn(
        service
            .clone()
            .build_tonic_server()
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );

    let log_tmp = tempfile::tempdir().context("creating shuffle-log tempdir")?;
    let log_dir = log_tmp.path().to_path_buf();

    let shards = vec![shuffle::proto::Shard {
        id: SHARD_ID.to_string(),
        range: Some(proto_flow::flow::RangeSpec {
            key_begin: 0,
            key_end: u32::MAX,
            r_clock_begin: 0,
            r_clock_end: u32::MAX,
        }),
        endpoint: peer_endpoint.clone(),
        directory: log_dir.to_string_lossy().into_owned(),
    }];

    let mut session =
        shuffle::SessionClient::open(&service, task, shards, shuffle::Frontier::default())
            .await
            .context("opening shuffle Session")?;

    let result = drain_loop(
        &mut session,
        &log_dir,
        base_set,
        &mut on_entry,
        &mut on_checkpoint,
    )
    .await;

    // Close the Session before tearing down so its Slices keep their Log segments
    // (which we may still be scanning) alive until we've finished reading: a
    // tailing Slice holds them open until the Session RPC closes. Best-effort: a
    // drain error supersedes a close error.
    let close = session.close().await;
    server_task.abort();
    drop(log_tmp);

    result.and(close.context("closing shuffle Session"))
}

/// The per-checkpoint scan loop, factored out so `drain_task` can always run its
/// teardown (close, abort, tempdir cleanup) regardless of the outcome.
///
/// `base_set` is `Some` for a non-following read (the journal names it must read
/// through) and `None` for follow mode, which never stops on its own.
async fn drain_loop(
    session: &mut shuffle::SessionClient,
    log_dir: &std::path::Path,
    base_set: Option<BTreeSet<Box<str>>>,
    on_entry: &mut impl FnMut(u16, &Entry) -> anyhow::Result<()>,
    on_checkpoint: &mut impl FnMut(&shuffle::Frontier) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    // Cumulative frontier into which per-checkpoint deltas are reduced, and the
    // single-shard Reader/Remainder state carried across checkpoints.
    let mut frontier = shuffle::Frontier::default();
    let mut shard_state: Option<(Reader, VecDeque<Remainder>)> = None;

    // Base-set journals not yet read through. `None` in follow mode. A
    // CollectionPartitions read has a single binding, so journals are keyed by
    // name alone.
    let mut pending = base_set;

    // Register the Ctrl-C listener ONCE and poll the same future across iterations.
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    loop {
        // Non-following stop condition, checked *before* awaiting so an
        // already-satisfied read (e.g. every base-set journal empty) stops
        // without blocking on a checkpoint that may never arrive:
        //  - the cumulative frontier has caught up to every journal's live write
        //    head with no unresolved causal hints. `bytes_behind` sums each
        //    journal's cumulative `write_head - read_offset`, which is never
        //    negative, so a zero sum means every journal is at its head; AND
        //  - every base-set journal has appeared in a checkpoint. Combined with
        //    the caught-up check, having been read means it has been read through
        //    its probed head (the live head is at or beyond it).
        if let Some(pending) = &pending {
            let (_, _, _, bytes_behind) = frontier.measures();
            if bytes_behind == 0 && frontier.unresolved_hints == 0 && pending.is_empty() {
                tracing::debug!("read through all base-set journals; stopping read");
                return Ok(());
            }
        }

        let delta = tokio::select! {
            biased;
            // Ctrl-C interrupts both a follow tail and a non-blocking drain.
            _ = &mut ctrl_c => {
                tracing::info!("interrupted; stopping read");
                return Ok(());
            }
            result = session.next_checkpoint() => result?,
        };

        let (reader, remainders) = shard_state
            .take()
            .unwrap_or_else(|| (Reader::new(log_dir, 0), VecDeque::new()));
        let mut scan =
            FrontierScan::new(delta.clone(), reader, remainders).context("opening FrontierScan")?;

        while scan.advance_block().context("advancing log scan")? {
            // Honor Ctrl-C between blocks so a large delta can be interrupted
            // mid-drain, not only at checkpoint boundaries.
            if matches!(futures::poll!(&mut ctrl_c), std::task::Poll::Ready(_)) {
                tracing::info!("interrupted; stopping read");
                return Ok(());
            }

            for entry in scan.block_iter() {
                // Skip transaction acknowledgements: they carry no user content.
                if uuid::Flags(entry.meta.flags.to_native()).is_ack() {
                    continue;
                }
                on_entry(entry.meta.binding.to_native(), &entry)?;
            }
        }

        let (_, reader, remainders) = scan.into_parts();
        shard_state = Some((reader, remainders));

        on_checkpoint(&delta)?;

        // Drop base-set journals that progressed in this checkpoint: any journal
        // present in a delta has been read, and the caught-up check above
        // confirms it's been read through its write head.
        if let Some(pending) = &mut pending {
            for jf in &delta.journals {
                pending.remove(jf.journal.as_ref());
            }
        }

        // The Session always tails, so we detect "caught up" client-side rather
        // than wait for an EOF that won't come.
        frontier = frontier.reduce(delta);
    }
}

/// Snapshot the journals a non-following read must read through: list the task's
/// journals and, for each, probe where a read would begin once fragments before
/// the read's `not_before` are fast-forwarded, keeping those that still have
/// content past that bound. The returned set is the read's completion target —
/// once each has been read through to its head, the read is done.
///
/// flowctl only issues `CollectionPartitions` tasks (a single binding), so
/// journals are identified by name alone.
async fn list_base_set(
    task: &shuffle::proto::Task,
    factory: &gazette::journal::ClientFactory,
) -> anyhow::Result<BTreeSet<Box<str>>> {
    let Some(shuffle::proto::task::Task::CollectionPartitions(partitions)) = &task.task else {
        anyhow::bail!("flowctl shuffle reads must use a CollectionPartitions task");
    };
    let collection = partitions
        .collection
        .as_ref()
        .context("task is missing its collection spec")?;
    let partition_selector = partitions
        .partition_selector
        .clone()
        .context("task is missing its partition selector")?;
    let partition_template = collection
        .partition_template
        .as_ref()
        .context("collection is missing its partition template")?;

    // `not_before` lower bound in Unix seconds, passed to the probe as
    // `begin_mod_time`: the broker fast-forwards over fragments that precede it.
    // This mirrors the bound the SliceActor wires into its own reads.
    let begin_mod_time = partitions.not_before.as_ref().map_or(0, |ts| ts.seconds);

    // The factory's AuthZ subject is this read's shard, and its object is the
    // collection's partition-template prefix — matching the clients the
    // SliceActor builds to read these same journals.
    let partition_prefix = format!("{}/", partition_template.name);
    let client = (*factory)(SHARD_ID.to_string(), partition_prefix);

    let response = client
        .list(broker::ListRequest {
            selector: Some(partition_selector),
            ..Default::default()
        })
        .await
        .context("listing source journals")?;

    let mut journals = BTreeSet::new();
    for journal in response.journals {
        let spec = journal.spec.context("listed journal is missing its spec")?;

        // FULL-suspended journals are scaled to zero replicas over an empty
        // fragment index: reads fail with SUSPENDED and the SliceActor's own
        // listing filters them out, so one would never appear in a Frontier.
        // Skip them, mirroring that filter.
        if spec
            .suspend
            .as_ref()
            .is_some_and(|s| s.level == broker::journal_spec::suspend::Level::Full as i32)
        {
            tracing::debug!(%spec.name, "skipping FULL-suspended journal");
            continue;
        }

        // Probe where a read of this journal would begin once fragments before
        // `not_before` are fast-forwarded, along with its current write head.
        // A probed offset already at the write head means all content precedes
        // `not_before`: the read is caught up and the journal may never appear
        // in a Frontier, so it must not join the read-through set.
        let (offset, write_head, _header) = shuffle::slice::read::probe_read_start(
            client.clone(),
            &spec.name,
            "ad-hoc",
            None,
            journal.create_revision,
            0,
            begin_mod_time,
        )
        .await?;

        if offset < write_head {
            journals.insert(spec.name.into_boxed_str());
        } else {
            tracing::debug!(
                %spec.name,
                offset,
                write_head,
                "skipping journal with no new content",
            );
        }
    }
    Ok(journals)
}
