//! The run's own gazette broker, and its single etcd node, to which every
//! task's writes go: collection partitions, ops stats, and ACK intents.
//!
//! Tasks publish through production's `JournalPublisherFactory`, so partition
//! creation, append buffering, journal flow control (`max_append_rate`), and
//! automatic partition splits are all as in production. Reads of source
//! collections still go to production brokers, through the sidecars.
//!
//! The broker stores fragments under a local file root, which the controller
//! reclaims as fragments persist, except for `ops/` journals. Stats documents
//! are tailed from the ops stats journals into `<run-dir>/stats.ndjson`.
//! Partition specs of each task's collections are written to its journals
//! file every sample, which `--resume` and snapshots restore into a later run.

use anyhow::Context;
use proto_gazette::broker;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Prefix of the ops journals into which tasks publish stats documents.
pub const OPS_STATS_PREFIX: &str = "ops/lab/stats";

/// Persisted fragments of non-ops journals older than this are reclaimed.
/// Nothing reads them; the grace only guards a fragment still being written.
const RECLAIM_GRACE: std::time::Duration = std::time::Duration::from_secs(30);

/// A loopback port which is free now, for a process which must be told its
/// port up front. The window in which another process could take it is small.
pub fn free_port() -> anyhow::Result<u16> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").context("finding a free port")?;
    Ok(listener.local_addr()?.port())
}

/// Arguments of a single-node etcd whose data lives in `data_dir`.
pub fn etcd_args(data_dir: &Path, client_port: u16, peer_port: u16) -> Vec<String> {
    let client = format!("http://127.0.0.1:{client_port}");
    let peer = format!("http://127.0.0.1:{peer_port}");
    vec![
        "--name=lab".to_string(),
        format!("--data-dir={}", data_dir.display()),
        format!("--listen-client-urls={client}"),
        format!("--advertise-client-urls={client}"),
        format!("--listen-peer-urls={peer}"),
        format!("--initial-advertise-peer-urls={peer}"),
        format!("--initial-cluster=lab={peer}"),
        "--log-level=warn".to_string(),
    ]
}

/// Arguments of `gazette serve`. It runs without auth keys, which disables
/// AuthN and AuthZ, and maps every fragment store onto `file_root`. As the
/// run's only broker, it bounds the replication of every journal to one.
pub fn broker_args(file_root: &Path, port: u16, etcd_endpoint: &str) -> Vec<String> {
    vec![
        "serve".to_string(),
        "--broker.host=127.0.0.1".to_string(),
        format!("--broker.port={port}"),
        format!("--broker.file-root={}", file_root.display()),
        "--broker.file-only".to_string(),
        "--broker.max-replication=1".to_string(),
        format!("--etcd.address={etcd_endpoint}"),
        "--etcd.prefix=/gazette/lab".to_string(),
        "--log.level=info".to_string(),
    ]
}

/// A journal client of the run's broker.
pub fn client(endpoint: &str) -> gazette::journal::Client {
    crate::journal_client_factory(endpoint.to_string())(String::new(), String::new())
}

/// The ops stats journal of a task: a partition of a lab ops stats
/// collection, named as production's activation names a task's partition.
pub fn stats_journal(task_type: ops::TaskType, task_name: &str) -> broker::JournalSpec {
    let template = broker::JournalSpec {
        name: OPS_STATS_PREFIX.to_string(),
        replication: 1,
        fragment: Some(broker::journal_spec::Fragment {
            length: 1 << 26,
            compression_codec: broker::CompressionCodec::None as i32,
            stores: vec!["file:///".to_string()],
            refresh_interval: Some(std::time::Duration::from_secs(300).into()),
            flush_interval: Some(std::time::Duration::from_secs(300).into()),
            ..Default::default()
        }),
        labels: Some(labels::build_set([
            (labels::COLLECTION, OPS_STATS_PREFIX),
            (labels::CONTENT_TYPE, labels::CONTENT_TYPE_JSON_LINES),
            (labels::MANAGED_BY, labels::MANAGED_BY_FLOW),
        ])),
        flags: broker::journal_spec::Flag::ORdwr as u32,
        ..Default::default()
    };
    activate::ops_partition_spec(task_type, task_name, &template)
}

/// Create each of `specs`, which must not yet exist.
pub async fn create(
    client: &gazette::journal::Client,
    specs: Vec<broker::JournalSpec>,
) -> anyhow::Result<()> {
    if specs.is_empty() {
        return Ok(());
    }
    let changes = specs
        .into_iter()
        .map(|spec| broker::apply_request::Change {
            expect_mod_revision: 0,
            upsert: Some(spec),
            delete: String::new(),
        })
        .collect();

    client
        .apply(broker::ApplyRequest { changes })
        .await
        .context("creating journals")?;
    Ok(())
}

/// Partition specs of a journals file, restored into a later run.
pub fn load_journals(path: &Path) -> anyhow::Result<Vec<broker::JournalSpec>> {
    let content = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_slice(&content).with_context(|| format!("parsing {}", path.display()))
}

/// What the journals sampler writes each sample, for one task.
#[derive(Clone)]
pub struct TaskJournals {
    /// Collections the task writes.
    pub collections: Vec<String>,
    /// Journals file of the task's partition specs.
    pub path: PathBuf,
}

/// Lists the broker's journals into `<samples>/journals.ndjson`, records an
/// event when a collection's partitions change (as a split does), and
/// rewrites each task's journals file.
pub struct JournalsSampler {
    client: gazette::journal::Client,
    events: crate::layout::Events,
    out: std::io::BufWriter<std::fs::File>,
    tasks: Vec<TaskJournals>,
    // Partition names of each collection at the prior sample.
    prior: BTreeMap<String, Vec<String>>,
}

impl JournalsSampler {
    pub fn new(
        client: gazette::journal::Client,
        samples_dir: &Path,
        tasks: Vec<TaskJournals>,
        events: crate::layout::Events,
    ) -> anyhow::Result<Self> {
        let out = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(samples_dir.join("journals.ndjson"))?;
        Ok(Self {
            client,
            events,
            out: std::io::BufWriter::new(out),
            tasks,
            prior: BTreeMap::new(),
        })
    }

    pub async fn run(
        mut self,
        interval: std::time::Duration,
    ) -> anyhow::Result<std::convert::Infallible> {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            self.sample().await?;
        }
    }

    pub async fn sample(&mut self) -> anyhow::Result<()> {
        use std::io::Write;

        let ts = crate::layout::now();
        let listing = self
            .client
            .list(broker::ListRequest::default())
            .await
            .context("listing journals")?;

        let mut by_collection: BTreeMap<String, Vec<broker::JournalSpec>> = BTreeMap::new();
        let mut rows = Vec::new();
        for journal in listing.journals {
            let spec = journal.spec.context("listing is missing a spec")?;
            let collection = labels::expect_one(
                spec.labels.as_ref().context("journal has no labels")?,
                labels::COLLECTION,
            )
            .unwrap_or_default()
            .to_string();

            rows.push(serde_json::json!({
                "name": spec.name, "collection": collection, "modRevision": journal.mod_revision,
            }));
            by_collection.entry(collection).or_default().push(spec);
        }
        writeln!(
            self.out,
            "{}",
            serde_json::json!({"ts": ts, "journals": rows})
        )?;
        self.out.flush()?;

        for (collection, specs) in &by_collection {
            let names: Vec<String> = specs.iter().map(|s| s.name.clone()).collect();
            if self.prior.get(collection).is_some_and(|p| *p != names) {
                self.events.record(
                    "partitionsChanged",
                    serde_json::json!({"collection": collection, "partitions": names.len()}),
                );
            }
            self.prior.insert(collection.clone(), names);
        }
        for task in &self.tasks {
            let specs: Vec<&broker::JournalSpec> = task
                .collections
                .iter()
                .filter_map(|c| by_collection.get(c))
                .flatten()
                .collect();
            crate::layout::write_json_atomic(&task.path, &specs)?;
        }
        Ok(())
    }
}

/// Tail ops stats `journal` from its beginning, appending its whole lines to
/// `stats_file`. Every document of a stats journal is a stats document or an
/// ACK, which readers tell apart (an ACK has no `shard`).
pub async fn tail_stats(
    client: gazette::journal::Client,
    journal: String,
    stats_file: std::sync::Arc<std::sync::Mutex<std::fs::File>>,
) -> anyhow::Result<std::convert::Infallible> {
    use futures::StreamExt;
    use std::io::Write;

    let reads = client.read(broker::ReadRequest {
        journal: journal.clone(),
        offset: 0,
        block: true,
        ..Default::default()
    });
    tokio::pin!(reads);
    let mut partial = Vec::new();

    loop {
        let response = match reads.next().await {
            Some(Ok(response)) => response,
            Some(Err(err)) if err.inner.is_transient() => {
                tracing::warn!(%journal, err = %err.inner, "stats tail failed (will retry)");
                continue;
            }
            Some(Err(err)) => {
                return Err(err.inner).with_context(|| format!("tailing {journal}"));
            }
            None => anyhow::bail!("stats tail of {journal} ended unexpectedly"),
        };
        partial.extend_from_slice(&response.content);

        // Write only whole lines, as several journals' tails share the file.
        let Some(end) = partial.iter().rposition(|b| *b == b'\n') else {
            continue;
        };
        stats_file
            .lock()
            .unwrap()
            .write_all(&partial[..=end])
            .context("writing stats")?;
        partial.drain(..=end);
    }
}

/// Each `interval`, remove persisted fragments under `file_root` which are
/// older than `RECLAIM_GRACE`, except those of `ops/` journals. Nothing reads
/// the lab's collections, and at saturation their fragments would otherwise
/// fill the data root.
pub async fn reclaim_fragments(
    file_root: PathBuf,
    interval: std::time::Duration,
) -> anyhow::Result<std::convert::Infallible> {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        let root = file_root.clone();
        tokio::task::spawn_blocking(move || reclaim_dir(&root, &root.join("ops")))
            .await
            .expect("reclaim doesn't panic")?;
    }
}

fn reclaim_dir(dir: &Path, skip: &Path) -> anyhow::Result<()> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err).with_context(|| format!("reading {}", dir.display())),
    };
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path == skip {
            continue;
        }
        let meta = entry.metadata()?;
        if meta.is_dir() {
            reclaim_dir(&path, skip)?;
        } else if meta.modified()?.elapsed().unwrap_or_default() > RECLAIM_GRACE {
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(err).with_context(|| format!("removing {}", path.display()));
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    #[test]
    fn stats_journal_naming() {
        let spec = super::stats_journal(ops::TaskType::Capture, "acmeCo/lab/pg");
        insta::assert_json_snapshot!(spec);
    }

    #[test]
    fn reclaim_skips_ops_and_fresh_fragments() {
        let root = tempfile::tempdir().unwrap();
        let (ops, data) = (
            root.path().join("ops/lab/stats/frag"),
            root.path().join("acmeCo/lab/pg/pivot=00/frag"),
        );
        for path in [&ops, &data] {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, "x").unwrap();
        }
        let fresh = root.path().join("acmeCo/lab/pg/pivot=00/fresh");
        std::fs::write(&fresh, "x").unwrap();

        let old = std::time::SystemTime::now() - 2 * super::RECLAIM_GRACE;
        for path in [&ops, &data] {
            std::fs::File::options()
                .write(true)
                .open(path)
                .unwrap()
                .set_modified(old)
                .unwrap();
        }
        super::reclaim_dir(root.path(), &root.path().join("ops")).unwrap();

        assert!(ops.exists(), "ops fragments are retained");
        assert!(!data.exists(), "an old fragment is reclaimed");
        assert!(fresh.exists(), "a fresh fragment is retained");
    }
}
