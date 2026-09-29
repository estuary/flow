//! Periodic sampling of a run into `<run-dir>/samples/`, as NDJSON:
//!
//! - `cgroups.ndjson`: each node's CPU, memory, IO, and pressure accounting.
//! - `metrics.ndjson`: each process's `runtime_*`, `shuffle_*`, and `gazette_*`
//!   Prometheus series (histogram buckets excepted), including the broker's.
//! - `threads.ndjson`: each process's CPU time by thread name, which
//!   distinguishes a reactor's shard runtimes (`h1-t0-s000`) from one another.
//!
//! The sampler is deliberately ignorant of what it samples. Deriving measures
//! is the business of `scripts/report.py`.

use std::io::Write;
use std::path::PathBuf;

pub struct Node {
    /// Name within the run's tree, e.g. `h1/reactor`.
    pub name: String,
    pub path: PathBuf,
}

pub struct Process {
    /// e.g. `h1/reactor`.
    pub name: String,
    pub pid: u32,
    /// Prometheus endpoint of the process, if it has one worth sampling.
    pub metrics_url: Option<String>,
}

pub async fn run(
    samples_dir: PathBuf,
    interval: std::time::Duration,
    nodes: Vec<Node>,
    processes: Vec<Process>,
) -> anyhow::Result<std::convert::Infallible> {
    let open = |name: &str| {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(samples_dir.join(name))
            .map(std::io::BufWriter::new)
    };
    let (mut cgroups, mut metrics, mut threads) = (
        open("cgroups.ndjson")?,
        open("metrics.ndjson")?,
        open("threads.ndjson")?,
    );
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(1))
        .build()?;
    let clk_tck = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };

    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;
        let ts = crate::layout::now();

        for node in &nodes {
            let line = serde_json::json!({
                "ts": ts, "node": node.name, "sample": crate::cgroup::sample(&node.path),
            });
            writeln!(cgroups, "{line}")?;
        }

        // Scrape concurrently, so a slow process doesn't skew its peers' timestamps.
        let scrapes = futures::future::join_all(processes.iter().map(|process| {
            let http = &http;
            async move {
                let url = process.metrics_url.as_ref()?;
                let response = http.get(url).send().await.ok()?.error_for_status().ok()?;
                response.text().await.ok()
            }
        }))
        .await;

        for (process, scrape) in processes.iter().zip(scrapes) {
            // A process which isn't answering is observed by other means (the
            // controller's child watcher); here it's simply absent.
            if let Some(body) = scrape {
                let line = serde_json::json!({
                    "ts": ts, "process": process.name, "metrics": parse_metrics(&body),
                });
                writeln!(metrics, "{line}")?;
            }
            if let Some(by_name) = thread_times(process.pid) {
                let line = serde_json::json!({
                    "ts": ts, "process": process.name, "clkTck": clk_tck, "threads": by_name,
                });
                writeln!(threads, "{line}")?;
            }
        }
        cgroups.flush()?;
        metrics.flush()?;
        threads.flush()?;
    }
}

/// Parse Prometheus text exposition into `{"series{labels}": value}`, keeping
/// only the families of the systems under test.
fn parse_metrics(body: &str) -> serde_json::Map<String, serde_json::Value> {
    body.lines()
        .filter(|line| !line.starts_with('#'))
        .filter(|line| {
            ["runtime_", "shuffle_", "gazette_"]
                .iter()
                .any(|prefix| line.starts_with(prefix))
        })
        .filter_map(|line| {
            let (series, value) = line.rsplit_once(' ')?;
            if series.split('{').next()?.ends_with("_bucket") {
                return None;
            }
            let value = value.parse::<f64>().ok()?;
            Some((series.to_string(), serde_json::json!(value)))
        })
        .collect()
}

/// Sum each thread's (utime, stime) clock ticks, grouped by thread name.
fn thread_times(pid: u32) -> Option<std::collections::BTreeMap<String, [u64; 2]>> {
    let mut out = std::collections::BTreeMap::<String, [u64; 2]>::new();

    for task in std::fs::read_dir(format!("/proc/{pid}/task")).ok()? {
        let Ok(task) = task else { continue };
        let Ok(stat) = std::fs::read_to_string(task.path().join("stat")) else {
            continue; // The thread exited.
        };
        // `comm` is parenthesized and may contain spaces: fields follow the last ')'.
        let (Some(open), Some(close)) = (stat.find('('), stat.rfind(')')) else {
            continue;
        };
        let comm = &stat[open + 1..close];
        let fields: Vec<&str> = stat[close + 1..].split_whitespace().collect();
        // Fields 14 and 15 of proc_pid_stat(5), counting `state` as field 3.
        let (Some(utime), Some(stime)) = (
            fields.get(11).and_then(|f| f.parse::<u64>().ok()),
            fields.get(12).and_then(|f| f.parse::<u64>().ok()),
        ) else {
            continue;
        };
        let entry = out.entry(comm.to_string()).or_default();
        entry[0] += utime;
        entry[1] += stime;
    }
    Some(out)
}

#[cfg(test)]
mod test {
    #[test]
    fn parse_prometheus_text() {
        let body = r#"# HELP shuffle_slice_bytes_read bytes
# TYPE shuffle_slice_bytes_read counter
shuffle_slice_bytes_read{shard_id="a/b"} 1234
runtime_leader_transactions{shard_zero="x"} 7
runtime_leader_txn_seconds_bucket{le="1"} 3
tokio_workers 4
"#;
        insta::assert_json_snapshot!(super::parse_metrics(body), @r#"
        {
          "runtime_leader_transactions{shard_zero=\"x\"}": 7.0,
          "shuffle_slice_bytes_read{shard_id=\"a/b\"}": 1234.0
        }
        "#);
    }

    #[test]
    fn thread_times_of_self() {
        let times = super::thread_times(std::process::id()).unwrap();
        assert!(!times.is_empty());
    }
}
