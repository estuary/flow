use crate::cluster::{self, Cluster};

use futures::stream::{Stream, StreamExt, TryStreamExt};
use protocol::{consumer, protocol as broker};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

pub struct Local {
    pub cluster: Cluster,
    consumer: tokio::process::Child,
    etcd: tokio::process::Child,
    gazette: tokio::process::Child,
    ingester: tokio::process::Child,
    lambda_js: tokio::process::Child,

    _dir: tempfile::TempDir, // Held for Drop side-effects.
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("local development process I/O error")]
    IO(#[from] std::io::Error),
    #[error("task join error")]
    Join(#[from] tokio::task::JoinError),
    #[error("cluster RPC error")]
    Cluster(#[from] cluster::Error),
    #[error("system error")]
    Nix(#[from] nix::Error),
}

impl Local {
    pub async fn start(
        temp_dir: tempfile::TempDir,
        package_dir: &Path,
        catalog_url: &url::Url,
        gazette_port: u16,
        ingester_port: u16,
        consumer_port: u16,
    ) -> Result<Local, Error> {
        std::fs::create_dir(temp_dir.path().join("fragments"))?;

        // Start `etcd`.
        let i_dir = temp_dir.path().to_owned();
        let (etcd, etcd_logs) = Local::spawn("etcd", move |cmd| {
            cmd.args(&[
                "--listen-peer-urls",
                "unix://peer.sock:0",
                "--listen-client-urls",
                "unix://client.sock:0",
                "--advertise-client-urls",
                "unix://client.sock:0",
            ])
            .env("ETCD_LOG_LEVEL", "error")
            .env("ETCD_LOGGER", "zap")
            .current_dir(i_dir);
        })?;
        tokio::spawn(etcd_logs.for_each(|_| async {}));

        // Start `gazette`.
        let i_dir = temp_dir.path().to_owned();
        let (gazette, gazette_logs) = Local::spawn("gazette", move |cmd| {
            cmd.args(&[
                "--etcd.address",
                "unix://client.sock:0",
                "--broker.file-root",
                "fragments",
                "--broker.port",
                &format!("{}", gazette_port),
                "--log.format",
                "json",
                "serve",
            ])
            .current_dir(i_dir);
        })?;

        // Start `npm run develop`.
        let (i_dir, i_uds) = (
            package_dir.to_owned(),
            temp_dir.path().join("lambda-uds-js"),
        );
        let (lambda_js, lambda_js_logs) = Local::spawn("npm", move |cmd| {
            cmd.args(&["run", "develop"])
                .env("SOCKET_PATH", i_uds)
                .current_dir(i_dir);
        })?;

        // We must block for the broker address before we may continue.
        let (broker_address, gazette_logs) =
            Self::extract_endpoint(gazette_logs, "starting broker").await;
        tokio::spawn(gazette_logs.for_each(|_| async {}));

        // Start `flow-ingester`.
        let (i_dir, i_broker_address, i_catalog_url) = (
            temp_dir.path().to_owned(),
            broker_address.clone(),
            catalog_url.clone(),
        );
        let (ingester, ingester_logs) = Local::spawn("flow-ingester", move |cmd| {
            cmd.args(&[
                "--broker.address",
                &i_broker_address,
                "--broker.cache.size",
                "256",
                "--etcd.address",
                "unix://client.sock:0",
                "--ingest.catalog",
                // TODO(johnny): Pass as URL ?
                i_catalog_url.path(),
                "--ingest.port",
                &format!("{}", ingester_port),
                "--log.format",
                "json",
                "serve",
            ])
            .current_dir(i_dir);
        })?;

        // Start `flow-consumer`.
        let (i_dir, i_broker_address) = (temp_dir.path().to_owned(), broker_address.clone());
        let (consumer, logs) = Local::spawn("flow-consumer", move |cmd| {
            cmd.args(&[
                "--broker.address",
                &i_broker_address,
                "--broker.cache.size",
                "256",
                "--broker.file-root",
                "fragments",
                "--consumer.port",
                &format!("{}", consumer_port),
                "--etcd.address",
                "unix://client.sock:0",
                "--flow.lambda-uds-js",
                "lambda-uds-js",
                "--log.format",
                "json",
                "serve",
            ])
            .current_dir(i_dir);
        })?;

        // Block for ready notifications from remaining components.
        let lambda_js_logs = Self::extract_ready(lambda_js_logs).await;
        let (consumer_address, consumer_logs) =
            Self::extract_endpoint(logs, "starting consumer").await;
        let (ingester_address, ingester_logs) =
            Self::extract_endpoint(ingester_logs, "starting flow-ingester").await;

        tokio::spawn(lambda_js_logs.for_each(|_| async {}));
        tokio::spawn(ingester_logs.for_each(|_| async {}));
        tokio::spawn(consumer_logs.for_each(|_| async {}));

        Ok(Local {
            cluster: Cluster {
                broker_address,
                consumer_address,
                ingester_address,
            },
            _dir: temp_dir,
            etcd,
            gazette,
            ingester,
            consumer,
            lambda_js,
        })
    }

    fn spawn<F>(
        target: &str,
        details: F,
    ) -> std::io::Result<(
        tokio::process::Child,
        impl Stream<Item = std::io::Result<Log>> + Send + Sync,
    )>
    where
        F: FnOnce(&mut tokio::process::Command),
    {
        let mut cmd = Command::new(target);
        cmd.stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        details(&mut cmd);

        log::info!("starting {}: {:?}", target, cmd);
        let mut child = cmd.spawn()?;

        let logs = Log::stream(target, &mut child).inspect(proxy_log);
        Ok((child, logs))
    }

    async fn extract_ready<S>(mut s: S) -> S
    where
        S: Stream<Item = std::io::Result<Log>> + Unpin,
    {
        while let Some(l) = s.next().await {
            tracing::info!(?l, "HERE");
            match &l {
                Ok(Log::Unstructured(_, log)) if log == "READY" => {
                    tracing::info!("found needle");
                    return s;
                }
                _ => {}
            }
        }
        s
    }

    async fn extract_endpoint<S>(mut s: S, needle: &str) -> (String, S)
    where
        S: Stream<Item = std::io::Result<Log>> + Unpin,
    {
        while let Some(l) = s.next().await {
            match &l {
                Ok(Log::Structured(_, slog)) if slog.msg == needle => {
                    if let Some(Value::String(ep)) = slog.additional.get("endpoint") {
                        log::debug!("found {:?}; extracted endpoint {:?}", needle, ep);
                        return (ep.clone(), s);
                    }
                }
                _ => {}
            }
        }
        ("".to_owned(), s)
    }

    pub async fn stop(mut self) -> Result<(), Error> {
        log::info!("stopping local runtime");

        // Remove all ShardSpecs.
        let shards = self.cluster.list_shards(None).await?;
        let req = consumer::ApplyRequest {
            changes: shards
                .shards
                .into_iter()
                .map(|s| consumer::apply_request::Change {
                    expect_mod_revision: s.mod_revision,
                    delete: s.spec.map(|s| s.id).unwrap_or_default(),
                    upsert: None,
                })
                .collect::<Vec<_>>(),
            extension: Vec::new(),
        };
        self.cluster.apply_shards(req).await?;

        // Remove all JournalSpecs.
        let journals = self.cluster.list_journals(None).await?;
        let req = broker::ApplyRequest {
            changes: journals
                .journals
                .into_iter()
                .map(|j| broker::apply_request::Change {
                    expect_mod_revision: j.mod_revision,
                    delete: j.spec.map(|j| j.name).unwrap_or_default(),
                    upsert: None,
                })
                .collect::<Vec<_>>(),
        };
        self.cluster.apply_journals(req).await?;

        for (name, child) in &mut [
            ("consumer", &mut self.consumer),
            ("ingester", &mut self.ingester),
            ("gazette", &mut self.gazette),
            ("etcd", &mut self.etcd),
            ("npm", &mut self.lambda_js),
        ] {
            nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(child.id() as i32),
                nix::sys::signal::Signal::SIGTERM,
            )?;
            log::info!("{} exited: {}", name, child.await?);
        }
        Ok(())
    }
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "lowercase")]
enum LogLevel {
    Debug,
    Info,
    Warning,
    Error,
    Fatal,
}

#[derive(Deserialize, Serialize, Debug)]
struct StructuredLog {
    level: LogLevel,
    time: chrono::DateTime<chrono::Utc>,
    msg: String,
    err: Option<String>,

    #[serde(flatten)]
    additional: BTreeMap<String, Value>,
}

impl std::fmt::Display for StructuredLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&serde_json::to_string(self).unwrap())
    }
}

#[derive(Debug)]
enum Log {
    Structured(String, StructuredLog),
    Unstructured(String, String),
}

impl Log {
    fn parse(source: String, line: String) -> Log {
        if let Ok(structured) = serde_json::from_str::<StructuredLog>(&line) {
            Log::Structured(source, structured)
        } else {
            Log::Unstructured(source, line)
        }
    }

    pub fn stream(
        name: &str,
        child: &mut tokio::process::Child,
    ) -> impl Stream<Item = std::io::Result<Log>> {
        let name = format!("runtime::{}<{}>", name, child.id());

        BufReader::new(child.stderr.take().unwrap())
            .lines()
            .map_ok(move |l| Log::parse(name.clone(), l))
    }
}

fn proxy_log(log: &std::io::Result<Log>) {
    match log {
        Err(err) => {
            log::error!("failed to read subprocess log: {}", err);
        }
        Ok(Log::Structured(source, l)) => {
            let lvl = match l.level {
                LogLevel::Debug => log::Level::Debug,
                LogLevel::Info => log::Level::Info,
                LogLevel::Warning => log::Level::Warn,
                LogLevel::Error | LogLevel::Fatal => log::Level::Error,
            };
            log::log!(target: &source, lvl, "{}", l);
        }
        Ok(Log::Unstructured(source, l)) => {
            log::log!(target: &source, log::Level::Info, "{}", l);
        }
    }
}
