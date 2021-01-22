use crate::cluster::{self, Cluster};

use futures::stream::{Stream, StreamExt, TryStreamExt};
use protocol::{consumer, protocol as broker};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

pub struct Local {
    pub cluster: Cluster,
    _dir: tempfile::TempDir,

    etcd: tokio::process::Child,
    gazette: tokio::process::Child,
    ingester: tokio::process::Child,
    consumer: tokio::process::Child,
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
        gazette_port: u16,
        ingester_port: u16,
        consumer_port: u16,
        catalog_path: &str,
    ) -> Result<Local, Error> {
        let dir = tempfile::TempDir::new()?;
        std::fs::create_dir(dir.path().join("fragments"))?;
        log::info!("using local runtime directory: {:?}", &dir);

        // Start `etcd`.
        let i_dir = dir.path().to_owned();
        let (etcd, logs) = Local::spawn("etcd", move |cmd| {
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
        tokio::spawn(logs.for_each(|_| async {}));

        // Start `gazette`.
        let i_dir = dir.path().to_owned();
        let (gazette, logs) = Local::spawn("gazette", move |cmd| {
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

        let (broker_address, logs) = Self::extract_endpoint(logs, "starting broker").await;
        tokio::spawn(logs.for_each(|_| async {}));

        // Start `flow-ingester`.
        let (i_dir, i_broker_address, i_catalog_path) = (
            dir.path().to_owned(),
            broker_address.clone(),
            catalog_path.to_owned(),
        );
        let (ingester, logs) = Local::spawn("flow-ingester", move |cmd| {
            cmd.args(&[
                "--broker.address",
                &i_broker_address,
                "--broker.cache.size",
                "256",
                "--etcd.address",
                "unix://client.sock:0",
                "--ingest.catalog",
                &i_catalog_path,
                "--ingest.port",
                &format!("{}", ingester_port),
                "--log.format",
                "json",
                "serve",
            ])
            .current_dir(i_dir);
        })?;

        let (ingester_address, logs) = Self::extract_endpoint(logs, "starting flow-ingester").await;
        tokio::spawn(logs.for_each(|_| async {}));

        // Start `flow-consumer`.
        let (i_dir, i_broker_address) = (dir.path().to_owned(), broker_address.clone());
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
                "--log.format",
                "json",
                "serve",
            ])
            .current_dir(i_dir);
        })?;

        let (consumer_address, logs) = Self::extract_endpoint(logs, "starting consumer").await;
        tokio::spawn(logs.for_each(|_| async {}));

        Ok(Local {
            cluster: Cluster {
                broker_address,
                consumer_address,
                ingester_address,
            },
            _dir: dir,
            etcd,
            gazette,
            ingester,
            consumer,
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
        ] {
            // if id() returns None, then it is because the process has already been awaited.
            // It is expected that it should always return Some here, but seeing as we're just
            // trying to stop things, we won't treat it as an error condition.
            if let Some(pid) = child.id() {
                nix::sys::signal::kill(
                    nix::unistd::Pid::from_raw(pid as i32),
                    nix::sys::signal::Signal::SIGTERM,
                )?;
                log::info!("{} exited: {}", name, child.wait().await?);
            } else {
                log::warn!("unable to obtain PID of child: {}. This is likely because the process has already exited", name);
            }
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
        let name = format!("runtime::{}<{}>", name, child.id().unwrap_or_default());

        let lines = BufReader::new(child.stderr.take().unwrap()).lines();
        tokio_stream::wrappers::LinesStream::new(lines).map_ok(move |l| Log::parse(name.clone(), l))
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
