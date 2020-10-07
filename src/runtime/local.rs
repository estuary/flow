use super::Cluster;
use futures::stream::StreamExt;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

pub struct Local {
    pub cluster: Cluster,
    _dir: tempfile::TempDir,

    etcd: tokio::process::Child,
    broker: tokio::process::Child,
    ingester: tokio::process::Child,
    consumer: tokio::process::Child,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("local development process I/O error")]
    IO(#[from] std::io::Error),
    #[error("task join error")]
    Join(#[from] tokio::task::JoinError),
}

impl Local {
    pub async fn start(
        broker_port: u16,
        ingester_port: u16,
        consumer_port: u16,
        catalog_path: &str,
    ) -> Result<Local, Error> {
        let broker_address = format!("http://localhost:{}", broker_port);
        let ingester_address = format!("http://localhost:{}", ingester_port);
        let consumer_address = format!("http://localhost:{}", consumer_port);

        let dir = tempfile::TempDir::new()?;
        std::fs::create_dir(dir.path().join("fragments"))?;
        log::info!("using local runtime directory: {:?}", &dir);

        let mut etcd = Command::new("etcd");
        etcd.args(&[
            "--listen-peer-urls",
            "unix://peer.sock:0",
            "--listen-client-urls",
            "unix://client.sock:0",
            "--advertise-client-urls",
            "unix://client.sock:0",
        ])
        .current_dir(&dir)
        .env("ETCD_LOG_LEVEL", "error")
        .env("ETCD_LOGGER", "zap")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

        log::info!("starting etcd: {:?}", etcd);
        let mut etcd = etcd.spawn()?;

        tokio::spawn(pipe_logs(
            "etcd".to_owned(),
            etcd.stdout.take().unwrap(),
            etcd.stderr.take().unwrap(),
        ));

        let mut broker = Command::new("gazette");
        broker
            .args(&[
                "--etcd.address",
                "unix://client.sock:0",
                "--broker.file-root",
                "fragments",
                "--broker.port",
                &format!("{}", broker_port),
                "--log.format",
                "json",
                "serve",
            ])
            .current_dir(&dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        log::info!("starting gazette: {:?}", broker);
        let mut broker = broker.spawn()?;

        tokio::spawn(pipe_logs(
            "gazette".to_owned(),
            broker.stdout.take().unwrap(),
            broker.stderr.take().unwrap(),
        ));

        let mut ingester = Command::new("flow-ingester");
        ingester
            .args(&[
                "--etcd.address",
                "unix://client.sock:0",
                "--broker.address",
                &broker_address,
                "--ingest.port",
                &format!("{}", ingester_port),
                "--ingest.catalog",
                catalog_path,
                "--log.format",
                "json",
                "serve",
            ])
            .current_dir(&dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        log::info!("starting ingester: {:?}", ingester);
        let mut ingester = ingester.spawn()?;

        tokio::spawn(pipe_logs(
            "ingester".to_owned(),
            ingester.stdout.take().unwrap(),
            ingester.stderr.take().unwrap(),
        ));

        let mut consumer = Command::new("flow-consumer");
        consumer
            .args(&[
                "--etcd.address",
                "unix://client.sock:0",
                "--broker.address",
                &broker_address,
                "--consumer.port",
                &format!("{}", consumer_port),
                "--log.format",
                "json",
                "serve",
            ])
            .current_dir(&dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        log::info!("starting consumer: {:?}", consumer);
        let mut consumer = consumer.spawn()?;

        tokio::spawn(pipe_logs(
            "consumer".to_owned(),
            consumer.stdout.take().unwrap(),
            consumer.stderr.take().unwrap(),
        ));

        // TODO: Hacky delay to allow processes to start, before we return.
        // We should instead be reading and awaiting logs which indicate readiness from each component.
        // Bonus is this can let us pluck out bound ports if the argument port is zero (as makes sense for tests).
        tokio::time::delay_for(std::time::Duration::from_secs(2)).await;

        Ok(Local {
            cluster: Cluster {
                broker_address,
                consumer_address,
                ingester_address,
            },
            _dir: dir,
            etcd,
            broker,
            ingester,
            consumer,
        })
    }

    pub async fn stop(mut self) -> Result<(), Error> {
        log::info!("stopping local runtime");

        self.consumer.kill()?;
        let status = self.consumer.await?;
        log::info!("consumer exited: {}", status);

        self.ingester.kill()?;
        let status = self.ingester.await?;
        log::info!("ingester exited: {}", status);

        self.broker.kill()?;
        let status = self.broker.await?;
        log::info!("gazette exited: {}", status);

        self.etcd.kill()?;
        let status = self.etcd.await?;
        log::info!("etcd exited: {}", status);

        Ok(())
    }
}

async fn pipe_logs<O, E>(name: String, stdout: O, stderr: E)
where
    O: tokio::io::AsyncRead + Unpin,
    E: tokio::io::AsyncRead + Unpin,
{
    let mut out = BufReader::new(stdout).lines().fuse();
    let mut err = BufReader::new(stderr).lines().fuse();

    // stderr logs are structured JSON (unless there's a panic); we should attempt to
    // parse them and do something intelligent (eg, extract log level).
    loop {
        futures::select! {
            l = out.select_next_some() => {
                log::info!("{}:out {}", name, l.unwrap());
            }
            l = err.select_next_some() => {
                log::info!("{}:err {}", name, l.unwrap());
            }
            complete => break,
        }
    }
}
