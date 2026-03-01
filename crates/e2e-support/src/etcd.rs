/// A managed etcd process using Unix domain sockets in a temporary directory.
pub struct EtcdInstance {
    pub process: async_process::Child,
    pub tempdir: tempfile::TempDir,
}

impl EtcdInstance {
    /// Start an etcd process with UDS transport in a new temporary directory.
    pub async fn start() -> anyhow::Result<Self> {
        let tempdir = tempfile::TempDir::new()?;

        let process: async_process::Child = async_process::Command::new("etcd")
            .args([
                "--advertise-client-urls",
                "unix://client.sock:0",
                "--listen-client-urls",
                "unix://client.sock:0",
                "--listen-peer-urls",
                "unix://peer.sock:0",
                "--data-dir",
            ])
            .arg(tempdir.path().join("etcd.data"))
            .args(["--log-level", "error", "--logger", "zap", "--name", "test"])
            .current_dir(tempdir.path())
            .stdout(async_process::Stdio::inherit())
            .stderr(async_process::Stdio::inherit())
            .spawn()
            .map_err(|err| anyhow::anyhow!("failed to spawn etcd (is it on PATH?): {err}"))?
            .into();

        // etcd creates the socket file literally as "client.sock:0" on disk.
        let sock = tempdir.path().join("client.sock:0");
        crate::wait_for_socket(&sock).await?;

        let this = Self { process, tempdir };
        tracing::info!(endpoint = %this.endpoint(), "etcd is ready");

        Ok(this)
    }

    /// The UDS endpoint for connecting to this etcd instance.
    pub fn endpoint(&self) -> String {
        format!(
            "unix://localhost{}/client.sock:0",
            self.tempdir.path().display()
        )
    }
}
