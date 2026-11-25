//! A [`Provider`] of local Docker containers, for development against a local
//! stack. It drives the `docker` CLI. Containers use the host's network, so
//! commands reach the stack's services on localhost.
//!
//! The image is built from `local/sandbox.Dockerfile`.

use super::provider::{Error, Exec, FLOWCTL_PATH, Output, Provider, State};
use anyhow::Context;
use futures::{StreamExt, TryStreamExt};

/// Label of each container, whose value is `Docker::scope`.
pub const SCOPE_LABEL: &str = "dev.estuary.sandbox-scope";

/// The provider, whose fields are arguments of an agent which serves a local
/// stack. It's configured when any of them is given, and then all but mounts
/// are required.
#[derive(Debug, clap::Args)]
#[group(id = "docker_sandbox", multiple = true)]
pub struct Docker {
    /// Image of Docker sandbox containers.
    #[arg(long = "docker-sandbox-image", env = "DOCKER_SANDBOX_IMAGE",
        required = false, requires_all = ["flowctl", "scope"])]
    pub image: String,
    /// Host path of the flowctl binary which Docker sandboxes run, which must be
    /// named `flowctl`. Its whole directory is mounted read-only.
    //
    // A mount of the file itself would pin its inode, and a build replaces the
    // file, so running containers would keep the build they started with.
    #[arg(long = "docker-sandbox-flowctl", env = "DOCKER_SANDBOX_FLOWCTL",
        required = false, requires_all = ["image", "scope"])]
    pub flowctl: std::path::PathBuf,
    /// Read-only bind mount `HOST_PATH:CONTAINER_PATH` of Docker sandboxes.
    /// May be specified multiple times.
    #[arg(
        long = "docker-sandbox-mount",
        env = "DOCKER_SANDBOX_MOUNT",
        value_delimiter = ',',
        requires = "image"
    )]
    pub mounts: Vec<Mount>,
    /// Distinguishes this agent's Docker sandboxes from those of other agents
    /// (such as those of other local stacks) on the same Docker daemon.
    #[arg(long = "docker-sandbox-scope", env = "DOCKER_SANDBOX_SCOPE",
        required = false, requires_all = ["image", "flowctl"])]
    pub scope: String,
}

#[derive(Debug, Clone)]
pub struct Mount {
    pub host: std::path::PathBuf,
    pub container: String,
}

/// Parses `HOST_PATH:CONTAINER_PATH`.
impl std::str::FromStr for Mount {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let Some((host, container)) = s.split_once(':') else {
            return Err(format!("expected HOST_PATH:CONTAINER_PATH, not {s:?}"));
        };
        Ok(Self {
            host: host.into(),
            container: container.to_string(),
        })
    }
}

#[async_trait::async_trait]
impl Provider for Docker {
    fn kind(&self) -> &'static str {
        "docker"
    }

    async fn provision(&self, handle: &str) -> Result<State, Error> {
        let (host_dir, container_dir) = match (self.flowctl.parent(), self.flowctl.file_name()) {
            (Some(dir), Some(name)) if name == "flowctl" => (
                dir,
                FLOWCTL_PATH
                    .strip_suffix("/flowctl")
                    .expect("FLOWCTL_PATH names flowctl"),
            ),
            _ => {
                return Err(anyhow::anyhow!(
                    "flowctl path {:?} must name a file called flowctl",
                    self.flowctl
                )
                .into());
            }
        };

        let mut cmd = async_process::Command::new("docker");
        cmd.args(["run", "--detach", "--init", "--pull", "never"])
            .args(["--network", "host", "--name", handle, "--label"])
            .arg(format!("{SCOPE_LABEL}={}", self.scope))
            .arg("--mount")
            .arg(bind_mount(host_dir, container_dir));

        for mount in &self.mounts {
            cmd.arg("--mount")
                .arg(bind_mount(&mount.host, &mount.container));
        }
        // `--init` makes tini PID 1, which reaps processes orphaned by
        // commands, while `sleep` idles.
        cmd.arg(&self.image).args(["sleep", "infinity"]);

        let output = docker_output(&mut cmd).await?;
        if !output.status.success() {
            return Err(cli_error("docker run", &output).into());
        }
        tracing::info!(handle, scope = %self.scope, "started sandbox container");

        Ok(serde_json::json!({}))
    }

    async fn destroy(&self, handle: &str, _state: &State) -> Result<bool, Error> {
        let output =
            docker_output(async_process::Command::new("docker").args(["rm", "--force", handle]))
                .await?;

        // `rm --force` succeeds for a missing container, printing nothing to
        // stdout and an error to stderr.
        if !output.status.success() {
            return Err(cli_error("docker rm", &output).into());
        }
        if output.stdout.is_empty() && is_no_such_container(&output.stderr) {
            return Ok(false);
        }
        Ok(true)
    }

    async fn exec(&self, handle: &str, _state: &State, argv: &[&str]) -> Result<Exec, Error> {
        // `docker exec` reports a missing or stopped container only as stderr
        // and exit status 1, which can't be told apart from the process's own.
        // So the container is checked first. One removed in between still
        // shows up that way.
        let output = docker_output(async_process::Command::new("docker").args([
            "container",
            "inspect",
            "--format",
            "{{.State.Running}}",
            handle,
        ]))
        .await?;

        if !output.status.success() && is_no_such_container(&output.stderr) {
            return Err(Error::Gone);
        } else if !output.status.success() {
            return Err(cli_error("docker container inspect", &output).into());
        } else if output.stdout.trim_ascii() != b"true" {
            return Err(Error::Gone);
        }

        tracing::debug!(handle, ?argv, "starting docker exec");

        let mut child: async_process::Child = async_process::Command::new("docker")
            .args(["exec", "--interactive", handle])
            .args(argv)
            .stdin(async_process::Stdio::piped())
            .stdout(async_process::Stdio::piped())
            .stderr(async_process::Stdio::piped())
            .spawn()
            .context("failed to spawn docker exec")?
            .into();

        let stdin = futures::sink::unfold(
            child.stdin.take().expect("stdin is piped"),
            |mut stdin, data: bytes::Bytes| async move {
                tokio::io::AsyncWriteExt::write_all(&mut stdin, &data)
                    .await
                    .context("failed to write docker exec stdin")?;
                Ok::<_, Error>(stdin)
            },
        );
        // Closing the `docker exec` client's stdin closes the process's stdin
        // (tests/sandbox_docker.rs checks it). Only this process holds that
        // pipe's write end, so it also closes when this process dies.
        let (output_tx, output_rx) = tokio::sync::mpsc::channel(16);
        tokio::spawn(relay_output(child, output_tx));

        let output = futures::stream::unfold(output_rx, |mut output_rx| async move {
            let item = output_rx.recv().await?;
            Some((item, output_rx))
        });

        Ok(Exec {
            stdin: Box::pin(stdin),
            output: output.boxed(),
        })
    }

    async fn list(&self) -> Result<Vec<String>, Error> {
        let output = docker_output(
            async_process::Command::new("docker")
                .args(["ps", "--all", "--format", "{{.Names}}", "--filter"])
                .arg(format!("label={SCOPE_LABEL}={}", self.scope)),
        )
        .await?;

        if !output.status.success() {
            return Err(cli_error("docker ps", &output).into());
        }
        let stdout = String::from_utf8(output.stdout).context("docker ps output isn't UTF-8")?;

        Ok(stdout.lines().map(str::to_string).collect())
    }
}

/// Sends the output of `docker exec` client `child` to `output`, ending with its
/// exit status or an error. Sends are best effort: once `output` is dropped,
/// the rest of the output is read and discarded until the client exits.
async fn relay_output(
    mut child: async_process::Child,
    output: tokio::sync::mpsc::Sender<Result<Output, Error>>,
) {
    let stdout = tokio_util::io::ReaderStream::new(child.stdout.take().expect("piped"))
        .map_ok(Output::Stdout);
    let stderr = tokio_util::io::ReaderStream::new(child.stderr.take().expect("piped"))
        .map_ok(Output::Stderr);
    let mut pipes = futures::stream::select(stdout, stderr);

    while let Some(item) = pipes.next().await {
        let item = item.context("failed to read docker exec output");
        let failed = item.is_err();
        // Fails only if `output` was dropped.
        let _ = output.send(item.map_err(Error::Other)).await;
        if failed {
            return; // Dropping `child` kills the client.
        }
    }

    let exit = async {
        let status = child
            .wait()
            .await
            .context("failed to wait for docker exec")?;
        let code = status
            .code()
            .with_context(|| format!("docker exec was terminated: {status}"))?;
        Ok(Output::Exit(code))
    };
    let _ = output.send(exit.await).await;
}

fn bind_mount(host: &std::path::Path, container: &str) -> String {
    format!(
        "type=bind,source={},target={container},readonly",
        host.display()
    )
}

async fn docker_output(cmd: &mut async_process::Command) -> anyhow::Result<std::process::Output> {
    async_process::output(cmd)
        .await
        .context("failed to run the docker CLI")
}

fn is_no_such_container(stderr: &[u8]) -> bool {
    String::from_utf8_lossy(stderr).contains("No such container")
}

fn cli_error(what: &str, output: &std::process::Output) -> anyhow::Error {
    anyhow::anyhow!(
        "{what} failed ({}): {}",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    )
}
