use super::logs;
use anyhow::Context;

pub struct Repo {
    repo: String,
    clones: std::sync::Mutex<Vec<tempfile::TempDir>>,
}

impl Repo {
    pub fn new(repo: &str) -> Self {
        Self {
            repo: repo.to_string(),
            clones: Default::default(),
        }
    }

    pub async fn checkout(
        &self,
        logs_tx: &logs::Tx,
        logs_token: sqlx::types::Uuid,
        branch: &str,
    ) -> anyhow::Result<tempfile::TempDir> {
        // Attempt to obtain a pre-created checkout, or clone a new one.
        let dir = self.clones.lock().unwrap().pop();
        let dir = if let Some(dir) = dir {
            dir
        } else {
            self.create_clone(logs_tx, logs_token).await?
        };

        () = run_cmd(
            async_process::Command::new("git")
                .arg("fetch")
                .current_dir(dir.path()),
            "git-fetch",
            logs_tx,
            logs_token,
        )
        .await?;

        () = run_cmd(
            async_process::Command::new("git")
                .arg("checkout")
                .arg("--quiet")
                .arg(branch)
                .current_dir(dir.path()),
            "git-checkout",
            logs_tx,
            logs_token,
        )
        .await?;

        () = run_cmd(
            async_process::Command::new("poetry")
                .arg("install")
                .env("VIRTUAL_ENV", dir.path().join("venv"))
                .current_dir(dir.path()),
            "poetry-install",
            &logs_tx,
            logs_token,
        )
        .await?;

        tracing::info!(branch, dir=?dir.path(), "prepared checkout");

        Ok(dir)
    }

    pub fn return_checkout(&self, dir: tempfile::TempDir) {
        self.clones.lock().unwrap().push(dir);
    }

    async fn create_clone(
        &self,
        logs_tx: &logs::Tx,
        logs_token: sqlx::types::Uuid,
    ) -> anyhow::Result<tempfile::TempDir> {
        let dir = tempfile::TempDir::with_prefix(format!("dpc_checkout_"))
            .context("failed to create temp directory")?;

        () = run_cmd(
            async_process::Command::new("git")
                .arg("clone")
                .arg(&self.repo)
                .arg(dir.path()),
            "git-clone",
            logs_tx,
            logs_token,
        )
        .await?;

        () = run_cmd(
            async_process::Command::new("python3.12")
                .arg("-m")
                .arg("venv")
                .arg(dir.path().join("venv")),
            "python-venv",
            logs_tx,
            logs_token,
        )
        .await?;

        tracing::info!(repo=self.repo, dir=?dir.path(), "created repo clone");

        Ok(dir)
    }
}

async fn run_cmd(
    cmd: &mut async_process::Command,
    stream: &str,
    logs_tx: &logs::Tx,
    logs_token: sqlx::types::Uuid,
) -> anyhow::Result<()> {
    cmd.stdin(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::piped());
    cmd.stdout(std::process::Stdio::piped());

    logs_tx
        .send(logs::Line {
            token: logs_token,
            stream: "controller".to_string(),
            line: format!("Starting {stream}: {cmd:?}"),
        })
        .await
        .context("failed to send to logs sink")?;

    let mut child: async_process::Child = cmd.spawn()?.into();

    let stdout = logs::capture_lines(
        logs_tx,
        stream.to_string(),
        logs_token,
        child.stdout.take().unwrap(),
    );
    let stderr = logs::capture_lines(
        logs_tx,
        stream.to_string(),
        logs_token,
        child.stderr.take().unwrap(),
    );

    let ((), (), status) = futures::try_join!(stdout, stderr, child.wait())?;

    if !status.success() {
        anyhow::bail!("command {cmd:?} failed (logs: {logs_token})");
    }

    Ok(())
}
