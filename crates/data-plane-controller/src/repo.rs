use super::{logs, run_cmd};
use anyhow::Context;
use std::sync::Arc;

pub struct Repo {
    inner: Arc<Inner>,
}

struct Inner {
    repo: String,
    idle: std::sync::Mutex<Vec<tempfile::TempDir>>,
}

pub struct Checkout {
    dir: Option<tempfile::TempDir>,
    inner: Arc<Inner>,
}

impl Repo {
    pub fn new(repo: &str) -> Self {
        Self {
            inner: std::sync::Arc::new(Inner {
                repo: repo.to_string(),
                idle: Default::default(),
            }),
        }
    }

    pub async fn checkout(
        &self,
        logs_tx: &logs::Tx,
        logs_token: sqlx::types::Uuid,
        branch: &str,
    ) -> anyhow::Result<Checkout> {
        // Attempt to obtain a pre-created checkout, or clone a new one.
        let dir = self.inner.idle.lock().unwrap().pop();
        let dir = if let Some(dir) = dir {
            () = run_cmd(
                async_process::Command::new("git")
                    .arg("clean")
                    .arg("--force")
                    .current_dir(dir.path()),
                false,
                "git-clean",
                logs_tx,
                logs_token,
            )
            .await?;

            dir
        } else {
            self.create_clone(logs_tx, logs_token).await?
        };

        () = run_cmd(
            async_process::Command::new("git")
                .arg("fetch")
                .current_dir(dir.path()),
            false,
            "git-fetch",
            logs_tx,
            logs_token,
        )
        .await?;

        () = run_cmd(
            async_process::Command::new("git")
                .arg("checkout")
                .arg("--detach")
                .arg("--force")
                .arg("--quiet")
                .arg(format!("origin/{branch}"))
                .current_dir(dir.path()),
            false,
            "git-checkout",
            logs_tx,
            logs_token,
        )
        .await?;

        () = run_cmd(
            async_process::Command::new("poetry")
                .arg("install")
                .current_dir(dir.path())
                .env("VIRTUAL_ENV", dir.path().join("venv"))
                .env("PYTHON_KEYRING_BACKEND", "keyring.backends.null.Keyring"),
            false,
            "poetry-install",
            &logs_tx,
            logs_token,
        )
        .await?;

        tracing::info!(branch, dir=?dir.path(), "prepared checkout");

        Ok(Checkout {
            inner: self.inner.clone(),
            dir: Some(dir),
        })
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
                .arg(&self.inner.repo)
                .arg(dir.path()),
            false,
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
            false,
            "python-venv",
            logs_tx,
            logs_token,
        )
        .await?;

        tracing::info!(repo=self.inner.repo, dir=?dir.path(), "created repo clone");

        Ok(dir)
    }
}

impl Checkout {
    pub fn path(&self) -> &std::path::Path {
        self.dir.as_ref().unwrap().path()
    }
}

impl Drop for Checkout {
    fn drop(&mut self) {
        self.inner
            .idle
            .lock()
            .unwrap()
            .push(self.dir.take().unwrap());
    }
}
