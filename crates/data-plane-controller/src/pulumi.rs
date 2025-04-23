use super::logs;
use crate::repo;
use crate::run_cmd;
use crate::stack;
use anyhow::Context;
use mockall::automock;

#[derive(Debug, Default)]
pub struct Pulumi {}

#[automock]
impl Pulumi {
    pub async fn set_encryption(
        &self,
        stack_name: &str,
        secrets_provider: &str,
        checkout: &repo::Checkout,
        state_backend: &url::Url,
        logs_tx: &logs::Tx,
        logs_token: sqlx::types::Uuid,
        dry_run: bool,
        log_name: &str,
    ) -> anyhow::Result<()> {
        run_cmd(
            async_process::Command::new("pulumi")
                .arg("stack")
                .arg("init")
                .arg(&stack_name)
                .arg("--secrets-provider")
                .arg(&secrets_provider)
                .arg("--non-interactive")
                .arg("--cwd")
                .arg(&checkout.path())
                .env("PULUMI_BACKEND_URL", state_backend.as_str())
                .env("PULUMI_CONFIG_PASSPHRASE", "")
                .env("VIRTUAL_ENV", checkout.path().join("venv")),
            dry_run,
            log_name,
            &logs_tx,
            logs_token,
        )
        .await
    }

    pub async fn preview(
        &self,
        stack_name: &str,
        checkout: &repo::Checkout,
        pulumi_secret_envs: Vec<(String, String)>,
        state_backend: &url::Url,
        logs_tx: &logs::Tx,
        logs_token: sqlx::types::Uuid,
        dry_run: bool,
        log_name: &str,
    ) -> anyhow::Result<()> {
        run_cmd(
            async_process::Command::new("pulumi")
                .arg("preview")
                .arg("--stack")
                .arg(&stack_name)
                .arg("--diff")
                .arg("--non-interactive")
                .arg("--cwd")
                .arg(&checkout.path())
                .envs(pulumi_secret_envs)
                .env("PULUMI_BACKEND_URL", state_backend.as_str())
                .env("VIRTUAL_ENV", checkout.path().join("venv")),
            dry_run,
            log_name,
            logs_tx,
            logs_token,
        )
        .await
    }

    pub async fn refresh(
        &self,
        stack_name: &str,
        checkout: &repo::Checkout,
        pulumi_secret_envs: Vec<(String, String)>,
        state_backend: &url::Url,
        logs_tx: &logs::Tx,
        logs_token: sqlx::types::Uuid,
        dry_run: bool,
        expect_no_changes: bool,
        log_name: &str,
    ) -> anyhow::Result<()> {
        let mut cmd = async_process::Command::new("pulumi");
        cmd.arg("refresh")
            .arg("--stack")
            .arg(&stack_name)
            .arg("--diff")
            .arg("--non-interactive")
            .arg("--skip-preview")
            .arg("--cwd")
            .arg(&checkout.path())
            .arg("--yes")
            .envs(pulumi_secret_envs)
            .env("VIRTUAL_ENV", checkout.path().join("venv"))
            .env("PULUMI_BACKEND_URL", state_backend.as_str());

        if expect_no_changes {
            cmd.arg("--expect-no-changes");
        }

        run_cmd(&mut cmd, dry_run, log_name, &logs_tx, logs_token).await
    }

    pub async fn up(
        &self,
        stack_name: &str,
        checkout: &repo::Checkout,
        pulumi_secret_envs: Vec<(String, String)>,
        state_backend: &url::Url,
        logs_tx: &logs::Tx,
        logs_token: sqlx::types::Uuid,
        dry_run: bool,
        log_name: &str,
    ) -> anyhow::Result<()> {
        run_cmd(
            async_process::Command::new("pulumi")
                .arg("up")
                .arg("--stack")
                .arg(&stack_name)
                .arg("--diff")
                .arg("--non-interactive")
                .arg("--skip-preview")
                .arg("--cwd")
                .arg(&checkout.path())
                .arg("--yes")
                .envs(pulumi_secret_envs)
                .env("PULUMI_BACKEND_URL", state_backend.as_str())
                .env("VIRTUAL_ENV", checkout.path().join("venv")),
            dry_run,
            log_name,
            logs_tx,
            logs_token,
        )
        .await
    }

    pub async fn last_run(
        &self,
        stack_name: &str,
        checkout: &repo::Checkout,
        pulumi_secret_envs: Vec<(String, String)>,
        state_backend: &url::Url,
    ) -> anyhow::Result<stack::PulumiStackHistory> {
        let output = async_process::output(
            async_process::Command::new("pulumi")
                .arg("stack")
                .arg("history")
                .arg("--stack")
                .arg(&stack_name)
                .arg("--json")
                .arg("--page-size")
                .arg("1")
                .arg("--cwd")
                .arg(&checkout.path())
                .envs(pulumi_secret_envs)
                .env("PULUMI_BACKEND_URL", state_backend.as_str())
                .env("VIRTUAL_ENV", checkout.path().join("venv")),
        )
        .await?;

        if !output.status.success() {
            anyhow::bail!(
                "pulumi stack history output failed: {}",
                String::from_utf8_lossy(&output.stderr),
            );
        }

        let mut out: Vec<stack::PulumiStackHistory> = serde_json::from_slice(&output.stdout)
            .context("failed to parse pulumi stack history output")?;

        let Some(result) = out.pop() else {
            anyhow::bail!("failed to parse pulumi stack history output: empty array");
        };

        Ok(result)
    }
}
