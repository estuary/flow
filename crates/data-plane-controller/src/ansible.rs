use super::logs;
use crate::repo::Checkout;
use crate::run_cmd;
use mockall::automock;

#[derive(Debug, Default)]
pub struct Ansible {}

#[automock]
impl Ansible {
    // This allows mocking the constructor
    pub fn new() -> Self {
        Self {}
    }

    pub async fn install(
        &self,
        checkout: &Checkout,
        logs_tx: &logs::Tx,
        logs_token: sqlx::types::Uuid,
        log_name: &str,
    ) -> anyhow::Result<()> {
        run_cmd(
            async_process::Command::new(checkout.path().join("venv/bin/ansible-galaxy"))
                .arg("install")
                .arg("--role-file")
                .arg("requirements.yml")
                .current_dir(checkout.path()),
            false, // This can be run in --dry-run.
            log_name,
            &logs_tx,
            logs_token,
        )
        .await
    }

    pub async fn run_playbook(
        &self,
        checkout: &Checkout,
        logs_tx: &logs::Tx,
        logs_token: sqlx::types::Uuid,
        log_name: &str,
        dry_run: bool,
    ) -> anyhow::Result<()> {
        run_cmd(
            async_process::Command::new(checkout.path().join("venv/bin/ansible-playbook"))
                .arg("data-plane.ansible.yaml")
                .current_dir(checkout.path())
                .env("ANSIBLE_FORCE_COLOR", "1"),
            dry_run,
            log_name,
            &logs_tx,
            logs_token,
        )
        .await
    }
}
