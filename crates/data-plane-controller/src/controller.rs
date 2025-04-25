use super::{
    logs, run_cmd,
    stack::{self, State, Status},
};
use crate::repo;
use anyhow::Context;
use serde_json::json;
use std::collections::VecDeque;

pub struct Controller {
    pub dry_run: bool,
    pub logs_tx: super::logs::Tx,
    pub infra_repo: super::repo::Repo,
    pub ops_repo: super::repo::Repo,
    pub secrets_provider: String,
    pub state_backend: url::Url,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Message {
    Start(models::Id),
    Disable,
    Enable,
    Preview,
    Refresh,
    Converge,
}

#[derive(Debug)]
pub struct Outcome {
    data_plane_id: models::Id,
    task_id: models::Id,
    sleep: std::time::Duration,
    // Status to publish into data_planes row.
    status: Status,
    // When Some, stack exports to publish into data_planes row.
    publish_exports: Option<stack::ControlExports>,
    // When Some, updated configuration to publish into data_planes row.
    publish_stack: Option<stack::PulumiStack>,
}

impl automations::Executor for Controller {
    const TASK_TYPE: automations::TaskType = automations::task_types::DATA_PLANE_CONTROLLER;

    type Receive = Message;
    type State = Option<State>;
    type Outcome = Outcome;

    #[tracing::instrument(
        ret,
        err(Debug, level = tracing::Level::ERROR),
        skip_all,
        fields(?task_id),
    )]
    async fn poll<'s>(
        &'s self,
        pool: &'s sqlx::PgPool,
        task_id: models::Id,
        _parent_id: Option<models::Id>,
        state: &'s mut Self::State,
        inbox: &'s mut VecDeque<(models::Id, Option<Message>)>,
    ) -> anyhow::Result<Self::Outcome> {
        if state.is_none() {
            self.on_start(pool, task_id, state, inbox).await?;
        };
        let state = state.as_mut().unwrap();

        let sleep = match state.status {
            Status::Idle => self.on_idle(pool, task_id, state, inbox).await?,
            Status::SetEncryption => self.on_set_encryption(state).await?,
            Status::PulumiPreview => self.on_pulumi_preview(state).await?,
            Status::PulumiRefresh => self.on_pulumi_refresh(state).await?,
            Status::PulumiUp1 => self.on_pulumi_up_1(state).await?,
            Status::AwaitDNS1 => self.on_await_dns_1(state).await?,
            Status::Ansible => self.on_ansible(state).await?,
            Status::PulumiUp2 => self.on_pulumi_up_2(state).await?,
            Status::AwaitDNS2 => self.on_await_dns_2(state).await?,
        };

        // We publish an updated stack only when transitioning back to Idle.
        let publish_stack = if matches!(state.status, Status::Idle) {
            state.publish_stack.take()
        } else {
            None
        };

        Ok(Outcome {
            data_plane_id: state.data_plane_id,
            task_id,
            sleep,
            status: state.status,
            publish_exports: state.publish_exports.take(),
            publish_stack,
        })
    }
}

impl Controller {
    fn pulumi_secret_envs(&self) -> Vec<(&str, String)> {
        [
            "ARM_CLIENT_ID",
            "ARM_CLIENT_SECRET",
            "ARM_TENANT_ID",
            "ARM_SUBSCRIPTION_ID",
            "VULTR_API_KEY",
        ]
        .iter()
        .map(|key| {
            (
                *key,
                std::env::var(format!("DPC_{key}")).unwrap_or_default(),
            )
        })
        .collect()
    }

    async fn on_start(
        &self,
        pool: &sqlx::PgPool,
        task_id: models::Id,
        state: &mut Option<State>,
        inbox: &mut VecDeque<(models::Id, Option<Message>)>,
    ) -> anyhow::Result<()> {
        let data_plane_id = match inbox.pop_front() {
            Some((_from_id, Some(Message::Start(data_plane_id)))) => data_plane_id,
            message => {
                anyhow::bail!("expected 'start' message, not {message:?}");
            }
        };
        *state = Some(self.fetch_row_state(pool, task_id, data_plane_id).await?);

        Ok(())
    }

    #[tracing::instrument(
        skip_all,
        fields(data_plane_id = ?state.data_plane_id),
    )]
    async fn on_idle(
        &self,
        pool: &sqlx::PgPool,
        task_id: models::Id,
        state: &mut State,
        inbox: &mut VecDeque<(models::Id, Option<Message>)>,
    ) -> anyhow::Result<std::time::Duration> {
        while let Some((from_id, message)) = inbox.pop_front() {
            match message {
                Some(Message::Disable) => state.disabled = true,
                Some(Message::Enable) => state.disabled = false,
                Some(Message::Preview) => state.pending_preview = true,
                Some(Message::Refresh) => state.pending_refresh = true,
                Some(Message::Converge) => state.pending_converge = true,

                message => anyhow::bail!(
                    "received unexpected message from {from_id} while idle: {message:?}"
                ),
            }
        }

        // Refresh configuration from the current data_planes row.
        let next = self
            .fetch_row_state(pool, task_id, state.data_plane_id)
            .await?;

        // Sanity check that variables which should not change, haven't.
        () = State::verify_transition(&state, &next)?;

        // Periodically perform a refresh to detect remote changes to resources.
        if state.last_refresh + REFRESH_INTERVAL < chrono::Utc::now() {
            state.pending_refresh = true;
        }
        // Changes to branch or stack configuration require a convergence pass.
        if state.deploy_branch != next.deploy_branch {
            state.deploy_branch = next.deploy_branch;
            state.pending_converge = true;
        }
        if state.stack.config != next.stack.config {
            state.stack.config = next.stack.config;
            state.pending_converge = true;
        }

        // Decide upon an action to take given the current `state`.
        if state.stack.encrypted_key.is_empty() {
            state.status = Status::SetEncryption;
            Ok(POLL_AGAIN)
        } else if state.pending_preview {
            // We perform requested previews even when disabled.
            state.status = Status::PulumiPreview;
            state.pending_preview = false;
            Ok(POLL_AGAIN)
        } else if state.disabled {
            // When disabled, we don't perform refresh or converge operations.
            Ok(IDLE_INTERVAL)
        } else if state.pending_refresh {
            // Start a pending refresh operation.
            state.status = Status::PulumiRefresh;
            state.pending_refresh = false;
            Ok(POLL_AGAIN)
        } else if !state.pending_converge
            && (state.stack.config.model)
                .evaluate_release_steps(&Self::fetch_releases(pool, state.data_plane_id).await?)
        {
            // We intended to converge, but will first write back the updated config.
            state.status = Status::Idle;
            state.pending_converge = true;
            state.publish_stack = Some(state.stack.clone());
            Ok(POLL_AGAIN)
        } else if state.pending_converge {
            // Start a pending convergence operation.
            state.status = Status::PulumiUp1;
            state.pending_converge = false;
            Ok(POLL_AGAIN)
        } else {
            // We remain Idle.
            Ok(IDLE_INTERVAL)
        }
    }

    #[tracing::instrument(
        skip_all,
        fields(data_plane_id = ?state.data_plane_id, logs = ?state.logs_token),
    )]
    async fn on_set_encryption(&self, state: &mut State) -> anyhow::Result<std::time::Duration> {
        let checkout = self.checkout(state).await?;

        () = run_cmd(
            async_process::Command::new("pulumi")
                .arg("stack")
                .arg("init")
                .arg(&state.stack_name)
                .arg("--secrets-provider")
                .arg(&self.secrets_provider)
                .arg("--non-interactive")
                .arg("--cwd")
                .arg(&checkout.path())
                .env("PULUMI_BACKEND_URL", self.state_backend.as_str())
                .env("PULUMI_CONFIG_PASSPHRASE", "")
                .env("VIRTUAL_ENV", checkout.path().join("venv")),
            self.dry_run,
            "pulumi-change-secrets-provider",
            &self.logs_tx,
            state.logs_token,
        )
        .await?;

        // Pulumi wrote an updated stack YAML.
        // Parse it to extract the encryption key.
        let updated = std::fs::read(
            &checkout
                .path()
                .join(format!("Pulumi.{}.yaml", state.stack_name)),
        )
        .context("failed to read stack YAML")?;

        let mut updated: stack::PulumiStack =
            serde_yaml::from_slice(&updated).context("failed to parse stack from YAML")?;

        if self.dry_run {
            // We didn't actually run Pulumi, so it didn't set an encrypted key.
            updated.encrypted_key = "dry-run-fixture".to_string()
        }

        state.stack.secrets_provider = self.secrets_provider.clone();
        state.stack.encrypted_key = updated.encrypted_key;
        state.pending_converge = true;
        state.publish_stack = Some(state.stack.clone());
        state.status = Status::Idle;

        self.validate_state(&state).await?;

        Ok(POLL_AGAIN)
    }

    #[tracing::instrument(
        skip_all,
        fields(data_plane_id = ?state.data_plane_id, logs = ?state.logs_token),
    )]
    async fn on_pulumi_preview(&self, state: &mut State) -> anyhow::Result<std::time::Duration> {
        let checkout = self.checkout(state).await?;

        () = run_cmd(
            async_process::Command::new("pulumi")
                .arg("preview")
                .arg("--stack")
                .arg(&state.stack_name)
                .arg("--diff")
                .arg("--non-interactive")
                .arg("--cwd")
                .arg(&checkout.path())
                .envs(self.pulumi_secret_envs())
                .env("PULUMI_BACKEND_URL", self.state_backend.as_str())
                .env("VIRTUAL_ENV", checkout.path().join("venv")),
            self.dry_run,
            "pulumi-preview",
            &self.logs_tx,
            state.logs_token,
        )
        .await?;

        state.status = Status::Idle;

        Ok(POLL_AGAIN)
    }

    #[tracing::instrument(
        skip_all,
        fields(data_plane_id = ?state.data_plane_id, logs = ?state.logs_token),
    )]
    async fn on_pulumi_refresh(&self, state: &mut State) -> anyhow::Result<std::time::Duration> {
        let checkout = self.checkout(state).await?;

        // Refresh expecting to see no changes. We'll check exit status to see if there were.
        let result = run_cmd(
            async_process::Command::new("pulumi")
                .arg("refresh")
                .arg("--stack")
                .arg(&state.stack_name)
                .arg("--diff")
                .arg("--non-interactive")
                .arg("--skip-preview")
                .arg("--cwd")
                .arg(&checkout.path())
                .arg("--yes")
                .arg("--expect-no-changes")
                .envs(self.pulumi_secret_envs())
                .env("PULUMI_BACKEND_URL", self.state_backend.as_str())
                .env("VIRTUAL_ENV", checkout.path().join("venv")),
            self.dry_run,
            "pulumi-refresh",
            &self.logs_tx,
            state.logs_token,
        )
        .await;

        if matches!(&result, Err(err) if err.downcast_ref::<super::NonZeroExit>().is_some()) {
            // Run again, but this time allowing changes.
            () = run_cmd(
                async_process::Command::new("pulumi")
                    .arg("refresh")
                    .arg("--stack")
                    .arg(&state.stack_name)
                    .arg("--diff")
                    .arg("--non-interactive")
                    .arg("--skip-preview")
                    .arg("--cwd")
                    .arg(&checkout.path())
                    .arg("--yes")
                    .envs(self.pulumi_secret_envs())
                    .env("PULUMI_BACKEND_URL", self.state_backend.as_str())
                    .env("VIRTUAL_ENV", checkout.path().join("venv")),
                self.dry_run,
                "pulumi-refresh-changed",
                &self.logs_tx,
                state.logs_token,
            )
            .await?;

            // We refreshed some changes, and must converge to (for example)
            // provision a replaced EC2 instance.
            state.pending_converge = true;
        } else {
            () = result?;
        }

        state.status = Status::Idle;
        state.last_refresh = chrono::Utc::now();

        self.validate_state(&state).await?;

        Ok(POLL_AGAIN)
    }

    #[tracing::instrument(
        skip_all,
        fields(data_plane_id = ?state.data_plane_id, logs = ?state.logs_token),
    )]
    async fn on_pulumi_up_1(&self, state: &mut State) -> anyhow::Result<std::time::Duration> {
        let checkout = self.checkout(state).await?;

        () = run_cmd(
            async_process::Command::new("pulumi")
                .arg("up")
                .arg("--stack")
                .arg(&state.stack_name)
                .arg("--diff")
                .arg("--non-interactive")
                .arg("--skip-preview")
                .arg("--cwd")
                .arg(&checkout.path())
                .arg("--yes")
                .envs(self.pulumi_secret_envs())
                .env("PULUMI_BACKEND_URL", self.state_backend.as_str())
                .env("VIRTUAL_ENV", checkout.path().join("venv")),
            self.dry_run,
            "pulumi-up-one",
            &self.logs_tx,
            state.logs_token,
        )
        .await?;

        // DNS propagation backoff is relative to this moment.
        state.last_pulumi_up = chrono::Utc::now();

        let stack::PulumiStackHistory { resource_changes } =
            self.last_pulumi_run(&state, &checkout).await?;

        let log_line = if resource_changes.changed() {
            state.status = Status::AwaitDNS1;
            format!("Waiting {:?} for DNS propagation.", self.dns_ttl())
        } else {
            state.status = Status::Ansible;
            "No changes detected, continuing to Ansible.".to_string()
        };

        self.logs_tx
            .send(logs::Line {
                token: state.logs_token,
                stream: "controller".to_string(),
                line: log_line,
            })
            .await
            .context("failed to send to logs sink")?;

        self.validate_state(&state).await?;

        Ok(POLL_AGAIN)
    }

    #[tracing::instrument(
        skip_all,
        fields(data_plane_id = ?state.data_plane_id),
    )]
    async fn on_await_dns_1(&self, state: &mut State) -> anyhow::Result<std::time::Duration> {
        let remainder = (state.last_pulumi_up + self.dns_ttl()) - chrono::Utc::now();

        if remainder > chrono::TimeDelta::zero() {
            // PostgreSQL doesn't support nanosecond precision, so we must strip them.
            Ok(std::time::Duration::from_micros(
                remainder.num_microseconds().unwrap() as u64,
            ))
        } else {
            state.status = Status::Ansible;

            Ok(POLL_AGAIN)
        }
    }

    #[tracing::instrument(
        skip_all,
        fields(data_plane_id = ?state.data_plane_id, logs = ?state.logs_token),
    )]
    async fn on_ansible(&self, state: &mut State) -> anyhow::Result<std::time::Duration> {
        let checkout = self.checkout(state).await?;

        // Load exported Pulumi state.
        let output = if self.dry_run {
            include_bytes!("dry_run_fixture.json").to_vec()
        } else {
            let output = async_process::output(
                async_process::Command::new("pulumi")
                    .arg("stack")
                    .arg("output")
                    .arg("--stack")
                    .arg(&state.stack_name)
                    .arg("--json")
                    .arg("--non-interactive")
                    .arg("--show-secrets")
                    .arg("--cwd")
                    .arg(&checkout.path())
                    .envs(self.pulumi_secret_envs())
                    .env("PULUMI_BACKEND_URL", self.state_backend.as_str())
                    .env("VIRTUAL_ENV", checkout.path().join("venv")),
            )
            .await?;

            if !output.status.success() {
                anyhow::bail!(
                    "pulumi stack output failed: {}",
                    String::from_utf8_lossy(&output.stderr),
                );
            }
            output.stdout
        };

        let stack::PulumiExports {
            ansible,
            mut control,
        } = serde_json::from_slice(&output).context("failed to parse pulumi output")?;

        // Install Ansible requirements.
        () = run_cmd(
            async_process::Command::new(checkout.path().join("venv/bin/ansible-galaxy"))
                .arg("install")
                .arg("--role-file")
                .arg("requirements.yml")
                .current_dir(checkout.path()),
            false, // This can be run in --dry-run.
            "ansible-install",
            &self.logs_tx,
            state.logs_token,
        )
        .await?;

        // Write out Ansible inventory.
        std::fs::write(
            checkout.path().join("ansible-inventory.json"),
            serde_json::to_vec_pretty(&ansible).context("failed to serialize ansible inventory")?,
        )
        .context("failed to write ansible inventory")?;

        // Write out Ansible SSH key and set it to 0600.
        // Ansible is sensitive about their being a trailing newline.
        let ssh_key_path = checkout.path().join("ansible-ssh.key");
        let mut ssh_key = std::mem::take(&mut control.ssh_key);

        if !ssh_key.ends_with("\n") {
            ssh_key.push('\n');
        }

        std::fs::write(&ssh_key_path, ssh_key).context("failed to write ansible SSH key")?;
        std::fs::set_permissions(
            ssh_key_path,
            std::os::unix::fs::PermissionsExt::from_mode(0o600),
        )
        .context("failed to set permissions of ansible SSH key")?;

        // Run the Ansible playbook.
        () = run_cmd(
            async_process::Command::new(checkout.path().join("venv/bin/ansible-playbook"))
                .arg("data-plane.ansible.yaml")
                .current_dir(checkout.path())
                .env("ANSIBLE_FORCE_COLOR", "1"),
            self.dry_run,
            "ansible-playbook",
            &self.logs_tx,
            state.logs_token,
        )
        .await?;

        // Now that we've completed Ansible, all deployments are current and we
        // can prune empty deployments.
        state
            .stack
            .config
            .model
            .deployments
            .retain_mut(stack::Deployment::mark_current);

        state.status = Status::PulumiUp2;
        state.publish_exports = Some(control);
        state.publish_stack = Some(state.stack.clone());

        self.validate_state(&state).await?;

        Ok(POLL_AGAIN)
    }

    #[tracing::instrument(
        skip_all,
        fields(data_plane_id = ?state.data_plane_id, logs = ?state.logs_token),
    )]
    async fn on_pulumi_up_2(&self, state: &mut State) -> anyhow::Result<std::time::Duration> {
        let checkout = self.checkout(state).await?;

        () = run_cmd(
            async_process::Command::new("pulumi")
                .arg("up")
                .arg("--stack")
                .arg(&state.stack_name)
                .arg("--diff")
                .arg("--non-interactive")
                .arg("--skip-preview")
                .arg("--cwd")
                .arg(&checkout.path())
                .arg("--yes")
                .envs(self.pulumi_secret_envs())
                .env("PULUMI_BACKEND_URL", self.state_backend.as_str())
                .env("VIRTUAL_ENV", checkout.path().join("venv")),
            self.dry_run,
            "pulumi-up-two",
            &self.logs_tx,
            state.logs_token,
        )
        .await?;

        // DNS propagation backoff is relative to this moment.
        state.last_pulumi_up = chrono::Utc::now();

        let stack::PulumiStackHistory { resource_changes } =
            self.last_pulumi_run(&state, &checkout).await?;

        let log_line = if resource_changes.changed() {
            state.status = Status::AwaitDNS2;
            format!("Waiting {:?} for DNS propagation.", self.dns_ttl())
        } else {
            state.status = Status::Idle;
            "No changes detected, done.".to_string()
        };

        self.logs_tx
            .send(logs::Line {
                token: state.logs_token,
                stream: "controller".to_string(),
                line: log_line,
            })
            .await
            .context("failed to send to logs sink")?;

        self.validate_state(&state).await?;

        Ok(POLL_AGAIN)
    }

    #[tracing::instrument(
        skip_all,
        fields(data_plane_id = ?state.data_plane_id),
    )]
    async fn on_await_dns_2(&self, state: &mut State) -> anyhow::Result<std::time::Duration> {
        let remainder = (state.last_pulumi_up + self.dns_ttl()) - chrono::Utc::now();

        if remainder > chrono::TimeDelta::zero() {
            // PostgreSQL doesn't support nanosecond precision, so we must strip them.
            Ok(std::time::Duration::from_micros(
                remainder.num_microseconds().unwrap() as u64,
            ))
        } else {
            state.status = Status::Idle;

            Ok(POLL_AGAIN)
        }
    }

    async fn fetch_row_state(
        &self,
        pool: &sqlx::PgPool,
        task_id: models::Id,
        data_plane_id: models::Id,
    ) -> anyhow::Result<State> {
        let row = sqlx::query!(
            r#"
            SELECT
                config AS "config: sqlx::types::Json<stack::DataPlane>",
                deploy_branch AS "deploy_branch!",
                logs_token,
                data_plane_name,
                data_plane_fqdn,
                pulumi_key AS "pulumi_key",
                pulumi_stack AS "pulumi_stack!"
            FROM data_planes
            WHERE id = $1 and controller_task_id = $2
            "#,
            data_plane_id as models::Id,
            task_id as models::Id,
        )
        .fetch_one(pool)
        .await
        .context("failed to fetch data-plane row")?;

        let mut config = stack::PulumiStackConfig {
            model: row.config.0,
        };
        config.model.name = Some(row.data_plane_name);
        config.model.fqdn = Some(row.data_plane_fqdn);

        let stack = if let Some(key) = row.pulumi_key {
            stack::PulumiStack {
                config,
                secrets_provider: self.secrets_provider.clone(),
                encrypted_key: key,
            }
        } else {
            stack::PulumiStack {
                config,
                secrets_provider: "passphrase".to_string(),
                encrypted_key: String::new(),
            }
        };

        let state = State {
            data_plane_id,
            deploy_branch: row.deploy_branch,
            last_pulumi_up: chrono::DateTime::default(),
            last_refresh: chrono::DateTime::default(),
            logs_token: row.logs_token,
            stack,
            stack_name: row.pulumi_stack,
            status: Status::Idle,

            disabled: true,
            pending_preview: false,
            pending_refresh: false,
            pending_converge: false,
            publish_exports: None,
            publish_stack: None,
        };

        self.validate_state(&state).await?;

        Ok(state)
    }

    async fn validate_state(&self, state: &State) -> anyhow::Result<()> {
        let ops_checkout = self
            .ops_repo
            .checkout(
                &self.logs_tx,
                state.logs_token,
                "mahdi/data-plane-jsonschema",
            )
            .await?;

        // Read jsonschema validation schema for data planes
        let schema = serde_yaml::from_slice(
            &std::fs::read(&ops_checkout.path().join("data-planes-schema.yaml"))
                .context("failed to read data-planes-schema.yaml")?,
        )
        .context("failed to parse data-planes-schema.yaml")?;

        if let Err(e) = jsonschema::validate(&schema, &json!(state)) {
            anyhow::bail!("failed to validate data-plane state: {e}");
        }
        Ok(())
    }

    async fn fetch_releases(
        pool: &sqlx::PgPool,
        data_plane_id: models::Id,
    ) -> anyhow::Result<Vec<stack::Release>> {
        let rows = sqlx::query_as!(
            stack::Release,
            r#"
            SELECT
                prev_image,
                next_image,
                step
            FROM data_plane_releases
            WHERE active AND data_plane_id IN ($1, '00:00:00:00:00:00:00:00')
            "#,
            data_plane_id as models::Id,
        )
        .fetch_all(pool)
        .await
        .context("failed to fetch data-plane releases")?;

        Ok(rows)
    }

    async fn checkout(&self, state: &State) -> anyhow::Result<repo::Checkout> {
        let checkout = self
            .infra_repo
            .checkout(&self.logs_tx, state.logs_token, &state.deploy_branch)
            .await?;

        self.infra_repo
            .poetry_install(&checkout.path(), &self.logs_tx, state.logs_token)
            .await?;

        // Write out stack YAML file for Pulumi CLI.
        std::fs::write(
            &checkout
                .path()
                .join(format!("Pulumi.{}.yaml", state.stack_name)),
            serde_yaml::to_vec(&state.stack).context("failed to encode stack as YAML")?,
        )
        .context("failed to write stack YAML")?;

        Ok(checkout)
    }

    async fn last_pulumi_run(
        &self,
        state: &State,
        checkout: &repo::Checkout,
    ) -> anyhow::Result<stack::PulumiStackHistory> {
        if self.dry_run {
            // Return a fixture which "detects changes" roughly half the time,
            // so that dry runs exercise both awaiting and skipping DNS.
            return Ok(stack::PulumiStackHistory {
                resource_changes: stack::PulumiStackResourceChanges {
                    create: (state.last_pulumi_up.timestamp() % 2) as usize,
                    delete: 0,
                    same: 0,
                    update: 0,
                },
            });
        }

        // Check last run of pulumi
        let output = async_process::output(
            async_process::Command::new("pulumi")
                .arg("stack")
                .arg("history")
                .arg("--stack")
                .arg(&state.stack_name)
                .arg("--json")
                .arg("--page-size")
                .arg("1")
                .arg("--cwd")
                .arg(&checkout.path())
                .envs(self.pulumi_secret_envs())
                .env("PULUMI_BACKEND_URL", self.state_backend.as_str())
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

        return Ok(result);
    }

    fn dns_ttl(&self) -> std::time::Duration {
        if self.dry_run {
            DNS_TTL_DRY_RUN
        } else {
            DNS_TTL_ACTUAL
        }
    }
}

impl automations::Outcome for Outcome {
    async fn apply<'s>(
        self,
        txn: &'s mut sqlx::PgConnection,
    ) -> anyhow::Result<automations::Action> {
        sqlx::query!(
            r#"
            UPDATE data_planes SET
                status = $3,
                updated_at = NOW()
            WHERE id = $1 AND controller_task_id = $2
                AND status IS DISTINCT FROM $3
            "#,
            self.data_plane_id as models::Id,
            self.task_id as models::Id,
            format!("{:?}", self.status),
        )
        .execute(&mut *txn)
        .await
        .context("failed to update status of data_planes row")?;

        if let Some(stack::PulumiStack {
            config: stack::PulumiStackConfig { mut model },
            encrypted_key,
            secrets_provider: _,
        }) = self.publish_stack
        {
            // These fields are already implied by other row columns.
            model.name = None;
            model.fqdn = None;

            _ = sqlx::query!(
                r#"
                UPDATE data_planes SET
                    config = $3,
                    pulumi_key = $4
                WHERE id = $1 AND controller_task_id = $2
                "#,
                self.data_plane_id as models::Id,
                self.task_id as models::Id,
                sqlx::types::Json(model) as sqlx::types::Json<stack::DataPlane>,
                encrypted_key,
            )
            .execute(&mut *txn)
            .await
            .context("failed to publish stack into data_planes row")?;
        }

        if let Some(stack::ControlExports {
            aws_iam_user_arn,
            aws_link_endpoints,
            azure_link_endpoints,
            cidr_blocks,
            gcp_service_account_email,
            hmac_keys,
            ssh_key: _,
            bastion_tunnel_private_key,
            azure_application_name,
            azure_application_client_id,
        }) = self.publish_exports
        {
            _ = sqlx::query!(
                r#"
                UPDATE data_planes SET
                    aws_iam_user_arn = $3,
                    aws_link_endpoints = $4,
                    cidr_blocks = $5,
                    gcp_service_account_email = $6,
                    hmac_keys = $7,
                    bastion_tunnel_private_key = $8,
                    azure_application_name = $9,
                    azure_link_endpoints = $10,
                    azure_application_client_id = $11
                WHERE id = $1 AND controller_task_id = $2
                "#,
                self.data_plane_id as models::Id,
                self.task_id as models::Id,
                aws_iam_user_arn,
                &aws_link_endpoints,
                &cidr_blocks,
                gcp_service_account_email,
                &hmac_keys,
                bastion_tunnel_private_key,
                azure_application_name,
                &azure_link_endpoints,
                azure_application_client_id,
            )
            .execute(&mut *txn)
            .await
            .context("failed to publish exports into data_planes row")?;
        }

        Ok(automations::Action::Sleep(self.sleep))
    }
}

const DNS_TTL_ACTUAL: std::time::Duration = std::time::Duration::from_secs(5 * 60);
const DNS_TTL_DRY_RUN: std::time::Duration = std::time::Duration::from_secs(10);
const IDLE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
const POLL_AGAIN: std::time::Duration = std::time::Duration::ZERO;
const REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2 * 60 * 60);
