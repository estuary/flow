use super::{logs, run_cmd, stack};
use crate::repo;
use anyhow::Context;
use itertools::{EitherOrBoth, Itertools};
use std::collections::VecDeque;

pub struct Controller {
    pub logs_tx: super::logs::Tx,
    pub repo: super::repo::Repo,
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

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct State {
    // DataPlane which this controller manages.
    data_plane_id: models::Id,
    // Git branch of the dry-dock repo for this data-plane.
    deploy_branch: String,
    // DateTime of the last `pulumi up` for this data-plane.
    last_pulumi_up: chrono::DateTime<chrono::Utc>,
    // DateTime of the last `pulumi refresh` for this data-plane.
    last_refresh: chrono::DateTime<chrono::Utc>,
    // Token to which controller logs are directed.
    logs_token: sqlx::types::Uuid,
    // Pulumi configuration for this data-plane.
    stack: stack::PulumiStack,
    // Name of the data-plane "stack" within the Pulumi tooling.
    stack_name: String,
    // Status of this controller.
    status: Status,

    // Is this controller disabled?
    // When disabled, refresh and converge operations are queued but not run.
    #[serde(default, skip_serializing_if = "is_false")]
    disabled: bool,

    // Is there a pending preview for this data-plane?
    #[serde(default, skip_serializing_if = "is_false")]
    pending_preview: bool,
    // Is there a pending refresh for this data-plane?
    #[serde(default, skip_serializing_if = "is_false")]
    pending_refresh: bool,
    // Is there a pending converge for this data-plane?
    #[serde(default, skip_serializing_if = "is_false")]
    pending_converge: bool,

    // When Some, updated Pulumi stack exports to be written back into the `data_planes` row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    publish_exports: Option<stack::ControlExports>,
    // When true, an updated Pulumi stack model to be written back into the `data_planes` row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    publish_stack: Option<stack::PulumiStack>,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
pub enum Status {
    Idle,
    /// Controller is setting the encryption key for Pulumi stack secrets.
    SetEncryption,
    /// Controller is previewing changes proposed by Pulumi without applying them.
    PulumiPreview,
    /// Controller is refreshing any remotely-changed resources,
    /// such as replaced EC2 instances.
    PulumiRefresh,
    /// Controller is creating any scaled-up cloud resources,
    /// updating DNS records for resources which are scaling down,
    /// and updating the Ansible inventory.
    PulumiUp1,
    /// Controller is awaiting DNS propagation for any replaced resources
    /// as well as resources which are scaling down.
    AwaitDNS1,
    /// Controller is running Ansible to initialize and refresh servers.
    Ansible,
    /// Controller is updating DNS records for resources which have now
    /// started and is destroying any scaled-down cloud resources which
    /// have now stopped.
    PulumiUp2,
    /// Controller is awaiting DNS propagation for any scaled-up
    /// resources which have now started.
    AwaitDNS2,
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
        ["ARM_CLIENT_ID", "ARM_CLIENT_SECRET", "ARM_TENANT_ID", "ARM_SUBSCRIPTION_ID", "VULTR_API_KEY"]
            .iter()
            .flat_map(|key|
                std::env::var(format!("DPC_{key}")).ok().map(|value| (*key, value))
            ).collect()
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
        // Handle received messages by clearing corresponding state
        // to force the explicitly-requested action.
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
        let State {
            deploy_branch: next_deploy_branch,
            logs_token: next_logs_token,
            stack:
                stack::PulumiStack {
                    config: next_config,
                    encrypted_key: next_encrypted_key,
                    secrets_provider: next_secrets_provider,
                },
            stack_name: next_stack_name,
            ..
        } = self
            .fetch_row_state(pool, task_id, state.data_plane_id)
            .await?;

        // Sanity check that variables which should not change, haven't.
        if state.stack.encrypted_key != next_encrypted_key {
            anyhow::bail!(
                "pulumi stack encrypted key cannot change from {} to {next_encrypted_key}",
                state.stack.encrypted_key,
            );
        }
        if state.stack.secrets_provider != next_secrets_provider {
            anyhow::bail!(
                "pulumi stack secrets provider cannot change from {} to {next_secrets_provider}",
                state.stack.secrets_provider,
            );
        }
        if state.stack_name != next_stack_name {
            anyhow::bail!(
                "pulumi stack name cannot change from {} to {next_stack_name}",
                state.stack_name
            );
        }
        if state.logs_token != next_logs_token {
            anyhow::bail!(
                "data-plane logs token cannot change from {} to {next_logs_token}",
                state.logs_token
            );
        }
        if state.stack.config.model.gcp_project != next_config.model.gcp_project {
            anyhow::bail!(
                "pulumi stack gcp_project cannot change from {} to {}",
                state.stack.config.model.gcp_project,
                next_config.model.gcp_project,
            );
        }
        for (index, zipped) in state
            .stack
            .config
            .model
            .deployments
            .iter()
            .zip_longest(next_config.model.deployments.iter())
            .enumerate()
        {
            match zipped {
                EitherOrBoth::Left(cur_deployment) => {
                    anyhow::bail!(
                        "cannot remove deployment {cur_deployment:?} at index {index}; scale it down with `desired` = 0 instead"
                    );
                }
                EitherOrBoth::Right(next_deployment) => {
                    if next_deployment.current != 0 {
                        anyhow::bail!(
                            "new deployment {next_deployment:?} at index {index} must have `current` = 0; scale up using `desired` instead"
                        );
                    } else if next_deployment.desired == 0 {
                        anyhow::bail!(
                            "new deployment {next_deployment:?} at index {index} must have `desired` > 0"
                        );
                    }
                }
                EitherOrBoth::Both(
                    current @ stack::Deployment {
                        current: cur_current,
                        oci_image: cur_oci_image,
                        role: cur_role,
                        template: cur_template,
                        desired: _,            // Allowed to change.
                        oci_image_override: _, // Allowed to change.
                    },
                    next @ stack::Deployment {
                        current: next_current,
                        oci_image: next_oci_image,
                        role: next_role,
                        template: next_template,
                        desired: _,            // Allowed to change.
                        oci_image_override: _, // Allowed to change.
                    },
                ) => {
                    if cur_current != next_current
                        || cur_oci_image != next_oci_image
                        || cur_role != next_role
                        || cur_template != next_template
                    {
                        anyhow::bail!(
                            "invalid transition of deployment at index {index} (you many only append new deployments or update `desired` or `oci_image_override` of this one): {current:?} =!=> {next:?}"
                        );
                    }
                }
            }
        }

        // Periodically perform a refresh to detect remote changes to resources.
        if state.last_refresh + REFRESH_INTERVAL < chrono::Utc::now() {
            state.pending_refresh = true;
        }
        // Changes to branch or stack configuration require a convergence pass.
        if state.deploy_branch != next_deploy_branch {
            state.deploy_branch = next_deploy_branch;
            state.pending_converge = true;
        }
        if state.stack.config != next_config {
            state.stack.config = next_config;
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
                .arg("change-secrets-provider")
                .arg("--stack")
                .arg(&state.stack_name)
                .arg("--non-interactive")
                .arg("--cwd")
                .arg(&checkout.path())
                .arg(&self.secrets_provider)
                .env("PULUMI_BACKEND_URL", self.state_backend.as_str())
                .env("PULUMI_CONFIG_PASSPHRASE", "")
                .env("VIRTUAL_ENV", checkout.path().join("venv")),
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

        let updated: stack::PulumiStack =
            serde_yaml::from_slice(&updated).context("failed to parse stack from YAML")?;

        state.stack.secrets_provider = self.secrets_provider.clone();
        state.stack.encrypted_key = updated.encrypted_key;
        state.publish_stack = Some(state.stack.clone());
        state.status = Status::Idle;

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
            "pulumi-up-one",
            &self.logs_tx,
            state.logs_token,
        )
        .await?;

        self.logs_tx
            .send(logs::Line {
                token: state.logs_token,
                stream: "controller".to_string(),
                line: format!("Waiting {DNS_TTL:?} for DNS propagation before continuing."),
            })
            .await
            .context("failed to send to logs sink")?;

        state.status = Status::AwaitDNS1;
        state.last_pulumi_up = chrono::Utc::now();

        Ok(DNS_TTL)
    }

    #[tracing::instrument(
        skip_all,
        fields(data_plane_id = ?state.data_plane_id),
    )]
    async fn on_await_dns_1(&self, state: &mut State) -> anyhow::Result<std::time::Duration> {
        let remainder = (state.last_pulumi_up + DNS_TTL) - chrono::Utc::now();

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

        let stack::PulumiExports {
            ansible,
            mut control,
        } = serde_json::from_slice(&output.stdout).context("failed to parse pulumi output")?;

        // Install Ansible requirements.
        () = run_cmd(
            async_process::Command::new(checkout.path().join("venv/bin/ansible-galaxy"))
                .arg("install")
                .arg("--role-file")
                .arg("requirements.yml")
                .current_dir(checkout.path()),
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
            .retain_mut(|deployment| {
                deployment.current = deployment.desired;
                deployment.current != 0
            });

        state.status = Status::PulumiUp2;
        state.publish_exports = Some(control);
        state.publish_stack = Some(state.stack.clone());

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
            "pulumi-up-two",
            &self.logs_tx,
            state.logs_token,
        )
        .await?;

        self.logs_tx
            .send(logs::Line {
                token: state.logs_token,
                stream: "controller".to_string(),
                line: format!("Waiting {DNS_TTL:?} for DNS propagation before continuing."),
            })
            .await
            .context("failed to send to logs sink")?;

        state.status = Status::AwaitDNS2;
        state.last_pulumi_up = chrono::Utc::now();

        Ok(DNS_TTL)
    }

    #[tracing::instrument(
        skip_all,
        fields(data_plane_id = ?state.data_plane_id),
    )]
    async fn on_await_dns_2(&self, state: &mut State) -> anyhow::Result<std::time::Duration> {
        let remainder = (state.last_pulumi_up + DNS_TTL) - chrono::Utc::now();

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

        Ok(State {
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
        })
    }

    async fn checkout(&self, state: &State) -> anyhow::Result<repo::Checkout> {
        let checkout = self
            .repo
            .checkout(&self.logs_tx, state.logs_token, &state.deploy_branch)
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
            cidr_blocks,
            gcp_service_account_email,
            hmac_keys,
            ssh_key: _,
        }) = self.publish_exports
        {
            _ = sqlx::query!(
                r#"
                UPDATE data_planes SET
                    aws_iam_user_arn = $3,
                    aws_link_endpoints = $4,
                    cidr_blocks = $5,
                    gcp_service_account_email = $6,
                    hmac_keys = $7
                WHERE id = $1 AND controller_task_id = $2
                "#,
                self.data_plane_id as models::Id,
                self.task_id as models::Id,
                aws_iam_user_arn,
                &aws_link_endpoints,
                &cidr_blocks,
                gcp_service_account_email,
                &hmac_keys,
            )
            .execute(&mut *txn)
            .await
            .context("failed to publish exports into data_planes row")?;
        }

        Ok(automations::Action::Sleep(self.sleep))
    }
}

const DNS_TTL: std::time::Duration = std::time::Duration::from_secs(5 * 60);
const IDLE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
const POLL_AGAIN: std::time::Duration = std::time::Duration::ZERO;
const REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2 * 60 * 60);

fn is_false(b: &bool) -> bool {
    !b
}
