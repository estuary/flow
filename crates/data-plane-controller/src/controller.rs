use super::stack::{self, State, Status};
use anyhow::Context;
use futures::future::BoxFuture;
use serde_json::json;
use sqlx::types::uuid;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

/// EmitLogFn is a function type that emits a log message to a logs sink.
pub type EmitLogFn = Box<
    dyn Fn(
            uuid::Uuid,   // Logs token.
            &'static str, // Stream name.
            String,       // Log message
        ) -> BoxFuture<'static, anyhow::Result<()>>
        + Send
        + Sync,
>;

/// RunCmdFn is a function type that runs a command and optionally returns its stdout.
pub type RunCmdFn = Box<
    dyn Fn(
            async_process::Command,
            bool,         // Capture stdout?
            &'static str, // Stream name.
            uuid::Uuid,   // Logs token.
        ) -> BoxFuture<'static, anyhow::Result<Vec<u8>>>
        + Send
        + Sync,
>;

pub struct Controller {
    // How long to wait for DNS propagation.
    pub dns_ttl: std::time::Duration,
    // Remote git repository to clone for dry_dockstructure (pulumi and ansible).
    pub dry_dock_remote: String,
    // Remote git repository to clone for ops validation.
    pub ops_remote: String,
    // Secrets provider to use for Pulumi.
    pub secrets_provider: String,
    // State backend URL for Pulumi.
    pub state_backend: url::Url,
    // Type-erased closure to emit logs.
    pub emit_log_fn: EmitLogFn,
    // Type-erased closure to run subcommands.
    pub run_cmd_fn: RunCmdFn,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Preview {
    pub branch: String,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Message {
    Start(models::Id),
    Disable,
    Enable,
    Preview(Preview),
    Refresh,
    Converge,
}

#[derive(Debug)]
pub struct Outcome {
    pub data_plane_id: models::Id,
    pub task_id: models::Id,
    pub sleep: std::time::Duration,
    // Status to publish into data_planes row.
    pub status: Status,
    // When Some, stack exports to publish into data_planes row.
    pub publish_exports: Option<stack::ControlExports>,
    // When Some, updated configuration to publish into data_planes row.
    pub publish_stack: Option<stack::PulumiStack>,
    // KMS key used to encrypt HMAC keys
    pub kms_key: String,
}

pub struct Executor {
    controller: Controller,
    idle_dirs: Arc<Mutex<Vec<HashMap<String, tempfile::TempDir>>>>,
}

impl Executor {
    pub fn new(controller: Controller) -> Self {
        Self {
            controller,
            idle_dirs: Default::default(),
        }
    }
}

impl automations::Executor for Executor {
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
        let row_state = fetch_row_state(pool, task_id, &self.controller.secrets_provider).await?;
        let releases = fetch_releases(pool, row_state.data_plane_id).await?;

        let mut checkouts = if let Some(checkouts) = self.idle_dirs.lock().unwrap().pop() {
            checkouts
        } else {
            HashMap::new()
        };
        let result = self
            .controller
            .on_poll(task_id, state, inbox, &mut checkouts, releases, row_state)
            .await;

        if !checkouts.is_empty() {
            self.idle_dirs.lock().unwrap().push(checkouts);
        }

        result
    }
}

impl Controller {
    pub async fn on_poll(
        &self,
        task_id: models::Id,
        state: &mut Option<State>,
        inbox: &mut VecDeque<(models::Id, Option<Message>)>,
        checkouts: &mut HashMap<String, tempfile::TempDir>,
        releases: Vec<stack::Release>,
        row_state: State,
    ) -> anyhow::Result<Outcome> {
        if state.is_none() {
            () = self.on_start(state, inbox, &row_state)?;
        };
        let state = state.as_mut().unwrap();

        {
            // Validate the state before operating on it, to ensure the data-plane-controller
            // never operates on an invalid state
            let ops_checkout = self
                .git_checkout(state, &self.ops_remote, checkouts, "master")
                .await?;
            self.validate_state(ops_checkout, state).await?;
        }

        let sleep = match state.status {
            Status::Idle => self.on_idle(state, inbox, releases, row_state).await?,
            Status::SetEncryption => self.on_set_encryption(state, checkouts).await?,
            Status::PulumiPreview => self.on_pulumi_preview(state, checkouts).await?,
            Status::PulumiRefresh => self.on_pulumi_refresh(state, checkouts).await?,
            Status::PulumiUp1 => {
                self.on_pulumi_up_1(state, checkouts, row_state.stack.config.model.private_links)
                    .await?
            }
            Status::AwaitDNS1 => self.on_await_dns_1(state).await?,
            Status::Ansible => self.on_ansible(state, checkouts).await?,
            Status::PulumiUp2 => self.on_pulumi_up_2(state, checkouts).await?,
            Status::AwaitDNS2 => self.on_await_dns_2(state).await?,
        };

        {
            // Validate the state after operating on it, to prevent writing a bad state into the
            // database which would lead to manual recovery being required
            let ops_checkout = self
                .git_checkout(state, &self.ops_remote, checkouts, "master")
                .await?;
            self.validate_state(ops_checkout, state).await?;
        }

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
            kms_key: self.secrets_provider.clone(),
        })
    }

    fn on_start(
        &self,
        state: &mut Option<State>,
        inbox: &mut VecDeque<(models::Id, Option<Message>)>,
        row_state: &State,
    ) -> anyhow::Result<()> {
        match inbox.pop_front() {
            Some((_from_id, Some(Message::Start(data_plane_id)))) => {
                if data_plane_id != row_state.data_plane_id {
                    anyhow::bail!(
                        "unexpected data_plane_id {data_plane_id} in start message (row is {})",
                        row_state.data_plane_id
                    );
                }
                *state = Some(row_state.clone());
            }
            message => {
                anyhow::bail!("expected 'start' message, not {message:?}");
            }
        };

        Ok(())
    }

    #[tracing::instrument(
        skip_all,
        fields(data_plane_id = ?state.data_plane_id),
    )]
    async fn on_idle(
        &self,
        state: &mut State,
        inbox: &mut VecDeque<(models::Id, Option<Message>)>,
        releases: Vec<stack::Release>,
        row_state: State,
    ) -> anyhow::Result<std::time::Duration> {
        while let Some((from_id, message)) = inbox.pop_front() {
            match message {
                Some(Message::Disable) => state.disabled = true,
                Some(Message::Enable) => state.disabled = false,
                Some(Message::Preview(Preview { branch })) => {
                    state.pending_preview = true;
                    state.preview_branch = branch;
                }
                Some(Message::Refresh) => state.pending_refresh = true,
                Some(Message::Converge) => state.pending_converge = true,

                message => {
                    anyhow::bail!(
                        "received unexpected message from {from_id} while idle: {message:?}"
                    )
                }
            }
        }

        // Refresh configuration from the current data_planes row.
        let next = row_state;

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
            && (state.stack.config.model).evaluate_release_steps(&releases)
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
    async fn on_set_encryption(
        &self,
        state: &mut State,
        checkouts: &mut HashMap<String, tempfile::TempDir>,
    ) -> anyhow::Result<std::time::Duration> {
        let checkout = self
            .dry_dock_checkout(state, checkouts, &state.deploy_branch)
            .await?;

        () = self
            .run_cmd(
                async_process::Command::new("pulumi")
                    .arg("stack")
                    .arg("init")
                    .arg(&state.stack_name)
                    .arg("--secrets-provider")
                    .arg(&self.secrets_provider)
                    .arg("--non-interactive")
                    .current_dir(checkout.path())
                    .env("PULUMI_BACKEND_URL", self.state_backend.as_str())
                    .env("PULUMI_CONFIG_PASSPHRASE", "")
                    .env("VIRTUAL_ENV", checkout.path().join("venv")),
                "pulumi-change-secrets-provider",
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

        state.stack.secrets_provider = self.secrets_provider.to_string();
        state.stack.encrypted_key = updated.encrypted_key;
        state.pending_converge = true;
        state.publish_stack = Some(state.stack.clone());
        state.status = Status::Idle;

        Ok(POLL_AGAIN)
    }

    #[tracing::instrument(
        skip_all,
        fields(data_plane_id = ?state.data_plane_id, logs = ?state.logs_token),
    )]
    async fn on_pulumi_preview(
        &self,
        state: &mut State,
        checkouts: &mut HashMap<String, tempfile::TempDir>,
    ) -> anyhow::Result<std::time::Duration> {
        let checkout = self
            .dry_dock_checkout(state, checkouts, &state.preview_branch)
            .await?;

        () = self
            .run_cmd(
                async_process::Command::new("pulumi")
                    .arg("preview")
                    .arg("--stack")
                    .arg(&state.stack_name)
                    .arg("--diff")
                    .arg("--non-interactive")
                    .current_dir(checkout.path())
                    .env("PULUMI_BACKEND_URL", self.state_backend.as_str())
                    .env("VIRTUAL_ENV", checkout.path().join("venv")),
                "pulumi-preview",
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
    async fn on_pulumi_refresh(
        &self,
        state: &mut State,
        checkouts: &mut HashMap<String, tempfile::TempDir>,
    ) -> anyhow::Result<std::time::Duration> {
        let checkout = self
            .dry_dock_checkout(state, checkouts, &state.deploy_branch)
            .await?;

        // Refresh, expecting to see no changes. We'll check exit status to see if there were.
        let result = self
            .run_cmd(
                async_process::Command::new("pulumi")
                    .arg("refresh")
                    .arg("--stack")
                    .arg(&state.stack_name)
                    .arg("--diff")
                    .arg("--non-interactive")
                    .arg("--skip-preview")
                    .arg("--yes")
                    .arg("--expect-no-changes")
                    .current_dir(checkout.path())
                    .env("PULUMI_BACKEND_URL", self.state_backend.as_str())
                    .env("VIRTUAL_ENV", checkout.path().join("venv")),
                "pulumi-refresh",
                state.logs_token,
            )
            .await;

        if matches!(&result, Err(err) if err.downcast_ref::<crate::commands::NonZeroExit>().is_some())
        {
            // We refreshed some changes, and must converge to (for example)
            // provision a replaced EC2 instance.
            state.pending_converge = true;
        } else {
            result?;
        }

        state.status = Status::Idle;
        state.last_refresh = chrono::Utc::now();

        Ok(POLL_AGAIN)
    }

    #[tracing::instrument(
        skip_all,
        fields(data_plane_id = ?state.data_plane_id, logs = ?state.logs_token),
    )]
    async fn on_pulumi_up_1(
        &self,
        state: &mut State,
        checkouts: &mut HashMap<String, tempfile::TempDir>,
        private_links: Vec<stack::PrivateLink>,
    ) -> anyhow::Result<std::time::Duration> {
        state.stack.config.model.private_links = private_links;

        let checkout = self
            .dry_dock_checkout(state, checkouts, &state.deploy_branch)
            .await?;

        () = self
            .run_cmd(
                async_process::Command::new("pulumi")
                    .arg("up")
                    .arg("--stack")
                    .arg(&state.stack_name)
                    .arg("--diff")
                    .arg("--non-interactive")
                    .arg("--skip-preview")
                    .arg("--yes")
                    .current_dir(checkout.path())
                    .env("PULUMI_BACKEND_URL", self.state_backend.as_str())
                    .env("VIRTUAL_ENV", checkout.path().join("venv")),
                "pulumi-up-one",
                state.logs_token,
            )
            .await?;

        // DNS propagation backoff is relative to this moment.
        state.last_pulumi_up = chrono::Utc::now();

        let stack::PulumiStackHistory { resource_changes } =
            self.last_pulumi_run(state, checkout).await?;

        let log_line = if resource_changes.changed() {
            state.status = Status::AwaitDNS1;
            format!("Waiting {:?} for DNS propagation.", self.dns_ttl)
        } else {
            state.status = Status::Ansible;
            "No changes detected, continuing to Ansible.".to_string()
        };
        (self.emit_log_fn)(state.logs_token, "controller", log_line).await?;

        Ok(POLL_AGAIN)
    }

    #[tracing::instrument(
        skip_all,
        fields(data_plane_id = ?state.data_plane_id),
    )]
    async fn on_await_dns_1(&self, state: &mut State) -> anyhow::Result<std::time::Duration> {
        let remainder = (state.last_pulumi_up + self.dns_ttl) - chrono::Utc::now();

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
    async fn on_ansible(
        &self,
        state: &mut State,
        checkouts: &mut HashMap<String, tempfile::TempDir>,
    ) -> anyhow::Result<std::time::Duration> {
        let checkout = self
            .dry_dock_checkout(state, checkouts, &state.deploy_branch)
            .await?;

        // Load exported Pulumi state.
        let output = self
            .run_captured_cmd(
                async_process::Command::new("pulumi")
                    .arg("stack")
                    .arg("output")
                    .arg("--stack")
                    .arg(&state.stack_name)
                    .arg("--json")
                    .arg("--non-interactive")
                    .arg("--show-secrets")
                    .current_dir(checkout.path())
                    .env("PULUMI_BACKEND_URL", self.state_backend.as_str())
                    .env("VIRTUAL_ENV", checkout.path().join("venv")),
                "pulumi-stack-output",
                state.logs_token,
            )
            .await?;

        let stack::PulumiExports {
            ansible,
            mut control,
        } = serde_json::from_slice(&output).context("failed to parse pulumi output")?;

        // Install Ansible requirements.
        () = self
            .run_cmd(
                async_process::Command::new("./venv/bin/ansible-galaxy")
                    .arg("install")
                    .arg("--role-file")
                    .arg("requirements.yml")
                    .current_dir(checkout.path()),
                "ansible-install",
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
        () = self
            .run_cmd(
                async_process::Command::new("./venv/bin/ansible-playbook")
                    .arg("data-plane.ansible.yaml")
                    .current_dir(checkout.path())
                    .env("ANSIBLE_FORCE_COLOR", "1"),
                "ansible-playbook",
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

        Ok(POLL_AGAIN)
    }

    #[tracing::instrument(
        skip_all,
        fields(data_plane_id = ?state.data_plane_id, logs = ?state.logs_token),
    )]
    async fn on_pulumi_up_2(
        &self,
        state: &mut State,
        checkouts: &mut HashMap<String, tempfile::TempDir>,
    ) -> anyhow::Result<std::time::Duration> {
        let checkout = self
            .dry_dock_checkout(state, checkouts, &state.deploy_branch)
            .await?;

        () = self
            .run_cmd(
                async_process::Command::new("pulumi")
                    .arg("up")
                    .arg("--stack")
                    .arg(&state.stack_name)
                    .arg("--diff")
                    .arg("--non-interactive")
                    .arg("--skip-preview")
                    .arg("--yes")
                    .current_dir(checkout.path())
                    .env("PULUMI_BACKEND_URL", self.state_backend.as_str())
                    .env("VIRTUAL_ENV", checkout.path().join("venv")),
                "pulumi-up-two",
                state.logs_token,
            )
            .await?;

        // DNS propagation backoff is relative to this moment.
        state.last_pulumi_up = chrono::Utc::now();

        let stack::PulumiStackHistory { resource_changes } =
            self.last_pulumi_run(state, checkout).await?;

        let log_line = if resource_changes.changed() {
            state.status = Status::AwaitDNS2;
            format!("Waiting {:?} for DNS propagation.", self.dns_ttl)
        } else {
            state.status = Status::Idle;
            "No changes detected, done.".to_string()
        };
        (self.emit_log_fn)(state.logs_token, "controller", log_line).await?;

        Ok(POLL_AGAIN)
    }

    #[tracing::instrument(
        skip_all,
        fields(data_plane_id = ?state.data_plane_id),
    )]
    async fn on_await_dns_2(&self, state: &mut State) -> anyhow::Result<std::time::Duration> {
        let remainder = (state.last_pulumi_up + self.dns_ttl) - chrono::Utc::now();

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

    async fn git_checkout<'c>(
        &self,
        state: &State,
        remote: &str,
        checkouts: &'c mut HashMap<String, tempfile::TempDir>,
        branch: &str,
    ) -> anyhow::Result<&'c tempfile::TempDir> {
        let checkout = match checkouts.entry(remote.to_string()) {
            Entry::Occupied(e) => {
                let dir = e.into_mut();
                () = self
                    .run_cmd(
                        async_process::Command::new("git")
                            .arg("clean")
                            .arg("--force")
                            .current_dir(dir.path()),
                        "git-clean",
                        state.logs_token,
                    )
                    .await?;

                dir
            }
            Entry::Vacant(e) => e.insert(self.create_clone(remote, state.logs_token).await?),
        };

        () = self
            .run_cmd(
                async_process::Command::new("git")
                    .arg("fetch")
                    .current_dir(checkout.path()),
                "git-fetch",
                state.logs_token,
            )
            .await?;

        () = self
            .run_cmd(
                async_process::Command::new("git")
                    .arg("checkout")
                    .arg("--detach")
                    .arg("--force")
                    .arg("--quiet")
                    .arg(format!("origin/{}", branch))
                    .current_dir(checkout.path()),
                "git-checkout",
                state.logs_token,
            )
            .await?;

        tracing::info!(branch=branch, dir=?checkout.path(), "prepared checkout");

        Ok(checkout)
    }

    async fn dry_dock_checkout<'c>(
        &self,
        state: &State,
        checkouts: &'c mut HashMap<String, tempfile::TempDir>,
        branch: &str,
    ) -> anyhow::Result<&'c tempfile::TempDir> {
        let checkout = self
            .git_checkout(state, &self.dry_dock_remote, checkouts, branch)
            .await?;

        () = self
            .run_cmd(
                async_process::Command::new("python3.12")
                    .arg("-m")
                    .arg("venv")
                    .arg("./venv")
                    .current_dir(checkout.path()),
                "python-venv",
                state.logs_token,
            )
            .await?;

        () = self
            .run_cmd(
                async_process::Command::new("poetry")
                    .arg("install")
                    .current_dir(checkout.path())
                    .env("VIRTUAL_ENV", checkout.path().join("venv"))
                    .env("PYTHON_KEYRING_BACKEND", "keyring.backends.null.Keyring"),
                "poetry-install",
                state.logs_token,
            )
            .await?;

        // Write out stack YAML file for Pulumi CLI.
        std::fs::write(
            &checkout
                .path()
                .join(format!("Pulumi.{}.yaml", state.stack_name)),
            serde_yaml::to_string(&state.stack).context("failed to encode stack as YAML")?.as_bytes(),
        )
        .context("failed to write stack YAML")?;

        Ok(checkout)
    }

    async fn create_clone(
        &self,
        remote: &str,
        logs_token: uuid::Uuid,
    ) -> anyhow::Result<tempfile::TempDir> {
        let dir = tempfile::TempDir::with_prefix(format!("dpc_checkout_"))
            .context("failed to create temp directory")?;

        () = self
            .run_cmd(
                async_process::Command::new("git")
                    .arg("clone")
                    .arg(remote)
                    .arg(".")
                    .current_dir(dir.path()),
                "git-clone",
                logs_token,
            )
            .await?;

        tracing::info!(repo=remote, dir=?dir.path(), "created repo clone");

        Ok(dir)
    }

    async fn last_pulumi_run(
        &self,
        state: &State,
        checkout: &tempfile::TempDir,
    ) -> anyhow::Result<stack::PulumiStackHistory> {
        let output = self
            .run_captured_cmd(
                async_process::Command::new("pulumi")
                    .arg("stack")
                    .arg("history")
                    .arg("--stack")
                    .arg(&state.stack_name)
                    .arg("--json")
                    .arg("--page-size")
                    .arg("1")
                    .current_dir(checkout.path())
                    .env("PULUMI_BACKEND_URL", self.state_backend.as_str())
                    .env("VIRTUAL_ENV", checkout.path().join("venv")),
                "pulumi-stack-history",
                state.logs_token,
            )
            .await?;

        let mut history: Vec<stack::PulumiStackHistory> = serde_json::from_slice(&output)
            .context("failed to parse pulumi stack history output")?;

        history.pop().context("pulumi stack history is empty")
    }

    async fn run_cmd(
        &self,
        cmd: &mut async_process::Command,
        stream: &'static str,
        logs_token: sqlx::types::Uuid,
    ) -> anyhow::Result<()> {
        let mut owned = async_process::Command::new("false");
        std::mem::swap(cmd, &mut owned);

        let output = (self.run_cmd_fn)(owned, false, stream, logs_token).await?;
        assert!(output.is_empty(), "unexpected output from command");
        Ok(())
    }

    async fn run_captured_cmd(
        &self,
        cmd: &mut async_process::Command,
        stream: &'static str,
        logs_token: sqlx::types::Uuid,
    ) -> anyhow::Result<Vec<u8>> {
        let mut owned = async_process::Command::new("false");
        std::mem::swap(cmd, &mut owned);

        (self.run_cmd_fn)(owned, true, stream, logs_token).await
    }

    async fn validate_state(
        &self,
        ops_checkout: &tempfile::TempDir,
        state: &State,
    ) -> anyhow::Result<()> {
        // Read jsonschema validation schema for data planes
        let schema = serde_yaml::from_slice(
            &std::fs::read(&ops_checkout.path().join("data-planes-schema.yaml"))
                .context("failed to read data-planes-schema.yaml")?,
        )
        .context("failed to parse data-planes-schema.yaml")?;

        let validator = jsonschema::validator_for(&schema)?;
        let output = validator.apply(&json!(state)).basic();
        if let jsonschema::BasicOutput::Invalid(errs) = output {
            let messages = errs.iter().fold(String::new(), |acc, e| {
                format!(
                    "{acc}\n{} at {}",
                    e.error_description(),
                    e.instance_location()
                )
            });

            let err_message = format!("failed to validate data-plane state: {messages}");

            (self.emit_log_fn)(state.logs_token, "controller", err_message.clone()).await?;
            anyhow::bail!(err_message);
        }
        Ok(())
    }
}

async fn fetch_row_state(
    pool: &sqlx::PgPool,
    task_id: models::Id,
    secrets_provider: &str,
) -> anyhow::Result<State> {
    let row = sqlx::query!(
        r#"
        SELECT
            id as "data_plane_id: models::Id",
            config AS "config: sqlx::types::Json<stack::DataPlane>",
            deploy_branch AS "deploy_branch!",
            logs_token,
            data_plane_name,
            data_plane_fqdn,
            private_links AS "private_links: Vec<sqlx::types::Json<stack::PrivateLink>>",
            pulumi_key AS "pulumi_key",
            pulumi_stack AS "pulumi_stack!"
        FROM data_planes
        WHERE controller_task_id = $1
        "#,
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

    config.model.private_links = row.private_links.into_iter().map(|link| link.0).collect();

    let stack = if let Some(key) = row.pulumi_key {
        stack::PulumiStack {
            config,
            secrets_provider: secrets_provider.to_string(),
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
        data_plane_id: row.data_plane_id,
        deploy_branch: row.deploy_branch,
        last_pulumi_up: chrono::DateTime::default(),
        last_refresh: chrono::DateTime::default(),
        logs_token: row.logs_token,
        stack,
        stack_name: row.pulumi_stack,
        status: Status::Idle,

        disabled: true,
        pending_preview: false,
        preview_branch: String::new(),
        pending_refresh: false,
        pending_converge: false,
        publish_exports: None,
        publish_stack: None,
    })
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

async fn encrypt_hmac_keys(kms_key: &str, keys: Vec<String>) -> anyhow::Result<serde_json::Value> {
    let sops = locate_bin::locate("sops").context("failed to locate sops")?;

    #[derive(serde::Serialize)]
    struct HMACKeys {
        hmac_keys: Vec<String>,
    }

    let input = serde_json::to_vec(&HMACKeys { hmac_keys: keys })?;

    // Note that input_output() pre-allocates an output buffer as large as its input buffer,
    // and our decrypted result will never be larger than its input.
    let async_process::Output {
        stderr,
        stdout,
        status,
    } = async_process::input_output(
        async_process::Command::new(sops).args([
            "--encrypt",
            "--gcp-kms",
            kms_key.strip_prefix("gcpkms://").unwrap(),
            "--input-type",
            "json",
            "--output-type",
            "json",
            "/dev/stdin",
        ]),
        &input,
    )
    .await
    .context("failed to run sops")?;

    let stdout = zeroize::Zeroizing::from(stdout);

    if !status.success() {
        anyhow::bail!(
            "encrypting hmac sops document failed: {}",
            String::from_utf8_lossy(&stderr),
        );
    }

    serde_json::from_slice(&stdout).context("parsing encrypted sops document")
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
            let encrypted_hmac_keys = encrypt_hmac_keys(&self.kms_key, hmac_keys.clone()).await?;

            _ = sqlx::query!(
                r#"
                UPDATE data_planes SET
                    aws_iam_user_arn = $3,
                    aws_link_endpoints = $4,
                    cidr_blocks = $5,
                    gcp_service_account_email = $6,
                    hmac_keys = $7,
                    encrypted_hmac_keys = $8,
                    bastion_tunnel_private_key = $9,
                    azure_application_name = $10,
                    azure_link_endpoints = $11,
                    azure_application_client_id = $12
                WHERE id = $1 AND controller_task_id = $2
                "#,
                self.data_plane_id as models::Id,
                self.task_id as models::Id,
                aws_iam_user_arn,
                &aws_link_endpoints,
                &cidr_blocks,
                gcp_service_account_email,
                &hmac_keys,
                &encrypted_hmac_keys,
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

const IDLE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
const POLL_AGAIN: std::time::Duration = std::time::Duration::ZERO;
const REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2 * 60 * 60);
