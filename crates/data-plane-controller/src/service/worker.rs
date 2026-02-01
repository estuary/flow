use crate::protocol::{Action, ControllerConfig, ExecuteRequest, ExecuteResponse};
use crate::shared::{commands, logs, stack};
use anyhow::Context;
use std::collections::HashMap;
use std::collections::hash_map::Entry;

const POLL_AGAIN: std::time::Duration = std::time::Duration::ZERO;

/// Execute a single action on the given state.
/// This function contains all the work execution logic that was previously in Controller.
pub async fn execute_action(
    request: ExecuteRequest,
    logs_tx: logs::Tx,
) -> anyhow::Result<ExecuteResponse> {
    let ExecuteRequest {
        task_id: _,
        data_plane_id: _,
        logs_token: _,
        mut state,
        action,
        controller_config,
    } = request;

    let config: ControllerConfig = controller_config.into();

    // Build a type-erased RunCmdFn which dispatches to commands::dry_run()
    // when running in dry-run mode, or commands::run() otherwise.
    let run_cmd_fn: Box<
        dyn Fn(
                async_process::Command,
                bool,
                &'static str,
                sqlx::types::Uuid,
            ) -> futures::future::BoxFuture<'static, anyhow::Result<Vec<u8>>>
            + Send
            + Sync,
    > = if config.dry_run {
        let logs_tx_clone = logs_tx.clone();
        Box::new(move |cmd, capture_stdout, stream, logs_token| {
            Box::pin(commands::dry_run(
                cmd,
                capture_stdout,
                stream,
                logs_tx_clone.clone(),
                logs_token,
            ))
        })
    } else {
        let logs_tx_clone = logs_tx.clone();
        Box::new(move |cmd, capture_stdout, stream, logs_token| {
            Box::pin(commands::run(
                cmd,
                capture_stdout,
                stream,
                logs_tx_clone.clone(),
                logs_token,
            ))
        })
    };

    let emit_log_fn: Box<
        dyn Fn(
                sqlx::types::Uuid,
                &'static str,
                String,
            ) -> futures::future::BoxFuture<'static, anyhow::Result<()>>
            + Send
            + Sync,
    > = {
        let logs_tx_clone = logs_tx.clone();
        Box::new(move |token, stream, line| {
            let logs_tx = logs_tx_clone.clone();
            Box::pin(async move {
                logs_tx
                    .send(logs::Line {
                        token,
                        stream: stream.to_string(),
                        line,
                    })
                    .await
                    .context("failed to send to logs sink")
            })
        })
    };

    let worker = Worker {
        config,
        run_cmd_fn,
        emit_log_fn,
    };

    let mut checkouts = HashMap::new();
    let sleep = match action {
        Action::SetEncryption => worker.on_set_encryption(&mut state, &mut checkouts).await?,
        Action::PulumiPreview => worker.on_pulumi_preview(&mut state, &mut checkouts).await?,
        Action::PulumiRefresh => worker.on_pulumi_refresh(&mut state, &mut checkouts).await?,
        Action::PulumiUp1 => {
            let private_links = state.stack.config.model.private_links.clone();
            worker
                .on_pulumi_up_1(&mut state, &mut checkouts, private_links)
                .await?
        }
        Action::AwaitDNS1 => worker.on_await_dns_1(&mut state).await?,
        Action::Ansible => worker.on_ansible(&mut state, &mut checkouts).await?,
        Action::PulumiUp2 => worker.on_pulumi_up_2(&mut state, &mut checkouts).await?,
        Action::AwaitDNS2 => worker.on_await_dns_2(&mut state).await?,
    };

    Ok(ExecuteResponse::success(state, sleep))
}

struct Worker {
    config: ControllerConfig,
    run_cmd_fn: Box<
        dyn Fn(
                async_process::Command,
                bool,
                &'static str,
                sqlx::types::Uuid,
            ) -> futures::future::BoxFuture<'static, anyhow::Result<Vec<u8>>>
            + Send
            + Sync,
    >,
    emit_log_fn: Box<
        dyn Fn(
                sqlx::types::Uuid,
                &'static str,
                String,
            ) -> futures::future::BoxFuture<'static, anyhow::Result<()>>
            + Send
            + Sync,
    >,
}

// Implementation note: These methods are extracted from the original Controller
// in controller.rs.bak. They contain the actual work execution logic.
impl Worker {
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

    #[tracing::instrument(
        skip_all,
        fields(data_plane_id = ?state.data_plane_id, logs = ?state.logs_token),
    )]
    async fn on_set_encryption(
        &self,
        state: &mut stack::State,
        checkouts: &mut HashMap<String, tempfile::TempDir>,
    ) -> anyhow::Result<std::time::Duration> {
        let checkout = self
            .dry_dock_checkout(state, checkouts, &state.deploy_branch.clone())
            .await?;

        () = self
            .run_cmd(
                async_process::Command::new("pulumi")
                    .arg("stack")
                    .arg("init")
                    .arg(&state.stack_name)
                    .arg("--secrets-provider")
                    .arg(&self.config.secrets_provider)
                    .arg("--non-interactive")
                    .current_dir(checkout.path())
                    .env("PULUMI_BACKEND_URL", self.config.state_backend.as_str())
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

        state.stack.secrets_provider = self.config.secrets_provider.to_string();
        state.stack.encrypted_key = updated.encrypted_key;
        state.pending_converge = true;
        state.publish_stack = Some(state.stack.clone());
        state.status = stack::Status::Idle;

        Ok(POLL_AGAIN)
    }

    #[tracing::instrument(
        skip_all,
        fields(data_plane_id = ?state.data_plane_id, logs = ?state.logs_token),
    )]
    async fn on_pulumi_preview(
        &self,
        state: &mut stack::State,
        checkouts: &mut HashMap<String, tempfile::TempDir>,
    ) -> anyhow::Result<std::time::Duration> {
        let checkout = self
            .dry_dock_checkout(state, checkouts, &state.preview_branch.clone())
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
                    .env("PULUMI_BACKEND_URL", self.config.state_backend.as_str())
                    .env("VIRTUAL_ENV", checkout.path().join("venv")),
                "pulumi-preview",
                state.logs_token,
            )
            .await?;

        state.status = stack::Status::Idle;

        Ok(POLL_AGAIN)
    }

    #[tracing::instrument(
        skip_all,
        fields(data_plane_id = ?state.data_plane_id, logs = ?state.logs_token),
    )]
    async fn on_pulumi_refresh(
        &self,
        state: &mut stack::State,
        checkouts: &mut HashMap<String, tempfile::TempDir>,
    ) -> anyhow::Result<std::time::Duration> {
        let checkout = self
            .dry_dock_checkout(state, checkouts, &state.deploy_branch.clone())
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
                    .env("PULUMI_BACKEND_URL", self.config.state_backend.as_str())
                    .env("VIRTUAL_ENV", checkout.path().join("venv")),
                "pulumi-refresh",
                state.logs_token,
            )
            .await;

        if matches!(&result, Err(err) if err.downcast_ref::<commands::NonZeroExit>().is_some())
        {
            // We refreshed some changes, and must converge to (for example)
            // provision a replaced EC2 instance.
            state.pending_converge = true;
        } else {
            result?;
        }

        state.status = stack::Status::Idle;
        state.last_refresh = chrono::Utc::now();

        Ok(POLL_AGAIN)
    }

    #[tracing::instrument(
        skip_all,
        fields(data_plane_id = ?state.data_plane_id, logs = ?state.logs_token),
    )]
    async fn on_pulumi_up_1(
        &self,
        state: &mut stack::State,
        checkouts: &mut HashMap<String, tempfile::TempDir>,
        private_links: Vec<stack::PrivateLink>,
    ) -> anyhow::Result<std::time::Duration> {
        state.stack.config.model.private_links = private_links;

        let checkout = self
            .dry_dock_checkout(state, checkouts, &state.deploy_branch.clone())
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
                    .env("PULUMI_BACKEND_URL", self.config.state_backend.as_str())
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
            state.status = stack::Status::AwaitDNS1;
            format!("Waiting {:?} for DNS propagation.", self.config.dns_ttl)
        } else {
            state.status = stack::Status::Ansible;
            "No changes detected, continuing to Ansible.".to_string()
        };
        (self.emit_log_fn)(state.logs_token, "controller", log_line).await?;

        Ok(POLL_AGAIN)
    }

    #[tracing::instrument(
        skip_all,
        fields(data_plane_id = ?state.data_plane_id),
    )]
    async fn on_await_dns_1(&self, state: &mut stack::State) -> anyhow::Result<std::time::Duration> {
        let remainder = (state.last_pulumi_up + self.config.dns_ttl) - chrono::Utc::now();

        if remainder > chrono::TimeDelta::zero() {
            // PostgreSQL doesn't support nanosecond precision, so we must strip them.
            Ok(std::time::Duration::from_micros(
                remainder.num_microseconds().unwrap() as u64,
            ))
        } else {
            state.status = stack::Status::Ansible;

            Ok(POLL_AGAIN)
        }
    }

    #[tracing::instrument(
        skip_all,
        fields(data_plane_id = ?state.data_plane_id, logs = ?state.logs_token),
    )]
    async fn on_ansible(
        &self,
        state: &mut stack::State,
        checkouts: &mut HashMap<String, tempfile::TempDir>,
    ) -> anyhow::Result<std::time::Duration> {
        let checkout = self
            .dry_dock_checkout(state, checkouts, &state.deploy_branch.clone())
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
                    .env("PULUMI_BACKEND_URL", self.config.state_backend.as_str())
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

        state.status = stack::Status::PulumiUp2;
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
        state: &mut stack::State,
        checkouts: &mut HashMap<String, tempfile::TempDir>,
    ) -> anyhow::Result<std::time::Duration> {
        let checkout = self
            .dry_dock_checkout(state, checkouts, &state.deploy_branch.clone())
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
                    .env("PULUMI_BACKEND_URL", self.config.state_backend.as_str())
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
            state.status = stack::Status::AwaitDNS2;
            format!("Waiting {:?} for DNS propagation.", self.config.dns_ttl)
        } else {
            state.status = stack::Status::Idle;
            "No changes detected, done.".to_string()
        };
        (self.emit_log_fn)(state.logs_token, "controller", log_line).await?;

        Ok(POLL_AGAIN)
    }

    #[tracing::instrument(
        skip_all,
        fields(data_plane_id = ?state.data_plane_id),
    )]
    async fn on_await_dns_2(&self, state: &mut stack::State) -> anyhow::Result<std::time::Duration> {
        let remainder = (state.last_pulumi_up + self.config.dns_ttl) - chrono::Utc::now();

        if remainder > chrono::TimeDelta::zero() {
            // PostgreSQL doesn't support nanosecond precision, so we must strip them.
            Ok(std::time::Duration::from_micros(
                remainder.num_microseconds().unwrap() as u64,
            ))
        } else {
            state.status = stack::Status::Idle;

            Ok(POLL_AGAIN)
        }
    }

    async fn git_checkout<'c>(
        &self,
        state: &stack::State,
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
        state: &stack::State,
        checkouts: &'c mut HashMap<String, tempfile::TempDir>,
        branch: &str,
    ) -> anyhow::Result<&'c tempfile::TempDir> {
        let checkout = self
            .git_checkout(state, &self.config.dry_dock_remote, checkouts, branch)
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
            serde_yaml::to_string(&state.stack).context("failed to encode stack as YAML")?,
        )
        .context("failed to write stack YAML")?;

        Ok(checkout)
    }

    async fn create_clone(
        &self,
        remote: &str,
        logs_token: sqlx::types::Uuid,
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
        state: &stack::State,
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
                    .env("PULUMI_BACKEND_URL", self.config.state_backend.as_str())
                    .env("VIRTUAL_ENV", checkout.path().join("venv")),
                "pulumi-stack-history",
                state.logs_token,
            )
            .await?;

        let mut history: Vec<stack::PulumiStackHistory> = serde_json::from_slice(&output)
            .context("failed to parse pulumi stack history output")?;

        history.pop().context("pulumi stack history is empty")
    }
}
