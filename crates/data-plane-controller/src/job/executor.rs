use crate::protocol::{Action, ExecuteRequest, ExecuteResponse};
use crate::shared::controller::ControllerConfig;
use crate::shared::stack::{self, State, Status};
use anyhow::Context;
use std::collections::VecDeque;

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
    controller_config: ControllerConfig,
    service_url: url::Url,
    http_client: reqwest::Client,
}

impl Executor {
    pub fn new(controller_config: ControllerConfig, service_url: url::Url) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(3600)) // 1 hour timeout
            .build()
            .expect("failed to build HTTP client");

        Self {
            controller_config,
            service_url,
            http_client,
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
        let row_state =
            fetch_row_state(pool, task_id, &self.controller_config.secrets_provider).await?;
        let releases = fetch_releases(pool, row_state.data_plane_id).await?;

        self.on_poll(task_id, state, inbox, releases, row_state)
            .await
    }
}

impl Executor {
    async fn on_poll(
        &self,
        task_id: models::Id,
        state: &mut Option<State>,
        inbox: &mut VecDeque<(models::Id, Option<Message>)>,
        releases: Vec<stack::Release>,
        row_state: State,
    ) -> anyhow::Result<Outcome> {
        if state.is_none() {
            () = self.on_start(state, inbox, &row_state)?;
        };
        let state_ref = state.as_mut().unwrap();

        let sleep = match state_ref.status {
            Status::Idle => {
                self.on_idle(state_ref, inbox, releases, row_state).await?
            }
            status => {
                // For all non-Idle statuses, dispatch to service worker.
                let action = Action::from_status(status)
                    .context("cannot convert status to action")?;

                self.dispatch_to_service(task_id, state_ref, action)
                    .await?
            }
        };

        // We publish an updated stack only when transitioning back to Idle.
        let publish_stack = if matches!(state_ref.status, Status::Idle) {
            state_ref.publish_stack.take()
        } else {
            None
        };

        Ok(Outcome {
            data_plane_id: state_ref.data_plane_id,
            task_id,
            sleep,
            status: state_ref.status,
            publish_exports: state_ref.publish_exports.take(),
            publish_stack,
            kms_key: self.controller_config.secrets_provider.clone(),
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

    /// Dispatch an action to the service worker via HTTP.
    #[tracing::instrument(
        skip_all,
        fields(
            task_id = %task_id,
            data_plane_id = %state.data_plane_id,
            action = ?action,
        ),
    )]
    async fn dispatch_to_service(
        &self,
        task_id: models::Id,
        state: &mut State,
        action: Action,
    ) -> anyhow::Result<std::time::Duration> {
        let request = ExecuteRequest {
            task_id,
            data_plane_id: state.data_plane_id,
            logs_token: state.logs_token,
            state: state.clone(),
            action,
            controller_config: (&self.controller_config).into(),
        };

        tracing::info!("dispatching to service");

        let execute_url = self
            .service_url
            .join("/execute")
            .context("failed to build execute URL")?;

        let response = self
            .http_client
            .post(execute_url)
            .json(&request)
            .send()
            .await
            .context("HTTP request to service failed")?;

        if !response.status().is_success() {
            anyhow::bail!(
                "service returned non-success status: {}",
                response.status()
            );
        }

        let execute_response: ExecuteResponse = response
            .json()
            .await
            .context("failed to parse service response")?;

        if !execute_response.success {
            anyhow::bail!(
                "service returned error: {}",
                execute_response.error.unwrap_or_default()
            );
        }

        let next_state = execute_response
            .next_state
            .context("service did not return next_state")?;

        *state = next_state;

        Ok(std::time::Duration::from_millis(
            execute_response.sleep_duration_ms,
        ))
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
            gcp_psc_endpoints,
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
                    gcp_psc_endpoints = $6,
                    gcp_service_account_email = $7,
                    hmac_keys = $8,
                    encrypted_hmac_keys = $9,
                    bastion_tunnel_private_key = $10,
                    azure_application_name = $11,
                    azure_link_endpoints = $12,
                    azure_application_client_id = $13
                WHERE id = $1 AND controller_task_id = $2
                "#,
                self.data_plane_id as models::Id,
                self.task_id as models::Id,
                aws_iam_user_arn,
                &aws_link_endpoints,
                &cidr_blocks,
                &gcp_psc_endpoints,
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
