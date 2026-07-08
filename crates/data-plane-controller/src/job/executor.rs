use crate::protocol::{Action, ExecuteRequest, ExecuteResponse};
use crate::shared::controller::ControllerConfig;
use crate::shared::stack::{self, State, Status};
use anyhow::Context;
use futures::future::BoxFuture;
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
    // Private links pinned by (id, generation) at the `PulumiUp1` poll of this
    // converge. The post-converge status write only lands on rows whose
    // generation still matches, so a link edited mid-converge is skipped and
    // settled by the converge its own generation bump queued.
    pub pinned_links: Vec<stack::PinnedLink>,
}

/// Type-erased function for dispatching work execution.
/// In production this sends an HTTP request to the service; in tests
/// it can call the worker directly with mock functions.
pub type DispatchFn = Box<
    dyn Fn(ExecuteRequest) -> BoxFuture<'static, anyhow::Result<ExecuteResponse>> + Send + Sync,
>;

pub struct Executor {
    controller_config: ControllerConfig,
    dispatch_fn: DispatchFn,
}

impl Executor {
    pub fn new(controller_config: ControllerConfig, service_url: url::Url) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(3600)) // 1 hour timeout
            .build()
            .expect("failed to build HTTP client");

        let execute_url = service_url
            .join("/execute")
            .expect("failed to build execute URL");

        let service_url_for_auth = service_url.clone();

        let dispatch_fn: DispatchFn = Box::new(move |request: ExecuteRequest| {
            let client = http_client.clone();
            let url = execute_url.clone();
            let audience = service_url_for_auth.clone();

            Box::pin(async move {
                let mut req_builder = client.post(url).json(&request);

                // Attempt to add Google Cloud ID token for Cloud Run service-to-service authentication.
                // Fetch token directly from GCP metadata server.
                let metadata_url = format!(
                    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity?audience={}",
                    audience
                );

                match client
                    .get(&metadata_url)
                    .header("Metadata-Flavor", "Google")
                    .timeout(std::time::Duration::from_secs(5))
                    .send()
                    .await
                {
                    Ok(response) if response.status().is_success() => match response.text().await {
                        Ok(token) => {
                            req_builder = req_builder.bearer_auth(token.trim());
                        }
                        Err(err) => {
                            tracing::warn!(
                                ?err,
                                "failed to read GCP token from metadata server, proceeding without auth"
                            );
                        }
                    },
                    Ok(response) => {
                        tracing::warn!(
                            status = ?response.status(),
                            "metadata server returned non-success status, proceeding without auth"
                        );
                    }
                    Err(err) => {
                        tracing::warn!(
                            ?err,
                            "failed to fetch GCP token from metadata server, proceeding without auth"
                        );
                    }
                }

                let response = req_builder
                    .send()
                    .await
                    .context("HTTP request to service failed")?;

                if !response.status().is_success() {
                    anyhow::bail!("service returned non-success status: {}", response.status());
                }

                response
                    .json::<ExecuteResponse>()
                    .await
                    .context("failed to parse service response")
            })
        });

        Self {
            controller_config,
            dispatch_fn,
        }
    }

    pub fn new_with_dispatch(controller_config: ControllerConfig, dispatch_fn: DispatchFn) -> Self {
        Self {
            controller_config,
            dispatch_fn,
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
    pub async fn on_poll(
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
            Status::Idle => self.on_idle(state_ref, inbox, releases, row_state).await?,
            status => {
                // Refresh private_links from the current DB row on every poll,
                // so that retries pick up changes made to the table.
                state_ref.stack.config.model.private_links =
                    row_state.stack.config.model.private_links;

                // Pin the (id, generation) of the links this converge applies at
                // the poll that dispatches `pulumi up`, and deliberately do not
                // refresh it on later polls: the endpoint outputs reflect what
                // `PulumiUp1` provisioned, so the status write must attribute them
                // to the exact link versions read here.
                if matches!(status, Status::PulumiUp1) {
                    state_ref.pinned_links = row_state.pinned_links.clone();
                }

                // For all non-Idle statuses, dispatch to service worker.
                let action =
                    Action::from_status(status).context("cannot convert status to action")?;

                self.dispatch_to_service(task_id, state_ref, action).await?
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
            pinned_links: state_ref.pinned_links.clone(),
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
        // Periodically force a convergence pass even when nothing has changed,
        // to catch silent infrastructure drift or missed triggers.
        if state.last_pulumi_up + CONVERGE_INTERVAL < chrono::Utc::now() {
            state.pending_converge = true;
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

    /// Dispatch an action to the service worker.
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

        let execute_response = (self.dispatch_fn)(request).await?;

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

    // Desired links are read directly from `data_plane_private_links`, replacing
    // the retired `data_planes.private_links` projection. Each link's
    // (id, generation) is pinned so the post-converge status write attributes
    // endpoint results to the exact configuration version this converge applied.
    // Only the link `config` is handed to est-dry-dock; the id is withheld,
    // preserving the prior wire shape into the provisioner.
    let link_rows = sqlx::query!(
        r#"
        SELECT
            id AS "id: models::Id",
            generation,
            config AS "config!: sqlx::types::Json<stack::PrivateLink>"
        FROM internal.data_plane_private_links
        WHERE data_plane_id = $1
        ORDER BY created_at, id
        "#,
        row.data_plane_id as models::Id,
    )
    .fetch_all(pool)
    .await
    .context("failed to fetch data-plane private links")?;

    let mut private_links = Vec::with_capacity(link_rows.len());
    let mut pinned_links = Vec::with_capacity(link_rows.len());
    for link in link_rows {
        pinned_links.push(stack::PinnedLink {
            id: link.id,
            generation: link.generation,
        });
        private_links.push(link.config.0);
    }
    config.model.private_links = private_links;

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
        pinned_links,
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
            step,
            max_tier
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
            dekaf_address,
            dekaf_registry_address,
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
                    azure_application_client_id = $13,
                    dekaf_address = $14,
                    dekaf_registry_address = $15
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
                dekaf_address,
                dekaf_registry_address,
            )
            .execute(&mut *txn)
            .await
            .context("failed to publish exports into data_planes row")?;

            write_private_link_statuses(
                &mut *txn,
                self.data_plane_id,
                &self.pinned_links,
                &aws_link_endpoints,
                &azure_link_endpoints,
                &gcp_psc_endpoints,
            )
            .await?;
        }

        Ok(automations::Action::Sleep(self.sleep))
    }
}

/// Records each private link's observed status after a converge by matching this
/// converge's provisioned endpoints to the links it pinned on
/// `(provider, service_identity)`: a matched endpoint means `provisioned` with
/// the endpoint stored as `details`, no match means `pending`. This is the
/// temporary bridge until est-dry-dock emits a per-link result keyed by the link
/// id (which will also enable `failed`).
///
/// Two guards keep a converge from recording a stale status:
///  * Only providers that published at least one endpoint this converge are
///    re-evaluated (`published_providers`), so a transient empty or partial
///    export cannot flip an already-`provisioned` link back to `pending` and
///    null its details. A removed link is deleted, not emptied, so a genuine
///    teardown never relies on the array going empty.
///  * Only rows whose generation still matches the value pinned when this
///    converge read its desired links are updated. A link edited mid-converge
///    has a bumped generation, so it is skipped here (this converge did not
///    provision its current config) and is settled by the converge that edit's
///    generation bump queued.
async fn write_private_link_statuses(
    conn: &mut sqlx::PgConnection,
    data_plane_id: models::Id,
    pinned_links: &[stack::PinnedLink],
    aws_link_endpoints: &[serde_json::Value],
    azure_link_endpoints: &[serde_json::Value],
    gcp_psc_endpoints: &[serde_json::Value],
) -> anyhow::Result<()> {
    let pinned_ids: Vec<models::Id> = pinned_links.iter().map(|l| l.id).collect();
    let pinned_generations: Vec<i64> = pinned_links.iter().map(|l| l.generation).collect();

    sqlx::query!(
        r#"
        WITH pinned AS (
            SELECT id, generation
                FROM unnest($2::flowid[], $3::bigint[]) AS p(id, generation)
        ),
        endpoints AS (
            SELECT 'aws'::text AS provider, ep ->> 'service_name' AS identity, ep AS detail
                FROM unnest($4::jsonb[]) AS ep
            UNION ALL
            SELECT 'azure'::text, ep ->> 'service_name', ep
                FROM unnest($5::jsonb[]) AS ep
            UNION ALL
            SELECT 'gcp'::text, ep ->> 'service_attachment', ep
                FROM unnest($6::jsonb[]) AS ep
        ),
        published_providers AS (
            SELECT DISTINCT provider FROM endpoints
        )
        UPDATE internal.data_plane_private_links l SET
            status = CASE WHEN e.identity IS NOT NULL THEN 'provisioned' ELSE 'pending' END,
            details = e.detail,
            observed_at = now(),
            updated_at = now()
        FROM internal.data_plane_private_links l2
        JOIN pinned p ON p.id = l2.id AND p.generation = l2.generation
        LEFT JOIN endpoints e
            ON e.provider = l2.provider AND e.identity = l2.service_identity
        WHERE l.id = l2.id
          AND l2.data_plane_id = $1
          AND l2.provider IN (SELECT provider FROM published_providers)
        "#,
        data_plane_id as models::Id,
        pinned_ids as Vec<models::Id>,
        pinned_generations as Vec<i64>,
        aws_link_endpoints,
        azure_link_endpoints,
        gcp_psc_endpoints,
    )
    .execute(&mut *conn)
    .await
    .context("failed to update private link statuses")?;

    Ok(())
}

const IDLE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
const POLL_AGAIN: std::time::Duration = std::time::Duration::ZERO;
const REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2 * 60 * 60);
const CONVERGE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(4 * 60 * 60);

#[cfg(test)]
mod tests {
    use super::stack::PinnedLink;
    use super::write_private_link_statuses;

    async fn link_id(pool: &sqlx::PgPool, identity: &str) -> models::Id {
        sqlx::query_scalar!(
            r#"SELECT id as "id: models::Id"
               FROM internal.data_plane_private_links WHERE service_identity = $1"#,
            identity,
        )
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn status_of(pool: &sqlx::PgPool, identity: &str) -> (String, Option<serde_json::Value>) {
        let row = sqlx::query!(
            r#"
            SELECT status, details as "details: sqlx::types::Json<serde_json::Value>"
            FROM internal.data_plane_private_links WHERE service_identity = $1
            "#,
            identity,
        )
        .fetch_one(pool)
        .await
        .unwrap();
        (row.status, row.details.map(|d| d.0))
    }

    // Covers the two guards on the post-converge status write: the
    // published-providers guard (a provider with no endpoints this converge is
    // left untouched) and the generation guard (a row whose generation no longer
    // matches the pinned value is skipped; pinning the current generation
    // processes it).
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "fixtures", scripts("private_link_statuses"))
    )]
    async fn write_private_link_statuses_applies_guards(pool: sqlx::PgPool) {
        let data_plane_id: models::Id = sqlx::query_scalar!(
            r#"SELECT id as "id: models::Id" FROM data_planes WHERE data_plane_name = $1"#,
            "ops/dp/private/testCo/aws-1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        // This converge published one AWS endpoint (svc-a) and nothing for Azure
        // or GCP.
        let aws = vec![serde_json::json!({
            "service_name": "svc-a",
            "dns_entries": [{"dns_name": "svc-a.example"}]
        })];
        let none: Vec<serde_json::Value> = Vec::new();

        // Pin every link at generation 1. svc-edited is at generation 2 in the
        // fixture (an edit landed after this converge read its desired state), so
        // its pinned generation no longer matches the row.
        let pinned = vec![
            PinnedLink {
                id: link_id(&pool, "svc-a").await,
                generation: 1,
            },
            PinnedLink {
                id: link_id(&pool, "svc-orphan").await,
                generation: 1,
            },
            PinnedLink {
                id: link_id(&pool, "svc-edited").await,
                generation: 1,
            },
            PinnedLink {
                id: link_id(&pool, "svc-az").await,
                generation: 1,
            },
            PinnedLink {
                id: link_id(&pool, "svc-g").await,
                generation: 1,
            },
        ];

        let mut conn = pool.acquire().await.unwrap();
        write_private_link_statuses(&mut conn, data_plane_id, &pinned, &aws, &none, &none)
            .await
            .unwrap();

        // AWS published and matched -> provisioned.
        let (status, details) = status_of(&pool, "svc-a").await;
        assert_eq!(status, "provisioned");
        assert_eq!(details.unwrap()["service_name"], "svc-a");

        // AWS published but unmatched -> demoted to pending, details cleared.
        let (status, details) = status_of(&pool, "svc-orphan").await;
        assert_eq!(status, "pending");
        assert!(details.is_none());

        // AWS published but the pinned generation no longer matches, so the row
        // is skipped: it stays provisioned (an unguarded pass would demote it,
        // since no published endpoint matches svc-edited).
        assert_eq!(status_of(&pool, "svc-edited").await.0, "provisioned");

        // Azure and GCP published no endpoints, so the published-providers guard
        // leaves their links untouched.
        assert_eq!(status_of(&pool, "svc-az").await.0, "pending");
        assert_eq!(status_of(&pool, "svc-g").await.0, "provisioned");

        // Pinning svc-edited at its current generation (2) lets a later converge
        // process it: with svc-edited now among the published endpoints, its
        // status and details are refreshed.
        let aws = vec![
            serde_json::json!({"service_name": "svc-a"}),
            serde_json::json!({"service_name": "svc-edited", "fresh": true}),
        ];
        let pinned = vec![
            PinnedLink {
                id: link_id(&pool, "svc-a").await,
                generation: 1,
            },
            PinnedLink {
                id: link_id(&pool, "svc-edited").await,
                generation: 2,
            },
        ];
        write_private_link_statuses(&mut conn, data_plane_id, &pinned, &aws, &none, &none)
            .await
            .unwrap();
        let (status, details) = status_of(&pool, "svc-edited").await;
        assert_eq!(status, "provisioned");
        assert_eq!(details.unwrap()["fresh"], true);
    }
}
