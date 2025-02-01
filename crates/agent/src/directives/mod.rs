use crate::logs;

use agent_sql::directives::{fetch_directive, resolve, Row};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use tracing::info;
use validator::Validate;

pub mod accept_demo_tenant;
pub mod beta_onboard;
pub mod click_to_accept;
pub mod grant;
pub mod storage_mappings;

/// JobStatus is the possible outcomes of a handled directive operation.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum JobStatus {
    Queued,
    InvalidDirective { error: String },
    InvalidClaims { error: String },
    Success,
}

impl JobStatus {
    fn invalid_directive(err: anyhow::Error) -> Self {
        Self::InvalidDirective {
            error: format!("{err:?}"),
        }
    }
    fn invalid_claims(err: anyhow::Error) -> Self {
        Self::InvalidClaims {
            error: format!("{err:?}"),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum Directive {
    BetaOnboard(beta_onboard::Directive),
    ClickToAccept(click_to_accept::Directive),
    AcceptDemoTenant(accept_demo_tenant::Directive),
    Grant(grant::Directive),
    StorageMappings(storage_mappings::Directive),
}

#[derive(Clone)]
pub struct DirectiveHandler {
    accounts_user_email: String,
    logs_tx: logs::Tx,
}

impl DirectiveHandler {
    pub fn new(accounts_user_email: String, logs_tx: &logs::Tx) -> Self {
        Self {
            accounts_user_email,
            logs_tx: logs_tx.clone(),
        }
    }
}

impl automations::Executor for DirectiveHandler {
    const TASK_TYPE: automations::TaskType = automations::task_types::APPLIED_DIRECTIVES;

    type Receive = serde_json::Value;

    type State = ();

    type Outcome = automations::Action;

    async fn poll<'s>(
        &'s self,
        pool: &'s sqlx::PgPool,
        task_id: models::Id,
        _parent_id: Option<models::Id>,
        _state: &'s mut Self::State,
        inbox: &'s mut std::collections::VecDeque<(models::Id, Option<Self::Receive>)>,
    ) -> anyhow::Result<Self::Outcome> {
        tracing::debug!(?inbox, %task_id, "running directive task");

        let mut txn = pool.begin().await?;
        let row = fetch_directive(task_id, &mut txn).await?;

        // It's technically possible that we could commit the transaction to
        // handle an applied directive, but fail to commit the task resolution.
        // This check ensures that we don't try to apply the directive twice
        // should that happen.
        if Some("queued") != row.status_type.as_deref() {
            tracing::warn!(
                %task_id,
                status_type = ?row.status_type,
                "skipping directive application because job status is not queued"
            );
            txn.rollback().await?;
            return Ok(automations::Action::Done);
        }

        let time_queued = chrono::Utc::now().signed_duration_since(row.apply_updated_at);
        let status = self.process(row, &mut txn).await?;
        tracing::info!(%time_queued, ?status, "finished");
        resolve(task_id, status, &mut txn).await?;
        txn.commit().await.context("committing transaction")?;

        inbox.clear();
        Ok(automations::Action::Done)
    }
}

impl DirectiveHandler {
    #[tracing::instrument(err, skip_all, fields(id=?row.apply_id))]
    async fn process(
        &self,
        row: Row,
        txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> anyhow::Result<JobStatus> {
        info!(
            %row.apply_updated_at,
            %row.catalog_prefix,
            %row.directive_id,
            %row.logs_token,
            row.user_claims = %row.user_claims.0.get(),
            %row.user_id,
            "processing directive application",
        );

        let status = match serde_json::from_str::<Directive>(row.directive_spec.0.get()) {
            Err(err) => JobStatus::invalid_directive(err.into()),
            Ok(Directive::BetaOnboard(d)) => {
                beta_onboard::apply(d, row, &self.accounts_user_email, txn).await?
            }
            Ok(Directive::ClickToAccept(d)) => click_to_accept::apply(d, row, txn).await?,
            Ok(Directive::AcceptDemoTenant(d)) => accept_demo_tenant::apply(d, row, txn).await?,
            Ok(Directive::Grant(d)) => grant::apply(d, row, txn).await?,
            Ok(Directive::StorageMappings(d)) => {
                storage_mappings::apply(d, row, &self.logs_tx, txn).await?
            }
        };
        Ok(status)
    }
}

// extract user claims and jointly validate both the directive and claims.
// JsonSchema isn't technically required here, but we use it as a lever to ensure
// that all claim types are able to generate JSON schemas.
fn extract<'de, Directive: Validate, Claims: Deserialize<'de> + Validate + schemars::JsonSchema>(
    directive: Directive,
    claims: &'de serde_json::value::RawValue,
) -> Result<(Directive, Claims), JobStatus> {
    if let Err(err) = directive.validate() {
        return Err(JobStatus::invalid_directive(err.into()));
    }

    match serde_json::from_str::<Claims>(claims.get())
        .map_err(anyhow::Error::new)
        .and_then(|claims| {
            claims.validate()?;
            Ok(claims)
        }) {
        Ok(claims) => Ok((directive, claims)),
        Err(err) => Err(JobStatus::invalid_claims(err)),
    }
}
