use super::{Handler, Id};

use agent_sql::directives::Row;
use serde::{Deserialize, Serialize};
use tracing::info;
use validator::Validate;

pub mod beta_onboard;
pub mod click_to_accept;
pub mod grant;

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
    Grant(grant::Directive),
}

#[derive(Default)]
pub struct DirectiveHandler {
    tenant_template: models::Catalog,
    accounts_user_email: String,
}

impl DirectiveHandler {
    pub fn new(tenant_template: models::Catalog, accounts_user_email: String) -> Self {
        Self {
            tenant_template,
            accounts_user_email,
        }
    }
}

#[async_trait::async_trait]
impl Handler for DirectiveHandler {
    async fn handle(&mut self, pg_pool: &sqlx::PgPool) -> anyhow::Result<std::time::Duration> {
        let mut txn = pg_pool.begin().await?;

        let row: Row = match agent_sql::directives::dequeue(&mut txn).await? {
            None => return Ok(std::time::Duration::from_secs(5)),
            Some(row) => row,
        };

        let (id, status) = self.process(row, &mut txn).await?;
        info!(%id, ?status, "finished");

        agent_sql::directives::resolve(id, status, &mut txn).await?;
        txn.commit().await?;

        Ok(std::time::Duration::ZERO)
    }
}

impl DirectiveHandler {
    #[tracing::instrument(err, skip_all, fields(id=?row.apply_id))]
    async fn process(
        &mut self,
        row: Row,
        txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> anyhow::Result<(Id, JobStatus)> {
        info!(
            %row.apply_updated_at,
            %row.catalog_prefix,
            %row.directive_id,
            %row.logs_token,
            row.user_claims = %row.user_claims.0.get(),
            %row.user_id,
            "processing directive application",
        );
        let apply_id = row.apply_id;

        let status = match serde_json::from_str::<Directive>(row.directive_spec.0.get()) {
            Err(err) => JobStatus::invalid_directive(err.into()),
            Ok(Directive::BetaOnboard(d)) => {
                beta_onboard::apply(
                    d,
                    row,
                    &self.accounts_user_email,
                    &self.tenant_template,
                    txn,
                )
                .await?
            }
            Ok(Directive::ClickToAccept(d)) => click_to_accept::apply(d, row, txn).await?,
            Ok(Directive::Grant(d)) => grant::apply(d, row, txn).await?,
        };
        Ok((apply_id, status))
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
