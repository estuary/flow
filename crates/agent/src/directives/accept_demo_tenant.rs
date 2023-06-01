use super::{extract, JobStatus};

use agent_sql::directives::Row;
use serde::{Deserialize, Serialize};
use tracing::info;
use validator::Validate;

#[derive(Debug, Deserialize, Serialize, Validate, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Directive {}

#[derive(Debug, Deserialize, Serialize, Validate, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Claims {
    #[validate]
    tenant: models::PartitionField,
}

#[tracing::instrument(skip_all, fields(id=?row.apply_id))]
pub async fn apply(
    directive: Directive,
    row: Row,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<JobStatus> {
    let (Directive {}, Claims { tenant }) = match extract(directive, &row.user_claims) {
        Err(status) => return Ok(status),
        Ok(ok) => ok,
    };

    if row.catalog_prefix != "ops/" {
        return Ok(JobStatus::invalid_directive(anyhow::anyhow!(
            "AcceptDemoTenant directive must have ops/ catalog prefix, not {}",
            row.catalog_prefix
        )));
    }

    agent_sql::directives::accept_demo_tenant::create_demo_role_grant(
        Some("applied via directive".to_string()),
        &tenant,
        txn,
    )
    .await?;

    info!(%row.user_id, %tenant, "accept-demo-tenant");
    Ok(JobStatus::Success)
}