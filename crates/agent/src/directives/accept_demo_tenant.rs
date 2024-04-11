// This directive enables users to opt into having read access to the demo/ tenant.
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
    tenant: models::Prefix,
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

#[cfg(test)]
mod test {
    use super::super::DirectiveHandler;
    use sqlx::{Connection, Row};

    const FIXED_DATABASE_URL: &str = "postgresql://postgres:postgres@localhost:5432/postgres";

    #[tokio::test]
    async fn test_cases() {
        let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
            .await
            .unwrap();
        let mut txn = conn.begin().await.unwrap();

        sqlx::query(
            r#"
        with p1 as (
          insert into directives (id, catalog_prefix, spec) values
          ('aa00000000000000', 'ops/',   '{"type":"acceptDemoTenant"}'),
          ('bb00000000000000', 'InvalidPrefix/', '{"type":"acceptDemoTenant"}')
        ),
        p2 as (
          delete from applied_directives -- Clear seed fixture
        ),
        p3 as (
          insert into applied_directives (directive_id, user_id, user_claims) values
          ('aa00000000000000', '11111111-1111-1111-1111-111111111111', '{"tenant":"test/"}'),
          ('aa00000000000000', '11111111-1111-1111-1111-111111111111', '{}'),
          ('aa00000000000000', '11111111-1111-1111-1111-111111111111', '{"tenant":"invalid/tenant"}'),
          ('bb00000000000000', '22222222-2222-2222-2222-222222222222', '{"tenant":"test/"}')
        )
        select 1;
        "#,
        )
        .execute(&mut txn)
        .await
        .unwrap();

        let mut handler = DirectiveHandler::default();
        while let Some(row) = agent_sql::directives::dequeue(&mut txn, false)
            .await
            .unwrap()
        {
            let (id, status) = handler.process(row, &mut txn).await.unwrap();
            agent_sql::directives::resolve(id, status, &mut txn)
                .await
                .unwrap();
        }

        let applies = sqlx::query(
            r#"select json_build_object('status', d.job_status, 'did', d.directive_id, 'claims', d.user_claims)
            from applied_directives d order by id asc;"#,
        )
        .fetch_all(&mut txn)
        .await
        .unwrap();

        insta::assert_json_snapshot!(
          applies.iter().map(|r| -> serde_json::Value { r.get(0) }).collect::<Vec<_>>(),
          @r###"
        [
          {
            "claims": {
              "tenant": "test/"
            },
            "did": "aa:00:00:00:00:00:00:00",
            "status": {
              "type": "success"
            }
          },
          {
            "claims": {},
            "did": "aa:00:00:00:00:00:00:00",
            "status": {
              "error": "missing field `tenant` at line 1 column 2",
              "type": "invalidClaims"
            }
          },
          {
            "claims": {
              "tenant": "invalid/tenant"
            },
            "did": "aa:00:00:00:00:00:00:00",
            "status": {
              "error": "tenant.: invalid/tenant doesn't match pattern ([\\p{Letter}\\p{Number}\\-_\\.]+/)* (unmatched portion is: tenant)",
              "type": "invalidClaims"
            }
          },
          {
            "claims": {
              "tenant": "test/"
            },
            "did": "bb:00:00:00:00:00:00:00",
            "status": {
              "error": "AcceptDemoTenant directive must have ops/ catalog prefix, not InvalidPrefix/",
              "type": "invalidDirective"
            }
          }
        ]
        "###);
    }
}
