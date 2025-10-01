// This directive enables users to opt into having read access to the demo/ tenant.
use super::{extract, JobStatus, Row};

use serde::{Deserialize, Serialize};
use tracing::info;
use validator::Validate;

#[derive(Debug, Deserialize, Serialize, Validate, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Directive {}

#[derive(Debug, Deserialize, Serialize, Validate, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Claims {
    #[validate(nested)]
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

    control_plane_api::directives::accept_demo_tenant::create_demo_role_grant(
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
    use sqlx::Row;

    #[tokio::test]
    #[serial_test::serial]
    async fn test_cases() {
        let mut harness =
            crate::integration_tests::harness::TestHarness::init("accept demo tenant directives")
                .await;
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
        .execute(&harness.pool)
        .await
        .unwrap();

        let mut runs = 0;
        while harness
            .run_automation_task(automations::task_types::APPLIED_DIRECTIVES)
            .await
            .is_some()
        {
            runs += 1;
        }
        assert_eq!(4, runs, "expected 4 runs, got {runs}");

        let applies = sqlx::query(
            r#"select json_build_object('status', d.job_status, 'did', d.directive_id, 'claims', d.user_claims)
            from applied_directives d order by id asc;"#,
        )
        .fetch_all(&harness.pool)
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
