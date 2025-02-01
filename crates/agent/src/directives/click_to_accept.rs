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
    #[validate(length(min = 1))]
    version: String,
}

#[tracing::instrument(skip_all, fields(id=?row.apply_id))]
pub async fn apply(
    directive: Directive,
    row: Row,
    _txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<JobStatus> {
    let (Directive {}, Claims { version }) = match extract(directive, &row.user_claims) {
        Err(status) => return Ok(status),
        Ok(ok) => ok,
    };

    if row.catalog_prefix != "ops/" {
        return Ok(JobStatus::invalid_directive(anyhow::anyhow!(
            "ClickToAccept directive must have ops/ catalog prefix, not {}",
            row.catalog_prefix
        )));
    }

    info!(%row.user_id, %version, "click-to-accept");
    Ok(JobStatus::Success)
}

#[cfg(test)]
mod test {
    use sqlx::Row;

    #[tokio::test]
    #[serial_test::serial]
    async fn test_cases() {
        let mut harness =
            crate::integration_tests::harness::TestHarness::init("click-to-accept directives")
                .await;

        sqlx::query(
            r#"
        with p1 as (
          insert into directives (id, catalog_prefix, spec) values
          ('aa00000000000000', 'ops/',   '{"type":"clickToAccept"}'),
          ('bb00000000000000', 'other/', '{"type":"clickToAccept"}')
        ),
        p2 as (
          delete from applied_directives -- Clear seed fixture
        ),
        p3 as (
          insert into applied_directives (directive_id, user_id, user_claims) values
          ('aa00000000000000', '11111111-1111-1111-1111-111111111111', '{"version":"v1.2.3"}'),
          ('aa00000000000000', '11111111-1111-1111-1111-111111111111', '{}'),
          ('bb00000000000000', '11111111-1111-1111-1111-111111111111', '{"version":"v1.2.3"}')
        )
        select 1;
        "#,
        )
        .execute(&harness.pool)
        .await
        .unwrap();

        while harness
            .run_automation_task(automations::task_types::APPLIED_DIRECTIVES)
            .await
            .is_some()
        {
            // Run tasks until we're done
        }

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
              "version": "v1.2.3"
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
              "error": "missing field `version` at line 1 column 2",
              "type": "invalidClaims"
            }
          },
          {
            "claims": {
              "version": "v1.2.3"
            },
            "did": "bb:00:00:00:00:00:00:00",
            "status": {
              "error": "ClickToAccept directive must have ops/ catalog prefix, not other/",
              "type": "invalidDirective"
            }
          }
        ]
        "###);
    }
}
