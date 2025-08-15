use super::{extract, JobStatus, Row};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use tracing::info;
use validator::Validate;

#[derive(Debug, Deserialize, Serialize, Validate, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Directive {}

#[derive(Deserialize, Validate, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Claims {
    #[validate(nested)]
    requested_tenant: models::Token,
    // Survey results for the tenant.
    // This is persisted in the DB but is not actually used by the agent.
    #[allow(dead_code)]
    #[serde(default)]
    survey: serde_json::Value,
}

#[tracing::instrument(skip_all, fields(directive, row.claims))]
pub async fn apply(
    directive: Directive,
    row: Row,
    accounts_user_email: &str,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<JobStatus> {
    let (
        Directive {},
        Claims {
            requested_tenant,
            survey: _,
        },
    ) = match extract(directive, &row.user_claims) {
        Err(status) => return Ok(status),
        Ok(ok) => ok,
    };

    if row.catalog_prefix != "ops/" {
        return Ok(JobStatus::invalid_directive(anyhow::anyhow!(
            "BetaOnboard directive must have ops/ catalog prefix, not {}",
            row.catalog_prefix
        )));
    }
    if agent_sql::directives::beta_onboard::is_user_provisioned(row.user_id, &mut *txn).await? {
        return Ok(JobStatus::invalid_claims(anyhow::anyhow!(
            "Cannot provision a new tenant because the user has existing grants",
        )));
    }
    if agent_sql::directives::beta_onboard::tenant_exists(&requested_tenant, &mut *txn).await? {
        return Ok(JobStatus::invalid_claims(anyhow::anyhow!(
            "The organization name {} is already in use, please choose a different one or contact support@estuary.dev.",
            requested_tenant.as_str()
        )));
    }

    agent_sql::directives::beta_onboard::provision_tenant(
        accounts_user_email,
        Some("applied via directive".to_string()),
        &requested_tenant,
        row.user_id,
        txn,
    )
    .await
    .context("provision_tenant")?;

    info!(%row.user_id, requested_tenant=%requested_tenant.as_str(), "beta onboard");
    Ok(JobStatus::Success)
}

#[cfg(test)]
mod test {

    use sqlx::Row;

    #[tokio::test]
    #[serial_test::serial]
    async fn test_cases() {
        let mut harness =
            crate::integration_tests::harness::TestHarness::init("beta-onboard directives").await;

        sqlx::query(
            r#"
        with p1 as (
          insert into directives (id, catalog_prefix, spec) values
          ('aa00000000000000', 'InvalidPrefix/',  '{"type":"betaOnboard"}'),
          ('bb00000000000000', 'ops/', '{"type":"betaOnboard","invalid":"schema"}'),
          ('cc00000000000000', 'ops/',  '{"type":"betaOnboard"}')
        ),
        p2 as (
          insert into tenants (tenant) values ('takenTenant/')
        ),
        p3 as (
          insert into auth.users (id, email) values
          ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'new@example.com'),
          ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', 'accounts@example.com')
          on conflict do nothing
        ),
        p4 as (
          delete from applied_directives -- Clear seed fixture.
        ),
        p5 as (
          insert into user_grants (user_id, object_role, capability) values
            ('11111111-1111-1111-1111-111111111111', 'takenTenant/', 'admin'), -- Prevents new tenant.
            ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'takenTenant/', 'read')   -- New tenant allowed.
        ),
        p6 as (
          insert into applied_directives (directive_id, user_id, user_claims) values
          -- Fails: directive prefix is incorrect.
          ('aa00000000000000', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '{"requestedTenant":"AcmeTenant"}'),
          -- Fails: directive schema is invalid.
          ('bb00000000000000', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '{"requestedTenant":"AcmeTenant"}'),
          -- Fails: user is already 'admin' of takenTenant/
          ('cc00000000000000', '11111111-1111-1111-1111-111111111111', '{"requestedTenant":"AcmeTenant"}'),
          -- Fails: claims are malformed.
          ('cc00000000000000', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '{"invalid":"schema"}'),
          -- Fails: requestedTenant is malformed.
          ('cc00000000000000', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '{"requestedTenant":"invalid/requested/tenant"}'),
          -- Fails: tenant already exists.
          ('cc00000000000000', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '{"requestedTenant":"TakenTeNaNt"}'),
          -- Success: creates AcmeTenant.
          ('cc00000000000000', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '{"requestedTenant":"AcmeTenant","survey":"feedback"}')
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
              "requestedTenant": "AcmeTenant"
            },
            "did": "aa:00:00:00:00:00:00:00",
            "status": {
              "error": "BetaOnboard directive must have ops/ catalog prefix, not InvalidPrefix/",
              "type": "invalidDirective"
            }
          },
          {
            "claims": {
              "requestedTenant": "AcmeTenant"
            },
            "did": "bb:00:00:00:00:00:00:00",
            "status": {
              "error": "unknown field `invalid`, there are no fields",
              "type": "invalidDirective"
            }
          },
          {
            "claims": {
              "requestedTenant": "AcmeTenant"
            },
            "did": "cc:00:00:00:00:00:00:00",
            "status": {
              "error": "Cannot provision a new tenant because the user has existing grants",
              "type": "invalidClaims"
            }
          },
          {
            "claims": {
              "invalid": "schema"
            },
            "did": "cc:00:00:00:00:00:00:00",
            "status": {
              "error": "unknown field `invalid`, expected `requestedTenant` or `survey` at line 1 column 10",
              "type": "invalidClaims"
            }
          },
          {
            "claims": {
              "requestedTenant": "invalid/requested/tenant"
            },
            "did": "cc:00:00:00:00:00:00:00",
            "status": {
              "error": "requested_tenant.: invalid/requested/tenant doesn't match pattern [\\p{Letter}\\p{Number}\\-_\\.]+ (unmatched portion is: /requested/tenant)",
              "type": "invalidClaims"
            }
          },
          {
            "claims": {
              "requestedTenant": "TakenTeNaNt"
            },
            "did": "cc:00:00:00:00:00:00:00",
            "status": {
              "error": "The organization name TakenTeNaNt is already in use, please choose a different one or contact support@estuary.dev.",
              "type": "invalidClaims"
            }
          },
          {
            "claims": {
              "requestedTenant": "AcmeTenant",
              "survey": "feedback"
            },
            "did": "cc:00:00:00:00:00:00:00",
            "status": {
              "type": "success"
            }
          }
        ]
        "###);

        let grants = sqlx::query(
            r#"
            -- Expect a tenant was created.
            select json_build_object('tenant', t.tenant, 'detail', t.detail)
                from tenants t
            union all
            -- Expect a user grant was created.
            select json_build_object('userGrantObj', g.object_role, 'cap', g.capability)
                from user_grants g where user_id = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'
            union all
            -- Expect a role grant was created.
            select json_build_object('roleGrantObj', g.object_role, 'cap', g.capability)
                from role_grants g where subject_role = 'AcmeTenant/'
            union all
            -- Expect a storage mapping was created.
            select json_build_object('prefix', m.catalog_prefix, 'storageMapping', m.spec)
                from storage_mappings m where m.catalog_prefix like '%AcmeTenant%'
            union all
            -- Expect an alert subscription was created.
            select json_build_object('catalog_prefix', s.catalog_prefix, 'email', s.email)
                from alert_subscriptions s where s.catalog_prefix = 'AcmeTenant/'
            "#,
        )
        .fetch_all(&harness.pool)
        .await
        .unwrap();

        insta::assert_json_snapshot!(
          grants.iter().map(|r| -> serde_json::Value { r.get(0) }).collect::<Vec<_>>(),
          @r###"
        [
          {
            "detail": null,
            "tenant": "takenTenant/"
          },
          {
            "detail": "applied via directive",
            "tenant": "AcmeTenant/"
          },
          {
            "cap": "read",
            "userGrantObj": "takenTenant/"
          },
          {
            "cap": "admin",
            "userGrantObj": "AcmeTenant/"
          },
          {
            "cap": "write",
            "roleGrantObj": "AcmeTenant/"
          },
          {
            "cap": "read",
            "roleGrantObj": "ops/dp/public/"
          },
          {
            "prefix": "AcmeTenant/",
            "storageMapping": {
              "data_planes": [
                "ops/dp/public/test"
              ],
              "stores": [
                {
                  "bucket": "estuary-trial",
                  "prefix": "collection-data/",
                  "provider": "GCS"
                }
              ]
            }
          },
          {
            "prefix": "recovery/AcmeTenant/",
            "storageMapping": {
              "stores": [
                {
                  "bucket": "estuary-trial",
                  "provider": "GCS"
                }
              ]
            }
          },
          {
            "catalog_prefix": "AcmeTenant/",
            "email": "new@example.com"
          }
        ]
        "###);
    }
}
