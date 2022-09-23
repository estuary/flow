use super::{extract, JobStatus};

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
    // TODO(johnny): Introduce models::Tenant which, like PartitionField, also uses TOKEN_RE.
    #[validate]
    requested_tenant: models::PartitionField,
}

#[tracing::instrument(skip_all, fields(directive, row.claims))]
pub async fn apply(
    directive: Directive,
    row: agent_sql::directives::Row,
    accounts_user_email: &str,
    tenant_template: &models::Catalog,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<JobStatus> {
    let (Directive {}, Claims { requested_tenant }) = match extract(directive, &row.user_claims) {
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
            "requested tenant {} is not available",
            requested_tenant.as_str()
        )));
    }

    let provisioned_user = agent_sql::directives::beta_onboard::provision_tenant(
        accounts_user_email,
        Some("applied via directive".to_string()),
        &requested_tenant,
        row.user_id,
        txn,
    )
    .await
    .context("provision_tenant")?;

    // Fill out the tenant file spec template with the actual tenant name,
    // and upsert it into provisioned draft.
    let tenant_template = serde_json::to_string(tenant_template).unwrap();
    let tenant_template = tenant_template.replace("TENANT", requested_tenant.as_str());
    let tenant_template: models::Catalog = serde_json::from_str(&tenant_template).unwrap();
    crate::upsert_draft_specs(provisioned_user.draft_id, tenant_template, txn)
        .await
        .context("upsert_draft_specs")?;

    info!(%row.user_id, requested_tenant=%requested_tenant.as_str(), "beta onboard");
    Ok(JobStatus::Success)
}

#[cfg(test)]
mod test {

    use super::super::DirectiveHandler;
    use serde_json::json;
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
          ('cc00000000000000', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '{"requestedTenant":"AcmeTenant"}')
        )
        select 1;
        "#,
        )
        .execute(&mut txn)
        .await
        .unwrap();

        let tenant_template: models::Catalog = serde_json::from_value(json!({
          "collections": {
            "ops/TENANT/fixture":{
              "schema": {
                "type": "object",
                "properties": {
                  "k": {"type": "integer"}
                },
                "required": ["k"]
              },
              "key": ["/k"]
            }
          }
        }))
        .unwrap();

        let mut handler = DirectiveHandler {
            tenant_template,
            accounts_user_email: "accounts@example.com".to_string(),
        };
        while let Some(row) = agent_sql::directives::dequeue(&mut txn).await.unwrap() {
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
              "error": "unknown field `invalid`, expected `requestedTenant` at line 1 column 10",
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
              "error": "requested tenant TakenTeNaNt is not available",
              "type": "invalidClaims"
            }
          },
          {
            "claims": {
              "requestedTenant": "AcmeTenant"
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
            -- Expect a draft & publication was created.
            select json_build_object('name', s.catalog_name, 'spec', s.spec)
                from draft_specs s join drafts d on d.id = s.draft_id
                where d.user_id = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb'
            union all
            select json_build_object('publication', true)
                from publications p where p.user_id = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb'
            "#,
        )
        .fetch_all(&mut txn)
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
            "roleGrantObj": "ops/AcmeTenant/"
          },
          {
            "cap": "read",
            "roleGrantObj": "estuary/public/"
          },
          {
            "prefix": "AcmeTenant/",
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
            "name": "ops/AcmeTenant/fixture",
            "spec": {
              "key": [
                "/k"
              ],
              "schema": {
                "properties": {
                  "k": {
                    "type": "integer"
                  }
                },
                "required": [
                  "k"
                ],
                "type": "object"
              }
            }
          },
          {
            "publication": true
          }
        ]
        "###);
    }
}
