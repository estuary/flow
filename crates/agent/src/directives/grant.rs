use super::{extract, JobStatus};

use agent_sql::directives::Row;
use serde::{Deserialize, Serialize};
use tracing::info;
use validator::Validate;

#[derive(Debug, Deserialize, Serialize, Validate, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Directive {
    #[validate]
    granted_prefix: models::Prefix,
    capability: agent_sql::Capability,
}

#[derive(Deserialize, Validate, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Claims {
    #[validate]
    requested_prefix: Option<models::Prefix>,
}

#[tracing::instrument(skip_all, fields(directive, row.user_claims))]
pub async fn apply(
    directive: Directive,
    row: Row,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<JobStatus> {
    let (
        Directive {
            capability,
            granted_prefix,
        },
        Claims { requested_prefix },
    ) = match extract(directive, &row.user_claims) {
        Err(status) => return Ok(status),
        Ok(ok) => ok,
    };

    if !granted_prefix.starts_with(&row.catalog_prefix) {
        return Ok(JobStatus::invalid_directive(anyhow::anyhow!(
            "Grant directive cannot grant {} because it is not a suffix of its catalog prefix {}",
            granted_prefix.as_str(),
            row.catalog_prefix
        )));
    }

    if matches!(&requested_prefix, Some(p) if !p.starts_with(granted_prefix.as_str())) {
        return Ok(JobStatus::invalid_claims(anyhow::anyhow!(
            "Grant claims cannot request {} because it is not a suffix of granted prefix {}",
            requested_prefix.unwrap().as_str(),
            granted_prefix.as_str(),
        )));
    }

    let granted_prefix = requested_prefix
        .as_ref()
        .map(|r| r.as_str())
        .unwrap_or(granted_prefix.as_str());

    agent_sql::directives::grant::upsert_user_grant(
        row.user_id,
        granted_prefix,
        capability,
        Some("applied via directive".to_string()),
        txn,
    )
    .await?;

    info!(%row.user_id, %granted_prefix, ?capability, "user grant");
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
          ('aa00000000000000', 'One/',  '{"type":"grant","grantedPrefix":"One/Two/","capability":"write"}'),
          ('bb00000000000000', 'One/',  '{"type":"grant","grantedPrefix":"One/","capability":"read"}'),
          ('cc00000000000000', 'Path/', '{"type":"grant","grantedPrefix":"Wrong/Path/","capability":"read"}'),
          ('dd00000000000000', 'Not/',  '{"type":"grant","grantedPrefix":"Not/A/Prefix!","capability":"read"}')
        ),
        p2 as (
          insert into applied_directives (directive_id, user_id, user_claims) values
          -- Success: specific requested suffix.
          ('aa00000000000000', '11111111-1111-1111-1111-111111111111', '{"requestedPrefix":"One/Two/Three/"}'),
          -- Success: defaults to grantedPrefix One/Two/.
          ('aa00000000000000', '11111111-1111-1111-1111-111111111111', '{}'),
          -- Success: no-op that does not clobber write => read capability.
          ('bb00000000000000', '11111111-1111-1111-1111-111111111111', '{"requestedPrefix":"One/Two/Three/"}'),
          -- Success: specific suffix.
          ('bb00000000000000', '11111111-1111-1111-1111-111111111111', '{"requestedPrefix":"One/Four/"}'),
          -- Error: invalid claims (requested prefix is not prefixed by grant).
          ('aa00000000000000', '11111111-1111-1111-1111-111111111111', '{"requestedPrefix":"Something/Else/"}'),
          -- Error: invalid claims (requested prefix is missing required trailing '/').
          ('aa00000000000000', '11111111-1111-1111-1111-111111111111', '{"requestedPrefix":"One/Two/MissingSlash"}'),
          -- Error: invalid claims (wrong schema).
          ('aa00000000000000', '11111111-1111-1111-1111-111111111111', '{"invalid":"schema"}'),
          -- Error: invalid directive (granted prefix is not under its catalog prefix).
          ('cc00000000000000', '11111111-1111-1111-1111-111111111111', '{}'),
          -- Error: directive grant prefix is not a prefix.
          ('dd00000000000000', '11111111-1111-1111-1111-111111111111', '{}')
        )
        select 1;
        "#,
        )
        .execute(&mut txn)
        .await
        .unwrap();

        let mut handler = DirectiveHandler::new();
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
              "requestedPrefix": "One/Two/Three/"
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
              "type": "success"
            }
          },
          {
            "claims": {
              "requestedPrefix": "One/Two/Three/"
            },
            "did": "bb:00:00:00:00:00:00:00",
            "status": {
              "type": "success"
            }
          },
          {
            "claims": {
              "requestedPrefix": "One/Four/"
            },
            "did": "bb:00:00:00:00:00:00:00",
            "status": {
              "type": "success"
            }
          },
          {
            "claims": {
              "requestedPrefix": "Something/Else/"
            },
            "did": "aa:00:00:00:00:00:00:00",
            "status": {
              "error": "Grant claims cannot request Something/Else/ because it is not a suffix of granted prefix One/Two/",
              "type": "invalidClaims"
            }
          },
          {
            "claims": {
              "requestedPrefix": "One/Two/MissingSlash"
            },
            "did": "aa:00:00:00:00:00:00:00",
            "status": {
              "error": "requested_prefix.: One/Two/MissingSlash doesn't match pattern ([\\p{Letter}\\p{Number}\\-_\\.]+/)* (unmatched portion is: MissingSlash)",
              "type": "invalidClaims"
            }
          },
          {
            "claims": {
              "invalid": "schema"
            },
            "did": "aa:00:00:00:00:00:00:00",
            "status": {
              "error": "unknown field `invalid`, expected `requestedPrefix` at line 1 column 10",
              "type": "invalidClaims"
            }
          },
          {
            "claims": {},
            "did": "cc:00:00:00:00:00:00:00",
            "status": {
              "error": "Grant directive cannot grant Wrong/Path/ because it is not a suffix of its catalog prefix Path/",
              "type": "invalidDirective"
            }
          },
          {
            "claims": {},
            "did": "dd:00:00:00:00:00:00:00",
            "status": {
              "error": "granted_prefix.: Not/A/Prefix! doesn't match pattern ([\\p{Letter}\\p{Number}\\-_\\.]+/)* (unmatched portion is: Prefix!)",
              "type": "invalidDirective"
            }
          }
        ]
        "###);

        let grants = sqlx::query(
            r#"select json_build_object('obj', g.object_role, 'cap', g.capability) from user_grants g
            where user_id = '11111111-1111-1111-1111-111111111111' order by id asc;"#,
        )
        .fetch_all(&mut txn)
        .await
        .unwrap();

        insta::assert_json_snapshot!(
          grants.iter().map(|r| -> serde_json::Value { r.get(0) }).collect::<Vec<_>>(),
          @r###"
        [
          {
            "cap": "admin",
            "obj": "aliceCo/"
          },
          {
            "cap": "write",
            "obj": "One/Two/Three/"
          },
          {
            "cap": "write",
            "obj": "One/Two/"
          },
          {
            "cap": "read",
            "obj": "One/Four/"
          }
        ]
        "###);
    }
}
