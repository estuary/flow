use std::collections::HashMap;

use super::{CatalogType, Id, TextJson as Json};
use chrono::prelude::*;
use serde::Serialize;
use serde_json::value::RawValue;
use sqlx::types::Uuid;

// Row is the dequeued task shape of a discover operation.
#[derive(Debug)]
pub struct Row {
    pub capture_name: String,
    pub connector_tag_id: Id,
    pub connector_tag_job_success: bool,
    pub created_at: DateTime<Utc>,
    pub data_plane_name: String,
    pub draft_id: Id,
    pub endpoint_config: Json<Box<RawValue>>,
    pub id: Id,
    pub image_name: String,
    pub image_tag: String,
    pub logs_token: Uuid,
    pub protocol: String,
    pub update_only: bool,
    pub updated_at: DateTime<Utc>,
    pub user_id: Uuid,
    pub auto_publish: bool,
    pub auto_evolve: bool,
    pub background: bool,
}

#[tracing::instrument(level = "debug", skip(txn))]
pub async fn dequeue(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    allow_background: bool,
) -> sqlx::Result<Option<Row>> {
    sqlx::query_as!(
      Row,
      r#"select
          discovers.capture_name,
          discovers.connector_tag_id as "connector_tag_id: Id",
          connector_tags.job_status->>'type' = 'success' as "connector_tag_job_success!",
          discovers.created_at,
          discovers.data_plane_name,
          discovers.draft_id as "draft_id: Id",
          discovers.endpoint_config as "endpoint_config: Json<Box<RawValue>>",
          discovers.id as "id: Id",
          connectors.image_name,
          connector_tags.image_tag,
          discovers.logs_token,
          connector_tags.protocol as "protocol!",
          discovers.update_only,
          discovers.updated_at,
          discovers.auto_publish,
          discovers.auto_evolve,
          drafts.user_id,
          discovers.background
      from discovers
      join drafts on discovers.draft_id = drafts.id
      join connector_tags on discovers.connector_tag_id = connector_tags.id
      join connectors on connectors.id = connector_tags.connector_id
      where discovers.job_status->>'type' = 'queued' and connector_tags.job_status->>'type' != 'queued'
          and (discovers.background = $1 or discovers.background = false)
      order by discovers.background asc, discovers.id asc
      limit 1
      for update of discovers skip locked;
      "#,
      allow_background
  )
  .fetch_optional(txn).await
}

#[derive(Debug, Serialize)]
pub struct ResolvedRow {
    pub catalog_name: String,
    pub spec: Json<Box<RawValue>>,
}

pub async fn resolve_merge_target_specs<'a>(
    catalog_names: &'a [&'a str],
    catalog_type: CatalogType,
    draft_id: Id,
    user_id: Uuid,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<Vec<ResolvedRow>> {
    sqlx::query_as!(
        ResolvedRow,
        r#"
        -- Un-nested catalog names to fetch.
        with fetch_names as (
            select catalog_name from unnest($1::text[]) as catalog_name
        ),
        -- Determine user's effective read-authorized roles.
        user_read_access as (
            select distinct role_prefix
            from internal.user_roles($4)
            where capability >= 'read'
        ),
        -- Live specs which the user is authorized to read,
        -- filtered to the applicable catalog names and type.
        filtered_live_specs as (
            select
                l.catalog_name,
                l.spec,
                l.last_pub_id
            from fetch_names as f join live_specs as l
                on f.catalog_name = l.catalog_name
            where l.spec_type = $2
            and exists(
                select 1 from user_read_access as r
                where starts_with(l.catalog_name, r.role_prefix)
            )
        ),
        -- Existing draft specs that match the catalog names and type.
        existing_draft_specs as (
            select
                d.catalog_name,
                d.spec
            from fetch_names as f join draft_specs as d
                on f.catalog_name = d.catalog_name
            where d.draft_id = $3 and d.spec_type = $2 and d.spec is not null
        ),
        -- Draft specs which did not exist and are inserted from filtered_live_specs.
        -- Preserve the `last_pub_id` of the live spec so that we can detect if it
        -- changed between now and a future publication of this draft. But skip this
        -- for collections since we want to avoid conflicts due to inferred schema updates
        -- and the risk is relatively low. See: https://github.com/estuary/flow/issues/1520
        inserted_draft_specs as (
            insert into draft_specs (draft_id, catalog_name, spec, spec_type, expect_pub_id)
            select
                $3,
                f.catalog_name,
                f.spec,
                $2,
                case when $2 = 'collection' then
                    null
                else
                    f.last_pub_id
                end
            from filtered_live_specs as f
            on conflict do nothing
            returning *
        )
        select
            e.catalog_name as "catalog_name!",
            e.spec as "spec!: Json<Box<RawValue>>"
        from existing_draft_specs as e
        union all
        select
            i.catalog_name,
            i.spec
        from inserted_draft_specs as i
        "#,
        &catalog_names as &[&str],
        catalog_type as CatalogType,
        draft_id as Id,
        user_id,
    )
    .fetch_all(txn)
    .await
}

pub async fn resolve<S>(
    id: Id,
    status: S,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<()>
where
    S: Serialize + Send + Sync,
{
    sqlx::query!(
        r#"update discovers set
            job_status = $2,
            updated_at = clock_timestamp()
        where id = $1
        returning 1 as "must_exist";
        "#,
        id as Id,
        Json(status) as Json<S>,
    )
    .fetch_one(txn)
    .await?;

    Ok(())
}

/// Returns a map of catalog_name to md5 hash of the live spec. The map will only
/// include entities that exist and have a non-null md5 hash.
pub async fn fetch_spec_md5_hashes(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    spec_names: Vec<&str>,
) -> sqlx::Result<HashMap<String, String>> {
    let rows = sqlx::query!(
        r#"
            select
                ls.catalog_name,
                ls.md5
            from live_specs ls
            where ls.catalog_name = any ($1::text[]);
        "#,
        spec_names as Vec<&str>
    )
    .fetch_all(txn)
    .await?;

    let out = rows
        .into_iter()
        .filter_map(|r| r.md5.map(|md5| (r.catalog_name, md5)))
        .collect();
    Ok(out)
}

#[cfg(test)]
mod test {

    use super::{CatalogType, Id};
    use serde_json::json;
    use sqlx::{types::Uuid, Connection};

    const FIXED_DATABASE_URL: &str = "postgresql://postgres:postgres@localhost:5432/postgres";

    #[tokio::test]
    async fn test_spec_resolution_cases() {
        let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
            .await
            .unwrap();
        let mut txn = conn.begin().await.unwrap();

        sqlx::query(
            r#"
            with
            p1 as (
                insert into user_grants(user_id, object_role, capability) values
                ('11111111-1111-1111-1111-111111111111', 'aliceCo/', 'admin')
            ),
            p2 as (
                insert into drafts (id, user_id) values
                ('dddddddddddddddd', '11111111-1111-1111-1111-111111111111'),
                ('eeeeeeeeeeeeeeee', '11111111-1111-1111-1111-111111111111')
            ),
            p3 as (
                insert into live_specs (catalog_name, spec_type, spec) values
                ('aliceCo/conflict', 'test', '"err: should leave draft version"'),
                ('aliceCo/live', 'test', '"ok"'),
                ('aliceCo/live-unrelated', 'test', '"err: unrelated"'),
                ('aliceCo/wrong-type', 'capture', '"err: wrong type (live)"'),
                ('bobCo/private', 'test', '"err: private"'),
                ('bobCo/private-conflict', 'test', '"err: private"')
            ),
            p4 as (
                insert into draft_specs (catalog_name, spec_type, spec, draft_id) values
                ('aliceCo/conflict', 'test', '"ok"', 'dddddddddddddddd'),
                ('aliceCo/draft-unrelated', 'test', '"err: not fetched"', 'dddddddddddddddd'),
                ('bobCo/draft', 'test', '"err: wrong draft"', 'eeeeeeeeeeeeeeee'),
                ('bobCo/draft', 'test', '"ok"', 'dddddddddddddddd'),
                ('bobCo/private-conflict', 'test', '"ok"', 'dddddddddddddddd'),
                ('bobCo/wrong-type', 'capture', '"err: wrong type (draft)"', 'dddddddddddddddd')
            )
            select 1;
            "#,
        )
        .execute(&mut txn)
        .await
        .unwrap();

        let fetched = super::resolve_merge_target_specs(
            &[
                "aliceCo/conflict",
                "aliceCo/live",
                "aliceCo/not-found",
                "aliceCo/wrong-type",
                "bobCo/draft",
                "bobCo/not-found",
                "bobCo/private",
                "bobCo/private-conflict",
                "bobCo/wrong-type",
            ],
            CatalogType::Test,
            Id::from_hex("dddddddddddddddd").unwrap(),
            Uuid::from_bytes([
                0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
                0x11, 0x11,
            ]),
            &mut txn,
        )
        .await
        .unwrap();

        insta::assert_json_snapshot!(json!(fetched), @r###"
        [
          {
            "catalog_name": "aliceCo/conflict",
            "spec": "ok"
          },
          {
            "catalog_name": "bobCo/draft",
            "spec": "ok"
          },
          {
            "catalog_name": "bobCo/private-conflict",
            "spec": "ok"
          },
          {
            "catalog_name": "aliceCo/live",
            "spec": "ok"
          }
        ]
        "###);
    }
}
