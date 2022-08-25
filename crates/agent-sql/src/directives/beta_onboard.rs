use sqlx::types::Uuid;

pub async fn is_user_provisioned(
    user_id: Uuid,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<bool> {
    let exists = sqlx::query!(
        r#"
        select 1 as "exists" from user_grants
        where user_id = $1
        "#,
        user_id as Uuid,
    )
    .fetch_optional(txn)
    .await?;

    Ok(exists.is_some())
}

pub async fn tenant_exists(
    tenant: &str,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<bool> {
    let prefix = format!("{tenant}/");

    // A tenant is defined as "used" if it overlaps with an object
    // role of either user => role or role => role grant.
    let exists = sqlx::query!(
        r#"
        select 1 as "exists" from user_grants
        where starts_with($1, object_role) or starts_with(object_role, $1)
        union all
        select 1 from role_grants
        where starts_with($1, object_role) or starts_with(object_role, $1)
        "#,
        prefix as String,
    )
    .fetch_optional(txn)
    .await?;

    Ok(exists.is_some())
}

/*
TODO(johnny): A fast-follow will be inserting `ops/` specifications for the user:
insert into live_specs (catalog_name, last_build_id, last_pub_id, spec, spec_type) values
    ('ops/' || $2 || 'logs',  '00:00:00:00:00:00:00:00', '00:00:00:00:00:00:00:00', $4, 'collection'),
    ('ops/' || $2 || 'stats', '00:00:00:00:00:00:00:00', '00:00:00:00:00:00:00:00', $5, 'collection')
*/
pub async fn provision_user(
    user_id: Uuid,
    tenant: &str,
    detail: Option<String>,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<()> {
    let prefix = format!("{tenant}/");

    sqlx::query!(
        r#"with s1 as (
            insert into user_grants (user_id, object_role, capability, detail) values
                ($1, $2, 'admin', $3)
        ),
        s2 as (
            insert into role_grants (subject_role, object_role, capability, detail) values
                ($2, 'ops/' || $2, 'read', $3)
        )
        insert into storage_mappings (catalog_prefix, spec, detail) values
            ($2, '{"stores": [{"provider": "GCS", "bucket": "estuary-trial"}]}', $3),
            ('recovery/' || $2, '{"stores": [{"provider": "GCS", "bucket": "estuary-trial"}]}', $3)
        "#,
        user_id as Uuid,
        prefix as String,
        detail.clone() as Option<String>,
    )
    .execute(txn)
    .await?;

    Ok(())
}
