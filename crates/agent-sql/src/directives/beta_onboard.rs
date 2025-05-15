use sqlx::types::Uuid;

pub async fn is_user_provisioned(
    user_id: Uuid,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<bool> {
    let exists = sqlx::query!(
        r#"
        -- We prevent a user from provisioning a new tenant if they're
        -- already an administrator of at least one tenant.
        select 1 as "exists" from user_grants g
        join tenants t on t.tenant = g.object_role
        where g.user_id = $1 and g.capability = 'admin'
        "#,
        user_id as Uuid,
    )
    .fetch_optional(txn)
    .await?;

    Ok(exists.is_some())
}

// tenant_exists is true if the given tenant exists (case invariant).
pub async fn tenant_exists(
    tenant: &str,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<bool> {
    let prefix = format!("{tenant}/");

    let illegal = sqlx::query!(
        r#"
        select 1 as "exists" from internal.illegal_tenant_names
        where lower(name) = lower($1::catalog_tenant)
        "#,
        prefix.clone() as String,
    )
    .fetch_optional(&mut *txn)
    .await?;

    let exists = sqlx::query!(
        r#"
        select 1 as "exists" from tenants
        where lower(tenant) = lower($1::catalog_tenant)
        "#,
        prefix as String,
    )
    .fetch_optional(&mut *txn)
    .await?;

    Ok(illegal.is_some() || exists.is_some())
}

pub async fn provision_tenant(
    accounts_user_email: &str,
    detail: Option<String>,
    tenant: &str,
    tenant_user_id: Uuid,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<()> {
    let prefix = format!("{tenant}/");

    sqlx::query!(
        r#"with
        accounts_root_user as (
            -- Precondition: the accounts root user must exist.
            -- Use a sub-select to select either one match or an explicit null row,
            -- which will then fail a not-null constraint.
            select (select id from auth.users where email = $4 limit 1) as accounts_id
        ),
        grant_user_admin_to_tenant as (
            insert into user_grants (user_id, object_role, capability, detail) values
                ($1, $2, 'admin', $3)
            on conflict do nothing
        ),
        grant_to_tenant as (
            insert into role_grants (subject_role, object_role, capability, detail) values
                ($2, $2, 'write', $3),             -- Tenant specs may write to other tenant specs.
                ($2, 'ops/dp/public/', 'read', $3) -- Tenant may access public data-planes.
            on conflict do nothing
        ),
        public_planes as (
            select json_agg(data_plane_name order by id asc) as arr
            from data_planes
            where starts_with(data_plane_name, 'ops/dp/public/')
        ),
        create_storage_mappings as (
            insert into storage_mappings (catalog_prefix, spec, detail) values
                ($2, json_build_object(
                    'stores', '[{"provider": "GCS", "bucket": "estuary-trial", "prefix": "collection-data/"}]'::json,
                    'data_planes', (select arr from public_planes)
                ), $3),
                ('recovery/' || $2, '{"stores": [{"provider": "GCS", "bucket": "estuary-trial"}]}', $3)
            on conflict do nothing
        ),
        create_alert_subscription as (
            insert into alert_subscriptions (catalog_prefix, email) values ($2, (select email from auth.users where id = $1 limit 1))
        )
        insert into tenants (tenant, detail) values ($2, $3);
        "#,
        tenant_user_id as Uuid,
        &prefix as &str,
        detail.clone() as Option<String>,
        accounts_user_email as &str,
    )
    .execute(&mut *txn)
    .await?;

    Ok(())
}
