use super::Id;
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

    let exists = sqlx::query!(
        r#"
        select 1 as "exists" from tenants
        where lower(tenant) = lower($1::catalog_tenant)
        "#,
        prefix as String,
    )
    .fetch_optional(txn)
    .await?;

    Ok(exists.is_some())
}

// ProvisionedTenant is the shape of a provisioned tenant.
#[derive(Debug)]
pub struct ProvisionedTenant {
    // Draft into which provisioned catalog specs should be placed.
    // It will be queued for publication upon the commit of this transaction.
    pub draft_id: Id,
}

pub async fn provision_tenant(
    accounts_user_email: &str,
    detail: Option<String>,
    tenant: &str,
    tenant_user_id: Uuid,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<ProvisionedTenant> {
    let prefix = format!("{tenant}/");

    let provisioned = sqlx::query_as!(
        ProvisionedTenant,
        r#"with
        accounts_root_user as (
            -- Precondition: the accounts root user must exist.
            -- Use a sub-select to select either one match or an explicit null row,
            -- which will then fail a not-null constraint.
            select (select id from auth.users where email = $4 limit 1) as accounts_id
        ),
        create_tenant as (
            insert into tenants (tenant, detail) values ($2, $3)
        ),
        grant_user_admin_to_tenant as (
            insert into user_grants (user_id, object_role, capability, detail) values
                ($1, $2, 'admin', $3)
        ),
        grant_to_tenant as (
            insert into role_grants (subject_role, object_role, capability, detail) values
                ($2, $2, 'write', $3),              -- Tenant specs may write to other tenant specs.
                ($2, 'ops/' || $2, 'read', $3),     -- Tenant may read `ops/$tenant/...` collections.
                ($2, 'estuary/public/', 'read', $3) -- Tenant may read `estuary/pubic/` collections.
        ),
        create_storage_mappings as (
            insert into storage_mappings (catalog_prefix, spec, detail) values
                ($2, '{"stores": [{"provider": "GCS", "bucket": "estuary-trial"}]}', $3),
                ('recovery/' || $2, '{"stores": [{"provider": "GCS", "bucket": "estuary-trial"}]}', $3)
        ),
        -- Create a draft for provisioned catalog specifications owned by the accounts root user.
        -- It will be filled out later but within this same transaction.
        -- Then queue a publication of that draft, also owned by the accounts root user.
        create_draft as (
            insert into drafts (user_id)
                select accounts_id from accounts_root_user
                returning drafts.id as draft_id
        ),
        create_publication as (
            insert into publications (user_id, draft_id)
                select accounts_id, draft_id from create_draft, accounts_root_user
        )
        select draft_id as "draft_id: Id" from create_draft;
        "#,
        tenant_user_id as Uuid,
        &prefix as &str,
        detail.clone() as Option<String>,
        accounts_user_email as &str,
    )
    .fetch_one(&mut *txn)
    .await?;

    // Create partition of task_stats which will home all stats of the tenant.
    sqlx::query(&format!(
        r#"
        create table task_stat_partitions."{tenant}_stats"
            partition of public.task_stats for values in ('{prefix}');
        "#
    ))
    .execute(&mut *txn)
    .await?;

    // stats_loader must own the materialization target so that it can apply
    // related table DDL, such as comments.
    sqlx::query(&format!(
        r#"alter table task_stat_partitions."{tenant}_stats" owner to stats_loader;"#
    ))
    .execute(&mut *txn)
    .await?;

    Ok(provisioned)
}
