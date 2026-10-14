//! Shared tenant provisioning for GraphQL onboarding and test fixtures.

use sqlx::types::Uuid;

pub async fn open_public_planes(conn: &mut sqlx::PgConnection) -> sqlx::Result<Vec<String>> {
    sqlx::query_scalar(
        "SELECT data_plane_name FROM data_planes
         WHERE starts_with(data_plane_name, 'ops/dp/public/') AND NOT closed
         ORDER BY id DESC",
    )
    .fetch_all(conn)
    .await
}

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
    .fetch_optional(&mut **txn)
    .await?;

    Ok(exists.is_some())
}

pub async fn provision_tenant(
    detail: Option<String>,
    tenant: &str,
    tenant_user_id: Uuid,
    metadata: serde_json::Value,
    tenant_storage: serde_json::Value,
    recovery_storage: serde_json::Value,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<()> {
    let prefix = format!("{tenant}/");
    let default_alert_types: Vec<models::status::AlertType> = models::status::AlertType::all()
        .iter()
        .copied()
        .filter(models::status::AlertType::is_default)
        .collect();

    sqlx::query!(
        r#"with
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
        create_storage_mappings as (
            insert into storage_mappings (catalog_prefix, spec, detail) values
                ($2, $4::json, $3),
                ('recovery/' || $2, $5::json, $3)
            on conflict (catalog_prefix) do update set spec = excluded.spec
        ),
        create_alert_subscription as (
            insert into alert_subscriptions (catalog_prefix, email, include_alert_types)
            values ($2, (select email from auth.users where id = $1 limit 1), $6)
        )
        insert into tenants (tenant, detail, created_by, metadata) values ($2, $3, $1, $7);
        "#,
        tenant_user_id as Uuid,
        &prefix as &str,
        detail.clone() as Option<String>,
        tenant_storage,
        recovery_storage,
        &default_alert_types as &[models::status::AlertType],
        metadata,
    )
    .execute(&mut **txn)
    .await?;

    Ok(())
}

/// Sets up a tenant in a freshly-migrated test database: inserts an auth user,
/// runs `provision_tenant`, and drops the `estuary_support` grant so tests run
/// as the tenant's own admin. Reachable from another crate's tests via the
/// `test-support` feature; not compiled into production builds.
#[cfg(any(test, feature = "test-support"))]
pub async fn provision_test_tenant(
    pool: &sqlx::PgPool,
    tenant: &str,
    email: &str,
    user_meta: serde_json::Value,
) -> uuid::Uuid {
    let user_id = uuid::Uuid::new_v4();
    let mut txn = pool.begin().await.expect("begin txn");

    sqlx::query(r#"insert into auth.users (id, email, raw_user_meta_data) values ($1, $2, $3)"#)
        .bind(user_id)
        .bind(email)
        .bind(&user_meta)
        .execute(&mut *txn)
        .await
        .expect("insert auth user");

    let public_planes = open_public_planes(&mut txn)
        .await
        .expect("fetch test data planes");
    let tenant_storage = serde_json::json!({
        "stores": [{"provider": "GCS", "bucket": "estuary-trial", "prefix": "collection-data/"}],
        "data_planes": public_planes,
    });
    let recovery_storage = serde_json::json!({
        "stores": [{"provider": "GCS", "bucket": "estuary-trial"}],
    });

    provision_tenant(
        Some("test tenant".to_string()),
        tenant,
        user_id,
        serde_json::json!({}),
        tenant_storage,
        recovery_storage,
        &mut txn,
    )
    .await
    .expect("provision tenant");

    sqlx::query(r#"delete from role_grants where subject_role = 'estuary_support/';"#)
        .execute(&mut *txn)
        .await
        .expect("delete support grant");

    txn.commit().await.expect("commit tenant");
    user_id
}
