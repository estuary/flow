use sqlx::types::Uuid;

/// Derives the colocated trial bucket for a public AWS data-plane.
/// The name is a pure function of plane identity and is deliberately not
/// stored anywhere; est-dry-dock creates the bucket from the same formula
/// (est_dry_dock/models/__init__.py::trial_bucket_name).
pub fn trial_bucket_name(data_plane_name: &str, region: &str) -> String {
    use sha2::Digest;
    let digest = hex::encode(sha2::Sha256::digest(data_plane_name.as_bytes()));
    format!("estuary-trial-{region}-{}", &digest[..8])
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
    .fetch_optional(&mut **txn)
    .await?;

    let exists = sqlx::query!(
        r#"
        select 1 as "exists" from tenants
        where lower(tenant) = lower($1::catalog_tenant)
        "#,
        prefix as String,
    )
    .fetch_optional(&mut **txn)
    .await?;

    Ok(illegal.is_some() || exists.is_some())
}

/// The plane new tenants default to when signup carries no data-plane choice.
pub const DEFAULT_PUBLIC_DATA_PLANE: &str = "ops/dp/public/aws-us-east-1-c1";

/// Deprecated planes excluded from new-tenant storage mappings.
/// gcp-us-central1-c1 (combustible-cronut) and its successor c2 are being
/// deprecated and replaced.
pub const EXCLUDED_PUBLIC_DATA_PLANES: &[&str] = &[
    "ops/dp/public/gcp-us-central1-c1",
    "ops/dp/public/gcp-us-central1-c2",
];

/// Stable-sorts `planes` (already ordered id desc) so `default_plane` is
/// first; the first entry of a storage mapping's data_planes is the default.
fn order_public_planes(mut planes: Vec<String>, default_plane: &str) -> Vec<String> {
    planes.sort_by_key(|name| name != default_plane);
    planes
}

/// Builds the (tenant, recovery) storage_mappings specs for a new tenant.
///
/// When `colocate` is set and the default plane is a public AWS plane, the
/// specs point at the plane's colocated S3 trial bucket (created by
/// est-dry-dock from the same derivation). GCP/Azure planes, unparseable
/// plane names, and `colocate` being unset all keep the legacy GCS bucket.
fn storage_specs(
    default_plane: Option<&str>,
    all_planes: &[String],
    colocate: bool,
) -> (serde_json::Value, serde_json::Value) {
    use crate::server::public::graphql::{parse_data_plane_name, DataPlaneCloudProvider};

    let s3 = match default_plane {
        Some(name) if colocate => match parse_data_plane_name(name) {
            Some((DataPlaneCloudProvider::Aws, region, _tag, true)) => {
                Some((trial_bucket_name(name, &region), region))
            }
            _ => None,
        },
        _ => None,
    };

    match s3 {
        Some((bucket, region)) => (
            serde_json::json!({
                "stores": [{"provider": "S3", "bucket": bucket, "prefix": "collection-data/", "region": region}],
                "data_planes": all_planes,
            }),
            serde_json::json!({
                "stores": [{"provider": "S3", "bucket": bucket, "region": region}],
            }),
        ),
        None => (
            serde_json::json!({
                "stores": [{"provider": "GCS", "bucket": "estuary-trial", "prefix": "collection-data/"}],
                "data_planes": all_planes,
            }),
            serde_json::json!({
                "stores": [{"provider": "GCS", "bucket": "estuary-trial"}],
            }),
        ),
    }
}

pub async fn provision_tenant(
    accounts_user_email: &str,
    detail: Option<String>,
    tenant: &str,
    tenant_user_id: Uuid,
    requested_data_plane: Option<&str>,
    colocate_trial_bucket: bool,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<()> {
    let prefix = format!("{tenant}/");
    let default_alert_types: Vec<models::status::AlertType> = models::status::AlertType::all()
        .iter()
        .copied()
        .filter(models::status::AlertType::is_default)
        .collect();

    let excluded: Vec<String> = EXCLUDED_PUBLIC_DATA_PLANES
        .iter()
        .map(|s| s.to_string())
        .collect();
    let public_planes: Vec<String> = sqlx::query_scalar!(
        r#"select data_plane_name as "data_plane_name!"
        from data_planes
        where starts_with(data_plane_name, 'ops/dp/public/')
          and data_plane_name <> all($1::text[])
        order by id desc"#,
        &excluded,
    )
    .fetch_all(&mut **txn)
    .await?;

    // The requested plane was validated by the caller; the first entry of
    // the ordered list is the tenant's default data-plane.
    let default_plane = requested_data_plane.unwrap_or(DEFAULT_PUBLIC_DATA_PLANE);
    let public_planes = order_public_planes(public_planes, default_plane);
    let (tenant_spec, recovery_spec) = storage_specs(
        public_planes.first().map(String::as_str),
        &public_planes,
        colocate_trial_bucket,
    );

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
        create_storage_mappings as (
            insert into storage_mappings (catalog_prefix, spec, detail) values
                ($2, $6::json, $3),
                ('recovery/' || $2, $7::json, $3)
            on conflict do nothing
        ),
        create_alert_subscription as (
            insert into alert_subscriptions (catalog_prefix, email, include_alert_types)
            values ($2, (select email from auth.users where id = $1 limit 1), $5)
        )
        insert into tenants (tenant, detail) values ($2, $3);
        "#,
        tenant_user_id as Uuid,
        &prefix as &str,
        detail.clone() as Option<String>,
        accounts_user_email as &str,
        &default_alert_types as &[models::status::AlertType],
        tenant_spec as serde_json::Value,
        recovery_spec as serde_json::Value,
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

    provision_tenant(
        "support@estuary.dev",
        Some("test tenant".to_string()),
        tenant,
        user_id,
        None,
        false,
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

#[cfg(test)]
mod test {
    // The golden vector is shared with est-dry-dock's Python implementation
    // (est_dry_dock/models/__init__.py::trial_bucket_name). If this assertion
    // ever fails, the two implementations have drifted and a tenant's storage
    // would be misrouted — fix the drift, never the test.
    #[test]
    fn trial_bucket_name_golden_vector() {
        assert_eq!(
            super::trial_bucket_name("ops/dp/public/aws-us-east-1-c1", "us-east-1"),
            "estuary-trial-us-east-1-ccc98e22",
        );
    }

    #[test]
    fn orders_default_plane_first_preserving_id_desc_order() {
        let planes = vec![
            "ops/dp/public/gcp-europe-west1-c1".to_string(), // highest id
            "ops/dp/public/aws-us-east-1-c1".to_string(),
            "ops/dp/public/aws-eu-west-1-c1".to_string(),
        ];
        assert_eq!(
            super::order_public_planes(planes.clone(), "ops/dp/public/aws-us-east-1-c1"),
            vec![
                "ops/dp/public/aws-us-east-1-c1".to_string(),
                "ops/dp/public/gcp-europe-west1-c1".to_string(),
                "ops/dp/public/aws-eu-west-1-c1".to_string(),
            ],
        );
        // Default not present: order unchanged.
        assert_eq!(
            super::order_public_planes(planes.clone(), "ops/dp/public/aws-us-west-2-c1"),
            planes,
        );
    }

    #[test]
    fn storage_specs_default_to_gcs_trial() {
        let planes = vec!["ops/dp/public/aws-us-east-1-c1".to_string()];
        let (tenant, recovery) = super::storage_specs(Some(&planes[0]), &planes, false);
        assert_eq!(
            tenant,
            serde_json::json!({
                "stores": [{"provider": "GCS", "bucket": "estuary-trial", "prefix": "collection-data/"}],
                "data_planes": ["ops/dp/public/aws-us-east-1-c1"],
            }),
        );
        assert_eq!(
            recovery,
            serde_json::json!({
                "stores": [{"provider": "GCS", "bucket": "estuary-trial"}],
            }),
        );
    }

    #[test]
    fn storage_specs_colocate_aws_default_plane() {
        let planes = vec![
            "ops/dp/public/aws-us-east-1-c1".to_string(),
            "ops/dp/public/gcp-europe-west1-c1".to_string(),
        ];
        let (tenant, recovery) =
            super::storage_specs(Some("ops/dp/public/aws-us-east-1-c1"), &planes, true);
        assert_eq!(
            tenant,
            serde_json::json!({
                "stores": [{
                    "provider": "S3",
                    "bucket": "estuary-trial-us-east-1-ccc98e22",
                    "prefix": "collection-data/",
                    "region": "us-east-1",
                }],
                "data_planes": planes,
            }),
        );
        assert_eq!(
            recovery,
            serde_json::json!({
                "stores": [{
                    "provider": "S3",
                    "bucket": "estuary-trial-us-east-1-ccc98e22",
                    "region": "us-east-1",
                }],
            }),
        );
    }

    // Non-AWS default planes, unparseable names, and colocate=false all fall
    // back to the GCS trial bucket.
    #[test]
    fn storage_specs_fall_back_to_gcs() {
        for (default_plane, colocate) in [
            (Some("ops/dp/public/gcp-europe-west1-c1"), true),
            (Some("ops/dp/public/test"), true), // unparseable name (local dev env)
            (None, true),
            (Some("ops/dp/public/aws-us-east-1-c1"), false),
        ] {
            let planes = vec![default_plane.unwrap_or("ops/dp/public/x").to_string()];
            let (tenant, _) = super::storage_specs(default_plane, &planes, colocate);
            assert_eq!(
                tenant["stores"][0],
                serde_json::json!({"provider": "GCS", "bucket": "estuary-trial", "prefix": "collection-data/"}),
                "case: {default_plane:?} colocate={colocate}",
            );
        }
    }
}
