use sqlx::types::Uuid;

pub async fn upsert_user_grant(
    user: Uuid,
    prefix: &str,
    capability: models::Capability,
    detail: Option<String>,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<()> {
    sqlx::query!(
        r#"insert into user_grants (user_id, object_role, capability, detail)
          values ($1, $2, $3, $4)
        on conflict (user_id, object_role) do update set
          capability = $3,
          updated_at = now(),
          detail = $4
        where user_grants.capability < $3
        "#,
        user,
        prefix as &str,
        capability as models::Capability,
        detail as Option<String>,
    )
    .execute(&mut **txn)
    .await?;

    Ok(())
}

/// Upsert a user grant, unconditionally replacing the capability and detail of
/// any existing grant. Unlike [`upsert_user_grant`], this does not guard against
/// downgrades: the supplied `capability` always wins.
pub async fn overwrite_user_grant(
    user: Uuid,
    prefix: &str,
    capability: models::Capability,
    detail: Option<String>,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<()> {
    sqlx::query!(
        r#"insert into user_grants (user_id, object_role, capability, detail)
          values ($1, $2, $3, $4)
        on conflict (user_id, object_role) do update set
          capability = $3,
          updated_at = now(),
          detail = $4
        "#,
        user,
        prefix as &str,
        capability as models::Capability,
        detail as Option<String>,
    )
    .execute(&mut **txn)
    .await?;

    Ok(())
}

/// Whether `user_id` is a service-account identity.
///
/// Claims alone cannot answer this — a service-account access token is
/// shaped identically to a human one — so callers which restrict an
/// operation to human users pay this lookup at that operation, never
/// per-request. Consumers: the refresh-token GraphQL mutations (via
/// `verify_not_service_account`) and the REST `capability_token` mint.
pub(crate) async fn is_service_account(
    pg_pool: &sqlx::PgPool,
    user_id: Uuid,
) -> sqlx::Result<bool> {
    sqlx::query_scalar!(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM internal.service_accounts WHERE user_id = $1
        ) AS "is_service_account!"
        "#,
        user_id,
    )
    .fetch_one(pg_pool)
    .await
}
