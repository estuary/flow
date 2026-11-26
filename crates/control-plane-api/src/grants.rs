use sqlx::types::Uuid;

pub async fn upsert_user_grant(
    user: Uuid,
    prefix: &str,
    legacy_tier: models::authz::LegacyTier,
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
        legacy_tier as models::authz::LegacyTier,
        detail as Option<String>,
    )
    .execute(&mut **txn)
    .await?;

    Ok(())
}

/// Upsert a user grant, unconditionally replacing the legacy tier and detail of
/// any existing grant. Unlike [`upsert_user_grant`], this does not guard against
/// downgrades: the supplied `legacy_tier` always wins.
pub async fn overwrite_user_grant(
    user: Uuid,
    prefix: &str,
    legacy_tier: models::authz::LegacyTier,
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
        legacy_tier as models::authz::LegacyTier,
        detail as Option<String>,
    )
    .execute(&mut **txn)
    .await?;

    Ok(())
}
