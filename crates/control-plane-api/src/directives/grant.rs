use sqlx::types::Uuid;

pub async fn upsert_user_grant(
    user: Uuid,
    prefix: &str,
    capability: crate::Capability,
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
        capability as crate::Capability,
        detail as Option<String>,
    )
    .execute(txn)
    .await?;

    Ok(())
}
