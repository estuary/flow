pub async fn create_demo_role_grant(
    detail: Option<String>,
    tenant: &str,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> sqlx::Result<()> {
    sqlx::query!(
        r#"insert into role_grants (subject_role, object_role, capability, detail) values
                ($1, 'demo/', 'read', $2)   -- Tenant may read `demo/` collections.
            on conflict do nothing
        "#,
        &tenant as &str,
        detail.clone() as Option<String>,
    )
    .execute(&mut *txn)
    .await?;

    Ok(())
}