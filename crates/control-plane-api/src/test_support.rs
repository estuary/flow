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

    crate::directives::beta_onboard::provision_tenant(
        "support@estuary.dev",
        Some("test tenant".to_string()),
        tenant,
        user_id,
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
