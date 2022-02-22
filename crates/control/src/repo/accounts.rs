use futures::TryFutureExt;
use sqlx::PgPool;

use crate::models::accounts::{Account, NewAccount};
use crate::models::Id;

pub async fn fetch_all(db: &PgPool) -> Result<Vec<Account>, sqlx::Error> {
    sqlx::query_as!(
        Account,
        r#"
    SELECT id as "id!: Id", display_name, email, name, unique_name, created_at, updated_at
    FROM accounts
    "#
    )
    .fetch_all(db)
    .await
}

pub async fn fetch_one(db: &PgPool, id: Id) -> Result<Account, sqlx::Error> {
    sqlx::query_as!(
        Account,
        r#"
    SELECT id as "id!: Id", display_name, email, name, unique_name, created_at, updated_at
    FROM accounts
    WHERE id = $1
    "#,
        id as Id
    )
    .fetch_one(db)
    .await
}

pub async fn insert(db: &PgPool, input: NewAccount) -> Result<Account, sqlx::Error> {
    // TODO: normalize the name
    let unique_name = input.name.clone();

    sqlx::query!(
        r#"
    INSERT INTO accounts(display_name, email, name, unique_name, created_at, updated_at)
    VALUES ($1, $2, $3, $4, NOW(), NOW())
    RETURNING id as "id!: Id"
    "#,
        input.display_name,
        input.email.to_lowercase(),
        input.name,
        unique_name,
    )
    .fetch_one(db)
    .and_then(|row| fetch_one(db, row.id))
    .await
}
