use futures::TryFutureExt;
use sqlx::PgPool;

use crate::models::accounts::{Account, NewAccount};
use crate::models::names::{CatalogName, UniqueName};
use crate::models::Id;

pub async fn fetch_all(db: &PgPool) -> Result<Vec<Account>, sqlx::Error> {
    sqlx::query_as!(
        Account,
        r#"
    SELECT id as "id!: Id<Account>", display_name, email, name as "name!: CatalogName", unique_name as "unique_name!: UniqueName", created_at, updated_at
    FROM accounts
    "#
    )
    .fetch_all(db)
    .await
}

pub async fn fetch_one(db: &PgPool, id: Id<Account>) -> Result<Account, sqlx::Error> {
    sqlx::query_as!(
        Account,
        r#"
    SELECT id as "id!: Id<Account>", display_name, email, name as "name!: CatalogName", unique_name as "unique_name!: UniqueName", created_at, updated_at
    FROM accounts
    WHERE id = $1
    "#,
        id as Id<Account>
    )
    .fetch_one(db)
    .await
}

pub async fn insert(db: &PgPool, input: NewAccount) -> Result<Account, sqlx::Error> {
    sqlx::query!(
        r#"
    INSERT INTO accounts(display_name, email, name, unique_name, created_at, updated_at)
    VALUES ($1, $2, $3, $4, NOW(), NOW())
    RETURNING id as "id!: Id<Account>"
    "#,
        input.display_name,
        input.email.to_lowercase(),
        CatalogName::new(&input.name) as CatalogName,
        UniqueName::new(&input.name) as UniqueName,
    )
    .fetch_one(db)
    .and_then(|row| fetch_one(db, row.id))
    .await
}

pub async fn find_by_name(db: &PgPool, name: &str) -> Result<Option<Account>, sqlx::Error> {
    sqlx::query_as!(
        Account,
        r#"
    SELECT id as "id!: Id<Account>", display_name, email, name as "name!: CatalogName", unique_name as "unique_name!: UniqueName", created_at, updated_at
    FROM accounts
    WHERE name = $1
    LIMIT 1
    "#,
        CatalogName::new(&name) as CatalogName,
    )
    .fetch_optional(db)
    .await
}
