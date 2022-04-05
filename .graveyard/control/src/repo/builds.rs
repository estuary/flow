use crate::models::id::Id;
use crate::models::{
    accounts::Account,
    builds::{Build, State},
};
use futures::TryFutureExt;
use sqlx::{types::Json, PgPool, Postgres, Transaction};

pub async fn fetch_for_account<'e>(
    db: &PgPool,
    account_id: Id<Account>,
) -> Result<Vec<Build>, sqlx::Error> {
    sqlx::query_as!(
        Build,
        r#"
    SELECT
        account_id AS "account_id: Id<Account>",
        NULL AS "catalog: Json<serde_json::Value>", -- Skip catalog JSON in index listing.
        created_at,
        id AS "id: Id<Build>",
        state AS "state: Json<State>",
        updated_at
    FROM builds
    WHERE account_id = $1
    "#,
        account_id as Id<Account>,
    )
    .fetch_all(db)
    .await
}

pub async fn fetch_one(
    db: &PgPool,
    build_id: Id<Build>,
    account_id: Id<Account>,
) -> Result<Build, sqlx::Error> {
    sqlx::query_as!(
        Build,
        r#"
    SELECT
        account_id AS "account_id: Id<Account>",
        catalog AS "catalog: Option<Json<serde_json::Value>>",
        created_at,
        id AS "id: Id<Build>",
        state AS "state: Json<State>",
        updated_at
    FROM builds
    WHERE id = $1 AND account_id = $2
    "#,
        build_id as Id<Build>,
        account_id as Id<Account>,
    )
    .fetch_one(db)
    .await
}

pub async fn insert(
    db: &PgPool,
    catalog: serde_json::Value,
    account_id: Id<Account>,
) -> Result<Build, sqlx::Error> {
    sqlx::query!(
        r#"
    INSERT INTO builds(account_id, catalog, state, created_at, updated_at)
    VALUES ($1, $2, $3, NOW(), NOW())
    RETURNING
        id as "id!: Id<Build>"
    "#,
        account_id as Id<Account>,
        Json(catalog) as Json<serde_json::Value>,
        Json(State::Queued) as Json<State>,
    )
    .fetch_one(db)
    .and_then(|row| fetch_one(db, row.id, account_id))
    .await
}

// dequeue_build returns a queued Build to build, or None if the queue is empty.
// If a Some(Build) is returned, it's guaranteed that no parallel invocation of
// dequeue_build will return that same Build so long as the argument Transaction
// is alive.
pub async fn dequeue_build<'c>(
    txn: &mut Transaction<'c, Postgres>,
) -> Result<Option<Build>, sqlx::Error> {
    sqlx::query_as!(
        Build,
        r#"
    SELECT
        account_id as "account_id: Id<Account>",
        catalog as "catalog: Option<Json<serde_json::Value>>",
        created_at,
        id as "id: Id<Build>",
        state as "state: Json<State>",
        updated_at
    FROM builds
    WHERE state->>'type' = 'queued'
    ORDER BY id ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
    "#,
    )
    .fetch_optional(txn)
    .await
}

pub async fn update_build_state<'c>(
    txn: &mut Transaction<'c, Postgres>,
    build_id: Id<Build>,
    state: State,
) -> Result<(), sqlx::Error> {
    let _ = sqlx::query!(
        r#"
    UPDATE builds SET
        state = $2,
        updated_at = NOW()
    WHERE id = $1
    RETURNING
        true AS "r"
    "#,
        build_id as Id<Build>,
        Json(state) as Json<State>,
    )
    .fetch_one(txn)
    .await?;
    Ok(())
}
