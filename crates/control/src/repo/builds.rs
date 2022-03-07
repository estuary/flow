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
        NULL AS "catalog: Json<models::Catalog>", -- Skip catalog JSON in index listing.
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
        catalog AS "catalog: Option<Json<models::Catalog>>",
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
    catalog: models::Catalog,
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
        Json(catalog) as Json<models::Catalog>,
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
//
// The query leverages PostgreSQL's advisory lock mechanism to obtain a
// transaction-scoped exclusive lock on the `id` of its returned row.
// `pg_try_advisory_xact_lock(id)` returns true if it obtains a lock on `id`,
// or false if it's locked already.
//
// It's therefore important that the query plan call `pg_try_advisory_xact_lock`
// on as few actual `id`'s as possible, as extra locking reduces parallelism.
// For this query, we're capitalizing on the index `builds_id_where_queued`.
// The optimizer uses it in an Index Scan to try successive increasing `id`'s
// until one is found which is lock-able.
//
// This index also means that the common case (there are no queued builds) is
// very cheap to poll, even if the total number of builds is large.
//
// If the query plan were to change on us then `pg_try_advisory_xact_lock(id)`
// could lock additional non-returned `id`'s, which would reduce the potential
// parallelism but would not be a correctness issue.
//
// All obtained locks are automatically released on transaction close.
// If a builder fails its lock is automatically removed, making the Build
// eligible again for dequeue by other workers.
//
/*
                                         QUERY PLAN
---------------------------------------------------------------------------------------------
Limit  (cost=0.13..6.18 rows=1 width=68)
->  Index Scan using builds_id_where_queued on builds  (cost=0.13..12.22 rows=2 width=68)
        Filter: pg_try_advisory_xact_lock(id)
*/
pub async fn dequeue_build<'c>(
    txn: &mut Transaction<'c, Postgres>,
) -> Result<Option<Build>, sqlx::Error> {
    sqlx::query_as!(
        Build,
        r#"
    SELECT
        account_id as "account_id: Id<Account>",
        catalog as "catalog: Option<Json<models::Catalog>>",
        created_at,
        id as "id: Id<Build>",
        state as "state: Json<State>",
        updated_at
    FROM builds
    WHERE state->>'type' = 'queued' AND
        pg_try_advisory_xact_lock(id)
    ORDER BY id ASC
    LIMIT 1
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
