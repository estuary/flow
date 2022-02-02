use futures::TryFutureExt;
use sqlx::PgPool;

use crate::models::connector_images::{ConnectorImage, CreateConnectorImage};
use crate::models::Id;

pub async fn fetch_all(db: &PgPool) -> Result<Vec<ConnectorImage>, sqlx::Error> {
    sqlx::query_as!(
        ConnectorImage,
        r#"
    SELECT id as "id!: Id", connector_id as "connector_id!: Id", image, sha256, tag, created_at, updated_at
    FROM connector_images
    "#
    )
    .fetch_all(db)
    .await
}

pub async fn fetch_all_for_connector(
    db: &PgPool,
    connector_id: Id,
) -> Result<Vec<ConnectorImage>, sqlx::Error> {
    sqlx::query_as!(
        ConnectorImage,
        r#"
    SELECT id as "id!: Id", connector_id as "connector_id!: Id", image, sha256, tag, created_at, updated_at
    FROM connector_images
    WHERE connector_id = $1
    "#, connector_id as Id
    )
    .fetch_all(db)
    .await
}

pub async fn fetch_one(db: &PgPool, id: Id) -> Result<ConnectorImage, sqlx::Error> {
    sqlx::query_as!(
        ConnectorImage,
        r#"
    SELECT id as "id!: Id", connector_id as "connector_id!: Id", image, sha256, tag, created_at, updated_at
    FROM connector_images
    WHERE id = $1
    "#,
        id as Id
    )
    .fetch_one(db)
    .await
}

pub async fn insert(
    db: &PgPool,
    input: CreateConnectorImage,
) -> Result<ConnectorImage, sqlx::Error> {
    sqlx::query!(
        r#"
    INSERT INTO connector_images(connector_id, image, sha256, tag, created_at, updated_at)
    VALUES ($1, $2, $3, $4, NOW(), NOW())
    RETURNING id as "id!: Id"
    "#,
        input.connector_id as Id,
        input.image,
        input.sha256,
        input.tag
    )
    .fetch_one(db)
    .and_then(|row| fetch_one(db, row.id))
    .await
}
