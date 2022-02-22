use axum::extract::Extension;
use axum::extract::Path;
use axum::response::IntoResponse;
use axum::Json;
use hyper::StatusCode;
use sqlx::PgPool;

use crate::error::AppError;
use crate::models::accounts::Account;
use crate::models::accounts::NewAccount;
use crate::models::Id;
use crate::repo::accounts as accounts_repo;

pub mod routes;
mod view;

pub async fn index(Extension(db): Extension<PgPool>) -> Result<impl IntoResponse, AppError> {
    let accounts: Vec<Account> = accounts_repo::fetch_all(&db).await?;

    Ok((StatusCode::OK, view::index(accounts)))
}

pub async fn create(
    Extension(db): Extension<PgPool>,
    Json(input): Json<NewAccount>,
) -> Result<impl IntoResponse, AppError> {
    let account = accounts_repo::insert(&db, input).await?;

    Ok((StatusCode::CREATED, view::show(account)))
}

pub async fn show(
    Extension(db): Extension<PgPool>,
    Path(account_id): Path<Id>,
) -> Result<impl IntoResponse, AppError> {
    let account = accounts_repo::fetch_one(&db, account_id).await?;

    Ok((StatusCode::OK, view::show(account)))
}
