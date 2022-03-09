use axum::extract::Extension;
use axum::extract::Path;
use axum::response::IntoResponse;
use axum::Json;
use hyper::StatusCode;
use validator::Validate;

use crate::context::AppContext;
use crate::error::AppError;
use crate::middleware::sessions::CurrentAccount;
use crate::models::accounts::Account;
use crate::models::accounts::NewAccount;
use crate::models::id::Id;
use crate::repo::accounts as accounts_repo;

pub mod routes;
mod view;

pub async fn index(Extension(ctx): Extension<AppContext>) -> Result<impl IntoResponse, AppError> {
    let accounts: Vec<Account> = accounts_repo::fetch_all(ctx.db()).await?;

    Ok((StatusCode::OK, view::index(accounts)))
}

pub async fn create(
    Extension(ctx): Extension<AppContext>,
    Json(input): Json<NewAccount>,
) -> Result<impl IntoResponse, AppError> {
    input.validate()?;

    let account = accounts_repo::insert(ctx.db(), input).await?;

    Ok((StatusCode::CREATED, view::show(account)))
}

pub async fn show(
    Extension(ctx): Extension<AppContext>,
    Path(account_id): Path<Id<Account>>,
    CurrentAccount(current_account): CurrentAccount,
) -> Result<impl IntoResponse, AppError> {
    if account_id == current_account.id {
        let account = accounts_repo::fetch_one(ctx.db(), account_id).await?;

        Ok((StatusCode::OK, view::show(account)))
    } else {
        Err(AppError::AccessDenied)
    }
}
