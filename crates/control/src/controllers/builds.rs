use axum::extract::{Extension, Path};
use axum::response::IntoResponse;
use axum::Json;
use hyper::StatusCode;

use crate::context::AppContext;
use crate::error::AppError;
use crate::middleware::sessions::CurrentAccount;
use crate::models::{builds::Build, id::Id};
use crate::repo::builds as builds_repo;

pub mod routes;
mod view;

pub async fn index(
    Extension(ctx): Extension<AppContext>,
    CurrentAccount(account): CurrentAccount,
) -> Result<impl IntoResponse, AppError> {
    let builds = builds_repo::fetch_for_account(ctx.db(), account.id).await?;
    Ok((StatusCode::OK, view::index(builds)))
}

pub async fn create(
    Extension(ctx): Extension<AppContext>,
    CurrentAccount(account): CurrentAccount,
    Json(catalog): Json<serde_json::Value>,
) -> Result<impl IntoResponse, AppError> {
    let build = builds_repo::insert(ctx.db(), catalog, account.id).await?;
    Ok((StatusCode::CREATED, view::create(build)))
}

pub async fn show(
    Extension(ctx): Extension<AppContext>,
    CurrentAccount(account): CurrentAccount,
    Path(build_id): Path<Id<Build>>,
) -> Result<impl IntoResponse, AppError> {
    let image = builds_repo::fetch_one(ctx.db(), build_id, account.id).await?;
    Ok((StatusCode::OK, view::show(image)))
}
